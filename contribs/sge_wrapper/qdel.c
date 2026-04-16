#include <slurm/slurm_errno.h>
#include <slurm/slurm.h>
#include <slurm/slurmdb.h>
#include <sys/time.h>

#include "qdel.h"

// Function declarations
static void print_scancel_options(void);
static void _opt_default(void);
static bool _opt_verify(void);
static void _opt_env(void);
static bool _is_task_in_job(job_info_t *job_ptr, uint32_t array_id);
static int _verify_job_ids(void);
static void _filter_job_records(void);
static char **_xlate_job_step_ids(char **rest);
static int _fill_opt_from_qsub_args(void);
static void init_qargs(void);
static int set_user_args(const char *params);
static int set_force_args(const char *params);
static int set_help_args(const char *params);
static int set_version_args(const char *params);
static int set_debug_args(const char *params);
static void display_help_info(void);
static void display_version_info(void);
static void free_Qsub_args(void);
static void free_opt(void);
static char *_build_jobid_str(job_info_t *job_ptr);
static void _add_delay(void);
static void *_cancel_job_id(void *ci);
static void _cancel_jobid_by_state(uint32_t job_state, int *rc);
static void _cancel_jobs_by_state(uint32_t job_state, int *rc);
static int _cancel_jobs(void);
static int wrapper_cancel_jobs(void);
static int parse_command(CommandParser *parser, int argc, char *argv[]);
static void destroy_parser(CommandParser *parser);
static void add_handler(CommandParser *parser, CommandHandler handler);
static CommandParser *create_command_parser(void);
static bool handle_help_or_version(CommandParser *parser);

// Global variables
static qsub_args_t *qargs = NULL;
static opt_t opt;
static job_info_msg_t *job_buffer_ptr = NULL;
#define INITIAL_UID_CAPACITY 8

/**
 * Print the contents of the opt structure for debugging.
 */
static void print_scancel_options(void)
{
    printf("=== scancel_options ===\n");
    printf("job_name: %s\n", opt.job_name ? opt.job_name : "(null)");
    printf("signal: %u\n", opt.signal);
    printf("user_id: %u\n", (unsigned int)opt.user_id);
    printf("user_name: %s\n", opt.user_name ? opt.user_name : "(null)");
    printf("job_list: ");
    if (opt.job_list) {
        for (int i = 0; opt.job_list[i]; i++) {
            printf("%s%s", opt.job_list[i], opt.job_list[i + 1] ? ", " : "");
        }
        printf("\n");
    } else {
        printf("(null)\n");
    }
    printf("job_cnt: %u\n", opt.job_cnt);
    if (opt.job_cnt > 0) {
        printf("Jobs (count: %u):\n", opt.job_cnt);
        printf("  %-10s %-12s %-12s %-12s %-12s\n", "Index", "Job ID", "Array ID", "Found", "Pending");
        for (int i = 0; i < opt.job_cnt; i++) {
            char array_id_str[12];
            snprintf(array_id_str, sizeof(array_id_str), "%u", opt.array_id[i]);
            printf("  %-10d %-12u %-12s %-12s %-12s\n",
                   i,
                   opt.job_id[i],
                   opt.array_id[i] == NO_VAL ? "N/A" : array_id_str,
                   opt.job_found ? (opt.job_found[i] ? "Yes" : "No") : "N/A",
                   opt.job_pend ? (opt.job_pend[i] ? "Yes" : "No") : "N/A");
        }
    } else {
        printf("No jobs specified.\n");
    }
    printf("====================\n");
}

/**
 * Initialize the opt structure with default values.
 */
static void _opt_default(void)
{
    opt.job_cnt = 0;
    opt.job_list = NULL;
    opt.job_name = NULL;
    opt.signal = NO_VAL16;
    opt.user_id = 0;
    opt.user_name = NULL;
    opt.uids_array = NULL;
    opt.uids_count = 0;
    opt.uids_array_capacity = 0;
}

/**
 * Verify the options in the opt structure.
 * Returns true if valid, false otherwise.
 */
static bool _opt_verify(void)
{
    if (opt.user_name) {
        char *input_copy = xstrdup(opt.user_name);
        char *token = strtok(input_copy, ",");
        while (token) {
            uid_t temp;
            if (uid_from_string(token, &temp) != 0) {
                printf("There is no job registered for the following users: %s\n", token);
                xfree(input_copy);
                return false;
            }
            if (opt.uids_count >= opt.uids_array_capacity) {
                opt.uids_array_capacity = opt.uids_array_capacity ? opt.uids_array_capacity * 2 : INITIAL_UID_CAPACITY;
                opt.uids_array = xrealloc(opt.uids_array, opt.uids_array_capacity * sizeof(uid_t));
            }
            opt.uids_array[opt.uids_count++] = temp;
            token = strtok(NULL, ",");
        }
        xfree(input_copy);
    }

    opt.user_id = getuid();

    if (!opt.job_cnt && !opt.job_name && !opt.user_name) {
        error("No job identification provided");
        return false;
    }

    return true;
}

/**
 * Load options from environment variables (SCANCEL_NAME, SCANCEL_USER).
 */
static void _opt_env(void)
{
    char *val = NULL;
    if ((val = getenv("SCANCEL_NAME"))) {
        opt.job_name = xstrdup(val);
    }
    if ((val = getenv("SCANCEL_USER"))) {
        opt.user_name = xstrdup(val);
    }
}

/**
 * Check if a specific array task ID is part of a job.
 */
static bool _is_task_in_job(job_info_t *job_ptr, uint32_t array_id)
{
    if (job_ptr->array_task_id == array_id) {
        return true;
    }
    if (!job_ptr->array_bitmap || bit_size(job_ptr->array_bitmap) <= array_id) {
        return false;
    }
    return bit_test(job_ptr->array_bitmap, array_id);
}

/**
 * Verify job IDs against the job buffer, marking found and pending jobs.
 * Returns 1 if any job is not found, 0 otherwise.
 */
static int _verify_job_ids(void)
{
    if (!opt.job_cnt) {
        return 0;
    }

    opt.job_found = xmalloc(opt.job_cnt * sizeof(bool));
    opt.job_pend = xmalloc(opt.job_cnt * sizeof(bool));
    int rc = 0;

    job_info_t *job_ptr = job_buffer_ptr->job_array;
    for (uint32_t i = 0; i < job_buffer_ptr->record_count; i++, job_ptr++) {
        job_ptr->assoc_id = 0;
        if (IS_JOB_FINISHED(job_ptr)) {
            job_ptr->job_id = 0;
        }
        if (!job_ptr->job_id) {
            continue;
        }
        for (int j = 0; j < opt.job_cnt; j++) {
            if (opt.array_id[j] == NO_VAL) {
                if (opt.job_id[j] == job_ptr->job_id || opt.job_id[j] == job_ptr->array_job_id) {
                    opt.job_found[j] = true;
                }
            } else if (opt.array_id[j] == INFINITE) {
                if (opt.job_id[j] == job_ptr->array_job_id) {
                    opt.job_found[j] = true;
                }
            } else if (opt.job_id[j] == job_ptr->array_job_id && _is_task_in_job(job_ptr, opt.array_id[j])) {
                opt.job_found[j] = true;
            }
            if (opt.job_found[j]) {
                opt.job_pend[j] = IS_JOB_PENDING(job_ptr);
                job_ptr->assoc_id = 1;
            }
        }
        if (!job_ptr->assoc_id) {
            job_ptr->job_id = 0;
        }
    }

    for (int j = 0; j < opt.job_cnt; j++) {
        if (!opt.job_found[j]) {
            rc = 1;
            printf("denied: job \"%u\" does not exist\n", opt.job_id[j]);
            opt.job_id[j] = 0; // Mark invalid job to skip cancellation
        }
    }

    return rc;
}

/**
 * Filter job records based on user and job state permissions.
 */
static void _filter_job_records(void)
{
    if (!job_buffer_ptr || !job_buffer_ptr->job_array) {
        for (int j = 0; j < opt.uids_count; j++) {
            struct passwd *pw = getpwuid(opt.uids_array[j]);
            printf("There is no job registered for the following users: %s\n", pw->pw_name);
        }
        return;
    }

    struct passwd *pwd = getpwuid(opt.user_id);
    job_info_t *job_ptr = job_buffer_ptr->job_array;
    for (uint32_t i = 0; i < job_buffer_ptr->record_count; i++, job_ptr++) {
        if (!job_ptr->job_id) {
            continue;
        }
        if (IS_JOB_FINISHED(job_ptr)) {
            job_ptr->job_id = 0;
            continue;
        }

        uint32_t job_base_state = job_ptr->job_state & JOB_STATE_BASE;
        if (job_base_state != JOB_PENDING && job_base_state != JOB_RUNNING && job_base_state != JOB_SUSPENDED) {
            job_ptr->job_id = 0;
            continue;
        }

        if (opt.user_name) {
            if (opt.uids_count > 0) {
                if (opt.user_id == 0) { // Root user
                    bool match = false;
                    for (int j = 0; j < opt.uids_count; j++) {
                        if (job_ptr->user_id == opt.uids_array[j]) {
                            match = true;
                            break;
                        }
                    }
                    if (!match) {
                        job_ptr->job_id = 0;
                    }
                } else { // Non-root user
                    bool match = false;
                    for (int j = 0; j < opt.uids_count; j++) {
                        if (opt.uids_array[j] == opt.user_id) {
                            match = true;
                            break;
                        }
                    }
                    if (!match || job_ptr->user_id != opt.user_id) {
                        printf("%s - you do not have the necessary privileges to delete job %u\n",
                               pwd ? pwd->pw_name : "unknown", job_ptr->job_id);
                        job_ptr->job_id = 0;
                    }
                }
            } else {
                job_ptr->job_id = 0;
            }
        } else if (job_ptr->user_id != opt.user_id) {
            printf("%s - you do not have the necessary privileges to delete job %u\n",
                   pwd ? pwd->pw_name : "unknown", job_ptr->job_id);
            job_ptr->job_id = 0;
        }
    }
}

/**
 * Parse SGE-style job IDs (e.g., "12345.1-10:2,67890") and populate opt.job_id and opt.array_id.
 * Returns the parsed id_args array.
 */
static char **_xlate_job_step_ids(char **rest)
{
    if (!rest || !rest[0]) {
        return NULL;
    }

    int buf_size = 0xffff, buf_offset = 0;
    int id_args_size = 128, i = 0;
    char **id_args = xmalloc(id_args_size * sizeof(char *));

    // Initialize arrays for job and array IDs
    opt.array_id = xmalloc(buf_size * sizeof(uint32_t));
    opt.job_id = xmalloc(buf_size * sizeof(uint32_t));

    // Copy input arguments to id_args
    for (i = 0; rest[i]; i++) {
        id_args[i] = xstrdup(rest[i]);
        if ((i + 4) >= id_args_size) {
            id_args_size *= 2;
            id_args = xrealloc(id_args, id_args_size * sizeof(char *));
        }
    }
    id_args[i] = NULL;

    // Track unique tasks to prevent duplicates
    typedef struct {
        uint32_t job_id;
        uint32_t array_id;
    } job_task_t;
    job_task_t *tasks = xmalloc(buf_size * sizeof(job_task_t));
    int task_count = 0;

    // Parse job IDs
    for (i = 0; id_args[i] && buf_offset < buf_size; i++) {
        char *next_str;
        long job_id = strtol(id_args[i], &next_str, 10);
        if (job_id <= 0) {
            error("Invalid job id %s", id_args[i]);
            exit(1);
        }

        if (next_str[0] == '.') { // Array task, e.g., "12345.1-10:2"
            char *range_str = next_str + 1;
            char *step_str = strchr(range_str, ':');
            if (step_str) {
                *step_str = '\0';
                step_str++;
            }

            long start_id, end_id, step_id = 1;
            char *end_range = strchr(range_str, '-');
            if (end_range) {
                *end_range = '\0';
                end_range++;
                start_id = strtol(range_str, NULL, 10);
                end_id = strtol(end_range, NULL, 10);
                if (start_id < 0 || end_id < start_id) {
                    error("Invalid array range in job id %s", id_args[i]);
                    exit(1);
                }
            } else {
                start_id = strtol(range_str, NULL, 10);
                end_id = start_id;
                if (start_id < 0) {
                    error("Invalid array id in job id %s", id_args[i]);
                    exit(1);
                }
            }

            if (step_str && step_str[0]) {
                char *endptr;
                step_id = strtol(step_str, &endptr, 10);
                if (step_id <= 0) {
                    error("Invalid step size in job id %s", id_args[i]);
                    exit(1);
                }
                next_str = endptr;
            } else if (end_range) {
                next_str = end_range + strlen(end_range);
            } else {
                next_str = range_str + strlen(range_str);
            }

            // Expand array tasks with deduplication
            for (long task_id = start_id; task_id <= end_id && buf_offset < buf_size; task_id += step_id) {
                bool exists = false;
                for (int k = 0; k < task_count; k++) {
                    if (tasks[k].job_id == job_id && tasks[k].array_id == task_id) {
                        exists = true;
                        break;
                    }
                }
                if (!exists) {
                    tasks[task_count].job_id = job_id;
                    tasks[task_count].array_id = task_id;
                    task_count++;
                    opt.job_id[buf_offset] = job_id;
                    opt.array_id[buf_offset] = task_id;
                    buf_offset++;
                }
            }
        } else if (next_str[0] == '\0' || next_str[0] == ',') { // Single job
            bool exists = false;
            for (int k = 0; k < task_count; k++) {
                if (tasks[k].job_id == job_id && tasks[k].array_id == NO_VAL) {
                    exists = true;
                    break;
                }
            }
            if (!exists) {
                tasks[task_count].job_id = job_id;
                tasks[task_count].array_id = NO_VAL;
                task_count++;
                opt.job_id[buf_offset] = job_id;
                opt.array_id[buf_offset] = NO_VAL;
                buf_offset++;
            }
        } else {
            error("Invalid job id format %s", id_args[i]);
            exit(1);
        }

        // Handle comma-separated job IDs
        if (next_str[0] == ',' && next_str[1]) {
            if ((i + 4) >= id_args_size) {
                id_args_size *= 2;
                id_args = xrealloc(id_args, id_args_size * sizeof(char *));
            }
            for (int j = id_args_size - 1; j > i + 1; j--) {
                id_args[j] = id_args[j-1];
            }
            next_str[0] = '\0';
            id_args[i+1] = xstrdup(next_str + 1);
        } else if (next_str[0]) {
            error("Invalid job id %s", id_args[i]);
            exit(1);
        }
    }

    opt.job_cnt = buf_offset;
    xfree(tasks);
    return id_args;
}

/**
 * Populate opt structure from qargs.
 * Returns 0 on success, 1 on failure.
 */
static int _fill_opt_from_qsub_args(void)
{
    if (!qargs) {
        return 1;
    }

    if (qargs->user_list.is_set) {
        opt.user_name = xstrdup(qargs->user_list.value);
    }
    if (qargs->force.is_set) {
        opt.signal = sig_name2num("KILL");
    }
    if (qargs->job_list) {
        opt.job_list = _xlate_job_step_ids(qargs->job_list);
    }

    opt.uids_array = xmalloc(INITIAL_UID_CAPACITY * sizeof(uid_t));
    opt.uids_array_capacity = INITIAL_UID_CAPACITY;
    opt.uids_count = 0;

    if (!_opt_verify()) {
        return 1;
    }

    return 0;
}

/**
 * Initialize the global qargs structure.
 */
static void init_qargs(void)
{
    if (qargs) {
        return;
    }
    qargs = xmalloc(sizeof(qsub_args_t));
    qargs->user_list.value = NULL;
    qargs->job_list = NULL;
    qargs->job_list_set = false;
    qargs->job_list_count = 0;
	qargs->debug = false;
}

/**
 * Set user list from command-line arguments.
 */
static int set_user_args(const char *params)
{
    init_qargs();
    if (qargs->user_list.is_set) {
        return 0;
    }
    qargs->user_list.is_set = true;
    if (params) {
        qargs->user_list.value = xstrdup(params);
    }
    return 0;
}

/**
 * Enable force cancellation.
 */
static int set_force_args(const char *params)
{
    (void)params; /* unused param */
    init_qargs();
    if (qargs->force.is_set) {
        return 0;
    }
    qargs->force.is_set = true;
    return 0;
}

/**
 * Enable help display.
 */
static int set_help_args(const char *params)
{
    (void)params; /* unused param */
    init_qargs();
    if (qargs->help.is_set) {
        return 0;
    }
    qargs->help.is_set = true;
    return 0;
}

static int set_debug_args(const char *params) {
    (void)params; /* unused param */
	init_qargs();
	qargs->debug = true;
    return 0;
}

/**
 * Enable version display.
 */
static int set_version_args(const char *params)
{
    (void)params; /* unused param */
    init_qargs();
    if (qargs->version.is_set) {
        return 0;
    }
    qargs->version.is_set = true;
    return 0;
}

/**
 * Display command-line help information.
 */
static void display_help_info(void)
{
    printf("SGE 8.1.9\n");
    printf("usage: qdel [options] job_task_list\n");
    printf("   [-help]                          print this help\n");
    printf("   [-u user_list]                   delete all jobs of users specified in list\n");
    printf("   job_task_list                    delete all jobs given in list\n");
    printf("\n");
    printf("job_task_list: job_tasks[,job_tasks,...]\n");
    printf("job_tasks: [job_id[.task_id_range]|job_name|pattern][ -t task_id_range]\n");
    printf("task_id_range: task_id[-task_id[:step]]\n");
    printf("user_list: user[,user,...]\n");
}

/**
 * Display version information.
 */
static void display_version_info(void)
{
    printf("METASTACK SGE WRAPPER\n");
    printf("Version: %s\n", PROGRAM_VERSION);
    printf("Release Date: %s\n", RELEASE_DATE);
    printf("ADAPTED PARAMETERS: -u, -help\n");
}

/**
 * Free the global qargs structure.
 */
static void free_Qsub_args(void)
{
    if (!qargs) {
        return;
    }
    xfree(qargs->user_list.value);
    if (qargs->job_list_set) {
        for (uint32_t i = 0; i < qargs->job_list_count; i++) {
            xfree(qargs->job_list[i]);
        }
    }
    xfree(qargs);
    qargs = NULL;
}

static void free_opt(void) {

    xfree(opt.user_name);
    xfree(opt.uids_array);

    if (opt.job_list) {
        for (int i = 0; opt.job_list[i]; i++) {
            xfree(opt.job_list[i]);
        }
        xfree(opt.job_list);
    }
}

/**
 * Build a string representation of a job ID, including array tasks if applicable.
 */
static char *_build_jobid_str(job_info_t *job_ptr)
{
	char *result = NULL;

	if (job_ptr->array_task_str) {
		xstrfmtcat(result, "%u_[%s]",
			   job_ptr->array_job_id, job_ptr->array_task_str);
	} else if (job_ptr->array_task_id != NO_VAL) {
		xstrfmtcat(result, "%u_%u",
			   job_ptr->array_job_id, job_ptr->array_task_id);
	} else {
		xstrfmtcat(result, "%u", job_ptr->job_id);
	}

	return result;
}
/**
 * Add a delay to prevent overwhelming the Slurm controller with RPCs.
 */
static void _add_delay(void)
{
    static int target_resp_time = -1;
    static int delay_time = 10000, previous_delay = 0;
    int my_delay = 0;

    slurm_mutex_lock(&max_delay_lock);
    if (target_resp_time < 0) {
        target_resp_time = slurm_conf.msg_timeout / 4;
        target_resp_time = MAX(target_resp_time, 3);
        target_resp_time = MIN(target_resp_time, 5);
        target_resp_time *= USEC_IN_SEC;
    }
    if ((++request_count < MAX_THREADS) || (max_resp_time <= target_resp_time)) {
        slurm_mutex_unlock(&max_delay_lock);
        return;
    }

    my_delay = MIN((delay_time + previous_delay), USEC_IN_SEC);
    previous_delay = delay_time;
    delay_time = my_delay;
    slurm_mutex_unlock(&max_delay_lock);

    usleep(my_delay);
}


/**
 * Cancel a single job or job array task in a detached thread.
 */
static void *_cancel_job_id(void *ci)
{
    job_cancel_info_t *cancel_info = (job_cancel_info_t *)ci;
    int error_code = SLURM_SUCCESS, i;
    // uint16_t flags = cancel_info->array_flag ? KILL_JOB_ARRAY : 0;
    struct passwd *pw = getpwuid(getuid());
    uint16_t flags = 0;
    // char *job_type = "";
	DEF_TIMERS;

    // flags = _init_flags(&job_type);
    flags = 0;

    if (cancel_info->sig == NO_VAL16) {
		cancel_info->sig = SIGKILL;
	}

	if (!cancel_info->job_id_str) {
		if (cancel_info->array_job_id &&
		    (cancel_info->array_task_id == INFINITE)) {
			xstrfmtcat(cancel_info->job_id_str, "%u_*",
				   cancel_info->array_job_id);
		} else if (cancel_info->array_job_id) {
			xstrfmtcat(cancel_info->job_id_str, "%u_%u",
				   cancel_info->array_job_id,
				   cancel_info->array_task_id);
		} else {
			xstrfmtcat(cancel_info->job_id_str, "%u",
				   cancel_info->job_id);
		}
	}

	for (i = 0; i < MAX_CANCEL_RETRY; i++) {
		_add_delay();
		START_TIMER;

		error_code = slurm_kill_job2(cancel_info->job_id_str,
					     cancel_info->sig, flags,
					     NULL);

		END_TIMER;
		slurm_mutex_lock(&max_delay_lock);
		max_resp_time = MAX(max_resp_time, DELTA_TIMER);
		slurm_mutex_unlock(&max_delay_lock);

		if ((error_code == 0) ||
		    (errno != ESLURM_TRANSITION_STATE_NO_UPDATE))
			break;
		sleep(5 + i);
	}
	if (error_code) {
		error_code = slurm_get_errno();
		if (((error_code != ESLURM_ALREADY_DONE) &&
		     (error_code != ESLURM_INVALID_JOB_ID))) {
            error("Kill job error on job id %s: %s", cancel_info->job_id_str, slurm_strerror(slurm_get_errno()));
		}
		if (((error_code == ESLURM_ALREADY_DONE) ||
		     (error_code == ESLURM_INVALID_JOB_ID)) &&
		    (cancel_info->sig == SIGKILL)) {
			error_code = 0;	/* Ignore error if job done */
		}
	}

	/* Purposely free the struct passed in here, so the caller doesn't have
	 * to keep track of it, but don't destroy the mutex and condition
	 * variables contained. */
	slurm_mutex_lock(cancel_info->num_active_threads_lock);
	*(cancel_info->rc) = MAX(*(cancel_info->rc), error_code);
	(*(cancel_info->num_active_threads))--;

    if (strchr(cancel_info->job_id_str, '_')) {
        printf("%s has registered the job-array task %s for deletion\n",
               pw ? pw->pw_name : "unknown", cancel_info->job_id_str);
    } else {
        printf("%s has deleted job %s\n",
               pw ? pw->pw_name : "unknown", cancel_info->job_id_str);
    }

	slurm_cond_signal(cancel_info->num_active_threads_cond);
	slurm_mutex_unlock(cancel_info->num_active_threads_lock);

	xfree(cancel_info->job_id_str);
	xfree(cancel_info);
	return NULL;
}

/**
 * Cancel jobs or job array tasks matching the specified state.
 */
static void _cancel_jobid_by_state(uint32_t job_state, int *rc)
{
    if (!opt.job_cnt) {
        return;
    }

    // Track cancelled tasks to prevent duplicates
    typedef struct {
        uint32_t job_id;
        uint32_t array_id;
    } cancelled_task_t;
    cancelled_task_t *cancelled_tasks = xmalloc(opt.job_cnt * sizeof(cancelled_task_t));
    int cancelled_count = 0;

    for (int j = 0; j < opt.job_cnt; j++) {
        if (opt.job_found && !opt.job_found[j]) {
            continue;
        }
        if (!opt.job_id[j]) {
            continue;
        }
        if (job_state == JOB_PENDING && !opt.job_pend[j]) {
            continue;
        }

        // Check for duplicates
        bool already_cancelled = false;
        for (int k = 0; k < cancelled_count; k++) {
            if (cancelled_tasks[k].job_id == opt.job_id[j] &&
                cancelled_tasks[k].array_id == opt.array_id[j]) {
                already_cancelled = true;
                break;
            }
        }
        if (already_cancelled) {
            continue;
        }

        slurm_mutex_lock(&num_active_threads_lock);
        num_active_threads++;
        while (num_active_threads > MAX_THREADS) {
            slurm_cond_wait(&num_active_threads_cond, &num_active_threads_lock);
        }
        slurm_mutex_unlock(&num_active_threads_lock);

        char *job_id_str = NULL;
        if (opt.array_id[j] != NO_VAL) {
            xstrfmtcat(job_id_str, "%u_%u", opt.job_id[j], opt.array_id[j]);
        } else {
            xstrfmtcat(job_id_str, "%u", opt.job_id[j]);
        }

        job_cancel_info_t *cancel_info = xmalloc(sizeof(job_cancel_info_t));
        cancel_info->rc = rc;
        cancel_info->sig = opt.signal;
        cancel_info->num_active_threads = &num_active_threads;
        cancel_info->num_active_threads_lock = &num_active_threads_lock;
        cancel_info->num_active_threads_cond = &num_active_threads_cond;
        cancel_info->job_id_str = xstrdup(job_id_str);
        xfree(job_id_str);

        slurm_thread_create_detached(NULL, _cancel_job_id, cancel_info);

        cancelled_tasks[cancelled_count].job_id = opt.job_id[j];
        cancelled_tasks[cancelled_count].array_id = opt.array_id[j];
        cancelled_count++;
        opt.job_id[j] = 0; // Mark as processed
    }

    xfree(cancelled_tasks);
}

/**
 * Cancel jobs matching the specified state, handling both explicit job IDs and implicit filtering.
 */
static void _cancel_jobs_by_state(uint32_t job_state, int *rc)
{
    if (opt.job_cnt) {
        _cancel_jobid_by_state(job_state, rc);
        return;
    }

    if (!job_buffer_ptr || !job_buffer_ptr->job_array) {
        return;
    }

    job_info_t *job_ptr = job_buffer_ptr->job_array;
    for (uint32_t i = 0; i < job_buffer_ptr->record_count; i++, job_ptr++) {
        if (IS_JOB_FINISHED(job_ptr) || !job_ptr->job_id) {
            continue;
        }
        if (job_state < JOB_END && job_ptr->job_state != job_state) {
            continue;
        }

        slurm_mutex_lock(&num_active_threads_lock);
        num_active_threads++;
        while (num_active_threads > MAX_THREADS) {
            slurm_cond_wait(&num_active_threads_cond, &num_active_threads_lock);
        }
        slurm_mutex_unlock(&num_active_threads_lock);

        job_cancel_info_t *cancel_info = xmalloc(sizeof(job_cancel_info_t));
        cancel_info->job_id_str = _build_jobid_str(job_ptr);
        cancel_info->rc = rc;
        cancel_info->sig = opt.signal;
        cancel_info->num_active_threads = &num_active_threads;
        cancel_info->num_active_threads_lock = &num_active_threads_lock;
        cancel_info->num_active_threads_cond = &num_active_threads_cond;

        slurm_thread_create_detached(NULL, _cancel_job_id, cancel_info);
    }
}

/**
 * Filter and cancel jobs based on user request.
 * Returns the maximum error code encountered.
 */
static int _cancel_jobs(void)
{
    int rc = 0;

    slurm_mutex_init(&num_active_threads_lock);
    slurm_cond_init(&num_active_threads_cond, NULL);

    _cancel_jobs_by_state(JOB_PENDING, &rc);
    slurm_mutex_lock(&num_active_threads_lock);
    while (num_active_threads > 0) {
        slurm_cond_wait(&num_active_threads_cond, &num_active_threads_lock);
    }
    slurm_mutex_unlock(&num_active_threads_lock);

    _cancel_jobs_by_state(JOB_END, &rc);
    slurm_mutex_lock(&num_active_threads_lock);
    while (num_active_threads > 0) {
        slurm_cond_wait(&num_active_threads_cond, &num_active_threads_lock);
    }
    slurm_mutex_unlock(&num_active_threads_lock);

    slurm_mutex_destroy(&num_active_threads_lock);
    slurm_cond_destroy(&num_active_threads_cond);

    return rc;
}

/**
 * Wrapper function to load jobs and initiate cancellation.
 * Returns the maximum error code encountered.
 */
static int wrapper_cancel_jobs(void)
{
    setenv("SLURM_BITSTR_LEN", "0", 1);
    if (slurm_load_jobs((time_t)NULL, &job_buffer_ptr, SHOW_ALL)) {
        slurm_perror("slurm_load_jobs error");
        exit(1);
    }

    int rc = _verify_job_ids();
	
	if (qargs->debug) 
    	print_scancel_options();

    if (opt.job_name || opt.user_name) {
        _filter_job_records();
    }

    int rc2 = _cancel_jobs();
    rc = MAX(rc, rc2);

    slurm_free_job_info_msg(job_buffer_ptr);
    job_buffer_ptr = NULL;
    return rc;
}

/**
 * Parse command-line arguments and populate qargs.
 * Returns 0 on success, 1 on failure.
 */
static int parse_command(CommandParser *parser, int argc, char *argv[])
{
    if (!parser || !argv) {
        printf("Error: Invalid parser or arguments\n");
        return 1;
    }

    init_qargs();
    for (int i = 1; i < argc; i++) {
        bool command_found = false;
        for (int j = 0; j < parser->handler_count; j++) {
            if (strcmp(argv[i], parser->handlers[j].command) != 0) {
                continue;
            }
            command_found = true;
            if (parser->handlers[j].require_param == OPTION_TYPE_REQUIRED) {
                if (i + 1 >= argc) {
                    printf("Error: Command %s requires a parameter\n", argv[i]);
                    return 1;
                }
                if (parser->handlers[j].execute(argv[++i])) {
                    return 1;
                }
            } else if (parser->handlers[j].require_param == OPTION_TYPE_OPTIONAL) {
                if (i + 1 < argc && argv[i + 1][0] != '-') {
                    if (parser->handlers[j].execute(argv[++i])) {
                        return 1;
                    }
                } else {
                    if (parser->handlers[j].execute(NULL)) {
                        return 1;
                    }
                }
            } else if (parser->handlers[j].require_param == OPTION_TYPE_MULTIPLE) {
                char combined_params[1024] = {0};
                int k = i + 1;
                while (k < argc && argv[k][0] != '-') {
                    if (combined_params[0]) {
                        strcat(combined_params, ",");
                    }
                    strcat(combined_params, argv[k]);
                    k++;
                }
                if (parser->handlers[j].execute(combined_params)) {
                    return 1;
                }
                i = k - 1;
            } else {
                if (parser->handlers[j].execute(NULL)) {
                    return 1;
                }
            }
            break;
        }
        if (!command_found && argv[i][0] != '-') {
            qargs->job_list = xmalloc((argc - i + 1) * sizeof(char *));
            int count = 0;
            for (int k = i; k < argc; k++) {
                qargs->job_list[count++] = xstrdup(argv[k]);
            }
            qargs->job_list[count] = NULL;
            qargs->job_list_set = true;
            qargs->job_list_count = count;
            break;
        }
        if (!command_found && argv[i][0] == '-') {
            printf("Invalid option argument \"%s\"\n", argv[i]);
            return 1;
        }
    }
    return 0;
}

/**
 * Free resources allocated for the command parser.
 */
static void destroy_parser(CommandParser *parser)
{
    if (!parser) {
        return;
    }
    xfree(parser->handlers);
    xfree(parser->remain_args);
    xfree(parser);
}

/**
 * Add a command handler to the parser.
 */
static void add_handler(CommandParser *parser, CommandHandler handler)
{
    parser->handlers = xrealloc(parser->handlers, (parser->handler_count + 1) * sizeof(CommandHandler));
    parser->handlers[parser->handler_count++] = handler;
}

/**
 * Create and initialize a command parser.
 */
static CommandParser *create_command_parser(void)
{
    CommandParser *parser = xmalloc(sizeof(CommandParser));
    parser->handlers = NULL;
    parser->handler_count = 0;
    parser->add_handler = add_handler;
    parser->parse = parse_command;
    parser->destroy = destroy_parser;
    parser->remain_args = NULL;
    parser->remain_count = 0;

    CommandHandler commands[] = {
        {"-u", OPTION_TYPE_MULTIPLE, set_user_args},
        {"-f", OPTION_TYPE_NONE, set_force_args},
        {"-help", OPTION_TYPE_NONE, set_help_args},
        {"-version", OPTION_TYPE_NONE, set_version_args},
		{"-debug", OPTION_TYPE_NONE, set_debug_args}
    };
    for (size_t i = 0; i < sizeof(commands) / sizeof(commands[0]); i++) {
        parser->add_handler(parser, commands[i]);
    }

    return parser;
}

/**
 *  If '--help' or '--version' is set, display the corresponding information and release the resources.
 */
static bool handle_help_or_version(CommandParser *parser)
{
    if (qargs->help.is_set) {
        display_help_info();
    } else if (qargs->version.is_set) {
        display_version_info();
    } else {
        return false;
    }

    parser->destroy(parser);
    free_Qsub_args();
    return true;
}

/**
 * Main entry point for the qdel command.
 */
int main(int argc, char *argv[])
{
    int rc = 0;
    init_qargs();
    slurm_init(NULL);
    CommandParser *parser = create_command_parser();

    if (parser->parse(parser, argc, argv)) {
        parser->destroy(parser);
        free_Qsub_args();
        exit(1);
    }

    if (handle_help_or_version(parser))
        return 0;

    _opt_default();
    _opt_env();
    if (_fill_opt_from_qsub_args() != 0) 
        goto free_all_mem;

    rc = wrapper_cancel_jobs();

free_all_mem:
    parser->destroy(parser);
    free_Qsub_args();
    free_opt();
    return rc;
}