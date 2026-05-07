/**
 * @file qstat.c
 * @brief SGE-compatible job status tool for Slurm.
 *
 * This program queries and displays job status information from a Slurm cluster,
 * mimicking the behavior of SGE's qstat command. It supports filtering by job ID,
 * user, and displaying detailed job information in a format familiar to SGE users.
 *
 * @author HE JIALE
 * @date 2025-04-11
 * @version 0.0.1
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pwd.h>
#include <time.h>

#include <slurm/slurm.h>
#include <slurm/slurm_errno.h>

#include "qstat.h"

/** Maximum buffer size for string formatting. */
#define MAX_BUFFER_SIZE 256
/** Buffer size for time strings. */
#define TIME_BUFFER_SIZE 20

/** Static global pointer to command-line arguments. */
static qsub_args_t *qargs = NULL;

/* Forward declarations for static functions */
static void init_qargs(void);
static void free_qargs(void);
static int set_job_args(const char *params);
static int set_user_args(const char *params);
static int set_help_args(const char *params);
static int set_version_args(const char *params);
static CommandParser *create_command_parser(void);
static const char *map_slurm_state_to_sge(const slurm_job_info_t *job_info);
static const char *get_username(const slurm_job_info_t *job_info);
static char *format_sge_time(time_t timestamp, char *buf, size_t len);
static void print_header(void);
static void print_job_summary(const slurm_job_info_t *job_info);
// static void print_job_details(const slurm_job_info_t *job_info);
static void display_help_info(void);
static void display_version_info(void);
static void display_job_by_id(const char *job_ids);
static void display_jobs_by_user(const char *users);
static void display_all_jobs(void);
static void display_info(void);
static void print_resource_list(const slurm_job_info_t *job);

/**
 * @brief Initialize the global qsub_args_t structure.
 *
 * Allocates memory for qargs if not already initialized and sets default values.
 */
static void init_qargs(void)
{
    if (qargs) {
        return;
    }
    qargs = xmalloc(sizeof(qsub_args_t));
    qargs->job_list = NULL;
    qargs->user_list = NULL;
    qargs->is_help = false;
    qargs->is_version = false;
}

/**
 * @brief Free the global qsub_args_t structure.
 *
 * Releases all dynamically allocated memory within qargs and resets the pointer.
 */
static void free_qargs(void)
{
    if (!qargs) {
        return;
    }
    xfree(qargs->job_list);
    xfree(qargs->user_list);
    xfree(qargs);
    qargs = NULL;
}

/**
 * @brief Set job ID filter from command-line parameters.
 *
 * @param params Comma-separated list of job IDs.
 * @return 0 on success, 1 on error.
 */
static int set_job_args(const char *params)
{
    if (!qargs) {
        init_qargs();
    }
    xfree(qargs->job_list);
    if (params) {
        qargs->job_list = xstrdup(params);
    }
    return 0;
}

/**
 * @brief Set user filter from command-line parameters.
 *
 * @param params Comma-separated list of usernames.
 * @return 0 on success, 1 on error.
 */
static int set_user_args(const char *params)
{
    if (!qargs) {
        init_qargs();
    }
    xfree(qargs->user_list);
    if (params) {
        qargs->user_list = xstrdup(params);
    }
    return 0;
}

/**
 * @brief Set help flag.
 *
 * @param params Unused.
 * @return 0 on success.
 */
static int set_help_args(const char *params)
{
    (void)params; /* unused param */
    if (!qargs) {
        init_qargs();
    }
    qargs->is_help = true;
    return 0;
}

/**
 * @brief Set version flag.
 *
 * @param params Unused.
 * @return 0 on success.
 */
static int set_version_args(const char *params)
{
    (void)params; /* unused param */
    if (!qargs) {
        init_qargs();
    }
    qargs->is_version = true;
    return 0;
}

/**
 * @brief Create a command-line parser.
 *
 * Initializes a CommandParser with handlers for supported options.
 *
 * @return Pointer to the created parser, or NULL on failure.
 */
static CommandParser *create_command_parser(void)
{
    CommandParser *parser = xmalloc(sizeof(CommandParser));
    parser->handlers = NULL;
    parser->handler_count = 0;
    parser->add_handler = add_handler;
    parser->parse = parse_command;
    parser->destroy = destroy_parser;

    CommandHandler commands[] = {
        {"-j", OPTION_TYPE_REQUIRED, set_job_args},
        {"-u", OPTION_TYPE_REQUIRED, set_user_args},
        {"-help", OPTION_TYPE_NONE, set_help_args},
        {"-version", OPTION_TYPE_NONE, set_version_args}
    };

    for (size_t i = 0; i < sizeof(commands) / sizeof(commands[0]); i++) {
        parser->add_handler(parser, commands[i]);
    }

    return parser;
}

/**
 * @brief Add a command handler to the parser.
 *
 * @param parser The command parser.
 * @param handler The handler to add.
 */
void add_handler(CommandParser *parser, CommandHandler handler)
{
    if (!parser)
        return;
    parser->handlers = xrealloc(parser->handlers,
                               sizeof(CommandHandler) * (parser->handler_count + 1));
    parser->handlers[parser->handler_count] = handler;
    parser->handler_count++;
}

/**
 * @brief Parse command-line arguments.
 *
 * @param parser The command parser.
 * @param argc Argument count.
 * @param argv Argument vector.
 * @return 0 on success, 1 on error.
 */
int parse_command(CommandParser *parser, int argc, char *argv[])
{
    if (!parser || !argv) {
        slurm_error("Invalid parser or arguments.");
        return 1;
    }

    for (int i = 1; i < argc; i++) {
        bool command_found = false;
        for (int j = 0; j < parser->handler_count; j++) {
            if (strcmp(argv[i], parser->handlers[j].command) != 0) {
                continue;
            }
            command_found = true;
            if (parser->handlers[j].require_param == OPTION_TYPE_REQUIRED) {
                if (i + 1 >= argc) {
                    fprintf(stderr, "%s option must have argument\n", argv[i]);
                    return 1;
                }
                if (parser->handlers[j].execute(argv[i + 1])) {
                    return 1;
                }
                i++;
            } else {
                if (parser->handlers[j].execute(NULL)) {
                    return 1;
                }
            }
            break;
        }
        if (!command_found) {
            display_help_info();
            printf("invalid option argument \"%s\"\n", argv[i]);
            return 1;
        }
    }
    return 0;
}

/**
 * @brief Destroy the command parser.
 *
 * @param parser The parser to destroy.
 */
void destroy_parser(CommandParser *parser)
{
    if (!parser) {
        return;
    }
    xfree(parser->handlers);
    xfree(parser);
}

/**
 * @brief Map Slurm job state to SGE state string.
 *
 * Converts a Slurm job state to its equivalent SGE state, handling pending,
 * running, suspended, and error states. Some SGE states are reserved for future
 * implementation.
 *
 * @param job_info Slurm job information structure.
 * @return SGE state string (e.g., "qw", "r", "Eqw").
 */
static const char *map_slurm_state_to_sge(const slurm_job_info_t *job_info)
{
    uint32_t base_state = job_info->job_state & JOB_STATE_BASE;

    switch (base_state) {
    case JOB_PENDING:
        if (job_info->priority == 0) {
            if (job_info->job_state & JOB_REQUEUE) {
                return "hRqw"; /* Pending, hold, re-queue */
            }
            return "hqw"; /* Pending, user or system hold */
        }
        if (job_info->job_state & JOB_REQUEUE) {
            return "hRqw"; /* Pending, re-queue */
        }
        return "qw"; /* Pending */

    case JOB_RUNNING:
        if (job_info->job_state & JOB_REQUEUE) {
            if (job_info->priority == 0) {
                return "hRr"; /* Running, hold, re-run */
            }
            return "Rr"; /* Running, re-run */
        }
        if (job_info->priority == 0) {
            return "hr"; /* Running, hold */
        }
        return "r"; /* Running */
    
    case JOB_SUSPENDED:
        if (job_info->job_state & JOB_REQUEUE) {
            return "Rs"; /* Suspended, re-run */
        }
        return "s"; /* Suspended */

    case JOB_FAILED:
    case JOB_NODE_FAIL:
    case JOB_BOOT_FAIL:
    case JOB_OOM:
        if (job_info->priority == 0) {
            if (job_info->job_state & JOB_REQUEUE) {
                return "EhRqw"; /* Error, hold, re-queue */
            }
            return "Ehqw"; /* Error, hold */
        }
        return "Eqw"; /* Error */

    case JOB_CANCELLED:
        return "dr"; /* Deleting, running */
    case JOB_TIMEOUT:
    case JOB_DEADLINE:
        return "d"; /* Deleting */
    case JOB_COMPLETE:
        return "cd"; /* Completed */
    case JOB_PREEMPTED:
        if (job_info->job_state & JOB_REQUEUE) {
            return "Rs"; /* Suspended, re-run */
        }
        return "s"; /* Suspended */

    default:
        if (job_info->job_state & JOB_REQUEUE) {
            return "hRqw"; /* Default re-queue */
        }
        return "unk"; /* Unknown */
    }
}

/**
 * @brief Format and print job resource requirements in SGE-compatible format.
 *
 * Converts Slurm job resource specifications to a comma-separated string
 * matching the original resource request format. Handles memory, CPU count,
 * hostname requirements, time limits, and GPU specifications. Memory values
 * are processed to remove per-CPU flags before formatting.
 *
 * @param job Slurm job information structure containing resource requirements.
 */
static void print_resource_list(const slurm_job_info_t *job) {
    int resource_count = 0;
    char resource_buffer[1024] = {0};
    char temp_buffer[256] = {0};
    
    // mem
    uint64_t mem_value = job->pn_min_memory;
    if (mem_value & MEM_PER_CPU) {
        mem_value &= (~MEM_PER_CPU);
    }
    if (mem_value != NO_VAL64 && mem_value > (uint64_t)0) {
        if (resource_count > 0) {
            strcat(resource_buffer, ",");
        }
        snprintf(temp_buffer, sizeof(temp_buffer), "%s=%"PRIu64, KEY_MEM_FREE, mem_value);
        strcat(resource_buffer, temp_buffer);
        resource_count++;
    }
    
    // num_proc
    if (job->cpus_per_task > 0) {
        if (resource_count > 0) {
            strcat(resource_buffer, ",");
        }
        snprintf(temp_buffer, sizeof(temp_buffer), "%s=%d", KEY_NUM_PROC, job->cpus_per_task);
        strcat(resource_buffer, temp_buffer);
        resource_count++;
    }
    
    // hostname
    if (job->req_nodes != NULL) {
        if (resource_count > 0) {
            strcat(resource_buffer, ",");
        }
        snprintf(temp_buffer, sizeof(temp_buffer), "%s=%s", KEY_HOSTNAME, job->req_nodes);
        strcat(resource_buffer, temp_buffer);
        resource_count++;
    }
    
    // h_rt
    if (job->time_limit > 0) {
        if (resource_count > 0) {
            strcat(resource_buffer, ",");
        }
        int hours = job->time_limit / 60;
        int minutes = job->time_limit % 60;
        int seconds = 0; 
        snprintf(temp_buffer, sizeof(temp_buffer), "%s=%d:%02d:%02d", KEY_TIME_LIMIT, hours, minutes, seconds);
        strcat(resource_buffer, temp_buffer);
        resource_count++;
    }
    
    // GPU
    if (job->tres_per_node != NULL && strstr(job->tres_per_node, "gres:gpu:") != NULL) {
        if (resource_count > 0) {
            strcat(resource_buffer, ",");
        }
        // "gres:gpu:3"
        const char *gpu_str = job->tres_per_node;
        int gpu_count = 0;
        if (sscanf(gpu_str, "gres/gpu:%d", &gpu_count) == 1) {
            snprintf(temp_buffer, sizeof(temp_buffer), "%s=%d", KEY_GPU, gpu_count);
            strcat(resource_buffer, temp_buffer);
            resource_count++;
        }
    }
    
    printf("hard resource_list:         %s\n", resource_buffer);
}

/**
 * @brief Get username from job information.
 *
 * Retrieves the username from job_info->user_name or falls back to getpwuid.
 *
 * @param job_info Slurm job information structure.
 * @return Username string, or "N/A" if unavailable.
 */
static const char *get_username(const slurm_job_info_t *job_info)
{
    if (job_info->user_name) {
        return job_info->user_name;
    }
    struct passwd *pw = getpwuid(job_info->user_id);
    return pw ? pw->pw_name : "N/A";
}

/**
 * @brief Format a timestamp to SGE-style date-time string.
 *
 * Converts a Unix timestamp to MM/DD/YYYY HH:MM:SS format.
 *
 * @param timestamp Unix timestamp.
 * @param buf Output buffer.
 * @param len Buffer length.
 * @return Pointer to the formatted string.
 */
static char *format_sge_time(time_t timestamp, char *buf, size_t len)
{
    if (timestamp == 0) {
        snprintf(buf, len, "N/A           ");
        return buf;
    }
    struct tm tm_info;
    localtime_r(&timestamp, &tm_info);
    strftime(buf, len, "%m/%d/%Y %H:%M:%S", &tm_info);
    return buf;
}

/**
 * @brief Print the header for job summary table.
 *
 * Outputs a formatted header matching SGE qstat's table layout.
 */
static void print_header(void)
{
    printf("%-7s %-7s %-10s %-12s %-5s %-19s %-30s %-5s %-11s\n",
           "job-ID", "prior", "name", "user", "state", "submit/start at",
           "queue", "slots", "ja-task-ID");
    printf("-----------------------------------------------------------------------------------------------------------------\n");
}

/**
 * @brief Print summary information for a job.
 *
 * Outputs a single row in the job summary table, matching SGE qstat's format.
 *
 * @param job_info Slurm job information structure.
 */
static void print_job_summary(const slurm_job_info_t *job_info)
{
    char job_id_str[12];
    char queue_str[31];
    char ja_task_id[12] = "";
    char time_buf[TIME_BUFFER_SIZE];
    time_t time_to_show = 0;

    /* Format job ID and array task ID */
    if (job_info->array_job_id != 0) {
        snprintf(job_id_str, sizeof(job_id_str), "%u", job_info->array_job_id);
        if (job_info->array_task_id != NO_VAL) {
            snprintf(ja_task_id, sizeof(ja_task_id), "%u", job_info->array_task_id);
        } else if (job_info->array_task_str) {
            snprintf(ja_task_id, sizeof(ja_task_id), "%s", job_info->array_task_str);
            char *percent_pos = strchr(ja_task_id, '%');
            if (percent_pos) {
                *percent_pos = '\0';
            }
        }
    } else {
        snprintf(job_id_str, sizeof(job_id_str), "%u", job_info->job_id);
    }

    /* Format queue */
    if (job_info->partition && job_info->nodes && job_info->job_state == JOB_RUNNING) {
        snprintf(queue_str, sizeof(queue_str), "%s@%s", job_info->partition, job_info->nodes);
    } else if (job_info->partition) {
        snprintf(queue_str, sizeof(queue_str), "%s", job_info->partition);
    } else {
        queue_str[0] = '\0';
    }

    /* Format priority and time */
    float priority = job_info->priority > 10000.0 ? 1.0 : job_info->priority / 10000.0;
    if (IS_JOB_RUNNING(job_info)) 
        time_to_show = job_info->start_time;
    else 
        time_to_show = job_info->submit_time;

    printf("%7s %7.5f %-10.10s %-12.12s %-5s %-19s %-30.30s %5d %-11s\n",
           job_id_str,
           priority,
           job_info->name ? job_info->name : "N/A",
           get_username(job_info),
           map_slurm_state_to_sge(job_info),
           format_sge_time(time_to_show, time_buf, sizeof(time_buf)),
           queue_str,
           job_info->num_cpus,
           ja_task_id);
}

static void print_job_details_2(const job_info_msg_t *job_buffer_info)
{
    slurm_job_info_t *job_info;
    struct passwd *pw = NULL;
    struct group *grp = NULL;
    // char time_buf[TIME_BUFFER_SIZE];
    char *path = getenv("PATH");
    bool is_array_job = false;

    if (job_buffer_info->record_count > 1) {
        // is array job
        for (uint32_t i = 0; i < job_buffer_info->record_count; i++) {
            job_info = &job_buffer_info->job_array[i];
            if (job_info->array_job_id == job_info->job_id) {
                is_array_job = true;
                break;
            }
        }
    } else {
        job_info = &job_buffer_info->job_array[0];
    }

    if (job_info == NULL)
        return;

    pw = getpwuid(job_info->user_id);
    grp = getgrgid(job_info->group_id);

    printf("==============================================================\n");
    printf("job_number:                 %u\n", job_info->job_id);
    printf("exec_file:                  %s\n", job_info->command ? job_info->command : "STDIN");
    printf("submission_time:            %s", ctime(&job_info->submit_time));
    if (job_info->deadline > 0 && job_info->deadline != NO_VAL) {
        printf("deadline                    %s", ctime(&job_info->deadline));
    } 
    printf("owner:                      %s\n", get_username(job_info));
    printf("uid:                        %u\n", job_info->user_id);
    printf("group:                      %s\n", grp ? grp->gr_name : "NONE"); /* Slurm does not provide group name */
    printf("gid:                        %u\n", job_info->group_id);
    printf("sge_o_home:                 %s\n", pw ? pw->pw_dir : "N/A");
    printf("sge_o_log_name:             %s\n", get_username(job_info));
    printf("sge_o_path:                 %s\n", path ? path : "N/A");
    printf("sge_o_shell:                %s\n", pw ? pw->pw_shell : "/bin/bash");
    printf("sge_o_workdir:              %s\n", job_info->work_dir ? job_info->work_dir : "N/A");
    printf("sge_o_host:                 %s\n", job_info->alloc_node ? job_info->alloc_node : "NONE");
    if (!IS_JOB_RUNNING(job_info)) {
        printf("execution_time:             %s", ctime(&job_info->eligible_time));
    }
    printf("account:                    %s\n", job_info->account ? job_info->account : "sge");
    if (job_info->std_err) {
        printf("stderr_path_list:           NONE:NONE:%s\n", job_info->std_err ? job_info->std_err : "NONE");
    }
    
    print_resource_list(job_info);

    if (job_info->mail_type != 0) {
        printf("mail_options:               ");
        char mail_opts[8] = {0};
        int pos = 0;

        if (job_info->mail_type & MAIL_JOB_FAIL) {
            mail_opts[pos++] = 'a';
        }
        if (job_info->mail_type & MAIL_JOB_BEGIN) {
            mail_opts[pos++] = 'b';
        }
        if (job_info->mail_type & MAIL_JOB_END) {
            mail_opts[pos++] = 'e';
        }
        
        printf("%s\n", mail_opts);
    }

    printf("mail_list:                  %s@NONE\n", job_info->mail_user);
    printf("notify:                     %s\n", (job_info->mail_type & MAIL_JOB_BEGIN) ? "TRUE" : "FALSE");
    printf("job_name:                   %s\n", job_info->name ? job_info->name : "N/A");
    if (job_info->std_out) {
        printf("stdout_path_list:           NONE:NONE:%s\n", job_info->std_out ? job_info->std_out : "NONE");
    }
    if (job_info->std_in) {
        printf("stdin_path_list:            NONE:NONE:%s\n", job_info->std_in ? job_info->std_in : "NONE");
    }
    printf("jobshare:                   0\n");
    printf("hard_queue_list:            %s\n", job_info->partition ? job_info->partition : "N/A");
    printf("shell_list:                 NONE:/bin/sh\n");
    printf("env_list:                   TERM=xterm,PATH=%s\n", path ? path : "N/A");
    if (job_info->command) {
        printf("script_file:                %s\n", job_info->command);
    }

    printf("parallel environment:       N/A range: %u\n", job_info->num_cpus);
    if (job_info->array_job_id != 0) {
        char ja_task_id[12];
        snprintf(ja_task_id, sizeof(ja_task_id), "%s", job_info->array_task_str ? job_info->array_task_str : "");
        char *percent_pos = strchr(ja_task_id, '%');
        if (percent_pos) {
            *percent_pos = '\0';
        }
        printf("job-array tasks:            %s\n", ja_task_id);
        if (job_info->array_max_tasks != 0) {
            printf("maximum concurrency:        %u\n", job_info->array_max_tasks);
        }
    }
    
    if (IS_JOB_RUNNING(job_info)) {
        if (is_array_job) {
            for (int i = job_buffer_info->record_count - 1; i >= 0; i--) {
                slurm_job_info_t *job_ptr = &job_buffer_info->job_array[i];
                if (job_ptr->array_task_id != NO_VAL) {
                    printf("usage      %4u:            %s\n", job_ptr->array_task_id, "cpu=00:00:00, mem=0.00000 GB s, io=0.00000 GB, vmem=0.000M, maxvmem=0.000M");
                }
            }
        } else {
            printf("usage         1:            %s\n", "cpu=00:00:00, mem=0.00000 GB s, io=0.00000 GB, vmem=0.000M, maxvmem=0.000M");
        }
    }
    

    if (IS_JOB_RUNNING(job_info)) {
        if (is_array_job) {
            for (int i = job_buffer_info->record_count - 1; i >= 0; i--) {
                slurm_job_info_t *job_ptr = &job_buffer_info->job_array[i];
                if (job_ptr->array_task_id != NO_VAL) {
                    printf("binding    %4u:            %s\n", job_ptr->array_task_id, job_ptr->tres_bind ? job_ptr->tres_bind : "NONE");
                }
            }
        } else {
            printf("binding       1:            %s\n", job_info->tres_bind ? job_info->tres_bind : "NONE");
        }
    } else {
        printf("binding:                    %s\n", job_info->tres_bind ? job_info->tres_bind : "NONE");
    }
    printf("job_type:                   %s\n", "NONE");
    printf("scheduling info:            %s\n", "(Collecting of scheduler job information is turned off)");
}
/**
 * @brief Print detailed information for a job.
 *
 * Outputs detailed job information in a format matching SGE qstat -j.
 *
 * @param job_info Slurm job information structure.
 */
// static void print_job_details(const slurm_job_info_t *job_info)
// {
//     struct passwd *pw = getpwuid(job_info->user_id);
//     // char time_buf[TIME_BUFFER_SIZE];
//     char *path = getenv("PATH");

//     printf("==============================================================\n");
//     printf("job_number:                 %u\n", job_info->job_id);
//     printf("exec_file:                  %s\n", job_info->command ? job_info->command : "STDIN");
//     // printf("submission_time:            %s\n", format_sge_time(job_info->submit_time, time_buf, sizeof(time_buf)));
//     printf("submission_time:            %s", ctime(&job_info->submit_time));
//     printf("owner:                      %s\n", get_username(job_info));
//     printf("uid:                        %u\n", job_info->user_id);
//     printf("group:                      %s\n", "N/A"); /* Slurm does not provide group name */
//     printf("gid:                        %u\n", job_info->group_id);
//     printf("sge_o_home:                 %s\n", pw ? pw->pw_dir : "N/A");
//     printf("sge_o_log_name:             %s\n", get_username(job_info));
//     printf("sge_o_path:                 %s\n", path ? path : "N/A");
//     printf("sge_o_shell:                %s\n", pw ? pw->pw_shell : "/bin/bash");
//     printf("sge_o_workdir:              %s\n", job_info->work_dir ? job_info->work_dir : "N/A");
//     printf("account:                    %s\n", job_info->account ? job_info->account : "sge");
//     printf("cwd:                        %s\n", job_info->work_dir ? job_info->work_dir : "N/A");
//     printf("stderr_path_list:           NONE:NONE:%s\n", job_info->std_err ? job_info->std_err : "N/A");
//     if (job_info->tres_req_str) {
//         printf("hard resource_list:         %s\n", job_info->tres_req_str);
//     } else {
//         printf("hard resource_list:         num_proc=%d\n", job_info->num_cpus);
//     }
//     printf("mail_options:               %s\n", job_info->mail_type ? "a" : "NONE");
//     printf("mail_list:                  %s@%s\n", get_username(job_info), "N/A");
//     printf("notify:                     %s\n", (job_info->mail_type & MAIL_JOB_BEGIN) ? "TRUE" : "FALSE");
//     printf("job_name:                   %s\n", job_info->name ? job_info->name : "N/A");
//     printf("stdout_path_list:           NONE:NONE:%s\n", job_info->std_out ? job_info->std_out : "N/A");
//     printf("jobshare:                   0\n");
//     printf("hard_queue_list:            %s\n", job_info->partition ? job_info->partition : "N/A");
//     printf("shell_list:                 NONE:/bin/sh\n");
//     printf("env_list:                   TERM=xterm,PATH=%s\n", path ? path : "N/A");
//     if (job_info->command) {
//         printf("script_file:                %s\n", job_info->command);
//     }
//     printf("parallel environment:       N/A range: %u\n", job_info->num_cpus);
//     if (job_info->array_job_id != 0) {
//         char ja_task_id[12];
//         snprintf(ja_task_id, sizeof(ja_task_id), "%s", job_info->array_task_str ? job_info->array_task_str : "");
//         char *percent_pos = strchr(ja_task_id, '%');
//         if (percent_pos) {
//             *percent_pos = '\0';
//         }
//         printf("job-array tasks:            %s\n", ja_task_id);
//         if (job_info->array_max_tasks != 0) {
//             printf("maximum concurrency:        %u\n", job_info->array_max_tasks);
//         }
//     }
//     printf("binding:                    %s\n", job_info->tres_bind ? job_info->tres_bind : "NONE");
//     printf("job_type:                   %s\n", job_info->batch_flag ? "BATCH" : "NONE");
//     printf("scheduling info:            %s\n",
//            job_info->state_reason ? job_info->state_desc : "(Collecting of scheduler job information is turned off)");
// }

/**
 * @brief Display help information.
 *
 * Prints usage information and supported options, matching SGE qstat's help output.
 */
void display_help_info() {
    printf("SGE 8.1.9\n");
    printf("usage: qstat [options]\n");
    printf("        [-ext]                            view additional attributes\n");
    printf("        [-explain a|c|A|E]                show reason for queue c(onfiguration ambiguous), a(larm), suspend A(larm), E(rror) state\n");
    printf("        [-f]                              full output\n");
    printf("        [-F [resource_attributes]]        full output and show (selected) resources of queue(s)\n");
    printf("        [-g c]                            display cluster queue summary\n");
    printf("        [-g d]                            display all job-array tasks (do not group)\n");
    printf("        [-g t]                            display all parallel job tasks (do not group)\n");
    printf("        [-help]                           print this help\n");
    printf("        [-j [job_identifier_list]]        show scheduler job information\n");
    printf("        [-l resource_list]                request the given resources\n");
    printf("        [-ne]                             hide empty queues\n");
    printf("        [-ncb]                            suppress additional binding specific parameters\n");
    printf("        [-pe pe_list]                     select only queues with one of these parallel environments\n");
    printf("        [-q wc_queue_list]                print information on given queue\n");
    printf("        [-qs {a|c|d|o|s|u|A|C|D|E|S}]     selects queues which are in the given state(s)\n");
    printf("        [-r]                              show requested resources of job(s)\n");
    printf("        [-s {p|r|s|z|hu|ho|hs|hd|hj|ha|h|a}] show pending, running, suspended, zombie jobs,\n");
    printf("                                          jobs with a user/operator/system/array-dependency hold, \n");
    printf("                                          jobs with a start time in future or any combination only.\n");
    printf("                                          h is an abbreviation for huhohshdhjha\n");
    printf("                                          a is an abbreviation for prsh\n");
    printf("        [-t]                              show task information (implicitly -g t)\n");
    printf("        [-u user_list]                    view jobs of this user list\n");
    printf("        [-U user_list]                    select only queues where these users have access\n");
    printf("        [-urg]                            display job urgency information\n");
    printf("        [-pri]                            display job priority information\n");
    printf("        [-xml]                            display the information in XML format\n");
    printf("\n");
    printf("pe_list                  pe[,pe,...]\n");
    printf("job_identifier_list      [job_id|job_name|pattern]{, [job_id|job_name|pattern]}\n");
    printf("resource_list            resource[=value][,resource[=value],...]\n");
    printf("user_list                user|*|@group[,user|*|@group],...\n");
    printf("resource_attributes      resource,resource,...\n");
    printf("wc_cqueue                wildcard expression matching a cluster queue\n");
    printf("wc_host                  wildcard expression matching a host\n");
    printf("wc_hostgroup             wildcard expression matching a hostgroup\n");
    printf("wc_qinstance             wc_cqueue@wc_host\n");
    printf("wc_qdomain               wc_cqueue@wc_hostgroup\n");
    printf("wc_queue_list            wc_cqueue|wc_qdomain|wc_qinstance\n");
    printf("wc_queue_list            wc_queue[,wc_queue,...]\n");
}


/**
 * @brief Display version information.
 *
 * Prints program version and supported parameters.
 */
static void display_version_info(void)
{
    printf("METASTACK SGE WRAPPER\n");
    printf("Version: %s\n", PROGRAM_VERSION);
    printf("Release Date: %s\n", RELEASE_DATE);
    printf("Supported Parameters: -j, -u, -help, -version\n");
}

/**
 * @brief Display detailed information for specified job IDs.
 *
 * @param job_ids Comma-separated list of job IDs.
 */
static void display_job_by_id(const char *job_ids)
{
    char *ids = xstrdup(job_ids);
    char *saveptr = NULL;
    char *token = strtok_r(ids, ",", &saveptr);
    
    char *not_found_ids = NULL;
    size_t not_found_len = 0;
    int found_any_not_found = 0;
    
    while (token) {
        char *endptr;
        uint32_t job_id = strtoul(token, &endptr, 10);
        job_info_msg_t *resp = NULL;
        int rc = slurm_load_job(&resp, job_id, SHOW_ALL);
        
        if (rc != 0 || !resp) {
            if (!found_any_not_found) {
                not_found_ids = xmalloc(64);
                not_found_ids[0] = '\0';
                found_any_not_found = 1;
            } else {
                not_found_ids = xrealloc(not_found_ids, not_found_len + 3);
                xstrcat(not_found_ids, ", ");
                not_found_len += 2;
            }
            
            char job_id_str[32];
            snprintf(job_id_str, sizeof(job_id_str), "%"PRIu32, job_id);
            size_t job_id_len = strlen(job_id_str);
            not_found_ids = xrealloc(not_found_ids, not_found_len + job_id_len + 1);
            xstrcat(not_found_ids, job_id_str);
            not_found_len += job_id_len;
            
            if (resp) {
                slurm_free_job_info_msg(resp);
            }
        } else {
            print_job_details_2(resp);
            slurm_free_job_info_msg(resp);
        }
        
        token = strtok_r(NULL, ",", &saveptr);
    }
    
    if (found_any_not_found) {
        printf("Following jobs do not exist:\n");
        printf("%s\n", not_found_ids);
        xfree(not_found_ids);
    }
    
    xfree(ids);
}
/**
 * @brief Display job summaries for specified users.
 *
 * @param users Comma-separated list of usernames.
 */
static void display_jobs_by_user(const char *users)
{
    job_info_msg_t *job_info_ptr = NULL;
    int rc = slurm_load_jobs(0LL, &job_info_ptr, SHOW_ALL);
    if (rc != 0 || !job_info_ptr) {
        slurm_error("Failed to load jobs: %s.", slurm_strerror(slurm_get_errno()));
        return;
    }

    char *user_list = xstrdup(users);
    char *token = strtok(user_list, ",");
    bool header_printed = false;

    while (token) {
        for (uint32_t i = 0; i < job_info_ptr->record_count; i++) {
            slurm_job_info_t *job = &job_info_ptr->job_array[i];
            if (IS_JOB_FINISHED(job)) {
                continue;
            }
            if (strcmp(get_username(job), token) == 0) {
                if (!header_printed) {
                    print_header();
                    header_printed = true;
                }
                print_job_summary(job);
            }
        }
        token = strtok(NULL, ",");
    }

    xfree(user_list);
    slurm_free_job_info_msg(job_info_ptr);
}

/**
 * @brief Display summaries for all non-finished jobs.
 */
static void display_all_jobs(void)
{
    job_info_msg_t *job_info_ptr = NULL;
    int rc = slurm_load_jobs(0LL, &job_info_ptr, SHOW_ALL);
    if (rc != 0 || !job_info_ptr) {
        slurm_error("Failed to load jobs: %s.", slurm_strerror(slurm_get_errno()));
        return;
    }

    bool header_printed = false;
    for (uint32_t i = 0; i < job_info_ptr->record_count; i++) {
        slurm_job_info_t *job = &job_info_ptr->job_array[i];
        if (IS_JOB_FINISHED(job)) {
            continue;
        }
        if (!header_printed) {
            print_header();
            header_printed = true;
        }
        print_job_summary(job);
    }

    slurm_free_job_info_msg(job_info_ptr);
}

/**
 * @brief Display job information based on command-line arguments.
 */
static void display_info(void)
{
    if (qargs->is_help) {
        display_help_info();
    } else if (qargs->is_version) {
        display_version_info();
    } else if (qargs->job_list) {
        display_job_by_id(qargs->job_list);
    } else if (qargs->user_list) {
        display_jobs_by_user(qargs->user_list);
    } else {
        display_all_jobs();
    }
}

/**
 * @brief Main entry point for the qstat program.
 *
 * Parses command-line arguments, queries job information, and displays results.
 *
 * @param argc Argument count.
 * @param argv Argument vector.
 * @return 0 on success, 1 on error.
 */
int main(int argc, char *argv[])
{
    init_qargs();
    slurm_init(NULL);
    CommandParser *parser = create_command_parser();
    if (parse_command(parser, argc, argv)) {
        destroy_parser(parser);
        free_qargs();
        return 1;
    }

    display_info();

    destroy_parser(parser);
    free_qargs();
    return 0;
}