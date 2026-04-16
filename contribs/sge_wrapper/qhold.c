/**
 * @file qhold.c
 * @brief Slurm job hold/release tool mimicking SGE qhold and qrls.
 *
 * Implements a command-line interface for holding (`qhold`) or releasing (`qrls`)
 * Slurm jobs, supporting SGE-compatible options such as hold type (-h), user list (-u),
 * help (-help), and version (-version). Handles job IDs, array tasks, and user-based
 * job management.
 *
 * @author HE JIALE
 * @date 2025-04-13
 * @version 0.0.1
 */

#include <errno.h>
#include <libgen.h>
#include <pwd.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <slurm/slurm.h>
#include <slurm/slurm_errno.h>
#include "qhold.h"

/** Maximum number of users to process. */
#define MAX_USERS 1024
/** Maximum buffer size for combined parameters. */
#define MAX_PARAM_BUFFER 1024
/** Maximum length of job ID strings. */
#define MAX_JOB_ID_STR 32

/** Enumeration for program type (qhold or qrls). */
typedef enum {
    QHOLD,  /**< Hold jobs. */
    QRLS    /**< Release jobs. */
} ProgramType;

/** Static qhold_args_t instance for storing parsed arguments. */
static qhold_args_t *qargs = NULL;
/** Current program type (qhold or qrls). */
static ProgramType program = QHOLD;
/** Array of valid user IDs. */
static uint32_t valid_uids[MAX_USERS];
/** Number of valid user IDs. */
static int valid_uid_count = 0;
/** Recovered allocation SID for job updates. */
static uint32_t recover_alloc_sid = 0;
/** Recovered priority for job updates. */
static uint32_t recover_priority = 0;

static void init_job_msg(job_desc_msg_t *job_msg);
static void init_qargs(void);
static void free_qargs(void);
static int set_hold_type(const char *param);
static int set_user_list(const char *param);
static int set_help(const char *param);
static int set_version(const char *param);
static void display_help(void);
static void display_version(void);
static void parse_array_range(const char *range, uint32_t *job_id, uint32_t **tasks, int *task_count);
static int update_job(job_desc_msg_t *job_msg, uint32_t job_id, uint32_t task_id, uint32_t array_job_id);
static int handle_jobs(void);
static void add_handler(CommandParser *parser, CommandHandler handler);
static void destroy_parser(CommandParser *parser);
static int parse_command(CommandParser *parser, int argc, char *argv[]);
static CommandParser *create_command_parser(void);
static int determine_program_type(const char *progname);
static int handle_operation(void);

/**
 * @brief Initialize a job descriptor with recovered values.
 * @param job_msg Job descriptor to initialize.
 */
static void init_job_msg(job_desc_msg_t *job_msg) {
    slurm_init_job_desc_msg(job_msg);
    job_msg->alloc_sid = recover_alloc_sid;
    job_msg->priority = recover_priority;
}

/**
 * @brief Initialize the global qhold_args_t structure.
 */
static void init_qargs(void) {
    if (qargs) 
        return;
    qargs = xmalloc(sizeof(qhold_args_t));
    qargs->job_list = NULL;
    qargs->user_list = NULL;
    qargs->hold_type = NULL;
    qargs->is_help = false;
    qargs->is_version = false;
}

/**
 * @brief Free the global qhold_args_t structure.
 */
static void free_qargs(void) {
    if (!qargs) 
        return;
    xfree(qargs->job_list);
    xfree(qargs->user_list);
    xfree(qargs->hold_type);
    xfree(qargs);
    qargs = NULL;
}

/**
 * @brief Set the hold type option (-h).
 * @param param Hold type string (e.g., "u,s,o").
 * @return 0 on success, 1 on error.
 */
static int set_hold_type(const char *param) {
    if (!param || !*param) 
        return 0;

    xfree(qargs->hold_type);
    qargs->hold_type = xstrdup(param);
    return 0;
}

/**
 * @brief Set the user list option (-u).
 * @param param Comma-separated list of usernames.
 * @return 0 on success, 1 on error.
 */
static int set_user_list(const char *param) {
    if (!param || !*param) 
        return 0;
    xfree(qargs->user_list);
    valid_uid_count = 0;
    qargs->user_list = xstrdup(param);

    char *user_list_copy = xstrdup(param);
    char *saveptr = NULL;
    char *token = strtok_r(user_list_copy, ",", &saveptr);

    while (token) {
        struct passwd *pw = getpwnam(token);
        if (!pw) {
            fprintf(stderr, "User %s does not exist\n", token);
            xfree(user_list_copy);
            return 1;
        }

        if (valid_uid_count >= MAX_USERS) {
            fprintf(stderr, "Too many users, maximum is %d\n", MAX_USERS);
            xfree(user_list_copy);
            return 1;
        }

        valid_uids[valid_uid_count++] = pw->pw_uid;
        token = strtok_r(NULL, ",", &saveptr);
    }
    xfree(user_list_copy);
    return 0;
}

/**
 * @brief Set the help flag (-help).
 * @param param Unused parameter.
 * @return 0 on success.
 */
static int set_help(const char *param) {
    (void)param; /* unused param */
    qargs->is_help = true;
    return 0;
}

/**
 * @brief Set the version flag (-version).
 * @param param Unused parameter.
 * @return 0 on success.
 */
static int set_version(const char *param) {
    (void)param; /* unused param */
    qargs->is_version = true;
    return 0;
}

/**
 * @brief Display help information for qhold or qrls.
 */
static void display_help(void) {
    printf("SGE 8.1.9\n");
    printf("Usage: %s [options] job_task_list\n", program == QHOLD ? "qhold" : "qrls");
    printf("   [-h hold_list]                           list of hold types concerned\n");
    printf("   [-help]                                  print this help\n");
    printf("   [-u user_list]                           specify a list of users\n");
    printf("                                            (not allowed in combination with job_task_list)\n");
    printf("   job_task_list                            jobids (and taskids) of jobs to be altered\n");
    printf("\n");
    printf("hold_list               `u' `s' `o'\n");
    printf("job_task_list           job_tasks[,job_tasks,...]\n");
    printf("job_tasks               [job_id['.'task_id_range]|job_name|pattern][' -t 'task_id_range]\n");
    printf("task_id_range           task_id['-'task_id[':'step]]\n");
    printf("user_list               user[,user,...]\n");
}

/**
 * @brief Display version information.
 */
static void display_version(void) {
    printf("METASTACK SGE WRAPPER\n");
    printf("Version: %s\n", PROGRAM_VERSION);
    printf("Release Date: %s\n", RELEASE_DATE);
    printf("Supported options: -h, -u, -help, -version\n");
}

/**
 * @brief Parse a job array range string (e.g., "123.1-10:2").
 * @param range Range string to parse.
 * @param job_id [out] Job ID.
 * @param tasks [out] Array of task IDs (caller must free).
 * @param task_count [out] Number of tasks.
 */
static void parse_array_range(const char *range, uint32_t *job_id, uint32_t **tasks, int *task_count) {
    char *range_copy = xstrdup(range);
    char *task_range = strchr(range_copy, '.');

    if (!task_range) {
        *job_id = strtoul(range_copy, NULL, 10);
        *tasks = xmalloc(sizeof(uint32_t));
        *tasks[0] = 0;
        *task_count = 1;
        xfree(range_copy);
        return;
    }

    *task_range = '\0';
    *job_id = strtoul(range_copy, NULL, 10);
    task_range++;

    char *endptr;
    uint32_t start = strtoul(task_range, &endptr, 10);
    if (*endptr != '-') {
        *tasks = xmalloc(sizeof(uint32_t));
        *tasks[0] = start;
        *task_count = 1;
        xfree(range_copy);
        return;
    }

    endptr++;
    uint32_t end = strtoul(endptr, &endptr, 10);
    uint32_t step = 1;
    if (*endptr == ':') {
        endptr++;
        step = strtoul(endptr, NULL, 10);
    }

    *task_count = ((end - start) / step) + 1;
    *tasks = xmalloc(sizeof(uint32_t) * (*task_count));
    for (int i = 0; i < *task_count; i++) {
        (*tasks)[i] = start + i * step;
    }

    xfree(range_copy);
}

/**
 * @brief Update a job's hold status.
 * @param job_msg Job descriptor with hold parameters.
 * @param job_id Job ID.
 * @param task_id Task ID for array jobs (0 for non-array).
 * @param array_job_id Array job ID (if applicable).
 * @return 0 on success, 1 on error.
 */
static int update_job(job_desc_msg_t *job_msg, uint32_t job_id, uint32_t task_id, uint32_t array_job_id) {
    job_array_resp_msg_t *resp = NULL;
    char job_id_str[MAX_JOB_ID_STR];

    if (task_id && array_job_id) {
        snprintf(job_id_str, sizeof(job_id_str), "%u_%u", array_job_id, task_id);
        job_msg->job_id_str = xstrdup(job_id_str);
    } else {
        job_msg->job_id = job_id;
    }

    int rc = slurm_update_job2(job_msg, &resp);
    if (rc != SLURM_SUCCESS) {
        fprintf(stderr, "%s for job %s\n", slurm_strerror(slurm_get_errno()),
                task_id ? job_id_str : job_msg->job_id_str);
        xfree(job_msg->job_id_str);
        slurm_free_job_array_resp(resp);
        return 1;
    }

    if (task_id && array_job_id) {
        printf("Modified hold of job-array task %u.%u\n", array_job_id, task_id);
    } else {
        printf("Modified hold of job %u\n", job_id);
    }

    xfree(job_msg->job_id_str);
    slurm_free_job_array_resp(resp);
    return 0;
}

/**
 * @brief Hold or release jobs based on parsed arguments.
 * @return 0 on success, 1 on error.
 */
static int handle_jobs(void) {
    if (!qargs->job_list && !qargs->user_list) {
        fprintf(stderr, "No job_task_list or user_list specified\n");
        return 1;
    }

    if (qargs->job_list && qargs->user_list) {
        fprintf(stderr, "Cannot combine job_task_list and user_list\n");
        return 1;
    }

    job_desc_msg_t job_msg;
    slurm_init_job_desc_msg(&job_msg);

    uid_t uid = getuid();
    struct passwd *pw = getpwuid(uid);

    /*
        qhold allows you to select the pending type and verify that the passed pending type is correct.
    */
    if (qargs->hold_type) {
        for (size_t i = 0; qargs->hold_type[i]; i++) {
            if (qargs->hold_type[i] == ',') continue;
            switch (qargs->hold_type[i]) {
                case 'u':
                    job_msg.alloc_sid = ALLOC_SID_USER_HOLD;
                    break;
                case 'o':
                    if (uid != 0) {
                        fprintf(stderr, "Operator hold requires root privileges (%s)\n", pw->pw_name);
                        return 1;
                    }
                    job_msg.alloc_sid = ALLOC_SID_ADMIN_HOLD;
                    break;
                case 's':
                    if (uid != 0) {
                        fprintf(stderr, "System hold requires root privileges (%s)\n", pw->pw_name);
                        return 1;
                    }
                    job_msg.alloc_sid = ALLOC_SID_ADMIN_HOLD;
                    break;
                default:
                    fprintf(stderr, "Invalid hold type: %c\n", qargs->hold_type[i]);
                    return 1;
            }
        }
    } else {
        job_msg.alloc_sid = uid == 0 ? ALLOC_SID_ADMIN_HOLD : ALLOC_SID_USER_HOLD;
    }

    /*
        hold the job, then set the priority to 0
        release the job, then set the priority to INFINITE
    */
    job_msg.priority = (program == QHOLD ? 0 : INFINITE);
    recover_alloc_sid = job_msg.alloc_sid;
    recover_priority = job_msg.priority;

    if (qargs->job_list) {
        char *input_copy = xstrdup(qargs->job_list);
        char *saveptr = NULL;
        char *token = strtok_r(input_copy, ",", &saveptr);

        while (token) {
            uint32_t job_id;
            uint32_t *tasks = NULL;
            int task_count = 0;
            parse_array_range(token, &job_id, &tasks, &task_count);

            job_info_msg_t *resp = NULL;
            if (slurm_load_job(&resp, job_id, SHOW_ALL) != SLURM_SUCCESS) {
                fprintf(stderr, "Job %u does not exist\n", job_id);
                xfree(tasks);
                token = strtok_r(NULL, ",", &saveptr);
                continue;
            }

            bool is_valid_user = false;
            for (int i = 0; i < valid_uid_count; i++) {
                if (resp->job_array[0].user_id == valid_uids[i]) {
                    is_valid_user = true;
                    break;
                }
            }

            if (valid_uid_count > 0 && !is_valid_user) {
                fprintf(stderr, "Job %u does not belong to specified users\n", job_id);
                slurm_free_job_info_msg(resp);
                xfree(tasks);
                xfree(input_copy);
                return 1;
            }

            bool is_array_job = task_count > 1 || tasks[0] != 0;
            if (resp->record_count > 1) {
                if (is_array_job) {
                    /* Whether the command line is specified as an array, such as 123.1-10:2 */
                    slurm_job_info_t *job_ptr = &resp->job_array[0];
                    for (int i = 0; i < task_count; i++) {
                        // if (program == QHOLD && job_ptr->priority == 0) continue;
                        // if (program == QRLS && job_ptr->priority != 0) continue;
                        init_job_msg(&job_msg);
                        if (update_job(&job_msg, 0, tasks[i], job_ptr->array_job_id)) {
                            slurm_free_job_info_msg(resp);
                            xfree(tasks);
                            xfree(input_copy);
                            return 1;
                        }
                    }
                } else {
                    for (uint32_t i = 0; i < resp->record_count; i++) {
                        slurm_job_info_t *job_ptr = &resp->job_array[i];
                        if (program == QHOLD && job_ptr->priority == 0) continue;
                        if (program == QRLS && job_ptr->priority != 0) continue;
                        init_job_msg(&job_msg);
                        if (job_ptr->array_job_id && job_ptr->array_task_str) {
                            if (update_job(&job_msg, 0, 0, job_ptr->array_job_id)) {
                                slurm_free_job_info_msg(resp);
                                xfree(tasks);
                                xfree(input_copy);
                                return 1;
                            }
                        } else if (job_ptr->array_job_id) {
                            if (update_job(&job_msg, job_ptr->job_id, job_ptr->array_task_id, job_ptr->array_job_id)) {
                                slurm_free_job_info_msg(resp);
                                xfree(tasks);
                                xfree(input_copy);
                                return 1;
                            }
                        }
                    }
                }
            } else {
                slurm_job_info_t *job_ptr = &resp->job_array[0];
                if (program == QHOLD && job_ptr->priority == 0) {
                    slurm_free_job_info_msg(resp);
                    xfree(tasks);
                    token = strtok_r(NULL, ",", &saveptr);
                    continue;
                }
                if (program == QRLS && job_ptr->priority != 0) {
                    slurm_free_job_info_msg(resp);
                    xfree(tasks);
                    token = strtok_r(NULL, ",", &saveptr);
                    continue;
                }
                init_job_msg(&job_msg);
                if (update_job(&job_msg, job_ptr->job_id, 0, 0)) {
                    slurm_free_job_info_msg(resp);
                    xfree(tasks);
                    xfree(input_copy);
                    return 1;
                }
            }

            slurm_free_job_info_msg(resp);
            xfree(tasks);
            token = strtok_r(NULL, ",", &saveptr);
        }
        xfree(input_copy);
    } else if (qargs->user_list) {
        for (int i = 0; i < valid_uid_count; i++) {
            uid_t uid = valid_uids[i];
            struct passwd *pw = getpwuid(uid);
            job_info_msg_t *job_info = NULL;

            if (slurm_load_job_user(&job_info, uid, SHOW_ALL) != SLURM_SUCCESS) {
                fprintf(stderr, "Failed to load jobs for user %s: %s\n",
                        pw->pw_name, slurm_strerror(slurm_get_errno()));
                continue;
            }

            if (job_info->record_count == 0) {
                printf("No jobs found for user %s\n", pw->pw_name);
                slurm_free_job_info_msg(job_info);
                continue;
            }

            for (uint32_t j = 0; j < job_info->record_count; j++) {
                slurm_job_info_t *job_ptr = &job_info->job_array[j];
                if (program == QHOLD && job_ptr->priority == 0) continue;
                if (program == QRLS && job_ptr->priority != 0) continue;
                init_job_msg(&job_msg);
                if (job_ptr->array_job_id && job_ptr->array_task_str) {
                    if (update_job(&job_msg, 0, 0, job_ptr->array_job_id)) {
                        slurm_free_job_info_msg(job_info);
                        return 1;
                    }
                } else {
                    if (update_job(&job_msg, job_ptr->job_id, job_ptr->array_task_id, job_ptr->array_job_id)) {
                        slurm_free_job_info_msg(job_info);
                        return 1;
                    }
                }
            }
            slurm_free_job_info_msg(job_info);
        }
    }

    return 0;
}

/**
 * @brief Add a command handler to the parser.
 * @param parser Command parser.
 * @param handler Command handler to add.
 */
static void add_handler(CommandParser *parser, CommandHandler handler) {
    if (!parser)
        return;
    parser->handlers = xrealloc(parser->handlers,
                               sizeof(CommandHandler) * (parser->handler_count + 1));
    parser->handlers[parser->handler_count++] = handler;
}

/**
 * @brief Free the command parser's resources.
 * @param parser Command parser to free.
 */
static void destroy_parser(CommandParser *parser) {
    if (!parser) 
        return;
    xfree(parser->handlers);
    xfree(parser);
}

/**
 * @brief Parse command-line arguments.
 * @param parser Command parser.
 * @param argc Argument count.
 * @param argv Argument vector.
 * @return 0 on success, 1 on error.
 */
static int parse_command(CommandParser *parser, int argc, char *argv[]) {
    if (!parser || !argv) {
        fprintf(stderr, "Invalid parser or arguments\n");
        return 1;
    }

    for (int i = 1; i < argc; i++) {
        bool command_found = false;
        for (int j = 0; j < parser->handler_count; j++) {
            if (strcmp(argv[i], parser->handlers[j].command)) continue;

            command_found = true;
            switch (parser->handlers[j].require_param) {
                case OPTION_TYPE_NONE:
                    if (parser->handlers[j].execute(NULL)) return 1;
                    break;
                case OPTION_TYPE_REQUIRED:
                    if (i + 1 >= argc || argv[i + 1][0] == '-') {
                        fprintf(stderr, "Command %s requires a parameter\n", argv[i]);
                        return 1;
                    }
                    if (parser->handlers[j].execute(argv[++i])) return 1;
                    break;
                case OPTION_TYPE_OPTIONAL:
                    if (i + 1 < argc && argv[i + 1][0] != '-') {
                        if (parser->handlers[j].execute(argv[++i])) return 1;
                    } else {
                        if (parser->handlers[j].execute(NULL)) return 1;
                    }
                    break;
                case OPTION_TYPE_MULTIPLE: {
                    char combined_params[MAX_PARAM_BUFFER] = {0};
                    int k = i + 1;
                    while (k < argc && argv[k][0] != '-') {
                        if (strlen(combined_params) > 0) strcat(combined_params, ",");
                        strcat(combined_params, argv[k]);
                        k++;
                    }
                    if (parser->handlers[j].execute(combined_params)) return 1;
                    i = k - 1;
                    break;
                }
            }
            break;
        }

        if (!command_found) {
            if (argv[i][0] == '-') {
                /* 
                    The reason for the error is that in sge, qhold actually points to qalter, and the error message starts with "qalter". 
                */
                fprintf(stderr, "qalter: invalid option: \"%s\"\n", argv[i]);
                return 1;
            } else {
                if (qargs->job_list) {
                    fprintf(stderr, "Multiple job_task_list specifications\n");
                    return 1;
                }
                char buffer[MAX_PARAM_BUFFER] = {0};
                for (int k = i; k < argc; k++) {
                    strcat(buffer, argv[k]);
                    if (k < argc - 1) 
                        strcat(buffer, ",");
                }
                qargs->job_list = xstrdup(buffer);
                break;
            }
        }
    }
    return 0;
}

/**
 * @brief Create a command parser.
 * @return Allocated CommandParser, caller must free with destroy_parser.
 */
static CommandParser *create_command_parser(void) {
    CommandParser *parser = xmalloc(sizeof(CommandParser));
    parser->handlers = NULL;
    parser->handler_count = 0;
    parser->add_handler = add_handler;
    parser->parse = parse_command;
    parser->destroy = destroy_parser;

    CommandHandler commands[] = {
        {"-h", OPTION_TYPE_REQUIRED, set_hold_type},
        {"-u", OPTION_TYPE_REQUIRED, set_user_list},
        {"-help", OPTION_TYPE_NONE, set_help},
        {"-version", OPTION_TYPE_NONE, set_version}
    };

    for (size_t i = 0; i < sizeof(commands) / sizeof(commands[0]); i++) {
        parser->add_handler(parser, commands[i]);
    }

    return parser;
}

/**
 * @brief   Determine the program name and set the global variable "program".
 * @param   progname Program name (argv[0])
 * @return  0 Indicates successful recognition, with 1 representing successful 
 *          recognition and 0 representing failure.
 */
static int determine_program_type(const char *progname) {
    if (!progname || !*progname) {
        fprintf(stderr, "Invalid parameter name\n");
        return 1;
    }

    if (strstr(progname, "qhold")) {
        program = QHOLD;
    } else if (strstr(progname, "qrls")) {
        program = QRLS;
    } else {
        fprintf(stderr, "Program must be named qhold or qrls\n");
        return 1;
    }

    return 0;
}

/**
 * @brief   Perform corresponding operations based on command-line 
 *          parameters: display help, version information or execute the main logic.
 * @return  Return the execution result of the main logic, along with the help 
 *          or version information, with a value of 0.
 */
static int handle_operation(void) {
    if (qargs->is_help) {
        display_help();
        return 0;
    } else if (qargs->is_version) {
        display_version();
        return 0;
    } else {
        return handle_jobs();
    }
}

/**
 * @brief Main entry point for qhold/qrls.
 * @param argc Argument count.
 * @param argv Argument vector.
 * @return 0 on success, 1 on error.
 */
int main(int argc, char *argv[]) {
    int rc = 0;
    if (determine_program_type(argv[0]) != 0) {
        return 1;
    }
    slurm_init(NULL);
    init_qargs();
    CommandParser *parser = create_command_parser();

    if (parser->parse(parser, argc, argv)) {
        parser->destroy(parser);
        free_qargs();
        return 1;
    }
    
    rc = handle_operation();

    parser->destroy(parser);
    free_qargs();
    return rc;
}