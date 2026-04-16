/**
 * @file qmod.c
 * @brief Slurm job control tool mimicking SGE qmod.
 *
 * This program provides commands to suspend and resume Slurm jobs, supporting both
 * single and array jobs. It parses job IDs or ranges, checks permissions, and
 * performs the requested operations, outputting results in a format compatible with
 * SGE qmod. Supported options include -s, -sj, -us, -usj, and -version.
 *
 * @author HE JIALE
 * @date 2025-04-11
 * @version 0.0.1
 */

#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <string.h>
#include <pwd.h>
#include <unistd.h>
#include <stdarg.h>

#include <slurm/slurm.h>
#include <slurm/slurm_errno.h>

#include "qmod.h"

/** Maximum size for parameter buffers. */
#define MAX_PARAM_SIZE 1024
/** Maximum number of array tasks to process. */
#define MAX_ARRAY_TASKS 1000

/** Static global pointer to command-line arguments. */
static qmod_args_t *qmod_args = NULL;

/* Forward declarations for static functions */
extern char *xstrdup_printf(const char *fmt, ...)
  __attribute__ ((format (printf, 1, 2)));
static void init_qmod_args(void);
static void free_qmod_args(void);
static void log_error(const char *format, ...);
static void log_info(const char *format, ...);
static bool check_user_permission(uid_t uid, const slurm_job_info_t *job_info);
static bool validate_job_state_for_suspend(const slurm_job_info_t *job_info);
static bool validate_job_state_for_resume(const slurm_job_info_t *job_info);
static void format_job_id_output(const char *username, const char *action,
                                const slurm_job_info_t *job_info, bool is_array);
static int parse_array_range(const char *range, uint32_t *job_id,
                            uint32_t **tasks, int *task_count);

static int suspend_job(const char *job_ids);
static int resume_job(const char *job_ids);
static int version_display(const char *unused);
static int help_display(const char *unused);

static void add_handler(CommandParser *parser, CommandHandler handler);
static int parse_command(CommandParser *parser, int argc, char *argv[]);
static void destroy_parser(CommandParser *parser);
static CommandParser *create_command_parser(void);
static void process_job_ids(const char *job_ids);

/**
 * @brief Initialize the global qmod_args_t structure.
 *
 * Allocates memory for qmod_args if not already initialized and sets default values.
 */
static void init_qmod_args(void)
{
    if (qmod_args) {
        return;
    }
    qmod_args = xmalloc(sizeof(qmod_args_t));
    qmod_args->job_ids = NULL;
    qmod_args->is_version = false;
    qmod_args->is_suspend = false;
    qmod_args->is_resume = false;
    qmod_args->is_help = false;
}

/**
 * @brief Free the global qmod_args_t structure.
 *
 * Releases all dynamically allocated memory within qmod_args and resets the pointer.
 */
static void free_qmod_args(void)
{
    if (!qmod_args) {
        return;
    }
    xfree(qmod_args->job_ids);
    xfree(qmod_args);
    qmod_args = NULL;
}

/**
 * @brief Log an error message to stdout.
 *
 * Formats and outputs an error message with a consistent prefix.
 *
 * @param format Format string for the error message.
 * @param ... Variable arguments for the format string.
 */
static void log_error(const char *format, ...)
{
    va_list args;
    va_start(args, format);
    // fprintf(stderr, "ERROR: ");
    vfprintf(stderr, format, args);
    fprintf(stderr, "\n");
    va_end(args);
}

/**
 * @brief Log an informational message to stdout.
 *
 * Formats and outputs an info message without a prefix.
 *
 * @param format Format string for the message.
 * @param ... Variable arguments for the format string.
 */
static void log_info(const char *format, ...)
{
    va_list args;
    va_start(args, format);
    vfprintf(stdout, format, args);
    fprintf(stdout, "\n");
    va_end(args);
}

/**
 * @brief Check if the user has permission to modify a job.
 *
 * Verifies if the calling user is root or the job owner.
 *
 * @param uid User ID of the caller.
 * @param job_info Slurm job information structure.
 * @return true if permission is granted, false otherwise.
 */
static bool check_user_permission(uid_t uid, const slurm_job_info_t *job_info)
{
    if (!job_info)
        return false;
    return (uid == 0 || uid == job_info->user_id);
}

/**
 * @brief Validate job state for suspension.
 *
 * Checks if the job is in a state suitable for suspension (running).
 *
 * @param job_info Slurm job information structure.
 * @return true if the job can be suspended, false otherwise.
 */
static bool validate_job_state_for_suspend(const slurm_job_info_t *job_info)
{
    if (!job_info)
        return false;
    if (IS_JOB_SUSPENDED(job_info)) {
        return false;
    }
    if (!IS_JOB_RUNNING(job_info)) {
        return false;
    }
    return true;
}

/**
 * @brief Validate job state for resumption.
 *
 * Checks if the job is in a state suitable for resumption (suspended).
 *
 * @param job_info Slurm job information structure.
 * @return true if the job can be resumed, false otherwise.
 */
static bool validate_job_state_for_resume(const slurm_job_info_t *job_info)
{
    if (!job_info)
        return false;
    if (IS_JOB_RUNNING(job_info)) {
        return false;
    }
    if (IS_JOB_PENDING(job_info)) {
        return false;
    }
    return true;
}

/**
 * @brief Format and print job ID output for operations.
 *
 * Outputs a formatted message indicating the action performed on a job.
 *
 * @param username Username of the caller.
 * @param action Action performed (e.g., "suspended", "unsuspended").
 * @param job_info Slurm job information structure.
 * @param is_array True if the job is an array task.
 */
static void format_job_id_output(const char *username, const char *action,
                                const slurm_job_info_t *job_info, bool is_array)
{
    if (!job_info)
        return;
    
    if (is_array && job_info->array_task_id != NO_VAL) {
        log_info("%s - %s job %u.%u", username, action,
                 job_info->array_job_id, job_info->array_task_id);
    } else {
        log_info("%s - %s job %u", username, action, job_info->job_id);
    }
}

/**
 * @brief Parse a job ID range for single or array jobs.
 *
 * Processes a job ID string (e.g., "180", "180.1-20:2") and generates a list
 * of task IDs for array jobs.
 *
 * @param range Input range string.
 * @param job_id Output pointer for the base job ID.
 * @param tasks Output pointer for the array of task IDs.
 * @param task_count Output pointer for the number of tasks.
 * @return 0 on success, 1 on error.
 */
static int parse_array_range(const char *range, uint32_t *job_id,
                            uint32_t **tasks, int *task_count)
{
    if (!range || !*range) {
        log_error("Invalid job ID range: empty string");
        return 1;
    }

    char *range_copy = xstrdup(range);
    if (!range_copy) {
        log_error("Memory allocation failed for range parsing");
        return 1;
    }

    char *task_range = strchr(range_copy, '.');
    if (!task_range) {
        /* Single job ID */
        char *endptr;
        *job_id = strtoul(range_copy, &endptr, 10);
        if (*endptr != '\0') {
            log_error("Invalid job ID format: %s", range_copy);
            xfree(range_copy);
            return 1;
        }
        *tasks = xmalloc(sizeof(uint32_t));
        (*tasks)[0] = 0; /* 0 indicates non-array job */
        *task_count = 1;
        xfree(range_copy);
        return 0;
    }

    /* Array job */
    *task_range = '\0';
    char *endptr;
    *job_id = strtoul(range_copy, &endptr, 10);
    if (*endptr != '\0') {
        log_error("Invalid base job ID: %s", range_copy);
        xfree(range_copy);
        return 1;
    }

    task_range++;
    uint32_t start = strtoul(task_range, &endptr, 10);
    if (*endptr == '\0') {
        /* Single task ID (e.g., "180.1") */
        *tasks = xmalloc(sizeof(uint32_t));
        (*tasks)[0] = start;
        *task_count = 1;
        xfree(range_copy);
        return 0;
    }

    if (*endptr != '-') {
        log_error("Invalid task range format: %s", task_range);
        xfree(range_copy);
        return 1;
    }

    endptr++;
    uint32_t end = strtoul(endptr, &endptr, 10);
    uint32_t step = 1;
    if (*endptr == ':') {
        endptr++;
        step = strtoul(endptr, &endptr, 10);
        if (step == 0) {
            log_error("Invalid step value in range: %s", task_range);
            xfree(range_copy);
            return 1;
        }
    }

    if (*endptr != '\0') {
        log_error("Trailing characters in task range: %s", task_range);
        xfree(range_copy);
        return 1;
    }

    if (end < start) {
        log_error("Invalid range: end (%u) less than start (%u)", end, start);
        xfree(range_copy);
        return 1;
    }

    *task_count = ((end - start) / step) + 1;
    if (*task_count > MAX_ARRAY_TASKS) {
        log_error("Task count exceeds limit: %d > %d", *task_count, MAX_ARRAY_TASKS);
        xfree(range_copy);
        return 1;
    }

    *tasks = xmalloc(sizeof(uint32_t) * (*task_count));
    for (int i = 0; i < *task_count; i++) {
        (*tasks)[i] = start + i * step;
    }

    xfree(range_copy);
    return 0;
}

/**
 * @brief Suspend specified Slurm jobs.
 *
 * Processes a list of job IDs and suspends eligible jobs, checking permissions
 * and job states.
 *
 * @param job_ids Comma-separated list of job IDs or ranges.
 * @return 0 on success, 1 on error.
 */
static int suspend_job(const char *job_ids)
{
    if (!job_ids || !*job_ids) {
        log_error("No job IDs provided for suspend operation");
        return 1;
    }

    init_qmod_args();
    qmod_args->is_suspend = true;
    qmod_args->job_ids = xstrdup(job_ids);
    process_job_ids(job_ids);
    return 0;
}

/**
 * @brief Resume specified Slurm jobs.
 *
 * Processes a list of job IDs and resumes eligible jobs, checking permissions
 * and job states.
 *
 * @param job_ids Comma-separated list of job IDs or ranges.
 * @return 0 on success, 1 on error.
 */
static int resume_job(const char *job_ids)
{
    if (!job_ids || !*job_ids) {
        log_error("No job IDs provided for resume operation");
        return 1;
    }

    init_qmod_args();
    qmod_args->is_resume = true;
    qmod_args->job_ids = xstrdup(job_ids);
    process_job_ids(job_ids);
    return 0;
}

/**
 * @brief Display version information for qmod.
 *
 * Outputs the program version, release date, and supported parameters.
 *
 * @param unused Unused parameter (NULL).
 * @return 0 on success.
 */
static int version_display(const char *unused)
{
    (void)unused; /* unused */
    log_info("METASTACK SGE WRAPPER");
    log_info("VERSION %s", PROGRAM_VERSION);
    log_info("%s", RELEASE_DATE);
    log_info("ADAPTED PARAMETERS: -us, -s, -usj, -sj, -version, -help");
    return 0;
}

static int help_display(const char *unused)
{
    (void)unused; /* unused */
    log_info("SGE 8.1.9");
    log_info("usage: qmod [options]");
    log_info("   [-c job_wc_queue_list]  clear error state");
    log_info("   [-cj job_list]          clear job error state");
    log_info("   [-cq wc_queue_list]     clear queue error state");
    log_info("   [-d wc_queue_list]      disable");
    log_info("   [-e wc_queue_list]      enable");
    log_info("   [-f]                    force action");
    log_info("   [-help]                 print this help");
    log_info("   [-r job_wc_queue_list]  reschedule jobs (running in queue)");
    log_info("   [-rj job_list]          reschedule jobs");
    log_info("   [-rq wc_queue_list]     reschedule all jobs in a queue");
    log_info("   [-s job_wc_queue_list]  suspend");
    log_info("   [-sj job_list]          suspend jobs");
    log_info("   [-sq wc_queue_list]     suspend queues");
    log_info("   [-us job_wc_queue_list] unsuspend");
    log_info("   [-usj job_list]         unsuspend jobs");
    log_info("   [-usq wc_queue_list]    unsuspend queues");
    return 0;
}

/**
 * @brief Add a command handler to the parser.
 *
 * Expands the handler array and stores the new handler.
 *
 * @param parser Command parser instance.
 * @param handler Command handler to add.
 */
static void add_handler(CommandParser *parser, CommandHandler handler)
{
    if (!parser) {
        log_error("Cannot add handler to NULL parser");
        return;
    }
    parser->handlers = xrealloc(parser->handlers,
                               sizeof(CommandHandler) * (parser->handler_count + 1));
    parser->handlers[parser->handler_count] = handler;
    parser->handler_count++;
}

/**
 * @brief Parse command-line arguments.
 *
 * Processes argv and invokes handlers for recognized commands.
 *
 * @param parser Command parser instance.
 * @param argc Argument count.
 * @param argv Argument vector.
 * @return 0 on success, 1 on error.
 */
static int parse_command(CommandParser *parser, int argc, char *argv[])
{
    if (!parser || !argv) {
        log_error("Invalid parser or arguments");
        return 1;
    }

    for (int i = 1; i < argc; i++) {
        bool command_found = false;
        for (int j = 0; j < parser->handler_count; j++) {
            if (strcmp(argv[i], parser->handlers[j].command) != 0) {
                continue;
            }
            command_found = true;

            if (parser->handlers[j].require_param == OPTION_TYPE_MULTIPLE) {
                char combined_params[MAX_PARAM_SIZE] = {0};
                int k = i + 1;
                while (k < argc && argv[k][0] != '-') {
                    if (strlen(combined_params) + strlen(argv[k]) + 2 > MAX_PARAM_SIZE) {
                        log_error("Parameter list too long for command %s", argv[i]);
                        return 1;
                    }
                    if (strlen(combined_params) > 0) {
                        strcat(combined_params, ",");
                    }
                    strcat(combined_params, argv[k]);
                    k++;
                }
                if (parser->handlers[j].execute(combined_params)) {
                    log_error("Failed to execute command %s", argv[i]);
                    return 1;
                }
                i = k - 1;
            } else if (parser->handlers[j].require_param == OPTION_TYPE_NONE) {
                if (parser->handlers[j].execute(NULL)) {
                    log_error("Failed to execute command %s", argv[i]);
                    return 1;
                }
            } else {
                log_error("Unsupported option type for command %s", argv[i]);
                return 1;
            }
            break;
        }
        if (!command_found) {
            help_display(NULL);
            log_info("Invalid option argument \"%s\"", argv[i]);
            return 1;
        }
    }
    return 0;
}

/**
 * @brief Destroy the command parser.
 *
 * Frees all allocated memory associated with the parser.
 *
 * @param parser Command parser instance.
 */
static void destroy_parser(CommandParser *parser)
{
    if (!parser) {
        return;
    }
    xfree(parser->handlers);
    xfree(parser);
}

/**
 * @brief Create a command parser instance.
 *
 * Initializes a parser and registers supported commands.
 *
 * @return Pointer to the created parser, or NULL on failure.
 */
static CommandParser *create_command_parser(void)
{
    CommandParser *parser = xmalloc(sizeof(CommandParser));
    if (!parser) {
        log_error("Failed to allocate memory for command parser");
        return NULL;
    }
    parser->handlers = NULL;
    parser->handler_count = 0;
    parser->add_handler = add_handler;
    parser->parse = parse_command;
    parser->destroy = destroy_parser;

    CommandHandler commands[] = {
        {"-s", OPTION_TYPE_MULTIPLE, suspend_job},
        {"-sj", OPTION_TYPE_MULTIPLE, suspend_job},
        {"-us", OPTION_TYPE_MULTIPLE, resume_job},
        {"-usj", OPTION_TYPE_MULTIPLE, resume_job},
        {"-version", OPTION_TYPE_NONE, version_display},
        {"-help", OPTION_TYPE_NONE, help_display}
    };

    for (size_t i = 0; i < sizeof(commands) / sizeof(commands[0]); i++) {
        parser->add_handler(parser, commands[i]);
    }

    return parser;
}

/**
 * @brief Process a list of job IDs for suspension or resumption.
 *
 * Parses job IDs, queries job information, and performs the requested operation.
 *
 * @param job_ids Comma-separated list of job IDs or ranges.
 */
static void process_job_ids(const char *job_ids)
{
    uid_t uid = getuid();
    struct passwd *pw = getpwuid(uid);
    if (!pw) {
        log_error("Failed to get user info for UID %u", uid);
        return;
    }

    char *input_copy = xstrdup(job_ids);
    if (!input_copy) {
        log_error("Memory allocation failed for job IDs");
        return;
    }

    char *saveptr;
    char *token = strtok_r(input_copy, ",", &saveptr);
    while (token) {
        uint32_t job_id;
        uint32_t *tasks;
        int task_count;

        if (parse_array_range(token, &job_id, &tasks, &task_count)) {
            token = strtok_r(NULL, ",", &saveptr);
            continue;
        }

        for (int t = 0; t < task_count; t++) {
            job_info_msg_t *job_info = NULL;
            int rc = slurm_load_job(&job_info, job_id, SHOW_ALL);
            if (rc != SLURM_SUCCESS || !job_info) {
                log_error("%s - No permission or invalid job ID %u: %s",
                          pw->pw_name, job_id, slurm_strerror(slurm_get_errno()));
                continue;
            }

            for (uint32_t i = 0; i < job_info->record_count; i++) {
                slurm_job_info_t *job_ptr = &job_info->job_array[i];
                bool is_array_job = (tasks[t] != 0);

                if (is_array_job && job_ptr->array_task_id != tasks[t]) {
                    continue;
                }

                if (!check_user_permission(uid, job_ptr)) {
                    format_job_id_output(pw->pw_name,
                                        qmod_args->is_suspend ? "No permission to suspend" :
                                        "No permission to resume",
                                        job_ptr, is_array_job);
                    continue;
                }

                if (qmod_args->is_suspend) {
                    if (!validate_job_state_for_suspend(job_ptr)) {
                        if (IS_JOB_SUSPENDED(job_ptr)) {
                            format_job_id_output(pw->pw_name, "job is already suspended",
                                                 job_ptr, is_array_job);
                        } else {
                            log_info("Modify operation cannot be applied on job%s%s in pending/hold state",
                                     is_array_job ? "-array task " : " ",
                                     is_array_job ? xstrdup_printf("%u.%u", job_ptr->array_job_id, job_ptr->array_task_id) :
                                     xstrdup_printf("%u", job_ptr->job_id));
                        }
                        continue;
                    }

                    int rc2 = slurm_suspend(job_ptr->job_id);
                    if (rc2 == SLURM_SUCCESS) {
                        format_job_id_output(pw->pw_name, "suspended", job_ptr, is_array_job);
                    } else {
                        log_error("Failed to suspend job%s%s: %s",
                                  is_array_job ? "-array task " : " ",
                                  is_array_job ? xstrdup_printf("%u.%u", job_ptr->array_job_id, job_ptr->array_task_id) :
                                  xstrdup_printf("%u", job_ptr->job_id),
                                  slurm_strerror(rc2));
                    }
                } else if (qmod_args->is_resume) {
                    if (!validate_job_state_for_resume(job_ptr)) {
                        if (IS_JOB_RUNNING(job_ptr)) {
                            format_job_id_output(pw->pw_name, "job is already unsuspended",
                                                 job_ptr, is_array_job);
                        } else {
                            log_info("Modify operation cannot be applied on job%s%s in pending/hold state",
                                     is_array_job ? "-array task " : " ",
                                     is_array_job ? xstrdup_printf("%u.%u", job_ptr->array_job_id, job_ptr->array_task_id) :
                                     xstrdup_printf("%u", job_ptr->job_id));
                        }
                        continue;
                    }

                    int rc2 = slurm_resume(job_ptr->job_id);
                    if (rc2 == SLURM_SUCCESS) {
                        format_job_id_output(pw->pw_name, "unsuspended", job_ptr, is_array_job);
                    } else {
                        log_error("Failed to resume job%s%s: %s",
                                  is_array_job ? "-array task " : " ",
                                  is_array_job ? xstrdup_printf("%u.%u", job_ptr->array_job_id, job_ptr->array_task_id) :
                                  xstrdup_printf("%u", job_ptr->job_id),
                                  slurm_strerror(rc2));
                    }
                }
            }
            slurm_free_job_info_msg(job_info);
        }
        xfree(tasks);
        token = strtok_r(NULL, ",", &saveptr);
    }
    xfree(input_copy);
}

/**
 * @brief Main entry point for the qmod program.
 *
 * Parses command-line arguments, executes job control operations, and cleans up resources.
 *
 * @param argc Argument count.
 * @param argv Argument vector.
 * @return 0 on success, 1 on error.
 */
int main(int argc, char *argv[])
{
    init_qmod_args();
    slurm_init(NULL);
    CommandParser *parser = create_command_parser();
    if (!parser) {
        free_qmod_args();
        return 1;
    }

    int rc = parse_command(parser, argc, argv);
    parser->destroy(parser);
    free_qmod_args();
    return rc;
}