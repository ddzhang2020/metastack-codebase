/**
 * @file qacct.c
 * @brief SGE-compatible job accounting tool for Slurm.
 *
 * This program retrieves and displays job accounting information from the Slurm
 * database, mimicking the behavior of SGE's qacct command. It supports filtering
 * by job ID, user, start time, and end time, and provides detailed job statistics
 * as well as user and system summaries.
 *
 * @author [HE JIALE]
 * @date 2025-03-20
 * @version 0.0.1
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <pwd.h>
#include <grp.h>
#include <stdarg.h>

#include <slurm/slurm.h>
#include <slurm/slurm_errno.h>
#include <slurm/slurmdb.h>

#include "qacct.h"

/** Maximum buffer size for string formatting. */
#define MAX_BUFFER_SIZE 256
/** Maximum length for time strings. */
#define TIME_BUFFER_SIZE 32

/** Static global pointer to command-line arguments. */
static Qacct_args *qargs = NULL;

/* Forward declarations for static functions */
static void init_qargs(void);
static void free_qargs(void);
static int set_job_args(const char *params);
static int set_user_args(const char *params);
static int set_start_args(const char *params);
static int set_end_args(const char *params);
static int set_help_args(const char *params);
static int set_version_args(const char *params);
static CommandParser *create_command_parser(void);
static const char *get_signal_description(int signal_num);
static void log_exit_status(int signal_num);
static void add_handler(CommandParser *parser, CommandHandler handler);
static int parse_command(CommandParser *parser, int argc, char *argv[]);
static void destroy_parser(CommandParser *parser);
static void free_user_summary(void *ptr);
static user_summary_t *update_user_summary(List summary_list, const char *owner,
                                          long wallclock, double utime, double stime,
                                          double cpu, double memory, double io,
                                          double iow);
static char *format_time(time_t t, char *buf, size_t len);
static char *format_memory(double bytes, char *buf, size_t len);
static int sort_desc_submit_time(void *x, void *y);
static void preprocess_jobs(List jobs);
static uint32_t parse_sge_time(const char *value);
static int fill_job_cond_from_qsub_args(slurmdb_job_cond_t *job_cond);
static void display_help_info(void);
static void display_version_info(void);
static List fetch_jobs(void);
static void print_job_details(slurmdb_job_rec_t *job, List summary_list);
static void print_user_summary(List summary_list);
static void print_system_summary(List summary_list);
static bool handle_help_or_version(CommandParser *parser);
static List get_and_process_jobs(void);
static void process_job_records(List jobs, List summary_list);
static void display_results(List summary_list);
static void cleanup(CommandParser *parser, List jobs, List summary_list);
static void log_error(const char *format, ...);
static void log_info(const char *format, ...);

static int _get_int(const char *my_str)
{
	char *end = NULL;
	int value;

	if (!my_str)
		return -1;
	value = strtol(my_str, &end, 10);
	//info("from %s I get %d and %s: %m", my_str, value, end);
	/* means no numbers */
	if (my_str == end)
		return -1;

	return value;
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
    // fprintf(stdout, "ERROR: ");
    vfprintf(stdout, format, args);
    fprintf(stdout, "\n");
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
 * @brief Initialize the global Qacct_args structure.
 *
 * Allocates memory for qargs if not already initialized and sets default values.
 */
static void init_qargs(void)
{
    if (qargs) {
        return;
    }
    qargs = xmalloc(sizeof(Qacct_args));
}

/**
 * @brief Free the global Qacct_args structure.
 *
 * Releases all dynamically allocated memory within qargs and resets the pointer.
 */
static void free_qargs(void)
{
    if (!qargs) {
        return;
    }
    xfree(qargs->job_list.value);
    xfree(qargs->user_list.value);
    xfree(qargs->start_time.value);
    xfree(qargs->end_time.value);
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
    if (qargs->job_list.is_set) {
        return 0;
    }
    qargs->job_list.is_set = true;
    
    if (params) {
        int job_number = _get_int(params); 
        if (job_number == -1) {
            qargs->job_list.value = NULL;
            qargs->job_list.is_set = false;
        } else {
            char str[20] = {0};
            sprintf(str, "%d", job_number);
            qargs->job_list.value = xstrdup(str);
        }
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
 * @brief Set start time filter from command-line parameters.
 *
 * @param params Time string in SGE format ([[CC]YYMMDDhhmm[.SS]).
 * @return 0 on success, 1 on error.
 */
static int set_start_args(const char *params)
{
    if (qargs->start_time.is_set) {
        return 0;
    }
    qargs->start_time.is_set = true;
    if (params) {
        qargs->start_time.value = xstrdup(params);
    }
    return 0;
}

/**
 * @brief Set end time filter from command-line parameters.
 *
 * @param params Time string in SGE format ([[CC]YYMMDDhhmm[.SS]).
 * @return 0 on success, 1 on error.
 */
static int set_end_args(const char *params)
{
    if (qargs->end_time.is_set) {
        return 0;
    }
    qargs->end_time.is_set = true;
    if (params) {
        qargs->end_time.value = xstrdup(params);
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
    qargs->is_help.is_set = true;
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
    qargs->is_version.is_set = true;
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
        {"-j", OPTION_TYPE_OPTIONAL, set_job_args},
        {"-o", OPTION_TYPE_OPTIONAL, set_user_args},
        {"-u", OPTION_TYPE_OPTIONAL, set_user_args},
        {"-b", OPTION_TYPE_REQUIRED, set_start_args},
        {"-e", OPTION_TYPE_REQUIRED, set_end_args},
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
static void add_handler(CommandParser *parser, CommandHandler handler)
{
    if (!parser)
        return;
    parser->handlers = xrealloc(parser->handlers,
                               sizeof(CommandHandler) * (parser->handler_count + 1));
    parser->handlers[parser->handler_count] = handler;
    parser->handler_count++;
}

/**
 * @brief Get the signal description for a given signal number.
 *
 * @param signal_num The signal number.
 * @return const char* The signal description string, or NULL if invalid.
 */
static const char *get_signal_description(int signal_num) {
    switch(signal_num) {
        case 1: return "Hangup";
        case 2: return "Interrupt";
        case 3: return "Quit";
        case 4: return "Illegal instruction";
        case 5: return "Trace/breakpoint trap";
        case 6: return "Aborted";
        case 7: return "Bus error";
        case 8: return "Floating point exception";
        case 9: return "Killed";
        case 10: return "User defined signal 1";
        case 11: return "Segmentation fault";
        case 12: return "User defined signal 2";
        case 13: return "Broken pipe";
        case 14: return "Alarm clock";
        case 15: return "Terminated"; // "Terminated"
        case 16: return "Stack fault";
        case 17: return "Child exited";
        case 18: return "Continued";
        case 19: return "Stopped (signal)";
        case 20: return "Stopped";
        case 21: return "Stopped (tty input)";
        case 22: return "Stopped (tty output)";
        case 23: return "Urgent I/O condition";
        case 24: return "CPU time limit exceeded";
        case 25: return "File size limit exceeded";
        case 26: return "Virtual timer expired";
        case 27: return "Profiling timer expired";
        case 28: return "Window changed";
        case 29: return "I/O possible";
        case 30: return "Power failure";
        case 31: return "Bad system call";
        default: 
            if (signal_num > 0 && signal_num <= 64) {
                return "Unknown signal";
            } else {
                return NULL;
            }
    }
}

/**
 * @brief Log the exit status in SGE accounting format.
 *
 * @param signal_num The signal number (already subtracted 128).
 */
static void log_exit_status(int signal_num) {

    if (signal_num > 0 && signal_num <= 64) {
        if (signal_num == 15) { /* In SGE, cancelling a job is done with SIGKILL, while in Slurm, it is done with SIGTERM. */ 
            signal_num = 9;
        }
        int sge_exit_status = 128 + signal_num;
        const char *signal_desc = get_signal_description(signal_num);
        if (signal_desc) {
            log_info("%-13s%-20d (%s)", "exit_status", sge_exit_status, signal_desc);
        } else {
            log_info("%-13s%-20d", "exit_status", sge_exit_status);
        }
    } else {
        log_info("%-13s%-20d", "exit_status", signal_num);
    }
}

/**
 * @brief Parse command-line arguments.
 *
 * @param parser The command parser.
 * @param argc Argument count.
 * @param argv Argument vector.
 * @return 0 on success, 1 on error.
 */
static int parse_command(CommandParser *parser, int argc, char *argv[])
{
    if (!parser || !argv) {
        log_error("Invalid parser or arguments.");
        return 1;
    }

    for (int i = 1; i < argc; i++) {
        int command_found = 0;
        for (int j = 0; j < parser->handler_count; j++) {
            if (strcmp(argv[i], parser->handlers[j].command) != 0) {
                continue;
            }
            command_found = 1;
            if (parser->handlers[j].require_param == OPTION_TYPE_REQUIRED) {
                if (i + 1 >= argc) {
                    log_error("Command %s requires a parameter.", argv[i]);
                    return 1;
                }
                if (parser->handlers[j].execute(argv[i + 1])) {
                    return 1;
                }
                i++;
            } else if (parser->handlers[j].require_param == OPTION_TYPE_OPTIONAL) {
                if (i + 1 < argc && argv[i + 1][0] != '-') {
                    if (parser->handlers[j].execute(argv[i + 1])) {
                        return 1;
                    }
                    i++;
                } else {
                    if (parser->handlers[j].execute(NULL)) {
                        return 1;
                    }
                }
            } else {
                if (parser->handlers[j].execute(NULL)) {
                    return 1;
                }
            }
            break;
        }
        if (!command_found) {
            log_error("Invalid option: \"%s\".", argv[i]);
            display_help_info();
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
static void destroy_parser(CommandParser *parser)
{
    if (!parser) {
        return;
    }
    xfree(parser->handlers);
    xfree(parser);
}

/**
 * @brief Free a user summary structure.
 *
 * @param ptr Pointer to the user_summary_t structure.
 */
static void free_user_summary(void *ptr)
{
    user_summary_t *summary = (user_summary_t *)ptr;
    if (summary) {
        xfree(summary->owner);
        xfree(summary);
    }
}

/**
 * @brief Update or create a user summary.
 *
 * Aggregates job statistics for a given user, updating an existing summary or
 * creating a new one.
 *
 * @param summary_list List of user summaries.
 * @param owner Username.
 * @param wallclock Wallclock time in seconds.
 * @param utime User CPU time in seconds.
 * @param stime System CPU time in seconds.
 * @param cpu Total CPU time in seconds.
 * @param memory Memory usage in GB-seconds.
 * @param io I/O usage in GB.
 * @param iow I/O wait time in seconds.
 * @return Pointer to the updated or new summary.
 */
static user_summary_t *update_user_summary(List summary_list, const char *owner,
                                          long wallclock, double utime, double stime,
                                          double cpu, double memory, double io,
                                          double iow)
{
    user_summary_t *summary = NULL;
    list_itr_t *iterator = list_iterator_create(summary_list);
    while ((summary = list_next(iterator))) {
        if (strcmp(summary->owner, owner) == 0) {
            summary->wallclock += wallclock;
            summary->utime += utime;
            summary->stime += stime;
            summary->cpu += cpu;
            summary->memory += memory;
            summary->io += io;
            summary->iow += iow;
            list_iterator_destroy(iterator);
            return summary;
        }
    }
    list_iterator_destroy(iterator);

    summary = xmalloc(sizeof(user_summary_t));
    summary->owner = xstrdup(owner);
    summary->wallclock = wallclock;
    summary->utime = utime;
    summary->stime = stime;
    summary->cpu = cpu;
    summary->memory = memory;
    summary->io = io;
    summary->iow = iow;
    list_append(summary_list, summary);
    return summary;
}

/**
 * @brief Format a time value into a string.
 *
 * Converts a Unix timestamp to a human-readable string, mimicking SGE's ctime format.
 *
 * @param t Unix timestamp.
 * @param buf Output buffer.
 * @param len Buffer length.
 * @return Pointer to the formatted string.
 */
static char *format_time(time_t t, char *buf, size_t len)
{
    if (t == (time_t) NO_VAL) {
        snprintf(buf, len, "None");  // 返回 "None" 如果 t == NO_VAL
        return buf;
    }

    if (t <= 0) {
        snprintf(buf, len, "-/-");
        return buf;
    }

    char *time_str = ctime(&t);
    if (!time_str) {
        snprintf(buf, len, "-/-");
        return buf;
    }

    snprintf(buf, len, "%s", time_str);
    char *nl = strchr(buf, '\n');
    if (nl) {
        *nl = '\0';
    }
    return buf;
}

/**
 * @brief Format a memory value into a string.
 *
 * Converts bytes to a human-readable format (B, KB, MB, GB, TB).
 *
 * @param bytes Memory size in bytes.
 * @param buf Output buffer.
 * @param len Buffer length.
 * @return Pointer to the formatted string.
 */
static char *format_memory(double bytes, char *buf, size_t len)
{
    if (bytes <= 0) {
        snprintf(buf, len, "0.000B");
    } else if (bytes < 1024.0) {
        snprintf(buf, len, "%.3fB", bytes);
    } else if (bytes < 1024.0 * 1024.0) {
        snprintf(buf, len, "%.3fKB", bytes / 1024.0);
    } else if (bytes < 1024.0 * 1024.0 * 1024.0) {
        snprintf(buf, len, "%.3fMB", bytes / (1024.0 * 1024.0));
    } else if (bytes < 1024.0 * 1024.0 * 1024.0 * 1024.0) {
        snprintf(buf, len, "%.3fGB", bytes / (1024.0 * 1024.0 * 1024.0));
    } else {
        snprintf(buf, len, "%.3fTB", bytes / (1024.0 * 1024.0 * 1024.0 * 1024.0));
    }
    return buf;
}

/**
 * @brief Sort jobs by submission time (descending).
 *
 * @param x First job record.
 * @param y Second job record.
 * @return -1 if x < y, 1 if x > y, 0 if equal.
 */
static int sort_desc_submit_time(void *x, void *y)
{
    slurmdb_job_rec_t *j1 = *(slurmdb_job_rec_t **)x;
    slurmdb_job_rec_t *j2 = *(slurmdb_job_rec_t **)y;

    if (j1->submit < j2->submit) {
        return -1;
    }
    if (j1->submit > j2->submit) {
        return 1;
    }
    if (j1->array_job_id < j2->array_job_id) {
        return -1;
    }
    if (j1->array_job_id > j2->array_job_id) {
        return 1;
    }
    if (j1->array_task_id < j2->array_task_id) {
        return -1;
    }
    if (j1->array_task_id > j2->array_task_id) {
        return 1;
    }
    if (j1->jobid < j2->jobid) {
        return -1;
    }
    if (j1->jobid > j2->jobid) {
        return 1;
    }
    return 0;
}

void print_job_errors(const char *job_list) {
    if (job_list == NULL || *job_list == '\0') {
        return;
    }

    char *copy = xstrdup(job_list);

    char *token = strtok(copy, ",");
    while (token != NULL) {
        char *start = token;
        char *end = token + strlen(token) - 1;
        while (*start == ' ' && start <= end) start++;
        while (*end == ' ' && end >= start) end--;
        *(end + 1) = '\0';

        if (*start != '\0') {
            log_error("error: job id %s not found", start);
        }

        token = strtok(NULL, ",");
    }

    xfree(copy);
}


/**
 * @brief Preprocess job records to aggregate step statistics.
 *
 * Computes total CPU usage for each job based on its steps.
 *
 * @param jobs List of job records.
 */
static void preprocess_jobs(List jobs)
{
    if (!jobs) {
        return;
    }

    if (jobs == NULL || list_count(jobs) == 0) {

        if (qargs->user_list.is_set) {
            print_user_summary(NULL);
            return;
        }
        
        if (qargs->job_list.is_set) {
            if (qargs->job_list.value != NULL && qargs->job_list.value[0] != '\0') {
                print_job_errors(qargs->job_list.value);
                return;
            } else {
                print_system_summary(NULL);
                return;
            }
        }
    }

    list_sort(jobs, sort_desc_submit_time);
    list_itr_t *iterator = list_iterator_create(jobs);
    slurmdb_job_rec_t *job;

    while ((job = list_next(iterator))) {
        if (!job->steps || !list_count(job->steps)) {
            continue;
        }
        list_itr_t *step_iterator = list_iterator_create(job->steps);
        slurmdb_step_rec_t *step;

        while ((step = list_next(step_iterator))) {
            if (step->state < JOB_COMPLETE) {
                continue;
            }
            job->tot_cpu_sec += step->tot_cpu_sec;
            job->tot_cpu_usec += step->tot_cpu_usec;
            job->user_cpu_sec += step->user_cpu_sec;
            job->user_cpu_usec += step->user_cpu_usec;
            job->sys_cpu_sec += step->sys_cpu_sec;
            job->sys_cpu_usec += step->sys_cpu_usec;
        }
        list_iterator_destroy(step_iterator);
    }
    list_iterator_destroy(iterator);
}

/**
 * @brief Parse SGE-style time format to Unix timestamp.
 *
 * Supports formats like [[CC]YYMMDDhhmm[.SS].
 *
 * @param value Time string.
 * @return Unix timestamp, or 0 on error.
 */
static uint32_t parse_sge_time(const char *value)
{
    if (!value || value[0] == '\0') {
        snprintf(SGE_EVENT, SFNMAX, MSG_PARSE_NODATE);
        fprintf(stderr, "\n%s\n", SGE_EVENT);
        return 0;
    }

    // Assume maximum length is 32, similar to stringT type
    if (strlen(value) > 32) {
        snprintf(SGE_EVENT, SFNMAX, MSG_PARSE_STARTTIMETOOLONG);
        fprintf(stderr, "\n%s\n", SGE_EVENT);
        return 0;
    }

    struct tm timeinfo = {0};
    time_t now = time(NULL);
    struct tm *tmp_timeptr = localtime(&now);

    char *dot_pos = strchr(value, '.');
    int seconds = 0;
    char non_seconds_buf[13] = {0};
    char *non_seconds = non_seconds_buf; // Use pointer for string manipulation

    // Split seconds part if present
    if (dot_pos) {
        if (strchr(dot_pos + 1, '.')) {
            snprintf(SGE_EVENT, SFNMAX, MSG_PARSE_INVALIDSECONDS);
            fprintf(stderr, "\n%s\n", SGE_EVENT);
            return 0;
        }
        if (strlen(dot_pos + 1) != 2) {
            snprintf(SGE_EVENT, SFNMAX, MSG_PARSE_INVALIDSECONDS);
            fprintf(stderr, "\n%s\n", SGE_EVENT);
            return 0;
        }
        seconds = atoi(dot_pos + 1);
        if (seconds < 0 || seconds > 59) {
            snprintf(SGE_EVENT, SFNMAX, MSG_PARSE_INVALIDSECOND);
            fprintf(stderr, "\n%s\n", SGE_EVENT);
            return 0;
        }
        strncpy(non_seconds, value, dot_pos - value);
        non_seconds[dot_pos - value] = '\0'; // Ensure null-terminated string
    } else {
        strcpy(non_seconds, value);
    }

    // Validate non-seconds part length
    size_t len = strlen(non_seconds);
    if (len != 8 && len != 10 && len != 12) {
        snprintf(SGE_EVENT, SFNMAX, MSG_PARSE_INVALIDHOURMIN);
        fprintf(stderr, "\n%s\n", SGE_EVENT);
        return 0;
    }

    int year_fieldlen = (len == 12) ? 4 : 2;
    char temp[5] = {0};

    // Parse year
    if (len >= 10) {
        strncpy(temp, non_seconds, year_fieldlen);
        timeinfo.tm_year = atoi(temp);
        if (len == 12) {
            timeinfo.tm_year -= 1900;
        } else {
            if (timeinfo.tm_year < 70) {
                timeinfo.tm_year += 100; // Assume 20XX for two-digit years < 70
            }
        }
        non_seconds += year_fieldlen; // Move pointer forward
    } else {
        timeinfo.tm_year = tmp_timeptr->tm_year; // Use current year if not provided
    }

    // Parse month
    strncpy(temp, non_seconds, 2);
    temp[2] = '\0';
    timeinfo.tm_mon = atoi(temp) - 1;
    if (timeinfo.tm_mon < 0 || timeinfo.tm_mon > 11) {
        snprintf(SGE_EVENT, SFNMAX, MSG_PARSE_INVALIDMONTH);
        fprintf(stderr, "\n%s\n", SGE_EVENT);
        return 0;
    }
    non_seconds += 2;

    // Parse day
    strncpy(temp, non_seconds, 2);
    temp[2] = '\0';
    timeinfo.tm_mday = atoi(temp);
    if (timeinfo.tm_mday < 1 || timeinfo.tm_mday > 31) {
        snprintf(SGE_EVENT, SFNMAX, MSG_PARSE_INVALIDDAY);
        fprintf(stderr, "\n%s\n", SGE_EVENT);
        return 0;
    }
    non_seconds += 2;

    // Parse hour
    strncpy(temp, non_seconds, 2);
    temp[2] = '\0';
    timeinfo.tm_hour = atoi(temp);
    if (timeinfo.tm_hour < 0 || timeinfo.tm_hour > 23) {
        snprintf(SGE_EVENT, SFNMAX, MSG_PARSE_INVALIDHOUR);
        fprintf(stderr, "\n%s\n", SGE_EVENT);
        return 0;
    }
    non_seconds += 2;

    // Parse minute
    strncpy(temp, non_seconds, 2);
    temp[2] = '\0';
    timeinfo.tm_min = atoi(temp);
    if (timeinfo.tm_min < 0 || timeinfo.tm_min > 59) {
        snprintf(SGE_EVENT, SFNMAX, MSG_PARSE_INVALIDMINUTE);
        fprintf(stderr, "\n%s\n", SGE_EVENT);
        return 0;
    }

    // Set seconds
    timeinfo.tm_sec = seconds;

    // Let system determine DST
    timeinfo.tm_isdst = -1;

    // Convert to time_t
    time_t gmt_secs = mktime(&timeinfo);
    if (gmt_secs < 0) {
        snprintf(SGE_EVENT, SFNMAX, MSG_PARSE_NODATEFROMINPUT);
        fprintf(stderr, "\n%s\n", SGE_EVENT);
        return 0;
    }

    return (uint32_t)gmt_secs;
}

/**
 * @brief Fill Slurm job condition structure from command-line arguments.
 *
 * @param job_cond Job condition structure to fill.
 * @return 0 on success, 1 on error.
 */
static int fill_job_cond_from_qsub_args(slurmdb_job_cond_t *job_cond)
{
    if (!job_cond) {
        log_error("Invalid job condition structure.");
        return 1;
    }

    job_cond->db_flags = SLURMDB_JOB_FLAG_NOTSET;
    job_cond->flags |= JOBCOND_FLAG_NO_TRUNC;

    if (qargs->job_list.is_set) {      
        if (!job_cond->step_list) {
            job_cond->step_list = list_create(slurm_destroy_selected_step);
        }
        if (qargs->job_list.value && qargs->job_list.value[0] != '\0') {
            slurm_addto_step_list(job_cond->step_list, qargs->job_list.value);
        }
    }

    if (!job_cond->userid_list) {
        job_cond->userid_list = list_create(xfree_ptr);
    }

    if (qargs->user_list.is_set) {
        
        uid_t uid = getuid();
        struct passwd *pw = getpwuid(uid);
        
        // Determine whether the username is valid
        if (qargs->user_list.value != NULL && uid == 0) {
            struct passwd *pw = getpwnam(qargs->user_list.value);
            if (pw == NULL) {
                print_user_summary(NULL);
                return 1;
            } else {
                slurm_addto_id_char_list(job_cond->userid_list, qargs->user_list.value, 0);
            }
        } else if (uid != 0) {
            slurm_addto_id_char_list(job_cond->userid_list, pw->pw_name, 0);
        }
    } else {
        uid_t uid = getuid();
        if (uid != 0) {
            struct passwd *pw = getpwuid(uid);
            slurm_addto_id_char_list(job_cond->userid_list, pw->pw_name, 0);
        }
    }

    if (qargs->start_time.is_set) {
        job_cond->usage_start = parse_sge_time(qargs->start_time.value);
        if (job_cond->usage_start == 0) {
            display_help_info();
            return 1;
        }
    }

    if (qargs->end_time.is_set) {
        job_cond->usage_end = parse_sge_time(qargs->end_time.value);
        if (job_cond->usage_end == 0) {
            display_help_info();
            return 1;
        }
    }

    return 0;
}

/**
 * @brief Display help information.
 */
static void display_help_info(void)
{
    log_info("SGE 8.1.9");
    log_info("Usage: qacct [options]");
    log_info("  -b begin_time        Jobs started after begin_time ([[CC]YYMMDDhhmm[.SS])");
    log_info("  -e end_time          Jobs ended before end_time ([[CC]YYMMDDhhmm[.SS])");
    log_info("  -j [job_id]          List details for specific jobs");
    log_info("  -o owner             List jobs for a specific owner");
    log_info("  -u owner             Same as -o");
    log_info("  -help                Display this message");
    log_info("  -version             Display version information");
}

/**
 * @brief Display version information.
 */
static void display_version_info(void)
{
    log_info("METASTACK SGE WRAPPER");
    log_info("Version: %s", PROGRAM_VERSION);
    log_info("Release Date: %s", RELEASE_DATE);
    log_info("Supported Parameters: -b, -e, -j, -o, -u, -help, -version");
}

/**
 * @brief Fetch jobs from the Slurm database based on command-line filters.
 *
 * @return List of job records, or NULL on failure.
 */
static List fetch_jobs(void)
{
    slurmdb_job_cond_t *job_cond = xmalloc(sizeof(slurmdb_job_cond_t));
    void *db_conn = slurmdb_connection_get(NULL);
    List jobs = NULL;
    slurmdb_job_rec_t *job = NULL;
    list_itr_t *iterator = NULL;

    if (!db_conn) {
        error("Failed to connect to Slurm database.");
        slurmdb_destroy_job_cond(job_cond);
        return NULL;
    }

    if (fill_job_cond_from_qsub_args(job_cond)) {
        slurmdb_connection_close(&db_conn);
        slurmdb_destroy_job_cond(job_cond);
        return NULL;
    }

    slurmdb_job_cond_def_start_end(job_cond);
    if (job_cond->usage_end &&
	    (job_cond->usage_start > job_cond->usage_end)) {
		char start_str[32], end_str[32];
		slurm_make_time_str(&job_cond->usage_start, start_str,
				    sizeof(start_str));
		slurm_make_time_str(&job_cond->usage_end, end_str,
				    sizeof(end_str));
		log_error("Start time (%s) requested is after end time (%s).",
		      start_str, end_str);
		return NULL;
	}

    jobs = slurmdb_jobs_get(db_conn, job_cond);
    slurmdb_connection_close(&db_conn);
    slurmdb_destroy_job_cond(job_cond);

    iterator = list_iterator_create(jobs);
    while ((job = list_next(iterator))) {
        if (job->start == (time_t) NO_VAL) {
            list_delete_item(iterator);
        }
    }
    list_iterator_destroy(iterator);

    return jobs;
}

/**
 * @brief Print detailed statistics for a single job.
 *
 * @param job Job record.
 * @param summary_list List to store user summaries.
 */
static void print_job_details(slurmdb_job_rec_t *job, List summary_list)
{
    char qsub_time[TIME_BUFFER_SIZE], start_time[TIME_BUFFER_SIZE], end_time[TIME_BUFFER_SIZE];
    char buf[TIME_BUFFER_SIZE];
    long wallclock = (job->end >= job->start) ? (job->end - job->start - job->suspended) : 0;
    double utime = job->user_cpu_sec + (double)job->user_cpu_usec / 1000000.0;
    double stime = job->sys_cpu_sec + (double)job->sys_cpu_usec / 1000000.0;
    double cpu_total = utime + stime;
    uint64_t maxrss = 0, total_mem = 0, total_vmem = 0;
    double memory = 0.0, io = 0.0, iow = 0.0;

    list_itr_t *step_iterator = list_iterator_create(job->steps);
    slurmdb_step_rec_t *step;

    while ((step = list_next(step_iterator))) {
        uint64_t temp_maxrss = slurmdb_find_tres_count_in_string(step->stats.tres_usage_in_max, TRES_MEM);
        maxrss = (temp_maxrss > maxrss && temp_maxrss != INFINITE64) ? temp_maxrss : maxrss;

        uint64_t temp_total_mem = slurmdb_find_tres_count_in_string(step->stats.tres_usage_in_tot, TRES_MEM);
        total_mem = (temp_total_mem > total_mem && temp_total_mem != INFINITE64) ? temp_total_mem : total_mem;

        uint64_t temp_total_vmem = slurmdb_find_tres_count_in_string(step->stats.tres_usage_in_tot, TRES_VMEM);
        total_vmem = (temp_total_vmem > total_vmem && temp_total_vmem != INFINITE64) ? temp_total_vmem : total_vmem;
    }
    list_iterator_destroy(step_iterator);

    memory = (double)total_mem * wallclock; // Convert to GB-seconds

    format_time(job->submit, qsub_time, sizeof(qsub_time));
    format_time(job->start, start_time, sizeof(start_time));
    format_time(job->end, end_time, sizeof(end_time));

    // struct passwd *pw = getpwuid(job->uid);
    struct group *gr = getgrgid(job->gid);
    char resvid_str[32], taskid_buf[32];
    snprintf(resvid_str, sizeof(resvid_str), "%u", job->resvid);
    snprintf(taskid_buf, sizeof(taskid_buf), "%u", job->array_task_id);

    log_info("==============================================================");
    log_info("%-13s%-20s", "qname", job->partition ? job->partition : "unknown");
    log_info("%-13s%-20s", "hostname", job->nodes ? job->nodes : "unknown");
    log_info("%-13s%-20s", "group", gr && gr->gr_name ? gr->gr_name : "unknown");
    log_info("%-13s%-20s", "owner", job->user ? job->user : "unknown");
    log_info("%-13s%-20s", "project", job->wckey && job->wckey[0] ? job->wckey : "NONE");
    log_info("%-13s%-20s", "department", job->wckey && job->wckey[0] ? job->wckey : "defaultdepartment");
    log_info("%-13s%-20s", "jobname", job->jobname ? job->jobname : "STDIN");
    log_info("%-13s%-20u", "jobnumber", job->jobid);
    log_info("%-13s%-20s", "taskid", job->array_task_id != NO_VAL ? taskid_buf : "undefined");
    log_info("%-13s%-20s", "account", job->account ? job->account : "sge");
    log_info("%-13s%-20u", "priority", job->priority);
    log_info("%-13s%-20s", "qsub_time", qsub_time);
    log_info("%-13s%-20s", "start_time", start_time);
    log_info("%-13s%-20s", "end_time", end_time);
    log_info("%-13s%-20s", "granted_pe", "NONE");
    log_info("%-13s%-20u", "slots", job->req_cpus);
    log_info("%-13s%-3d %s", "failed", ((job->state & JOB_STATE_BASE) > JOB_COMPLETE ) ? 100 : 0,
           ((job->state & JOB_STATE_BASE) > JOB_COMPLETE ) ? ": assumedly after job" : "");
    if (job->state == JOB_CANCELLED)
        log_exit_status((uint32_t)9);
    else
        log_exit_status(job->exitcode);
    log_info("%-13s%.0fs", "ru_wallclock", (double)wallclock);
    log_info("%-13s%.3fs", "ru_utime", utime);
    log_info("%-13s%.3fs", "ru_stime", stime);
    log_info("%-13s%s", "ru_maxrss", format_memory((double)maxrss, buf, sizeof(buf)));
    log_info("%-13s%s", "ru_ixrss", format_memory(0, buf, sizeof(buf)));
    log_info("%-13s%s", "ru_ismrss", format_memory(0, buf, sizeof(buf)));
    log_info("%-13s%s", "ru_idrss", format_memory(0, buf, sizeof(buf)));
    log_info("%-13s%s", "ru_isrss", format_memory(0, buf, sizeof(buf)));
    log_info("%-13s%-20" PRIu64 "", "ru_minflt", (uint64_t)0);
    log_info("%-13s%-20" PRIu64 "", "ru_majflt", (uint64_t)0);
    log_info("%-13s%-20" PRIu64 "", "ru_nswap", (uint64_t)0);
    log_info("%-13s%-20" PRIu64 "", "ru_inblock", (uint64_t)0);
    log_info("%-13s%-20" PRIu64 "", "ru_oublock", (uint64_t)0);
    log_info("%-13s%-20" PRIu64 "", "ru_msgsnd", (uint64_t)0);
    log_info("%-13s%-20" PRIu64 "", "ru_msgrcv", (uint64_t)0);
    log_info("%-13s%-20" PRIu64 "", "ru_nsignals", (uint64_t)0);
    log_info("%-13s%-20" PRIu64 "", "ru_nvcsw", (uint64_t)0);
    log_info("%-13s%-20" PRIu64 "", "ru_nivcsw", (uint64_t)0);
    log_info("%-13s%.3fs", "cpu", cpu_total);
    log_info("%-13s%s", "mem", format_memory(memory, buf, sizeof(buf)));
    log_info("%-13s%.3f", "io", io);
    log_info("%-13s%.3fs", "iow", iow);
    log_info("%-13s%s", "maxvmem", format_memory(total_vmem, buf, sizeof(buf)));
    log_info("%-13s%-20s", "arid", job->resvid == 0 ? "undefined" : resvid_str);
    log_info("%-13s%-20s", "ar_sub_time", "undefined");
    log_info("%-13s%-20s", "category", job->submit_line ? job->submit_line : "NONE");

    memory /= (1024.0 * 1024.0 * 1024.0); // Convert to GB-seconds
    update_user_summary(summary_list, job->user ? job->user : "unknown",
                        wallclock, utime, stime, cpu_total, memory, io, iow);
}

/**
 * @brief Print user-level job usage summary.
 *
 * @param summary_list List of user summaries.
 */
static void print_user_summary(List summary_list)
{
    char buffer[MAX_BUFFER_SIZE];
    int dashcnt;

    dashcnt = snprintf(buffer, sizeof(buffer),
                       "%-13s %13s %13s %13s %13s %18s %18s %18s\n",
                       "OWNER", "WALLCLOCK", "UTIME", "STIME", "CPU", "MEMORY", "IO", "IOW");
    fprintf(stdout, "%s", buffer);
    for (int i = 0; i < dashcnt - 1; i++) {
        fprintf(stdout, "=");
    }
    fprintf(stdout, "\n");

    if (summary_list == NULL || list_count(summary_list) == 0)
        return;

    list_itr_t *iterator = list_iterator_create(summary_list);
    user_summary_t *summary;

    while ((summary = list_next(iterator))) {
        if (qargs->user_list.value && strcmp(summary->owner, qargs->user_list.value) != 0) {
            continue;
        }

        char owner_str[14], wallclock_str[14], utime_str[14], stime_str[14], cpu_str[14];
        char memory_str[19], io_str[19], iow_str[19];

        snprintf(owner_str, sizeof(owner_str), "%-13.12s", summary->owner);
        snprintf(wallclock_str, sizeof(wallclock_str), "%13.0f", (double)summary->wallclock);
        snprintf(utime_str, sizeof(utime_str), "%13.3f", summary->utime);
        snprintf(stime_str, sizeof(stime_str), "%13.3f", summary->stime);
        snprintf(cpu_str, sizeof(cpu_str), "%13.3f", summary->cpu);
        snprintf(memory_str, sizeof(memory_str), "%18.3f", summary->memory);
        snprintf(io_str, sizeof(io_str), "%18.3f", summary->io);
        snprintf(iow_str, sizeof(iow_str), "%18.3f", summary->iow);

        owner_str[13] = '\0';
        wallclock_str[13] = '\0';
        utime_str[13] = '\0';
        stime_str[13] = '\0';
        cpu_str[13] = '\0';
        memory_str[18] = '\0';
        io_str[18] = '\0';
        iow_str[18] = '\0';

        log_info("%s %s %s %s %s %s %s %s",
               owner_str, wallclock_str, utime_str, stime_str, cpu_str,
               memory_str, io_str, iow_str);
    }
    list_iterator_destroy(iterator);
}

/**
 * @brief Print system-level job usage summary.
 *
 * @param summary_list List of user summaries.
 */
static void print_system_summary(List summary_list)
{
    double total_wallclock = 0.0, total_utime = 0.0, total_stime = 0.0;
    double total_cpu = 0.0, total_memory = 0.0, total_io = 0.0, total_iow = 0.0;

    printf("Total System Usage\n");

    char buffer[MAX_BUFFER_SIZE];
    int dashcnt;

    dashcnt = snprintf(buffer, sizeof(buffer),
                       "%13s %13s %13s %13s %18s %18s %18s\n",
                       "WALLCLOCK", "UTIME", "STIME", "CPU", "MEMORY", "IO", "IOW");
    fprintf(stdout, "%s", buffer);
    for (int i = 0; i < dashcnt - 1; i++) {
        fprintf(stdout, "=");
    }
    fprintf(stdout, "\n");

    if (summary_list == NULL || list_count(summary_list) == 0)
        return;

    list_itr_t *iterator = list_iterator_create(summary_list);
    user_summary_t *summary;

    while ((summary = list_next(iterator))) {
        total_wallclock += summary->wallclock;
        total_utime += summary->utime;
        total_stime += summary->stime;
        total_cpu += summary->cpu;
        total_memory += summary->memory;
        total_io += summary->io;
        total_iow += summary->iow;
    }
    list_iterator_destroy(iterator);


    char wallclock_str[14], utime_str[14], stime_str[14], cpu_str[14];
    char memory_str[19], io_str[19], iow_str[19];

    snprintf(wallclock_str, sizeof(wallclock_str), "%13.0f", total_wallclock);
    snprintf(utime_str, sizeof(utime_str), "%13.3f", total_utime);
    snprintf(stime_str, sizeof(stime_str), "%13.3f", total_stime);
    snprintf(cpu_str, sizeof(cpu_str), "%13.3f", total_cpu);
    snprintf(memory_str, sizeof(memory_str), "%18.3f", total_memory);
    snprintf(io_str, sizeof(io_str), "%18.3f", total_io);
    snprintf(iow_str, sizeof(iow_str), "%18.3f", total_iow);

    wallclock_str[13] = '\0';
    utime_str[13] = '\0';
    stime_str[13] = '\0';
    cpu_str[13] = '\0';
    memory_str[18] = '\0';
    io_str[18] = '\0';
    iow_str[18] = '\0';

    log_info("%s %s %s %s %s %s %s",
           wallclock_str, utime_str, stime_str, cpu_str,
           memory_str, io_str, iow_str);
}

/**
 *  If '--help' or '--version' is set, display the corresponding information and release the resources.
 */
static bool handle_help_or_version(CommandParser *parser) 
{
    if (qargs->is_help.is_set) {
        display_help_info();
    } else if (qargs->is_version.is_set) {
        display_version_info();
    } else {
        return false;
    }

    cleanup(parser, NULL, NULL);
    return true;
}

/**
 * @brief Fetch and preprocess job records from the Slurm database.
 *
 * This function retrieves job records using fetch_jobs() and
 * performs necessary preprocessing via preprocess_jobs().
 *
 * @return A list of job records on success, or NULL if fetching fails.
 */
static List get_and_process_jobs(void)
{
    List jobs = fetch_jobs();
    preprocess_jobs(jobs);
    return jobs;
}

/**
 * @brief Process job records and update the summary list.
 *
 * Iterates through the list of job records. If `qargs->job_list.is_set` is true,
 * detailed job information is printed. Otherwise, it computes aggregated statistics
 * (wallclock time, CPU usage, memory) and updates a per-user summary.
 *
 * @param jobs A list of job records (List of slurmdb_job_rec_t*).
 * @param summary_list A list to store aggregated user summaries.
 */
static void process_job_records(List jobs, List summary_list)
{
    list_itr_t *iterator = list_iterator_create(jobs);
    slurmdb_job_rec_t *job;

    while ((job = list_next(iterator))) {
        if (job->state < JOB_COMPLETE) {
            continue;
        }

        if (qargs->job_list.is_set) {
            print_job_details(job, summary_list);
        } else {
            long wallclock = (job->end >= job->start) ? (job->end - job->start - job->suspended) : 0;
            double utime = job->user_cpu_sec + (double)job->user_cpu_usec / 1000000.0;
            double stime = job->sys_cpu_sec + (double)job->sys_cpu_usec / 1000000.0;
            double cpu_total = utime + stime;
            double memory = 0.0;

            list_itr_t *step_iterator = list_iterator_create(job->steps);
            slurmdb_step_rec_t *step;
            uint64_t total_mem = 0;

            while ((step = list_next(step_iterator))) {
                uint64_t temp_total_mem = slurmdb_find_tres_count_in_string(step->stats.tres_usage_in_tot, TRES_MEM);
                total_mem = (temp_total_mem > total_mem && temp_total_mem != INFINITE64) ? temp_total_mem : total_mem;
            }
            list_iterator_destroy(step_iterator);

            memory = (double)total_mem * wallclock / (1024.0 * 1024.0 * 1024.0);
            update_user_summary(summary_list, job->user ? job->user : "unknown",
                                wallclock, utime, stime, cpu_total, memory, 0.0, 0.0);
        }
    }

    list_iterator_destroy(iterator);
}

/**
 * @brief Display the final results based on collected summary data.
 *
 * If the summary list is empty, this function does nothing.
 * Depending on the query options set (`qargs`), it prints either a system
 * summary or a per-user summary.
 *
 * @param summary_list A list of user summaries to be printed.
 */
static void display_results(List summary_list)
{
    if (qargs->job_list.is_set) {
        if (qargs->job_list.value == NULL)
            print_system_summary(summary_list);
    } else if (qargs->user_list.is_set) {
        print_user_summary(summary_list);
    } else {
        print_system_summary(summary_list);
    }
}

/**
 * @brief Free memory and clean up resources before exiting.
 *
 * This function destroys the command parser, frees job and summary lists,
 * and releases any argument structures.
 *
 * @param parser The command parser object.
 * @param jobs The list of job records to free.
 * @param summary_list The list of user summaries to free.
 */
static void cleanup(CommandParser *parser, List jobs, List summary_list)
{
    FREE_NULL_LIST(jobs);
    FREE_NULL_LIST(summary_list);
    destroy_parser(parser);
    free_qargs();
}

/**
 * @brief Main entry point for the qacct program.
 *
 * Parses command-line arguments, fetches and processes job records,
 * and displays the result summary to the user.
 *
 * @param argc Argument count.
 * @param argv Argument vector.
 * @return 0 on success, 1 on command parse error.
 */
int main(int argc, char *argv[])
{
    // Initialize Slurm configuration (loads slurm.conf or default values)
    slurm_init(NULL);
    // Initialize global arguments or command-line argument structures
    init_qargs();

    // Declare lists to store job data and user summaries
    List summary_list = NULL;
    List jobs = NULL;

    // Create a command parser to handle command-line arguments
    CommandParser *parser = create_command_parser();

    // Parse the command-line arguments; if there's an error, clean up and exit
    if (parse_command(parser, argc, argv)) {
        cleanup(parser, NULL, NULL);
        return 1;
    }

    // If the user requested -help or -version, handle it and exit normally
    if (handle_help_or_version(parser))
        return 0;

    // Retrieve and process job records; returns a list of job objects
    jobs = get_and_process_jobs();

    // If no job data was retrieved, clean up and exit with error
    if (!jobs || list_count(jobs) == 0) {
        cleanup(parser, jobs, summary_list);
        return 1;
    }

    // Create a list to store per-user job summaries, with custom free function
    summary_list = list_create(free_user_summary);

    // Process the job records and populate the user summary list
    process_job_records(jobs, summary_list);

    // Display the final results (e.g., print per-user summaries)
    display_results(summary_list);

    // Free all allocated resources and clean up before exiting
    cleanup(parser, jobs, summary_list);

    // Return success
    return 0;
}