#ifndef __QSUB_H
#define __QSUB_H

#include "common_util.h"

/**
 * Structure to represent a command-line option.
 */
typedef struct {
    const char *name;         // Option name
    OptionType type;          // Option type (e.g., required, optional)
    char *value;              // Stored option value
    bool set;                 // Flag indicating if the option is set
    void (*handler)(void*, const char*); // Handler function for the option
} Option;

/* External declarations */
extern char **environ;
extern uint64_t str_to_mbytes(const char *arg);
extern char *slurm_option_get_argv_str(const int argc, char **argv);
extern int setenvf(char ***envp, const char *name, const char *fmt, ...)
    __attribute__ ((format (printf, 3, 4)));
extern void env_array_merge(char ***dest_array, const char **src_array);
extern int envcount(char **env);

/* Resource key definitions */
#define KEY_MEM_FREE    "mem_free"
#define KEY_NUM_PROC    "num_proc"
#define KEY_HOSTNAME    "hostname"
#define KEY_TIME_LIMIT  "h_rt"       // Format: hh:mm:ss
#define KEY_GPU         "GPU"        // Format: GPU=3

/* Constant definitions */
#define MAX_ARGS 256
#define MAX_LINE_LEN 2048
#define MAX_OPTIONS 128
#define MAX_WAIT_SLEEP_TIME 32

/**
 * Structure to hold parsed qsub arguments and options.
 */
typedef struct {
    Option interpreter;      // Shell interpreter option
    Option start_time;      // Job start time option
    Option deadline;        // Job deadline like sbatch --deadline=time
    Option account;         // Account string option
    Option is_binary;       // Binary job flag
    Option use_cwd;         // Use current working directory flag
    Option error_path;      // Standard error path
    Option user_hold;       // User hold flag
    Option merge_output;    // Merge stdout and stderr flag
    Option resources;       // Resource request option
    Option mail_list;       // Email notification list
    Option mail_options;    // Email notification events
    Option job_name;        // Job name option
    Option start_now;       // Immediate start flag
    Option notify;          // Notify before kill/suspend flag
    Option output_path;     // Standard output path
    Option project;         // Project name option
    Option priority;        // Job priority option
    Option parallel_env;    // Parallel environment option
    Option queue;           // Queue selection option
    Option export_env;      // Export environment variables flag
    Option verify_mode;     // Verification mode option
    Option working_dir;     // Working directory option
    Option rerunnable;      // Job rerunnable flag
    Option task_id_range;   // Task ID range for job arrays
    Option task_limit;      // Task concurrency limit
    Option dependency;      // Job dependency list
    Option hard_limit;      // Hard resource limit flag
    Option soft_limit;      // Soft resource limit flag
    Option in_path;         // Standard input path
    Option sync_job;        // Synchronous execution flag
    Option is_help;         // Help flag
    Option is_version;      // Version flag
    Option is_debug;        // Debug flag
    char *script_file;      // Script file name
    char *script_argv[MAX_ARGS]; // Script arguments
    uint32_t script_argc;   // Number of script arguments
    uint32_t script_file_argc; // Number of script file arguments
    char *script_file_argv[MAX_ARGS]; // Script file arguments
    char *script;           // Script content
    Option options[MAX_OPTIONS]; // Array of all options
    Option *option_ptrs[MAX_OPTIONS]; // Pointers to options
    size_t option_count;    // Number of defined options
} qsub_args_t;

/**
 * Macro to define option handler functions.
 * Ensures each option is set only once and handles value duplication.
 */
#define DEFINE_OPTION_HANDLER(option_name) \
    static void set_##option_name(void* args, const char* value) { \
        qsub_args_t* qargs = (qsub_args_t*)args; \
        if (qargs->option_name.set) { \
            return; /* Skip if already set */ \
        } \
        if (value) { \
            qargs->option_name.value = xstrdup(value); \
        } else { \
            qargs->option_name.value = NULL; \
        } \
        qargs->option_name.set = true; \
    }

/* Declare option handlers */
DEFINE_OPTION_HANDLER(interpreter)
DEFINE_OPTION_HANDLER(account)
DEFINE_OPTION_HANDLER(is_binary)
DEFINE_OPTION_HANDLER(use_cwd)
DEFINE_OPTION_HANDLER(error_path)
DEFINE_OPTION_HANDLER(user_hold)
DEFINE_OPTION_HANDLER(merge_output)
DEFINE_OPTION_HANDLER(resources)
DEFINE_OPTION_HANDLER(mail_list)
DEFINE_OPTION_HANDLER(mail_options)
DEFINE_OPTION_HANDLER(job_name)
DEFINE_OPTION_HANDLER(start_now)
DEFINE_OPTION_HANDLER(notify)
DEFINE_OPTION_HANDLER(output_path)
DEFINE_OPTION_HANDLER(project)
DEFINE_OPTION_HANDLER(queue)
DEFINE_OPTION_HANDLER(export_env)
DEFINE_OPTION_HANDLER(verify_mode)
DEFINE_OPTION_HANDLER(working_dir)
DEFINE_OPTION_HANDLER(rerunnable)
DEFINE_OPTION_HANDLER(task_id_range)
DEFINE_OPTION_HANDLER(task_limit)
DEFINE_OPTION_HANDLER(dependency)
DEFINE_OPTION_HANDLER(hard_limit)
DEFINE_OPTION_HANDLER(soft_limit)
DEFINE_OPTION_HANDLER(in_path)
DEFINE_OPTION_HANDLER(sync_job)
DEFINE_OPTION_HANDLER(is_help)
DEFINE_OPTION_HANDLER(is_version)
DEFINE_OPTION_HANDLER(is_debug)

#endif