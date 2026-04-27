/**
 * @file qsub.c
 * @brief Slurm job submission tool with SGE-compatible interface.
 *
 * This module provides a command-line utility for submitting jobs to Slurm, implementing
 * a subset of Sun Grid Engine (SGE) parameters for compatibility. It supports job submission
 * with detailed configuration options, script execution, and synchronous job tracking.
 *
 * Supported SGE options:
 *   -S: Specifies the shell interpreter for the job script.
 *   -a: Sets the job start time (format: YYYYMMDDHHMM.SS).
 *   -A: Defines the account string for billing.
 *   -b: Indicates if the job is a binary executable (y/n).
 *   -cwd: Uses the current working directory as the job's working directory.
 *   -e: Specifies the path for standard error output.
 *   -h: Places the job in a user hold state.
 *   -j: Merges standard output and error streams (y/n).
 *   -l: Requests resources (e.g., mem_free, num_proc, GPU, h_rt).
 *   -M: Sets the email list for notifications.
 *   -m: Configures email notification options (b, e, a).
 *   -N: Assigns a name to the job.
 *   -o: Specifies the path for standard output.
 *   -P: Defines the project name for the job.
 *   -p: Sets the job priority (-1023 to 1024).
 *   -pe: Configures the parallel environment (e.g., mpi slots).
 *   -q: Specifies the target queue or partition.
 *   -V: Exports all environment variables to the job.
 *   -w: Sets the verification mode (not implemented).
 *   -wd: Specifies the working directory for the job.
 *   -r: Marks the job as rerunnable (y/n).
 *   -t: Defines the task ID range for array jobs (e.g., 1-10:2).
 *   -tc: Limits concurrent tasks for array jobs.
 *   -hold_jid: Specifies job dependencies by job ID.
 *   -i: Sets the standard input path.
 *   -sync: Runs the job synchronously, waiting for completion (y/n).
 *
 * The tool parses command-line arguments, loads job scripts from files or stdin,
 * configures Slurm job descriptors, and submits jobs with retry logic for robustness.
 * It ensures proper resource cleanup and provides detailed error reporting.
 *
 * @author HE JIALE
 * @date 2025-04-13
 * @version 0.0.1
 */

#include <slurm/slurm_errno.h>
#include <slurm/slurm.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <errno.h>
#include <time.h>
#include <libgen.h>
#include <limits.h>
#include <stdarg.h>
#include "qsub.h"


/* Forward declarations */
static void print_args(qsub_args_t *qargs);
static void print_job_desc(job_desc_msg_t *job);
static void _free_job_desc_msg_memory(job_desc_msg_t *job_desc);
static void _free_qsub_args(qsub_args_t *args);
static int _job_wait(uint32_t job_id);
static void set_parallel_env(void *args, const char *value);
static void set_start_time(void *args, const char *value);
static void set_deadline(void *args, const char *value);
static void set_priority(void *args, const char *value);
static void init_option(Option *opt, const char *name, OptionType type, void (*handler)(void*, const char*));
static void _set_prio_process_env(void);
static void _set_submit_dir_env(void);
static int _set_umask_env(void);
static void init_qsub_args(qsub_args_t *result);
static int fill_job_desc_from_opt(job_desc_msg_t *job, qsub_args_t *qargs);
// static bool is_valid_file(const char *file_path);
static void load_script(qsub_args_t *qargs);
static void parse_args(int argc, char *argv[], qsub_args_t *qargs, bool is_cmd);
static void set_environment_variables(void);
static void parse_all_args(int argc, char *argv[], qsub_args_t *qargs);
static void free_all_mem(job_desc_msg_t *job, qsub_args_t *qargs, submit_response_msg_t *resp);
static void display_help_info(void);
static void display_version_info(void);
static char* extract_shell_from_shebang(const char* script);

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
    vfprintf(stderr, format, args);
    fprintf(stderr, "\n");
    va_end(args);
}

static void print_args(qsub_args_t *qargs) {
    log_info("Parsed Arguments:");
    if (qargs->interpreter.set) {
        log_info("-S (shell interpreter) : %s", qargs->interpreter.value);
    }
    if (qargs->start_time.set) {
        log_info("-a (start_time): %s", qargs->start_time.value);
    }
    if (qargs->deadline.set) {
        log_info("-dl (deadline): %s", qargs->deadline.value);
    }
    if (qargs->account.set) {
        log_info("-A (account): %s", qargs->account.value);
    }
    if (qargs->is_binary.set) {
        log_info("-b (is_binary): %s", qargs->is_binary.value);
    }
    if (qargs->use_cwd.set) {
        log_info("-cwd (use_cwd): (no value)");
    }
    if (qargs->error_path.set) {
        log_info("-e (error_path): %s", qargs->error_path.value);
    }
    if (qargs->user_hold.set) {
        log_info("-h (user_hold): (no value)");
    }
    if (qargs->merge_output.set) {
        log_info("-j (merge_output): %s", qargs->merge_output.value);
    }
    if (qargs->resources.set) {
        log_info("-l (resources): %s", qargs->resources.value);
    }
    if (qargs->mail_list.set) {
        log_info("-M (mail_list): %s", qargs->mail_list.value);
    }
    if (qargs->mail_options.set) {
        log_info("-m (mail_options): %s", qargs->mail_options.value);
    }
    if (qargs->job_name.set) {
        log_info("-N (job_name): %s", qargs->job_name.value);
    }
    if (qargs->start_now.set) {
        log_info("-now (start_now): (no value)");
    }
    if (qargs->notify.set) {
        log_info("-notify (notify): (no value)");
    }
    if (qargs->output_path.set) {
        log_info("-o (output_path): %s", qargs->output_path.value);
    }
    if (qargs->project.set) {
        log_info("-P (project): %s", qargs->project.value);
    }
    if (qargs->priority.set) {
        log_info("-p (priority): %s", qargs->priority.value);
    }
    if (qargs->parallel_env.set) {
        log_info("-pe (parallel_env): %s", qargs->parallel_env.value);
    }
    if (qargs->queue.set) {
        log_info("-q (queue): %s", qargs->queue.value);
    }
    if (qargs->export_env.set) {
        log_info("-V (export_env): (no value)");
    }
    if (qargs->verify_mode.set) {
        log_info("-w (verify_mode): %s", qargs->verify_mode.value);
    }
    if (qargs->working_dir.set) {
        log_info("-wd (working_dir): %s", qargs->working_dir.value);
    }
    if (qargs->rerunnable.set) {
        log_info("-r (rerunnable): %s", qargs->rerunnable.value);
    }
    if (qargs->script_file) {
        log_info("qargs.script_file : %s", qargs->script_file);
    }
    if (qargs->script) {
        log_info("qargs.script : %s", qargs->script);
    }
    if (qargs->script_file_argc) {
        log_info("qargs.script_file_argc : %u", qargs->script_file_argc);
    }
}

static void print_job_desc(job_desc_msg_t *job) {
    log_info(" Job Descriptor Contents:");
    log_info(" account: %s", job->account ? job->account : "NULL");
    log_info(" acctg_freq: %s", job->acctg_freq ? job->acctg_freq : "NULL");
    log_info(" admin_comment: %s", job->admin_comment ? job->admin_comment : "NULL");
    log_info(" alloc_node: %s", job->alloc_node ? job->alloc_node : "NULL");
    log_info(" alloc_resp_port: %u", job->alloc_resp_port);
    log_info(" alloc_sid: %u", job->alloc_sid);
    log_info(" argc: %u", job->argc);
    log_info(" argv: ");
    if (job->argv && job->argc > 0) {
        for (uint32_t i = 0; i < job->argc; i++) {
            log_info("%s%s", job->argv[i] ? job->argv[i] : "NULL", i < job->argc - 1 ? ", " : "");
        }
    } else {
        log_info("NULL");
    }
    log_info("");
    log_info(" array_inx: %s", job->array_inx ? job->array_inx : "NULL");
    log_info(" array_bitmap: %p", (void *)job->array_bitmap);
    log_info(" batch_features: %s", job->batch_features ? job->batch_features : "NULL");
    log_info(" begin_time: %ld", (long)job->begin_time);
    log_info(" bitflags: %lu", (unsigned long)job->bitflags);
    log_info(" burst_buffer: %s", job->burst_buffer ? job->burst_buffer : "NULL");
    log_info(" clusters: %s", job->clusters ? job->clusters : "NULL");
    log_info(" cluster_features: %s", job->cluster_features ? job->cluster_features : "NULL");
    log_info(" comment: %s", job->comment ? job->comment : "NULL");
    log_info(" contiguous: %u", job->contiguous);
    log_info(" container: %s", job->container ? job->container : "NULL");
    log_info(" core_spec: %u", job->core_spec);
    log_info(" cpu_bind: %s", job->cpu_bind ? job->cpu_bind : "NULL");
    log_info(" cpu_bind_type: %u", job->cpu_bind_type);
    log_info(" cpu_freq_min: %u", job->cpu_freq_min);
    log_info(" cpu_freq_max: %u", job->cpu_freq_max);
    log_info(" cpu_freq_gov: %u", job->cpu_freq_gov);
    log_info(" cpus_per_tres: %s", job->cpus_per_tres ? job->cpus_per_tres : "NULL");
    log_info(" crontab_entry: %p", job->crontab_entry);
    log_info(" deadline: %ld", (long)job->deadline);
    log_info(" delay_boot: %u", job->delay_boot);
    log_info(" dependency: %s", job->dependency ? job->dependency : "NULL");
    log_info(" end_time: %ld", (long)job->end_time);
    log_info(" environment: ");
    if (job->environment && job->env_size > 0) {
        for (uint32_t i = 0; i < job->env_size; i++) {
            log_info("%s%s", job->environment[i] ? job->environment[i] : "NULL", i < job->env_size - 1 ? ", " : "");
        }
    } else {
        log_info("NULL");
    }
    log_info("");
    log_info(" env_size: %u", job->env_size);
    log_info(" extra: %s", job->extra ? job->extra : "NULL");
    log_info(" exc_nodes: %s", job->exc_nodes ? job->exc_nodes : "NULL");
    log_info(" features: %s", job->features ? job->features : "NULL");
    log_info(" fed_siblings_active: %lu", (unsigned long)job->fed_siblings_active);
    log_info(" fed_siblings_viable: %lu", (unsigned long)job->fed_siblings_viable);
    log_info(" group_id: %u", job->group_id);
    log_info(" het_job_offset: %u", job->het_job_offset);
    log_info(" immediate: %u", job->immediate);
    log_info(" job_id: %u", job->job_id);
    log_info(" job_id_str: %s", job->job_id_str ? job->job_id_str : "NULL");
    log_info(" kill_on_node_fail: %u", job->kill_on_node_fail);
    log_info(" licenses: %s", job->licenses ? job->licenses : "NULL");
    log_info(" mail_type: %u", job->mail_type);
    log_info(" mail_user: %s", job->mail_user ? job->mail_user : "NULL");
    log_info(" mcs_label: %s", job->mcs_label ? job->mcs_label : "NULL");
    log_info(" mem_bind: %s", job->mem_bind ? job->mem_bind : "NULL");
    log_info(" mem_bind_type: %u", job->mem_bind_type);
    log_info(" mem_per_tres: %s", job->mem_per_tres ? job->mem_per_tres : "NULL");
    log_info(" name: %s", job->name ? job->name : "NULL");
    log_info(" network: %s", job->network ? job->network : "NULL");
    log_info(" nice: %u", job->nice);
    log_info(" num_tasks: %u", job->num_tasks);
    log_info(" open_mode: %u", job->open_mode);
    log_info(" origin_cluster: %s", job->origin_cluster ? job->origin_cluster : "NULL");
    log_info(" other_port: %u", job->other_port);
    log_info(" overcommit: %u", job->overcommit);
    log_info(" partition: %s", job->partition ? job->partition : "NULL");
    log_info(" plane_size: %u", job->plane_size);
    // log_info(" power_flags: %u", job->power_flags);
    log_info(" prefer: %s", job->prefer ? job->prefer : "NULL");
    log_info(" priority: %u", job->priority);
    log_info(" profile: %u", job->profile);
    log_info(" qos: %s", job->qos ? job->qos : "NULL");
    log_info(" reboot: %u", job->reboot);
    log_info(" resp_host: %s", job->resp_host ? job->resp_host : "NULL");
    log_info(" restart_cnt: %u", job->restart_cnt);
    log_info(" req_nodes: %s", job->req_nodes ? job->req_nodes : "NULL");
    log_info(" requeue: %u", job->requeue);
    log_info(" reservation: %s", job->reservation ? job->reservation : "NULL");
    log_info(" script: %s", job->script ? job->script : "NULL");
    log_info(" script_buf: %p", job->script_buf);
    log_info(" shared: %u", job->shared);
    log_info(" site_factor: %u", job->site_factor);
    log_info(" spank_job_env: ");
    if (job->spank_job_env && job->spank_job_env_size > 0) {
        for (uint32_t i = 0; i < job->spank_job_env_size; i++) {
            log_info("%s%s", job->spank_job_env[i] ? job->spank_job_env[i] : "NULL", i < job->spank_job_env_size - 1 ? ", " : "");
        }
    } else {
        log_info("NULL");
    }
    log_info("");
    log_info(" spank_job_env_size: %u", job->spank_job_env_size);
    log_info(" submit_line: %s", job->submit_line ? job->submit_line : "NULL");
    log_info(" task_dist: %u", job->task_dist);
    log_info(" time_limit: %u", job->time_limit);
    log_info(" time_min: %u", job->time_min);
    log_info(" predict_job: %u", job->predict_job);
    log_info(" tres_bind: %s", job->tres_bind ? job->tres_bind : "NULL");
    log_info(" tres_freq: %s", job->tres_freq ? job->tres_freq : "NULL");
    log_info(" tres_per_job: %s", job->tres_per_job ? job->tres_per_job : "NULL");
    log_info(" tres_per_node: %s", job->tres_per_node ? job->tres_per_node : "NULL");
    log_info(" tres_per_socket: %s", job->tres_per_socket ? job->tres_per_socket : "NULL");
    log_info(" tres_per_task: %s", job->tres_per_task ? job->tres_per_task : "NULL");
    log_info(" user_id: %u", job->user_id);
    log_info(" wait_all_nodes: %u", job->wait_all_nodes);
    log_info(" warn_flags: %u", job->warn_flags);
    log_info(" warn_signal: %u", job->warn_signal);
    log_info(" warn_time: %u", job->warn_time);
    log_info(" work_dir: %s", job->work_dir ? job->work_dir : "NULL");
    log_info(" cpus_per_task: %u", job->cpus_per_task);
    log_info(" min_cpus: %u", job->min_cpus);
    log_info(" max_cpus: %u", job->max_cpus);
    log_info(" min_nodes: %u", job->min_nodes);
    log_info(" max_nodes: %u", job->max_nodes);
    log_info(" boards_per_node: %u", job->boards_per_node);
    log_info(" sockets_per_board: %u", job->sockets_per_board);
    log_info(" sockets_per_node: %u", job->sockets_per_node);
    log_info(" cores_per_socket: %u", job->cores_per_socket);
    log_info(" threads_per_core: %u", job->threads_per_core);
    log_info(" ntasks_per_node: %u", job->ntasks_per_node);
    log_info(" ntasks_per_socket: %u", job->ntasks_per_socket);
    log_info(" ntasks_per_core: %u", job->ntasks_per_core);
    log_info(" ntasks_per_board: %u", job->ntasks_per_board);
    log_info(" ntasks_per_tres: %u", job->ntasks_per_tres);
    log_info(" pn_min_cpus: %u", job->pn_min_cpus);
    log_info(" pn_min_memory: %lu", (unsigned long)job->pn_min_memory);
    log_info(" pn_min_tmp_disk: %u", job->pn_min_tmp_disk);
    log_info(" req_context: %s", job->req_context ? job->req_context : "NULL");
    log_info(" req_switch: %u", job->req_switch);
    // log_info(" select_jobinfo: %p", job->select_jobinfo);
    log_info(" selinux_context: %s", job->selinux_context ? job->selinux_context : "NULL");
    log_info(" std_err: %s", job->std_err ? job->std_err : "NULL");
    log_info(" std_in: %s", job->std_in ? job->std_in : "NULL");
    log_info(" std_out: %s", job->std_out ? job->std_out : "NULL");
    log_info(" tres_req_cnt: %p", (void *)job->tres_req_cnt);
    log_info(" wait4switch: %u", job->wait4switch);
    log_info(" wckey: %s", job->wckey ? job->wckey : "NULL");
    log_info(" x11: %u", job->x11);
    log_info(" x11_magic_cookie: %s", job->x11_magic_cookie ? job->x11_magic_cookie : "NULL");
    log_info(" x11_target: %s", job->x11_target ? job->x11_target : "NULL");
    log_info(" x11_target_port: %u", job->x11_target_port);
    log_info(" reason_detail: %s", job->reason_detail ? job->reason_detail : "NULL");
}

/* Free the memory allocated for a job_desc_msg_t structure */
static void _free_job_desc_msg_memory(job_desc_msg_t *job_desc) {
    if (job_desc == NULL) {
        return;
    }

    // Free all dynamically allocated string fields in the job_desc structure
    if (job_desc->account) xfree(job_desc->account);
    if (job_desc->acctg_freq) xfree(job_desc->acctg_freq);
    if (job_desc->admin_comment) xfree(job_desc->admin_comment);
    if (job_desc->alloc_node) xfree(job_desc->alloc_node);
    if (job_desc->argv) {
        for (uint32_t i = 0; i < job_desc->argc; i++) {
            if (job_desc->argv[i]) xfree(job_desc->argv[i]);
        }
        xfree(job_desc->argv);
    }
    if (job_desc->array_inx) xfree(job_desc->array_inx);
    if (job_desc->array_bitmap) xfree(job_desc->array_bitmap);
    if (job_desc->batch_features) xfree(job_desc->batch_features);
    if (job_desc->burst_buffer) xfree(job_desc->burst_buffer);
    if (job_desc->clusters) xfree(job_desc->clusters);
    if (job_desc->cluster_features) xfree(job_desc->cluster_features);
    if (job_desc->comment) xfree(job_desc->comment);
    if (job_desc->container) xfree(job_desc->container);
    if (job_desc->cpu_bind) xfree(job_desc->cpu_bind);
    if (job_desc->cpus_per_tres) xfree(job_desc->cpus_per_tres);
    if (job_desc->crontab_entry) xfree(job_desc->crontab_entry);
    if (job_desc->dependency) xfree(job_desc->dependency);
    if (job_desc->environment) {
        for (uint32_t i = 0; i < job_desc->env_size; i++) {
            if (job_desc->environment[i]) xfree(job_desc->environment[i]);
        }
        xfree(job_desc->environment);
    }
    if (job_desc->exc_nodes) xfree(job_desc->exc_nodes);
    if (job_desc->features) xfree(job_desc->features);
    if (job_desc->job_id_str) xfree(job_desc->job_id_str);
    if (job_desc->licenses) xfree(job_desc->licenses);
    if (job_desc->mail_user) xfree(job_desc->mail_user);
    if (job_desc->mcs_label) xfree(job_desc->mcs_label);
    if (job_desc->mem_bind) xfree(job_desc->mem_bind);
    if (job_desc->mem_per_tres) xfree(job_desc->mem_per_tres);
    if (job_desc->name) xfree(job_desc->name);
    if (job_desc->network) xfree(job_desc->network);
    if (job_desc->origin_cluster) xfree(job_desc->origin_cluster);
    if (job_desc->partition) xfree(job_desc->partition);
    if (job_desc->prefer) xfree(job_desc->prefer);
    if (job_desc->qos) xfree(job_desc->qos);
    if (job_desc->resp_host) xfree(job_desc->resp_host);
    if (job_desc->reservation) xfree(job_desc->reservation);
    if (job_desc->script) xfree(job_desc->script);
    if (job_desc->submit_line) xfree(job_desc->submit_line);
    if (job_desc->tres_bind) xfree(job_desc->tres_bind);
    if (job_desc->tres_freq) xfree(job_desc->tres_freq);
    if (job_desc->tres_per_job) xfree(job_desc->tres_per_job);
    if (job_desc->tres_per_node) xfree(job_desc->tres_per_node);
    if (job_desc->tres_per_socket) xfree(job_desc->tres_per_socket);
    if (job_desc->tres_per_task) xfree(job_desc->tres_per_task);
    if (job_desc->req_context) xfree(job_desc->req_context);
    if (job_desc->selinux_context) xfree(job_desc->selinux_context);
    if (job_desc->std_err) xfree(job_desc->std_err);
    if (job_desc->std_in) xfree(job_desc->std_in);
    if (job_desc->std_out) xfree(job_desc->std_out);
    if (job_desc->tres_req_cnt) xfree(job_desc->tres_req_cnt);
    if (job_desc->wckey) xfree(job_desc->wckey);
    if (job_desc->x11_magic_cookie) xfree(job_desc->x11_magic_cookie);
    if (job_desc->x11_target) xfree(job_desc->x11_target);
    if (job_desc->reason_detail) xfree(job_desc->reason_detail);

    // 最后释放结构体本身
    xfree(job_desc);
}

// Extract shell path from shebang line
static char* extract_shell_from_shebang(const char* script) {
    if (!script || strlen(script) < 2 || script[0] != '#' || script[1] != '!') {
        return NULL;
    }
    
    const char* start = script + 2;  // Skip "#!"
    while (*start == ' ') start++;   // Skip spaces
    
    const char* end = strchr(start, '\n');
    if (!end) end = start + strlen(start);
    
    // Extract path (until space or end of line)
    const char* path_end = start;
    while (path_end < end && *path_end != ' ' && *path_end != '\t') {
        path_end++;
    }
    
    size_t len = path_end - start;
    if (len == 0) return NULL;
    
    char* result = (char*)xmalloc(len + 1);
    strncpy(result, start, len);
    result[len] = '\0';
    
    return result;
}

/* Free the memory allocated for a qsub_args_t structure */
static void _free_qsub_args(qsub_args_t *args) {
    if (!args) return;
    
    for (size_t i = 0; i < args->option_count; i++) {
        xfree(args->options[i].value);
    }

    xfree(args->script_file);
    for (uint32_t i = 0; i < args->script_argc; i++) {
        xfree(args->script_argv[i]);
    }
    for (uint32_t i = 0; i < args->script_file_argc; i++) {
        xfree(args->script_file_argv[i]);
    }
    xfree(args->script);

    // 释放 qsub_args_t 结构体本身
    xfree(args);
}

/* Wait for a specified job ID to terminate and return its exit code */
static int _job_wait(uint32_t job_id)
{
	slurm_job_info_t *job_ptr;
	job_info_msg_t *resp = NULL;
	int ec = 0, ec2, rc;
	uint32_t sleep_time = 2, i;
	bool complete = false;

	while (!complete) {
		complete = true;
		sleep(sleep_time);
		/*
		 * min_job_age is factored into this to ensure the job can't
		 * run, complete quickly, and be purged from slurmctld before
		 * we've woken up and queried the job again.
		 */
		if ((sleep_time < (slurm_conf.min_job_age / 2)) &&
		    (sleep_time < MAX_WAIT_SLEEP_TIME))
			sleep_time *= 4;

		rc = slurm_load_job(&resp, job_id, SHOW_ALL);
		if (rc == SLURM_SUCCESS) {
			for (i = 0, job_ptr = resp->job_array;
			     (i < resp->record_count) && complete;
			     i++, job_ptr++) {
				if (IS_JOB_FINISHED(job_ptr)) {
					if (WIFEXITED(job_ptr->exit_code)) {
						ec2 = WEXITSTATUS(job_ptr->
								  exit_code);
					} else
						ec2 = 1;
					ec = MAX(ec, ec2);
				} else {
					complete = false;
				}
			}
			slurm_free_job_info_msg(resp);
		} else if (rc == ESLURM_INVALID_JOB_ID) {
			error("Job %u no longer found and exit code not found",
			      job_id);
		} else {
			complete = false;
			error("Currently unable to load job state information, retrying: %m");
		}
	}

	return ec;
}

/* Set the parallel environment option for a qsub_args_t structure */
static void set_parallel_env(void *args, const char *value) {
    qsub_args_t* qargs = (qsub_args_t*)args;
    if (qargs->parallel_env.set || !value || value[0] == '\0')
        return;

    char *input_copy = xstrdup(value);
    char *space_pos = strchr(input_copy, ' ');
    char *cleaned_value = input_copy;

    if (space_pos) {
        *space_pos = '\0';  // Replace spaces with string termination characters
        space_pos++;
        while (*space_pos == ' ') space_pos++;  // Skip consecutive spaces
        // If there is valid content after the space, use that part as the value.
        if (*space_pos != '\0') {
            cleaned_value = xstrdup(space_pos);
            xfree(input_copy);
            input_copy = cleaned_value;  // Update "cleaned_value" to the new "input_copy"
        }
    }

    qargs->parallel_env.value = input_copy;
    qargs->parallel_env.set = true;
}


static void set_start_time(void* args, const char* value) {
    qsub_args_t* qargs = (qsub_args_t*)args;
    if (qargs->start_time.set) {
        return;
    }

    qargs->start_time.value = (char*)xmalloc(START_TIME_BUFFER_SIZE);
    struct tm timeinfo;
    time_t now = time(NULL);
    localtime_r(&now, &timeinfo);

    char* dot_pos = xstrchr(value, '.');
    int seconds = 0;

    if (dot_pos) {
        if (xstrchr(dot_pos + 1, '.')) {
            log_error("Invalid format: multiple '.' detected");
            log_error("qsub: Wrong date/time format \"%s\" specified to -a option", value);
            exit(1);
        }
        seconds = atoi(dot_pos + 1);
        if (seconds < 0 || seconds > 59) {
            log_error("Invalid seconds: %d", seconds);
            log_error("qsub: Wrong date/time format \"%s\" specified to -a option", value);
            exit(1);
        }
    }

    char buf[15] = {0};
    strncpy(buf, value, dot_pos ? (size_t)(dot_pos - value) : strlen(value));
    size_t len = strlen(buf);

    if (len < 10 || len > 12) {
        log_error("Invalid format of date/hour-minute field.");
        log_error("qsub: Wrong date/time format \"%s\" specified to -a option", value);
        exit(1);
    }

    char temp[3] = {0};

    // 月份（MM）
    strncpy(temp, buf + len - 8, 2);
    int month = atoi(temp);
    if (month < 1 || month > 12) {
        log_error("Invalid month specification.");
        log_error("qsub: Wrong date/time format \"%s\" specified to -a option", value);
        exit(1);
    }
    timeinfo.tm_mon = month - 1;

    // 日（DD）
    strncpy(temp, buf + len - 6, 2);
    int day = atoi(temp);
    timeinfo.tm_mday = day;

    // 小时（hh）
    strncpy(temp, buf + len - 4, 2);
    timeinfo.tm_hour = atoi(temp);

    // 分钟（mm）
    strncpy(temp, buf + len - 2, 2);
    timeinfo.tm_min = atoi(temp);

    // 年（YY 或 YYYY）
    if (len >= 10) {
        char year_buf[5] = {0};
        if (len == 12) {
            strncpy(year_buf, buf + len - 12, 4);
            timeinfo.tm_year = atoi(year_buf) - 1900;
        } else {
            strncpy(year_buf, buf + len - 10, 2);
            int year = atoi(year_buf);
            timeinfo.tm_year = (year >= 69) ? (year + 1900 - 1900) : (year + 2000 - 1900);
        }
    }

    timeinfo.tm_sec = seconds;

    time_t timestamp = mktime(&timeinfo);
    snprintf(qargs->start_time.value, 20, "%ld", timestamp);
    qargs->start_time.set = true;
}

static void set_deadline(void* args, const char* value) {
    qsub_args_t* qargs = (qsub_args_t*)args;
    if (qargs->deadline.set) {
        return;
    }

    qargs->deadline.value = (char*)xmalloc(START_TIME_BUFFER_SIZE);
    struct tm timeinfo;
    time_t now = time(NULL);
    localtime_r(&now, &timeinfo);

    char* dot_pos = xstrchr(value, '.');
    int seconds = 0;

    if (dot_pos) {
        if (xstrchr(dot_pos + 1, '.')) {
            log_error("Invalid format: multiple '.' detected");
            log_error("qsub: Wrong date/time format \"%s\" specified to -a option", value);
            exit(1);
        }
        seconds = atoi(dot_pos + 1);
        if (seconds < 0 || seconds > 59) {
            log_error("Invalid seconds: %d", seconds);
            log_error("qsub: Wrong date/time format \"%s\" specified to -a option", value);
            exit(1);
        }
    }

    char buf[15] = {0};
    strncpy(buf, value, dot_pos ? (size_t)(dot_pos - value) : strlen(value));
    size_t len = strlen(buf);

    if (len < 10 || len > 12) {
        log_error("Invalid format of date/hour-minute field.");
        log_error("qsub: Wrong date/time format \"%s\" specified to -a option", value);
        exit(1);
    }

    char temp[3] = {0};

    // 月份（MM）
    strncpy(temp, buf + len - 8, 2);
    int month = atoi(temp);
    if (month < 1 || month > 12) {
        log_error("Invalid month specification.");
        log_error("qsub: Wrong date/time format \"%s\" specified to -a option", value);
        exit(1);
    }
    timeinfo.tm_mon = month - 1;

    // 日（DD）
    strncpy(temp, buf + len - 6, 2);
    int day = atoi(temp);
    timeinfo.tm_mday = day;

    // 小时（hh）
    strncpy(temp, buf + len - 4, 2);
    timeinfo.tm_hour = atoi(temp);

    // 分钟（mm）
    strncpy(temp, buf + len - 2, 2);
    timeinfo.tm_min = atoi(temp);

    // 年（YY 或 YYYY）
    if (len >= 10) {
        char year_buf[5] = {0};
        if (len == 12) {
            strncpy(year_buf, buf + len - 12, 4);
            timeinfo.tm_year = atoi(year_buf) - 1900;
        } else {
            strncpy(year_buf, buf + len - 10, 2);
            int year = atoi(year_buf);
            timeinfo.tm_year = (year >= 69) ? (year + 1900 - 1900) : (year + 2000 - 1900);
        }
    }

    timeinfo.tm_sec = seconds;

    time_t timestamp = mktime(&timeinfo);
    snprintf(qargs->deadline.value, 20, "%ld", timestamp);
    qargs->deadline.set = true;
}

static void set_priority(void* args, const char* value) {
    qsub_args_t* qargs = (qsub_args_t*)args;
    if (qargs->start_time.set) {
        return;
    }

    char* endptr;
    long priority = strtol(value, &endptr, 10);

    if (*endptr != '\0' || priority < -1023 || priority > 1024) {
        log_error("qsub: invalid priority \"%ld\".  Must be an integer from -1023 to 1024", priority);
        exit(1);
    }

    qargs->priority.value = xstrdup(value);
    qargs->priority.set = true;
}

/* Initialize an Option structure with a name, type, and handler function */
static void init_option(Option *opt, const char *name, OptionType type, void (*handler)(void*, const char*)) {
    opt->name = name;
    opt->type = type;
    opt->value = NULL;
    opt->set = false;
    opt->handler = handler;
}

static void _set_prio_process_env(void)
{
	int retval;

	errno = 0; /* needed to detect a real failure since prio can be -1 */

	if ((retval = getpriority (PRIO_PROCESS, 0)) == -1)  {
		if (errno) {
			error ("getpriority(PRIO_PROCESS): %m");
			return;
		}
	}

	if (setenvf (NULL, "SLURM_PRIO_PROCESS", "%d", retval) < 0) {
		error ("unable to set SLURM_PRIO_PROCESS in environment");
		return;
	}

	// printf ("propagating SLURM_PRIO_PROCESS=%d\n", retval);
}

static void _set_submit_dir_env(void)
{
	char buf[MAXPATHLEN + 1], host[256];

	if ((getcwd(buf, MAXPATHLEN)) == NULL)
		error("getcwd failed: %m");
	else if (setenvf(NULL, "SLURM_SUBMIT_DIR", "%s", buf) < 0)
		error("unable to set SLURM_SUBMIT_DIR in environment");

	if ((gethostname(host, sizeof(host))))
		error("gethostname_short failed: %m");
	else if (setenvf(NULL, "SLURM_SUBMIT_HOST", "%s", host) < 0)
		error("unable to set SLURM_SUBMIT_HOST in environment");
}

static int _set_umask_env(void)
{
	char mask_char[5];
	mode_t mask;

	if (getenv("SLURM_UMASK"))	/* use this value */
		return SLURM_SUCCESS;

    mask = (int)umask(0);
    umask(mask);

	sprintf(mask_char, "0%d%d%d",
		((mask>>6)&07), ((mask>>3)&07), mask&07);
	if (setenvf(NULL, "SLURM_UMASK", "%s", mask_char) < 0) {
		error ("unable to set SLURM_UMASK in environment");
		return SLURM_ERROR;
	}
	// printf ("propagating UMASK=%s\n", mask_char);
	return SLURM_SUCCESS;
}

void init_qsub_args(qsub_args_t* result) {
    result->script = NULL;
    result->script_argc = 0;
    result->script_file = NULL;
    result->script_file_argc = 0;
    result->option_count = 0;

    #define ADD_OPTION(name, type, handler) \
        do { \
            init_option(&result->options[result->option_count], name, type, handler); \
            result->option_ptrs[result->option_count] = &result->options[result->option_count]; \
            result->option_count++; \
        } while (0)

    ADD_OPTION("-S", OPTION_TYPE_REQUIRED, set_interpreter);
    ADD_OPTION("-a", OPTION_TYPE_REQUIRED, set_start_time);
    ADD_OPTION("-dl", OPTION_TYPE_REQUIRED, set_deadline);
    ADD_OPTION("-A", OPTION_TYPE_REQUIRED, set_account);
    ADD_OPTION("-b", OPTION_TYPE_REQUIRED, set_is_binary);
    ADD_OPTION("-cwd", OPTION_TYPE_NONE, set_use_cwd);
    ADD_OPTION("-e", OPTION_TYPE_REQUIRED, set_error_path);
    ADD_OPTION("-h", OPTION_TYPE_NONE, set_user_hold);
    ADD_OPTION("-j", OPTION_TYPE_OPTIONAL, set_merge_output);
    ADD_OPTION("-l", OPTION_TYPE_REQUIRED, set_resources);
    ADD_OPTION("-M", OPTION_TYPE_REQUIRED, set_mail_list);
    ADD_OPTION("-m", OPTION_TYPE_REQUIRED, set_mail_options);
    ADD_OPTION("-N", OPTION_TYPE_REQUIRED, set_job_name);
    ADD_OPTION("-now", OPTION_TYPE_OPTIONAL, set_start_now);
    ADD_OPTION("-notify", OPTION_TYPE_NONE, set_notify);
    ADD_OPTION("-o", OPTION_TYPE_REQUIRED, set_output_path);
    ADD_OPTION("-P", OPTION_TYPE_REQUIRED, set_project);
    ADD_OPTION("-p", OPTION_TYPE_REQUIRED, set_priority);
    ADD_OPTION("-pe", OPTION_TYPE_MULTIPLE, set_parallel_env);
    ADD_OPTION("-q", OPTION_TYPE_REQUIRED, set_queue);
    ADD_OPTION("-V", OPTION_TYPE_NONE, set_export_env);
    ADD_OPTION("-w", OPTION_TYPE_REQUIRED, set_verify_mode);
    ADD_OPTION("-wd", OPTION_TYPE_REQUIRED, set_working_dir);
    ADD_OPTION("-r", OPTION_TYPE_OPTIONAL, set_rerunnable);
    ADD_OPTION("-t", OPTION_TYPE_REQUIRED, set_task_id_range);
    ADD_OPTION("-tc", OPTION_TYPE_REQUIRED, set_task_limit);
    ADD_OPTION("-hold_jid", OPTION_TYPE_REQUIRED, set_dependency);
    ADD_OPTION("-hard", OPTION_TYPE_NONE, set_hard_limit);
    ADD_OPTION("-soft", OPTION_TYPE_NONE, set_soft_limit);
    ADD_OPTION("-i", OPTION_TYPE_REQUIRED, set_in_path);
    ADD_OPTION("-sync", OPTION_TYPE_REQUIRED, set_sync_job);
    ADD_OPTION("-help", OPTION_TYPE_NONE, set_is_help);
    ADD_OPTION("-version", OPTION_TYPE_NONE, set_is_version);
    ADD_OPTION("-debug", OPTION_TYPE_NONE, set_is_debug);

    #undef ADD_OPTION
}

static int fill_job_desc_from_opt(job_desc_msg_t* job, qsub_args_t* qargs) {
    
    /* init some Members */
    job->mail_type = 0;

    if (qargs->interpreter.set || qargs->script != NULL) {
        char* input = qargs->script;
        char* first_line_end = strchr(input, '\n');
        size_t input_len = strlen(input);
        size_t new_script_len = 0;
        char* new_script = NULL;
        char* final_interpreter = NULL;  // For environment variable setting
        char* default_interpreter = "#!/bin/bash";

        // Handle interpreter prefix
        if (qargs->interpreter.set && qargs->interpreter.value) {
            size_t interpreter_len = strlen(qargs->interpreter.value) + 3;
            final_interpreter = (char*)xmalloc(interpreter_len);
            snprintf(final_interpreter, interpreter_len, "#!%s", qargs->interpreter.value);
        } else {
            final_interpreter = xstrdup(default_interpreter);
        }

        // Process script based on whether interpreter is set
        if (qargs->interpreter.set) {
            // Check if the script is using the #! syntax at the beginning
            int has_shebang = (input_len >= 2 && input[0] == '#' && input[1] == '!');

            if (has_shebang) {
                // Replace the first line
                size_t remaining_len = first_line_end ? (input_len - (first_line_end - input)) : 0;
                new_script_len = strlen(final_interpreter) + remaining_len + 1;
                new_script = (char*)xmalloc(new_script_len);
                snprintf(new_script, new_script_len, "%s%s", final_interpreter, first_line_end ? first_line_end : "");
            } else {
                // Insert a new first line while retaining the original content
                new_script_len = strlen(final_interpreter) + input_len + 2;
                new_script = (char*)xmalloc(new_script_len);
                snprintf(new_script, new_script_len, "%s\n%s", final_interpreter, input);
            }
            
            // Set SHELL environment variable (skip #! prefix)
            if (setenvf(NULL, "SHELL", "%s", final_interpreter + 2) < 0) {
                log_error("unable to set SHELL in environment");
            }
        } else {
            // No interpreter has been set by user
            int has_shebang = (input_len >= 2 && input[0] == '#' && input[1] == '!');
            
            if (!has_shebang) {
                // Script has no shebang, add default one
                new_script_len = strlen(default_interpreter) + input_len + 2;
                new_script = (char*)xmalloc(new_script_len);
                snprintf(new_script, new_script_len, "%s\n%s", default_interpreter, input);
                
                // Set default SHELL environment variable
                if (setenv("SHELL", "/bin/bash", 1) < 0) {
                    log_error("unable to set SHELL in environment");
                }
            } else {
                // Script already has a shebang, keep it as is
                new_script = xstrdup(input);
                // Extract interpreter from existing shebang and set SHELL
                char* shell_path = extract_shell_from_shebang(input);
                if (shell_path) {
                    if (setenv("SHELL", shell_path, 1) < 0) {
                        log_error("unable to set SHELL in environment");
                    }
                    xfree(shell_path);
                }
            }
        }

        // Clean up and update script
        xfree(qargs->script);
        qargs->script = new_script;
        xfree(final_interpreter);  // Free temporarily allocated memory
    }

    if (qargs->job_name.set) {
        job->name = xstrdup(qargs->job_name.value);
    } else {
        job->name = xstrdup(qargs->script_file);
    }

    if (qargs->project.set) {
        job->wckey = xstrdup(qargs->project.value);
    }

    if (qargs->queue.set) {
        job->partition = xstrdup(qargs->queue.value);
    } 

    if (qargs->start_time.set) {
        job->begin_time = (time_t)atoi(qargs->start_time.value);
    }

    if (qargs->deadline.set) {
        job->deadline = (time_t)atoi(qargs->deadline.value);
    }

    job->user_id = getuid();

    job->group_id = getgid();

    if (qargs->account.set) {
        job->account = xstrdup(qargs->account.value);
    }

    if (qargs->mail_list.set) { 
        job->mail_user = xstrdup(qargs->mail_list.value);
    }

    if (qargs->mail_options.set) { 
        uint16_t rc = 0;
        bool none_set = false;

        for (int i = 0; qargs->mail_options.value[i] != '\0'; i++) {
            // Process each character
            char current_char = qargs->mail_options.value[i];
            if (current_char == 'b') 
                rc |= MAIL_JOB_BEGIN;
            else if (current_char == 'e')
                rc |= MAIL_JOB_END;
            else if (current_char == 'a')
                rc |= MAIL_JOB_FAIL;
            else if (current_char == 'n') {
                rc = 0;
                none_set = true;
                break;
            } else if (current_char == 's') {
                // This option is not supported by slurm
                continue;
            }
        }
        
        if (!rc && !none_set) 
            rc = INFINITE16;
        job->mail_type |= rc;
    }
    if (job->priority == NO_VAL) 
        job->priority = 1024;
    if (qargs->priority.set) {
        int a = atoi(qargs->priority.value);
        job->priority = (uint32_t)(a + 1024);
    }

    if (qargs->user_hold.set) { 
        job->priority = 0;
    }

    if (qargs->rerunnable.set) {
        if (!xstrcmp("y", qargs->rerunnable.value) || !xstrcmp("yes", qargs->rerunnable.value))
            job->requeue = 1;
    }

    if (qargs->resources.set) {
        char *saveptr = NULL, *token = NULL;
        char *input_copy = xstrdup(qargs->resources.value);
        token = strtok_r(input_copy, ",", &saveptr);
        while (token != NULL) {
            char* saveptr2;
            char* key = strtok_r(token, "=", &saveptr2);
            char* value = strtok_r(NULL, "=", &saveptr2);
            if (key != NULL && value != NULL) {
                if(xstrcmp(key, KEY_MEM_FREE) == 0) {
                    if ((job->pn_min_memory = str_to_mbytes(value)) == NO_VAL64) {
                        log_error("Unable to run job: attribute \"%s\" is not a memory value", KEY_MEM_FREE);
                        return -1;
                    }
                } else if (xstrcmp(key, KEY_NUM_PROC) == 0) {
		            job->cpus_per_task = (uint16_t)atoi(value);
                } else if (xstrcmp(key, KEY_HOSTNAME) == 0) {
                    job->req_nodes = xstrdup(value);
                } else if (xstrcmp(key, KEY_TIME_LIMIT) == 0) {
                    int hours, minutes, seconds;
                    if (sscanf(value, "%d:%d:%d", &hours, &minutes, &seconds) != 3) {
                        log_error("Unable to run job: attribute \"%s\" is not a time value", KEY_TIME_LIMIT);
                        return -1;
                    }
                    job->time_limit = hours * 60 + minutes + (seconds > 0 ? 1 : 0);
                } else if (xstrcmp(key, KEY_GPU) == 0) {
                    char *endptr = NULL;
                    int gpu_count = (int)strtol(value, &endptr, 10);
                    if (endptr == value || endptr[0] != '\0') {
                        log_error("Unable to run job: attribute \"%s\" is not a integer value", KEY_GPU);
                        return -1;
                    }
                    size_t buf_size = snprintf(NULL, 0, "gres/gpu:%d", gpu_count) + 1;
                    job->tres_per_node = (char *)xmalloc(buf_size);
                    snprintf(job->tres_per_node, buf_size, "gres/gpu:%d", gpu_count);
                }
            }
            token = strtok_r(NULL, ",", &saveptr);
        }
        xfree(input_copy);
    }

    if (qargs->task_id_range.set) {
        if (qargs->task_limit.set) {
            size_t total_len = strlen(qargs->task_id_range.value) + strlen(qargs->task_limit.value) + 2;
            job->array_inx = (char *)xmalloc(total_len * sizeof(char));
            xstrcat(job->array_inx, qargs->task_id_range.value);
            xstrcat(job->array_inx, "%");
            xstrcat(job->array_inx, qargs->task_limit.value);
        } else {
            job->array_inx = xstrdup(qargs->task_id_range.value);
        }
    }


    if (qargs->output_path.set) {
        job->std_out = xstrdup(qargs->output_path.value);
    } 

    if (qargs->error_path.set) { 
        job->std_err = xstrdup(qargs->error_path.value);
    }

    if (qargs->merge_output.set) {
        xfree(job->std_err);
        job->std_err = xstrdup(job->std_out);
    }

    // Set up dependencies
    if (qargs->dependency.set) {
        char* saveptr = NULL, *token = NULL;
        char *input_copy = xstrdup(qargs->dependency.value);
        token = strtok_r(input_copy, ",", &saveptr);
        char result[256] = "";
        int offset = 0;
        bool first = true;

        while (token) {
            if (!first) {
                offset += snprintf(result + offset, sizeof(result) - offset, ",");
            }
            offset += snprintf(result + offset, sizeof(result) - offset, "afterok:%s", token);
            first = false;

            token = strtok_r(NULL, ",", &saveptr);
        }

        xfree(job->dependency);
        job->dependency = xstrdup(result);
        xfree(input_copy);
    }

    if (qargs->parallel_env.set) {
        uint32_t min_pe = 0, max_pe = 0;
        char *temp = xstrdup(qargs->parallel_env.value);
        char *dash_pos = xstrchr(temp, '-');
        if (dash_pos != NULL) {
            *dash_pos = '\0';
            min_pe = atoi(temp);
            max_pe = atoi(dash_pos + 1);
        } else {
            max_pe = min_pe = (uint32_t)atoi(temp);
        }
        job->num_tasks = max_pe;
        xfree(temp);
    } 

    if (qargs->working_dir.set) {
        job->work_dir = qargs->working_dir.value;
    } else {
        long pc = pathconf(".", _PC_PATH_MAX);
        size_t size = (pc == -1) ? PATH_MAX : (size_t)pc;
        char *buffer = (char *)xmalloc((size_t)size);
        if (getcwd(buffer, size) != NULL)
            job->work_dir = buffer;
        else
            xfree(buffer);
    }

    job->open_mode = 2;
    if (qargs->in_path.set) {
        job->std_in = xstrdup(qargs->in_path.value);
    } else {
        job->std_in = xstrdup("/dev/null");
    }

    job->script = xstrdup(qargs->script);
    job->argc = qargs->script_argc;
    job->argv = xmalloc(sizeof(char*) * qargs->script_argc);
    for (uint32_t i = 0; i < qargs->script_argc; i++) {
        job->argv[i] = xstrdup(qargs->script_argv[i]);
    }

    job->environment = NULL;
    if (job->name) {
        setenv("SLURM_JOB_NAME", job->name, 1);
    } 
    
    env_array_merge(&job->environment, (const char **) environ);
    job->env_size = envcount(job->environment);
    return 0;

}

static void load_script(qsub_args_t *qargs) {
    FILE *fp = NULL;
    char *script_content = NULL;
    size_t script_size = 0;
    qargs->script_file_argc = 0;
    // If qargs->script_file is empty, read from standard input instead.
    if (qargs->script_file == NULL) {
        fp = stdin;
        qargs->script_file_argv[qargs->script_file_argc++] = xstrdup("STDIN");
        qargs->script_file = xstrdup("STDIN");
    } else {
        fp = fopen(qargs->script_file, "r");
        if (!fp) {
            log_error("Error opening script file : %s", qargs->script_file);
            return;
        }
        qargs->script_file_argv[qargs->script_file_argc++] = xstrdup(qargs->script_file);
    }

    char line[MAX_LINE_LEN];
    int has_input = 0;

    // Read the script content
    while (fgets(line, sizeof(line), fp)) {
        has_input = 1;  // Mark at least one line of input

        // Dynamicly expand the "script_content" to store the entire script.
        size_t line_len = strlen(line);
        char *new_script_content = xrealloc(script_content, script_size + line_len + 1);
        if (!new_script_content) {
            log_error("Memory allocation faile");
            xfree(script_content);
            return;
        }
        script_content = new_script_content;
        strcpy(script_content + script_size, line);
        script_size += line_len;

        // Parse parameters that start with "#$"
        if (xstrncmp(line, "#$", 2) == 0) {
            char *arg = line + 2;  // Skip "#$"
            while (*arg == ' ' || *arg == '\t') arg++;  // Skip spaces and tabs

            if (*arg != '\0') {
                // Remove line breaks
                char *newline = xstrchr(arg, '\n');
                if (newline) *newline = '\0';

                // Analyze each parameter one by one
                char *token = strtok(arg, " \t");
                while (token != NULL) {
                    if (token[0] == '#') break;
                    if (qargs->script_file_argc < MAX_ARGS) {
                        qargs->script_file_argv[qargs->script_file_argc] = xstrdup(token);
                        qargs->script_file_argc++;
                    }
                    token = strtok(NULL, " \t");
                }
            }
        }
    }

    // If there is no input, an error will occur.
    if (!has_input) {
        log_error("Error: No input received from script file or standard input.");
    }

    if (fp != stdin) {
        fclose(fp);
    }

    qargs->script = script_content;
}

static void parse_args(int argc, char *argv[], qsub_args_t* qargs, bool is_cmd) {

    for (int i = 1; i < argc; i++) { // Starting from 1, skip the program name
        bool matched = false;

        if (is_cmd && *argv[i] != '-') {
            qargs->script_file = xstrdup(argv[i]);
            qargs->script_argv[qargs->script_argc++] = xstrdup(argv[i++]);

            while (i < argc && qargs->script_argc < MAX_ARGS) {
                qargs->script_argv[qargs->script_argc++] = xstrdup(argv[i++]);
            }
            break;
        }
       
        // Traverse all parameter options
        for (size_t j = 0; j < qargs->option_count; j++) {
            Option *opt = qargs->option_ptrs[j];

            // Check if the option names match
            if (strcmp(argv[i], opt->name) == 0) {
                matched = true;

                // Determine the type of the options and adopt different processing logic accordingly
                switch (opt->type) {
                    case OPTION_TYPE_NONE:
                        opt->handler(qargs, NULL);
                        break;
                    case OPTION_TYPE_REQUIRED:
                        // Required parameter
                        if (i + 1 < argc) { // Determine if there is a value
                            opt->handler(qargs, argv[i + 1]);
                            i++;
                        } else {
                            log_error("Error: Missing value for required option %s", argv[i]);
                        }
                        break;
                    case OPTION_TYPE_OPTIONAL:
                        // Optional parameters
                        if (i + 1 < argc) { // Determine if there is a value
                            opt->handler(qargs, argv[i + 1]);
                            i++;
                        } else {
                            opt->handler(qargs, NULL);
                        }
                        break;
                    case OPTION_TYPE_MULTIPLE:
                        // Multiple parameters
                        {
                            char *combined_values = NULL;
                            size_t combined_len = 0;
                            int k = i + 1;
                            int param_count = 0; // The number of counting parameters

                            // If opt->name is "-pe", only take the two parameters
                            if (xstrcmp(opt->name, "-pe") == 0) {
                                // Only take two parameters
                                while (k < argc && param_count < 2 && argv[k][0] != '-') {
                                    size_t arg_len = strlen(argv[k]);
                                    size_t new_len = combined_len + arg_len + (combined_len > 0 ? 1 : 0); // An additional 1 is used for the space.

                                    char *new_combined = xrealloc(combined_values, new_len + 1);
                                    if (!new_combined) {
                                        xfree(combined_values);
                                        log_error("Memory allocation failed");
                                        exit(EXIT_FAILURE);
                                    }
                                    combined_values = new_combined;

                                    if (combined_len > 0) {
                                        xstrcat(combined_values, " ");
                                    } else {
                                        combined_values[0] = '\0';
                                    }
                                    xstrcat(combined_values, argv[k]);

                                    combined_len = new_len;
                                    param_count++;
                                    k++;
                                }
                            } else {
                                // Default handling of multiple parameters
                                while (k < argc && argv[k][0] != '-') {
                                    size_t arg_len = strlen(argv[k]);
                                    size_t new_len = combined_len + arg_len + (combined_len > 0 ? 1 : 0);

                                    char *new_combined = xrealloc(combined_values, new_len + 1);
                                    if (!new_combined) {
                                        xfree(combined_values);
                                        log_error("Memory allocation failed");
                                        exit(EXIT_FAILURE);
                                    }
                                    combined_values = new_combined;

                                    if (combined_len > 0) {
                                        xstrcat(combined_values, " ");
                                    } else {
                                        combined_values[0] = '\0';
                                    }
                                    xstrcat(combined_values, argv[k]);

                                    combined_len = new_len;
                                    k++;
                                }
                            }

                            opt->handler(qargs, combined_values);

                            xfree(combined_values);
                            i = k - 1;
                        }
                        break;
                }
                break;
            }
        }

        if (!matched) {
            log_error("unknown option : %s", argv[i]);
            // exit(1); /* Just display the error message, without affecting the program's continued operation. */
        }
    }
}

static void set_environment_variables() {
    _set_submit_dir_env();
    _set_prio_process_env();
    _set_umask_env();
}

static void parse_all_args(int argc, char *argv[], qsub_args_t *qargs) {
    parse_args(argc, argv, qargs, true);
    if (qargs->is_help.set) {
        return; 
    }
    if (qargs->is_version.set) {
        return;
    }
    load_script(qargs);
    parse_args(qargs->script_file_argc, qargs->script_file_argv, qargs, false);
}

static void free_all_mem(job_desc_msg_t *job, qsub_args_t *qargs, submit_response_msg_t *resp) {
    _free_job_desc_msg_memory(job);
    _free_qsub_args(qargs);
    slurm_free_submit_response_response_msg(resp);
}

static void display_help_info() {
    log_info("SGE 8.1.9");
    log_info("usage: qsub [options]");
    log_info("   [-a date_time]                           request a start time");
    // log_info("   [-ac context_list]                       add context variable(s)");
    // log_info("   [-Ap fname]                              add a new parallel environment from file");
    // log_info("   [-astnode node_shares_list]              add sharetree node(s)");
    // log_info("   [-at thread_name]                        add/start qmaster thread");
    log_info("   [-A account_string]                      account string in accounting record");
    log_info("   [-b y[es]|n[o]]                          handle command as binary");
    log_info("   [-binding [env|pe|set] exp|lin|str]      binds job to processor cores");
    // log_info("   [-c ckpt_selector]                       define type of checkpointing for job");
    log_info("   [-cwd]                                   use current working directory");
    // log_info("   [-C directive_prefix]                    define command prefix for job script");
    // log_info("   [-dc simple_context_list]                delete context variable(s)");
    // log_info("   [-dul listname_list]                     delete userset list(s) completely");
    log_info("   [-e path_list]                           specify standard error stream path(s)");
    log_info("   [-h]                                     place user hold on job");
    log_info("   [-hard]                                  consider following requests \"hard\"");
    // log_info("   [-he  y[es]|n[o]]                        enable/disable hard error handling");
    log_info("   [-help]                                  print this help");
    log_info("   [-hold_jid job_identifier_list]          define jobnet interdependencies");
    // log_info("   [-hold_jid_ad job_identifier_list]       define jobnet array interdependencies");
    log_info("   [-i file_list]                           specify standard input stream file(s)");
    log_info("   [-j y[es]|n[o]]                          merge stdout and stderr stream of job");
    // log_info("   [-js job_share]                          share tree or functional job share");
    // log_info("   [-jsv jsv_url]                           job submission verification script to be used");
    log_info("   [-l resource_list]                       request the given resources");
    log_info("   [-M mail_list]                           notify these e-mail addresses");
    log_info("   [-m mail_options]                        define mail notification events");
    // log_info("   [-masterq wc_queue_list]                 bind master task to queue(s)");
    // log_info("   [-mattr obj_nm attr_nm val obj_id_list]  modify an attribute (or element in a sublist) of an object");
    log_info("   [-N name]                                specify job name");
    log_info("   [-notify]                                notify job before killing/suspending it");
    // log_info("   [-now y[es]|n[o]]                        start job immediately or not at all");
    log_info("   [-o path_list]                           specify standard output stream path(s)");
    // log_info("   [-ot tickets]                            set job's override tickets");
    log_info("   [-P project_name]                        set job's project");
    log_info("   [-p priority]                            define job's relative priority");
    log_info("   [-pe wc_pe_name slot_range]              request slot range for parallel jobs");
    log_info("   [-q wc_queue_list]                       bind job to queue(s)");
    log_info("   [-r y[es]|n[o]]                          define job as (not) restartable");
    // log_info("   [-sc context_list]                       set job context (replaces old context)");
    // log_info("   [-shell y[es]|n[o]]                      start command with or without wrapping <loginshell> -c");
    log_info("   [-soft]                                  consider following requests as soft");
    // log_info("   [-srqs [rqs_list]]                       show resource quota set(s)");
    log_info("   [-sync y[es]|n[o]]                       wait for job to end and return exit code");
    // log_info("   [-S path_list]                           command interpreter to be used");
    log_info("   [-t task_id_range]                       create a job-array with these tasks");
    // log_info("   [-v variable_list]                       export these environment variables");
    // log_info("   [-verify]                                do not submit, just verify");
    log_info("   [-V]                                     export all environment variables");
    log_info("   [-wd working_directory]                  use working_directory");
    // log_info("   [-@ file]                                read commandline input from file");
    log_info("   [{command|-} [command_args]]");

    log_info("account_string          account_name");
    log_info("complex_list            complex[,complex,...]");
    log_info("context_list            variable[=value][,variable[=value],...]");
    log_info("ckpt_selector           `n' `s' `m' `x' `r' <interval>");
    log_info("date_time               [[CC]YY]MMDDhhmm[.SS]");
    log_info("job_identifier_list     {job_id|job_name|reg_exp}[,{job_id|job_name|reg_exp},...]");
    log_info("jsv_url                 [script:][username@]path");
    log_info("listname_list           listname[,listname,...]");
    log_info("rqs_list                rqs_name[,rqs_name,...]");
    log_info("mail_address            username[@host]");
    log_info("mail_list               mail_address[,mail_address,...]");
    log_info("mail_options            `e' `b' `a' `n' `s'");
    log_info("node_path               [/]node_name[[/.]node_name...]");
    log_info("node_shares_list        node_path=shares[,node_path=shares,...]");
    log_info("working_directory       path");
    log_info("path_list               [host:]path[,[host:]path,...]");
    log_info("priority                -1023 - 1024");
    log_info("resource_list           resource[=value][,resource[=value],...]");
    log_info("simple_context_list     variable[,variable,...]");
    log_info("slot_range              [n[-m]|[-]m] - n,m > 0");
    log_info("task_id_range           task_id['-'task_id[':'step]]");
    log_info("variable_list           variable[=value][,variable[=value],...]");
    log_info("obj_nm                  \"queue\"|\"exechost\"|\"pe\"|\"ckpt\"|\"hostgroup\"|\"resource_quota\"");
    log_info("attr_nm                 (see man pages)");
    log_info("obj_id_list             objectname [ objectname ...]");
    log_info("wc_cqueue               wildcard expression matching a cluster queue");
    log_info("wc_host                 wildcard expression matching a host");
    log_info("wc_hostgroup            wildcard expression matching a hostgroup");
    log_info("wc_qinstance            wc_cqueue@wc_host");
    log_info("wc_qdomain              wc_cqueue@wc_hostgroup");
    log_info("wc_queue                wc_cqueue|wc_qdomain|wc_qinstance");
    log_info("wc_queue_list           wc_queue[,wc_queue,...]");
    log_info("thread_name             \"scheduler\"|\"jvm\"");
    log_info("exp                     explicit:<socket>,<core>[:...]");
    log_info("lin                     linear[:<amount>[:<socket>,<core>]]");
    log_info("str                     striding:<amount>:<stepsize>[:<socket>,<core>]");
}

static void display_version_info() {
    log_info("METASTACK SGE WRAPPER");
    log_info("Version: %s", PROGRAM_VERSION);
    log_info("Release Date: %s", RELEASE_DATE);
}

/**
 *  If '-help' or '-version' is set, display the corresponding information and release the resources.
 */
static bool handle_help_or_version(qsub_args_t *qargs) 
{
    if (qargs->is_help.set) {
        display_help_info();
        return 1;
    }

    if (qargs->is_version.set) {
        display_version_info();
        return 1;
    }

    return 0;
}

int main(int argc, char *argv[]) {
    qsub_args_t *qargs = xmalloc(sizeof(qsub_args_t));
    job_desc_msg_t *job = xmalloc(sizeof(job_desc_msg_t));
    submit_response_msg_t *resp = NULL;
    int retry = 0;
    slurm_init(NULL);
    slurm_init_job_desc_msg(job);

    job->submit_line = slurm_option_get_argv_str(argc, argv);

    init_qsub_args(qargs);
    parse_all_args(argc, argv, qargs);

    if (handle_help_or_version(qargs)) {
        free_all_mem(job, qargs, resp);
        return 0;
    }

    set_environment_variables();

    if (fill_job_desc_from_opt(job, qargs)){
        log_error("Exiting.");
        goto free_mem;
    }

    if (qargs->is_debug.set) {
        print_job_desc(job);
        print_args(qargs);
    }

    while (true) {
        int rc = slurm_submit_batch_job(job, &resp);
        static char *msg;
        if (rc > 0) 
            break;

        int err = *__errno_location();

        if (err == 2000) {
            log_info("Unable to run job: Job was rejected because job requests unknown queue \"%s\"", job->partition);
            break;
        } else if (err == 2007) {
            msg = "job queue full, sleeping and retrying";
        } else if (err == 1800 || err == 5004 || err == 107) {
            msg = "controller connect failure, sleeping and retrying";
        } else if (err == 2016) {
            msg = "job creation temporarily disabled, retrying";
        } else if (err == 11) {
            msg = "temporarily unable to accept job, sleeping and retrying";
        } else
            msg = NULL;

        if ((msg == NULL) || (retry > 14)) {
            if(strcmp("No error", slurm_strerror(err))) {
                log_error("%s", slurm_strerror(err));
                goto free_mem;
            } else 
                break;
        } 

        log_info("%s", msg);

        slurm_free_submit_response_response_msg(resp);
        ++retry;
        sleep(retry);

    }

    /* Sge-style job submission output information */
    if (resp && job) {
        if (qargs->task_id_range.set){
            if (qargs->task_limit.set == false)
                log_info("Your job-array %u.%s:1 (\"%s\") has been submitted", resp->job_id, qargs->task_id_range.value, job->name);
            else
                log_info("Your job-array %u.%s:%s (\"%s\") has been submitted", resp->job_id, qargs->task_id_range.value, qargs->task_limit.value, job->name);
        } else {
            log_info("Your job %u (\"%s\") has been submitted", resp->job_id, job->name);
        }
    } else {
        log_info("Exiting.");
    }
    if (qargs->sync_job.set && 
        (strcasecmp("y", qargs->sync_job.value) == 0 || 
        strcasecmp("yes", qargs->sync_job.value) == 0)) {
        _job_wait(resp->job_id);
    }

    free_all_mem(job, qargs, resp);
    return 0;
free_mem:
    free_all_mem(job, qargs, resp);
    return 1;
}

