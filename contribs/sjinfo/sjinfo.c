/*****************************************************************************\
 *  sjinfo.c - implementation-independent job of influxdb info
 *  functions
 *****************************************************************************
 *  Copyright (C) 2005 Hewlett-Packard Development Company, L.P.

 *  Slurm is free software; you can redistribute it and/or modify it under
 *  the terms of the GNU General Public License as published by the Free
 *  Software Foundation; either version 2 of the License, or (at your option)
 *  any later version.
 *
 *  In addition, as a special exception, the copyright holders give permission
 *  to link the code of portions of this program with the OpenSSL library under
 *  certain conditions as described in each individual source file, and
 *  distribute linked combinations including the two. You must obey the GNU
 *  General Public License in all respects for all of the code used other than
 *  OpenSSL. If you modify file(s) with this exception, you may extend this
 *  exception to your version of the file(s), but you are not obligated to do
 *  so. If you do not wish to do so, delete this exception statement from your
 *  version.  If you delete this exception statement from all source files in
 *  the program, then also delete it here.
 *
 *  Slurm is distributed in the hope that it will be useful, but WITHOUT ANY
 *  WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 *  FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more
 *  details.
 *
 *  You should have received a copy of the GNU General Public License along
 *  with Slurm; if not, write to the Free Software Foundation, Inc.,
 *  51 Franklin Street, Fifth Floor, Boston, MA 02110-1301  USA.
\*****************************************************************************/

/*****************************************************************************\
 *  Modification history
 *  
\*****************************************************************************/
#include <stdio.h>
#include <time.h>
#include <string.h>
#include <strings.h>
#include <stdint.h>
#include <stdlib.h>
#include <stddef.h>
#include <stdbool.h>
#include <curl/curl.h>
#include <stdarg.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <getopt.h>
#include <unistd.h>
#include <pwd.h>
#include <assert.h>
#include <ctype.h>
#include <inttypes.h>
#include <errno.h>
#include <math.h> 
#include "sjinfo.h"
#include "aes.h"
#include "xstring2.h"
#include "list2.h"
#include "influx_src.h" 
#include "time_format.h"
#include "sql_api.h"
#include "print_json.h"
/* Names for the values of the `has_arg' field of `struct option'.  */
#define no_argument		0
#define required_argument	1
#define optional_argument	2
#define SJINFO_VERSION_STRING "SLURM220508-0.0.4"
#define PACKAGE_NAME "sjinfo "


List print_display_list = NULL;

/*stepd table*/
List print_fields_list = NULL;
List print_value_list = NULL;
list_itr_t *print_fields_itr = NULL;

List print_query_value_list = NULL;

/*event table*/
List print_events_list = NULL;
List print_events_value_list = NULL;
list_itr_t *print_events_itr = NULL;

/*overall table*/
List print_overall_list = NULL;
List print_overall_value_list = NULL;
list_itr_t *print_overall_itr = NULL;

/*apptype table*/
List print_apptype_list = NULL;
List print_apptype_value_list = NULL;
list_itr_t *print_apptype_itr = NULL;

/*apptype table only job*/
List print_apptype_job_list = NULL;
List print_apptype_job_value_list = NULL;
list_itr_t *print_apptype_job_itr = NULL;

/* job summary table */
List print_job_summary_list = NULL;
List print_job_summary_value_list = NULL;
list_itr_t *print_job_summary_itr = NULL;

/*
 *External application output data memory, used to store decrypted data
 */
long int jobid_digit = 0;
c_string_t *job_list = NULL;
sjinfo_parameters_t params;

label_flags_t sql_labels; 
join_sql_t    sql_option;
void print_fields_header(list_t *print_fields_list);
int parse_sacct_line(const char *line, int count, List print_head_list);
extern void destroy_query_key_pair(void *object)
{
	spost_record_t *key_query_ptr = (spost_record_t *)object;
	if (key_query_ptr) {
        xfree(key_query_ptr->nodename);
		xfree(key_query_ptr->username);
		xfree(key_query_ptr->data);
    	xfree(key_query_ptr->uid);    
		xfree(key_query_ptr);
	}
}

/* print this version of sjinfo */
void print_sjinfo_version(void)
{
	printf("%s %s\n", PACKAGE_NAME, SJINFO_VERSION_STRING);
}

/* print help */
void print_sjinfo_help(void)
{
    printf(
"sjinfo [<OPTION>]                                                         \n"
"    Valid <OPTION> values are:                                            \n"
"     -a, --all:                                                      \n"
"        When this value is specified, it is equivalent to specifying -l, -A, \n"
"        and -O simultaneously.                                             \n"
"     -A, --abnormal:                                                      \n"
"        Displays information about abnormal events during job execution   \n"
"     -d, --desc:                                                      \n"
"        Output data in reverse order  \n"
"     -D, --display:                                                      \n"
"       Print brief job information. CPU Efficiency and MEM Efficiency are normalized values. \n"
"     -e, --event:                                                        \n"
"        Print the abnormal events of the jobs.                            \n"
"        Supported fields:                                                 \n"
"        CPUUSA - CPU Utilization State Anomaly                            \n"
"        PidSA - Process State Anomaly                                    \n"
"        NodeSA - Node State Anomaly                                      \n"
"        GPUUSA - GPU Utilization State Anomaly                           \n"
"     -E, --end:                                                           \n"
"        The end time of the abnormal event.                              \n"
"     -h, --help:                                                          \n"
"        Help manual.                                                      \n"
"     -j, --jobs:                                                          \n"
"        Specify the job ID.                                               \n"
"     -J, --job-summary:                                                   \n"
"        Display job step data aggregated to job level                           \n"
"     -l, --load:                                                           \n"
"        Displays load information during job run time          \n"
"     -o, --format:                                                        \n"
"        Print a list of fields that can be specified with the            \n"
"        '--format' option                                                 \n"
"        '--format='    JobID,StepID,StepCPU,StepAVECPU,StepMEM,StepVMEM,         \n"
"                       StepPages,MaxStepCPU,MinStepCPU,MaxStepMEM,            \n"
"                       MinStepMEM,MaxStepVMEM,MinStepVMEM,CPUthreshold,GRESthreshold,\n"
"                       Start,End,Type,Last_start,Last_end,CPU_Abnormal_CNT,     \n"
"                       PROC_Abnormal_CNT,NODE_Abnormal_CNT,GPU_Abnormal_CNT     \n"
"                                                                           \n"
"        Fields related to resource consumption:                           \n"
"        JobID:         Job ID                                                 \n"
"        StepID:        Job step ID                                            \n"
"        StepCPU:       CPU utilization within job step anomaly detection interval. \n"
"        StepAVECPU:    Average CPU utilization of job step.                 \n"
"        StepMEM:       Real-time usage of job step memory.                    \n"
"        StepVMEM:      Real-time virtual memory usage of job steps.          \n"
"        StepPages:     Job step pagefault real-time size during                \n"
"                       the current job step running cycle.                    \n"
"        MaxStepCPU:    Maximum CPU utilization during the current job        \n"
"                       step running cycle.                                    \n"
"        MinStepCPU:    Minimum CPU utilization during the current job        \n"
"                       step running cycle.                                    \n"
"        MaxStepMEM:    Maximum memory value within the current                \n"
"                       job step running cycle.                                \n"
"        MinStepMEM:    Minimum memory value within the current job            \n"
"                       step running cycle.                                    \n"
"        MaxStepVMEM:   The maximum value of virtual memory during            \n"
"                       the current job step running cycle.                   \n"
"        MinStepVMEM:   The minimum value of virtual memory during the        \n"
"                       current job step running cycle.                       \n"
"                                                                             \n"
"        Fields related to job summary:                                    \n"
"        TotalCPU:      Total CPU utilization across all job steps            \n"
"        TotalMEM:      Total memory usage across all job steps               \n"
"        TotalVMEM:     Total virtual memory usage across all job steps       \n"
"        TotalPages:    Total page faults across all job steps                \n"
"        MaxCPU:        Maximum CPU utilization across all job steps          \n"
"        MinCPU:        Minimum CPU utilization across all job steps          \n"
"        MaxMEM:        Maximum memory usage across all job steps             \n"
"        MinMEM:        Minimum memory usage across all job steps             \n"
"        MaxVMEM:       Maximum virtual memory usage across all job steps     \n"
"        MinVMEM:       Minimum virtual memory usage across all job steps     \n"
"        TotalDCU:      Total DCU usage across all job steps                  \n"
"        TotalDCUMEM:   Total DCU memory usage across all job steps           \n"
"        MaxDCU:        Maximum DCU usage across all job steps                \n"
"        MinDCU:        Minimum DCU usage across all job steps                \n"
"        MaxDCUMEM:     Maximum DCU memory usage across all job steps         \n"
"        MinDCUMEM:     Minimum DCU memory usage across all job steps         \n"
"                                                                             \n"
"                                                                             \n"
"        Fields related to abnormal events:                                \n"
"        CPUthreshold:  Set the CPU utilization threshold for a job.         \n"
"        GRESthreshold: Set the GPU utilization threshold for a job.         \n"
"        Start:         The start time of the abnormal event.                  \n"
"        End:           The end time of the exception event.                   \n"
"        Type:          Type of abnormal event.                                \n"
"                                                                             \n"
"        Fields related to abnormal events overall:                             \n"
"        Last_start:    The start time of the most recent anomaly.        \n"
"        Last_end:      The end time of the most recent anomaly.                \n"
"        CPU_Abnormal_CNT:       Total number of CPU abnormal events.                   \n"
"        PROC_Abnormal_CNT:       Total number of PROCESS abnormal events.                    \n"
"        NODE_Abnormal_CNT:      Total number of NODE abnormal events.                    \n"
"        GPU_Abnormal_CNT:       Total number of GPU abnormal events.                    \n"
"     -O, --overall:                                                           \n"
"        Displays general information about the abnormal event          \n"
// "     -r, --running:                                                           \n"
// "        Display running job data (this option depends on the acquisition\n"
// "        interval set by the job-monitor and may not be real-time)          \n"
// "        When retrieving a running job, the time interval is limited to one hour, \n"
// "        meaning that any data that has not been updated in more than one hour is \n"
// "        considered finished. This will cover the vast majority of cases. If the \n"
// "        data collection interval is longer than one hour, you can use -S to \n"
// "        specify a larger interval.\n"
"     -s, --steps:                                                          \n"
"        Specify the steps.                                               \n"
"     -S, --start:                                                         \n"
"        The start time of the job's abnormal event (e.g., 2024-05-07T08:00:00). \n"
"     -t, --apptype:                                                          \n"
"        Displays application type information for the job. Specifying any  \n"
"        parameter will display the application type information at the job step level \n"
"     -V, --version:                                                       \n"
"        Print sjinfo version.                                              \n"
"     -m                                                                    \n"
"        Convert KB to MB (default is in KB).                               \n"
"     -g                                                                    \n"
"        Convert KB to GB (default is in KB).                               \n"
"     -q,--query                                                            \n"
"        Query user-defined messages.                                       \n"
"        jobid: Job ID.                                                     \n"
"        Username: user name.                                               \n"
"        StepID: Job Step ID,                                               \n"
"        If \"JSNS\" appears in the job step, it indicates that the         \n"
"        job step was not specified when the spost was written.             \n"
"        Uid: User UID                                                      \n"
"        Messages: User-defined messages                                    \n"
"        PostTime: The time when the user sent the data to the database     \n"
"\n");
}

extern void destroy_config_value(void *object)
{
	interface_sjinfo_t *key_pair_ptr = (interface_sjinfo_t *)object;

	if (key_pair_ptr) {
		xfree(key_pair_ptr->username);
        xfree(key_pair_ptr->type);
        xfree(key_pair_ptr->apptype_cli);
        xfree(key_pair_ptr->apptype_step);
        xfree(key_pair_ptr->apptype);
		xfree(key_pair_ptr);
	}
    
}

extern void destroy_config_key_pair(void *object)
{
	interface_sjinfo_t *key_pair_ptr = (interface_sjinfo_t *)object;

	if (key_pair_ptr) {
		xfree(key_pair_ptr->username);
        xfree(key_pair_ptr->type);
        xfree(key_pair_ptr->apptype_cli);
        xfree(key_pair_ptr->apptype_step);
        xfree(key_pair_ptr->apptype);
		xfree(key_pair_ptr);
	}
}

extern void destroy_brief_print_key_pair(void *object)
{
	interface_sjinfo_t *key_pair_ptr = (interface_sjinfo_t *)object;

	if (key_pair_ptr) {
		xfree(key_pair_ptr->username);
        xfree(key_pair_ptr->type);
        xfree(key_pair_ptr->apptype_cli);
        xfree(key_pair_ptr->apptype_step);
        xfree(key_pair_ptr->apptype);
		xfree(key_pair_ptr);
	}
}

void sjinfo_init(slurm_influxdb *influxdb_data)
{
    /*given sufficient length*/
    influxdb_data->username = xmalloc(32) ;
    influxdb_data->password = xmalloc(32);
    influxdb_data->database = xmalloc(640) ;
    influxdb_data->host = xmalloc(640);
    influxdb_data->policy = xmalloc(640);
    /*display table*/
    print_display_list = list_create(destroy_brief_print_key_pair);
	// print_display_itr = list_iterator_create(print_display_list);

    /*step table*/
	print_fields_list = list_create(NULL);
	print_fields_itr = list_iterator_create(print_fields_list);

    print_value_list = list_create(destroy_config_value);
    /*event table*/
    print_events_list = list_create(NULL);
	print_events_itr = list_iterator_create(print_events_list);
    print_events_value_list = list_create(destroy_config_key_pair);

    /*event overall table*/
    print_overall_list = list_create(NULL);
	print_overall_itr = list_iterator_create(print_overall_list);
    print_overall_value_list = list_create(destroy_config_key_pair);

    /*apptype table*/
    print_apptype_list = list_create(NULL);
	print_apptype_itr = list_iterator_create(print_apptype_list);
    print_apptype_value_list = list_create(destroy_config_key_pair);

    /*apptype job table*/
    print_apptype_job_list = list_create(NULL);
	print_apptype_job_itr = list_iterator_create(print_apptype_job_list);
    print_apptype_job_value_list = list_create(destroy_config_key_pair);

    print_job_summary_list = list_create(NULL);
    print_job_summary_itr = list_iterator_create(print_job_summary_list);
    print_job_summary_value_list = list_create(destroy_config_key_pair);

    print_query_value_list = list_create(destroy_query_key_pair);

    job_list = c_string_create();
}

void sjinfo_fini(slurm_influxdb * influxdb_data)
{
    xfree(influxdb_data->username);
    xfree(influxdb_data->password);
    xfree(influxdb_data->database);
    xfree(influxdb_data->host);
    xfree(influxdb_data->policy);
    if(print_display_list)
	    FREE_NULL_LIST(print_display_list);

    /*step table*/
	if (print_fields_itr)
		list_iterator_destroy(print_fields_itr);
    if(print_fields_list)
	    FREE_NULL_LIST(print_fields_list);
    if(print_value_list)
	    FREE_NULL_LIST(print_value_list);

    /*event table*/
    if(print_events_itr)
        list_iterator_destroy(print_events_itr);
    if(print_events_list)
        FREE_NULL_LIST(print_events_list);
    if(print_events_value_list)
	    FREE_NULL_LIST(print_events_value_list);
    
    /*apptype table*/
    if(print_apptype_job_itr)
        list_iterator_destroy(print_apptype_job_itr);
    if(print_apptype_job_list)
        FREE_NULL_LIST(print_apptype_job_list);
    if(print_apptype_itr)
        list_iterator_destroy(print_apptype_itr);
    if(print_apptype_list)
        FREE_NULL_LIST(print_apptype_list);
    if(print_apptype_value_list)
	    FREE_NULL_LIST(print_apptype_value_list);
    if(print_apptype_job_value_list)
	    FREE_NULL_LIST(print_apptype_job_value_list);
    
    /*event overall table*/
    if(print_overall_itr)
        list_iterator_destroy(print_overall_itr);
    if(print_overall_list)
        FREE_NULL_LIST(print_overall_list);
    if(print_overall_value_list)
	    FREE_NULL_LIST(print_overall_value_list);
    if(print_query_value_list)
	    FREE_NULL_LIST(print_query_value_list);   

     /* job summary table */
    if(print_job_summary_itr)
        list_iterator_destroy(print_job_summary_itr);
    if(print_job_summary_list)
        FREE_NULL_LIST(print_job_summary_list);
    if(print_job_summary_value_list)
        FREE_NULL_LIST(print_job_summary_value_list);
}

int parse_command_and_query(int argc, char **argv, slurm_influxdb *data, query_job_record_t *query_send) 
{
    int rc = 0;
	int c, optionIndex = 0;
    // char* steps = NULL;

    //char* events = NULL;
    sql_labels.start_label = false;
    sql_labels.end_label   = false;
    sql_labels.user_label  = false;
    sql_labels.jobid_out   = false;
    sql_labels.step_out    = false;
    sql_labels.no_step     = false;
    sql_labels.no_jobid    = false;
    sql_labels.query_label = false;

    if(data == NULL || query_send == NULL) {
        rc = SLURM_ERROR;
        return rc;
    }

    struct passwd *pw;
    /* record start time */
    sql_option.usage_start = 0;
    /* record end time */
    sql_option.usage_end = time(NULL);
    
    params.convert_flags = CONVERT_NUM_UNIT_EXACT;
	params.units = NO_VAL;
	params.opt_uid = getuid();
	params.opt_gid = getgid();
    /*
        |-e|overall|event|load|
    */
    params.level = INFLUXDB_NONE;
    assert(params.opt_uid != -1);
    pw = getpwuid(params.opt_uid);
    if(!pw) {
        return SLURM_ERROR;
    }
	static struct option long_options[] = {
                {"abnormal",    no_argument,        0,      'A'},
                {"all",         no_argument,        0,      'a'},
                {"desc",        no_argument,        0,      'd'},        
                {"display ",    no_argument,        0,      'D'},
                {"event",       required_argument,  0,      'e'},
                {"end",         required_argument,  0,      'E'},
                {"help",        no_argument,        0,      'h'},
                {"jobs",        required_argument,  0,      'j'},
                {"job-summary", no_argument,        0,      'J'},
                {"load",        no_argument,        0,      'l'},
                {"format",      required_argument,  0,      'o'},
                {"running",     no_argument,        0,      'r'},
                {"steps",       required_argument,  0,      's'},
                {"start",       required_argument,  0,      'S'}, 
                {"apptype",     optional_argument,  0,      't'},
                {"user",        required_argument,  0,      'u'},
                {"query",       no_argument,        0,      'q'},         
                {"version",     no_argument,        0,      'V'},
                {"overall",     no_argument,        0,      'O'},
                {0,             0,                  0,      0}};
    
    optind = 0;
    while ((c = getopt_long(argc, argv,
				       "dt:e:E:j:s:lo:rS:u:VOmgaAhqDJ",
				       long_options, &optionIndex)) != -1) {   
        if (c == -1) {
            sql_labels.no_jobid = true;
            sql_labels.no_step = true;
        }
        switch (c) {
            case 'a':
                params.level |= INFLUXDB_ALL;
                break;
            case 'A':
                params.level |= INFLUXDB_EVENT;
                break;
            case 'd':
                params.desc_set = true;
                break;
            case 'D':
                params.display = true;
                params.level |= INFLUXDB_DISPLAY;
                //params.level |= INFLUXDB_STEPD;
                break;
        	case 'e':
                params.level |= INFLUXDB_EVENT_FLAG;
                sql_option.events = xstrdup(optarg);
                break;
        	case 'E':
                sql_labels.end_label = true;
                sql_option.usage_end = parse_time(optarg, 1);
                break;
        	case 'h':
                print_sjinfo_help();
                exit(0);
                break;
        	case 'j':
                sql_option.jobids = xmalloc(strlen(optarg)+20);
                sprintf(sql_option.jobids,"%s",optarg);
                sql_labels.jobid_out = true;
                break;
            case 'J':
                params.level |= INFLUXDB_JOB_SUMMARY;
                break;
            case 'l':
                params.level |= INFLUXDB_STEPD;
                break;
        	case 'o':
                params.opt_field_list = xmalloc(strlen(optarg)+20);
                sprintf(params.opt_field_list,"%s",optarg);
                break;
            // case 'r':
            //     params.only_run_job = true;
            //     break;
            case 's':
                sql_option.steps = xmalloc(strlen(optarg)+20);
                sprintf(sql_option.steps,"%s",optarg);
                sql_labels.step_out = true;
                break;
        	case 'S':
                sql_labels.start_label = true;
                sql_option.usage_start = parse_time(optarg, 1);
                break;
            case (int)'t':
                if (optarg) {
                    if (strcasecmp(optarg, "job") == 0) {
                        params.show_jobstep_apptype = false;
                    } else if (strcasecmp(optarg, "step") == 0) {
                        params.show_jobstep_apptype = true;
                    } else {
                        fprintf(stderr, "Invalid argument for -t: %s. Expected 'job' or 'step'.\n", optarg);
                        exit(EXIT_FAILURE);
                    }
                } else {
                    params.show_jobstep_apptype = false;
                }
                params.level |= INFLUXDB_APPTYPE;
                break;
        	case 'u':
                sql_labels.user_label = true;
                sql_option.user = xmalloc(strlen(optarg)+20);
                sprintf(sql_option.user,"%s",optarg);
                break;
            case 'V':
                print_sjinfo_version();
                exit(0);
            case 'm':
                params.units = UNIT_MEGA;
                break;
            case 'g':
                params.units = UNIT_GIGA;
                break;
            case 'O':
                params.level |= INFLUXDB_OVERALL;
                break;
            case 'q':
                sql_labels.query_label = true;    
                break;                 
    		case '?':	/* getopt() has explained it */
			    exit(1);
            default:
                break;
        }
        if(sql_labels.no_jobid || sql_labels.no_step)
            break;
    }

    if(sql_labels.jobid_out &&  sql_option.jobids && strlen(sql_option.jobids) >= 6000 ) {
        rc = -1;
        goto fail;
    }
    if(params.display == true) {
        if(sql_labels.jobid_out == false) {
            printf("You need to specify a job ID using the -j option. For example: sjinfo -j 666 -D\n");
            goto fail;
        }

        rc = contains_non_digit(sql_option.jobids);
        if(rc == -1) {
            printf("Specifying `-D` only supports a single job ID. For example: sjinfo -j 666 -D\n");
            goto fail;
        }

        if(sql_labels.query_label == true) {
            printf("The `-D` and `-q` options cannot be used together.\n");
            goto fail;
        }
        if(params.opt_field_list) {
            printf("The `-D` and `-o` options cannot be used together.\n");
            goto fail;
        }
        if(params.level & INFLUXDB_EVENT) {
            printf("The `-D` and `-A` options cannot be used together.\n");
            goto fail;
        }
        if(params.level & INFLUXDB_ALL) {
            printf("The `-D`, `-l`, and `-a` options cannot be specified at the same time.\n");
            goto fail;
        }
        jobid_digit = atoi(sql_option.jobids);
        params.level |= INFLUXDB_STEPD;
    }

    query_send->data                          = data;
    query_send->params                        = &params;
    query_send->job_list                      = job_list;
    query_send->pw                            = pw;
    query_send->jobid_tran                    = jobid_digit;

    if(sql_labels.query_label) {
        rc = query_spost(query_send, sql_labels.jobid_out, sql_option.jobids);
        goto fail;
    }

    if(params.level == INFLUXDB_NONE || params.level == INFLUXDB_EVENT_FLAG)
        params.level |= INFLUXDB_STEPD;
    
    query_step_event(&sql_labels, &sql_option, query_send);

    if((params.level & INFLUXDB_STEPD) && (params.level & INFLUXDB_EVENT_FLAG)){
        printf("The -e option must be used with either -A or -O !\n");
        exit(1);
    }
fail:
    xfree(sql_option.events); 
    xfree(sql_option.jobids);    
    xfree(sql_option.steps);
    xfree(sql_option.user);
    return rc;
}
int main(int argc ,char** argv) {

    slurm_influxdb* influxdb_data = xmalloc(sizeof(slurm_influxdb));
    char* configpath = NULL;
    query_job_record_t query_send;
    int rc = 0 ;
    char tmp_conf[] = "/etc/acct_gather.conf.key";

    if(xstrcmp(KEYDIR, "NONE") == 0) {
         configpath = xstrdup("/etc/slurm/acct_gather.conf.key");
    } else {
        char* def_conf = NULL;
        def_conf = xmalloc(strlen(KEYDIR) + strlen(tmp_conf) + 2);
        sprintf(def_conf, "%s%s",KEYDIR,tmp_conf);
        configpath = xstrdup(def_conf);
        xfree(def_conf);
    }

    struct stat statbuf;
    if(stat(configpath, &statbuf) != 0){
        printf("File location does not exist of %s \n", configpath);
        goto file_fail;
    }

    /*16-bit encryption key*/
    const uint8_t key[]="fcad715bd73b5cb0";
    
    sjinfo_init(influxdb_data);

    query_send.print_display_list = print_display_list ;
    /*stepd table*/
    query_send.print_fields_list = print_fields_list;
    query_send.print_value_list  = print_value_list;
    query_send.print_fields_itr  = print_fields_itr;

    query_send.print_query_value_list  = print_query_value_list;

    /*event table*/
    query_send.print_events_list        = print_events_list;
    query_send.print_events_value_list  = print_events_value_list;
    query_send.print_events_itr         = print_events_itr;

    /*overall table*/
    query_send.print_overall_list       = print_overall_list;
    query_send.print_overall_value_list = print_overall_value_list;
    query_send.print_overall_itr        = print_overall_itr;

    /*apptype table*/
    query_send.print_apptype_list       = print_apptype_list;
    query_send.print_apptype_value_list = print_apptype_value_list;
    query_send.print_apptype_itr        = print_apptype_itr;

    /*apptype table only job*/
    query_send.print_apptype_job_list   = print_apptype_job_list;
    query_send.print_apptype_job_value_list = print_apptype_job_value_list;
    query_send.print_apptype_job_itr        = print_apptype_job_itr;

    /* job summary table */
    query_send.print_job_summary_list = print_job_summary_list;
    query_send.print_job_summary_value_list = print_job_summary_value_list;
    query_send.print_job_summary_itr = print_job_summary_itr;

    rc = read_hex_bytes_from_file(configpath, key, influxdb_data);
    if(rc == -1)
         goto file_fail;
    rc =  parse_command_and_query(argc, argv, influxdb_data, &query_send);
    if(rc == -1)
         goto file_fail;
    else if( rc != 2 )
        print_field(&query_send, &params);
    else
        print_query(&query_send, &params);

    sjinfo_fini(influxdb_data);
file_fail:
    if (configpath)
        xfree(configpath);
    if(rc == -1 )
         sjinfo_fini(influxdb_data);
    xfree(influxdb_data);
    if(params.opt_field_list)
        xfree(params.opt_field_list);

    return 0;
}


