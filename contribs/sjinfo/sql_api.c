/*****************************************************************************\
 *  sql_api.c - implementation-independent job of influxdb info
 *  functions
 *****************************************************************************
 *  Copyright (C) 2005 Hewlett-Packard Development Company, L.P.

 *  Slurm is xfree software; you can redistribute it and/or modify it under
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
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <pwd.h>
#include "influx_src.h"
#include "sjinfo.h"
#include "sql_api.h"
#include "time_format.h"
#include "xstring2.h"
#include "list2.h"
#include "json_api.h"

char deauft_events[] = "CPUUSA,PidSA,NodeSA,GPUUSA"; 

extern int strcat_stepd(const char* jobids, uint32_t* job_id, int* stepd)
{
    char *job = NULL;
    char *stepids = NULL;
    int rc = 0;
    char *p  = xstrchr(jobids, '.');
    if(p != NULL && p[0] !='\0') {
        stepids  = xmalloc(sizeof(char) * 300);
        job      = xmalloc(sizeof(char) * 300);
        memset(stepids, 0, 300);
        memset(job, 0, 300);
        strncpy(job, jobids, p - jobids); // 分配并复制第一部分
        job[p - jobids] = '\0';
        strcpy(stepids, p + 1); // 分配并复制第二部分
    } else {
        if(contains_non_digit(jobids) != 0) {
            printf("There is an error in the job ID format specification.\n");
            rc = -1;
            goto fail_stepd;
        }
        *job_id = (uint32_t) atol(jobids);
        rc = 2;
        goto fail_stepd;
    }

    if(job != NULL) {
        if(contains_non_digit(job) != 0) {
            printf("There is an error in the job ID format specification \n");
            rc = -1;
            goto fail_stepd;
        } 
        *job_id = (uint32_t) atol(job);
    } else {
        if(contains_non_digit(jobids) != 0) {
            printf("There is an error in the job ID format specification.\n");
            rc = -1;
            goto fail_stepd;
        }

        *job_id = (uint32_t) atol(jobids);
    }

    if(stepids) {
        if(strncmp(stepids, "batch", 5) == 0) {
            *stepd = -5;
        } else if(strncmp(stepids, "extern", 6) == 0){
            *stepd = -4;
        } else if(contains_non_digit(stepids) == 0) {
            *stepd = atol(stepids);
        } else {
            printf("Invalid characters have been specified. (other: only one job can be specified). \n");
            rc = -1;
            goto fail_stepd;
        }
    }
fail_stepd:
    if(stepids)
        xfree(stepids);
    if(job)
        xfree(job);
    return rc;
}


extern int strcat_jobid_tag(char *sql, const char *str, bool flag) 
{   
    int rc = 0;
    if(sql == NULL || str == NULL) {
        rc =-1;
        return rc;
    }
    const char delimiters[] = ",";
    const char and[] = " and ";
    const char or[]  = " or ";   
    bool first       =  false;     
    char *jobids     = xstrdup(str);
    //int length = strlen(jobids);
    char* tmp_jobids = NULL;
    char tmp[40960]  = {'\0'};
    char *jobid      = strtok(jobids, delimiters);
    int num          = 0;
    tmp_jobids       = xmalloc(5000 * sizeof(char) );
    /* Traverse and print the split string */
    while (jobid != NULL) {      
        num++;
        if(num > 10000)
            break; 
        if(!first) {
            if(flag) {
                xstrcat(tmp_jobids, and); 
            }
            first = true;
        } else {
            xstrcat(tmp_jobids, or); 
        }
        uint32_t job_id = 0;
        int stepd_id = 0x7FFFFFFF;
        rc = strcat_stepd(jobid, &job_id, &stepd_id);
        if(rc == -1) {
            xfree(tmp_jobids);
            xfree(jobids);
            return rc;
        } else if (rc == 2) 
            sprintf(tmp, "jobid = %d", job_id);
        else 
            sprintf(tmp, "jobid = %d and step= \'%d\'",job_id, stepd_id );
        xstrcat(tmp_jobids, tmp);
        jobid = strtok(NULL, delimiters);
    }

    if((strlen(tmp_jobids) + strlen(sql)) > 81920) {
        printf("jobid is too long \n");
        rc = -1;
    }

    if(rc != -1)
        xstrcat(sql, tmp_jobids);
    xfree(tmp_jobids);
    xfree(jobids);
    return rc;
}

extern char* reassemble_job_ids(const char *input) {
    const char *delim = ",";
    const char *prefix = "jobid = '";
    const char *suffix = "'";
    size_t i = 0;
    char *output = NULL;
    /*The initial size is 1, leaving room for the string terminator '\0'.*/
    size_t output_size = 1; 
    size_t input_length = strlen(input);
    
    /*Iterate over the input string and calculate the required output string length*/ 
    for (i = 0; i < input_length; ++i) {
        if (input[i] == ',') {
            output_size += strlen(" or ") + strlen(prefix) + strlen(suffix);
        }
    }
    
    /*Allocate enough memory to store the output string*/
    output = (char*)xmalloc(output_size);
    if (output == NULL) {
        printf("Memory allocation error.\n");
        return NULL;
    }
    
    /*Start building the output string*/
    strcpy(output, "");
    char *token = strtok((char*)input, delim);
    while (token != NULL) {
        /*Append one " or jobid = 'token'" to the output string each time.
         *Add 7 to make room for " or " and the string terminator '\0'
         */
        output = xrealloc(output, output_size + strlen(token) + strlen(prefix) + strlen(suffix) + 7); 
        if (output == NULL) {
            printf("Memory allocation error.\n");
            return NULL;
        }
        xstrcat(output, " or ");
        xstrcat(output, prefix);
        xstrcat(output, token);
        xstrcat(output, suffix);
        token = strtok(NULL, delim);
    }
    
    /*If the output string is not empty, remove the first " or "*/
    if (strlen(output) > 4) {
        memmove(output, output + 4, strlen(output) - 3);
    }
    
    return output;
}

extern int strcat_field(c_string_t* sql, const char *str, int field)
{
    int rc = 0;
    if(sql == NULL || str == NULL || c_string_peek(sql) == NULL) {
        rc = -1;
        printf("strcat_field error.\n");
        return rc;
    }

    const char *prefix;
    switch (field){
        case JOBID:
            prefix = "jobid = '";
            break;
        case STEP:
            prefix = "step = '";
            break;
        case USERNAME:
            prefix = "username = '";
            break;
        default:
            rc = -1;
            return rc;
    }

    const char suffix[] = "' ";
    const char and[] = " and ";
    const char or[] = " or ";
    bool first = false;

    char *fields = xstrdup(str);
    if(!fields) {
        printf("xstrdup failed !\n");
        return -1; 
    }
    c_string_t *tmp_fields = c_string_create();
    const char delimiters[] = ",";

    char *field_value = strtok(fields, delimiters);
    while (field_value != NULL) {
        if(!first) {
            c_string_append_str(tmp_fields, and);
            c_string_append_str(tmp_fields, "(");
            first = true;
        }else {
            c_string_append_str(tmp_fields, or);
        }
        c_string_append_str(tmp_fields, prefix);

        c_string_append_str(tmp_fields, ((field == STEP && strcasecmp(field_value, "batch") == 0) ? "-5" : field_value));
        c_string_append_str(tmp_fields, suffix);
        field_value = strtok(NULL, delimiters);
    }
    if(first) c_string_append_str(tmp_fields, ")");

    if(rc != -1) {
        c_string_append_str(sql, c_string_peek(tmp_fields));
    }
    c_string_destroy(tmp_fields);
    if(fields) {
        xfree(fields);
    }
    return rc;
}

extern int stract_time(bool start_label, bool jobid_out, time_t usage_start,
                            time_t usage_end, char *start, c_string_t *sql_str) 
{
    int rc = 0;
    if(start == NULL || sql_str == NULL || c_string_peek(sql_str) == NULL) {
        rc =-1;
        return rc;
    }
    if(start_label && jobid_out) {
        /*Specify job and start time*/
        if(usage_start > usage_end) {
            printf("Start time requested is after end time.\n");
            rc = -1;
            return rc;
        }
        const char and[] = " and ";
        const char time[] = " time >= ";
        c_string_append_str(sql_str, and);
        c_string_append_str(sql_str, time);
        c_string_append_str(sql_str, start);
        
    } else if(start_label && !jobid_out){  
        /*Specify the start time without specifying the job*/
        const char and[] = " and ";
        const char time[] = " time >= ";
        c_string_append_str(sql_str, and);
        c_string_append_str(sql_str, time);
        c_string_append_str(sql_str, start);
    } else if(!start_label && jobid_out) {
        //do nothing

    } else if(!start_label && !jobid_out) {
        start = xmalloc(100);
        //char tmp_start[80];
        const char and[] = " and ";
        const char time[] = " time >= ";
        time_format(start, 0, false);
        c_string_append_str(sql_str, and);
        c_string_append_str(sql_str, time);
        c_string_append_str(sql_str, start);
        xfree(start);
    }
    return rc;
       
}

extern int query_step_event(label_flags_t* sql_labels, join_sql_t *sql_time_sd, query_job_record_t *query_send)
{
    int rc = SLURM_ERROR;
    if(!query_send->data || !sql_labels || !sql_time_sd)
        return rc;

    char* end                  = xmalloc(100);
    char* start                = xmalloc(100);
    char *buffer_str           = xmalloc(2048);
    char *buffer_brief_str     = xmalloc(2048);
    char *buffer_total_str     = xmalloc(2048);
    char *buffer_maxmin_str    = xmalloc(2048);
    c_string_t *sql_step       = c_string_create();
    c_string_t *sql_event      = c_string_create();
    c_string_t *sql_brief_step = c_string_create();
    c_string_t *sql_apptype    = c_string_create();
    c_string_t *sql_job_summary_total = c_string_create();
    c_string_t *sql_job_summary_maxmin = c_string_create();

    char sheet_step[]       =   "Stepd";
    // char sheet_event[]      =   "Event";
    // char sheet_apptype[]    =   "Apptype";
    /*parameter collection sql statement assembly*/
    char sql_step_head[]    =   "SELECT LAST(\"stepcpuave\") AS last_stepavecpu,LAST(\"stepcpu\") AS last_stepcpu, LAST(\"stepmem\") "
                                "AS last_stepmem, LAST(\"stepvmem\") AS last_stepvmem, LAST(\"steppages\") AS last_steppages, "
                                "MAX(\"stepcpu\") AS max_stepcpu, MIN(\"stepcpu\") AS min_stepcpu, MAX(\"stepmem\") AS max_stepmem, "
                                "MIN(\"stepmem\") AS min_stepmem, MAX(\"stepvmem\") AS max_stepvmem, MIN(\"stepvmem\") AS min_stepvmem,"
                                "LAST(\"stepdcuutil\") AS last_stepdcu, LAST(\"stepdcumem\") AS last_stepdcumem, "  
                                "MAX(\"stepdcuutil\") AS max_stepdcu, MIN(\"stepdcuutil\") AS min_stepdcu, "   
                                "MAX(\"stepdcumem\") AS max_stepdcumem, MIN(\"stepdcumem\") AS min_stepdcumem "
                                "FROM ("
                                    "SELECT * "
                                    "FROM Stepd"; 
    char sql_apptype_head[] =   "SELECT * "
                                "FROM ("
                                    "SELECT * "
                                    "FROM Apptype";

    /*event event sql statement assembly*/
    char sql_event_head[]   =   "SELECT \"cputhreshold\",\"gresthreshold\",\"end\",\"jobid\",\"start\",\"step\",\"stepcpu\",\"stepmem\",\"steppages\",\"stepvmem\","
                                "\"stepdcuutil\" as \"stepdcu\",\"stepdcumem\","  
                                "\"type1\"::tag as tag_type1,\"type2\"::tag as tag_type2,\"type3\"::tag as tag_type3,\"type4\"::tag as tag_type4,"
                                "\"type1\"::field as field_type1,\"type2\"::field as field_type2,\"type3\"::field as field_type3,\"type4\"::field as field_type4,"
                                "\"type\",\"username\" "
                                "FROM ("
                                    "SELECT * "
                                    "FROM Event";      

    /*job summary sql statement assembly*/
    char sql_job_summary_total_head[] = 
                                "SELECT SUM(last_stepcpu) AS total_cpu, "
                                "SUM(last_stepmem) AS total_mem, "
                                "SUM(last_stepvmem) AS total_vmem, "
                                "SUM(last_steppages) AS total_pages, "
                                "SUM(last_stepdcu) AS total_dcu, "
                                "SUM(last_stepdcumem) AS total_dcumem "
                                "FROM ("
                                    "SELECT LAST(\"stepcpu\") AS last_stepcpu, "
                                    "LAST(\"stepmem\") AS last_stepmem, "
                                    "LAST(\"stepvmem\") AS last_stepvmem, "
                                    "LAST(\"steppages\") AS last_steppages, "
                                    "LAST(\"stepdcuutil\") AS last_stepdcu, "
                                    "LAST(\"stepdcumem\") AS last_stepdcumem "
                                    "FROM ("
                                        "SELECT * "
                                        "FROM Stepd";
    char sql_job_summary_maxmin_head[] = 
                                "SELECT MAX(stepcpu_sum) AS max_cpu, "
                                "MIN(stepcpu_sum) AS min_cpu, "
                                "MAX(stepmem_sum) AS max_mem, "
                                "MIN(stepmem_sum) AS min_mem, "
                                "MAX(stepvmem_sum) AS max_vmem, "
                                "MIN(stepvmem_sum) AS min_vmem, "
                                "MAX(stepdcu_sum) AS max_dcu, "
                                "MIN(stepdcu_sum) AS min_dcu, "
                                "MAX(stepdcumem_sum) AS max_dcumem, "
                                "MIN(stepdcumem_sum) AS min_dcumem "
                                "FROM ("
                                    "SELECT SUM(\"stepcpu\") AS stepcpu_sum, "
                                    "SUM(\"stepmem\") AS stepmem_sum, "
                                    "SUM(\"stepvmem\") AS stepvmem_sum, "
                                    "SUM(\"stepdcuutil\") AS stepdcu_sum, "
                                    "SUM(\"stepdcumem\") AS stepdcumem_sum "
                                    "FROM ("
                                        "SELECT * "
                                        "FROM Stepd";


    /* Build ORDER BY clause for innermost query based on desc_set parameter */
    const char *inner_order_by = query_send->params->desc_set ? " ORDER BY time DESC" : " ORDER BY time ASC";
    char sql_tail[256];
    char sql_job_summary_total_tail[256];
    char sql_job_summary_maxmin_tail[256];
    char sql_tail_apptype[256];
    
    sprintf(sql_tail, "%s) GROUP BY step,jobid", inner_order_by);
    sprintf(sql_job_summary_total_tail, "%s) GROUP BY jobid,step) GROUP BY jobid", inner_order_by);
    sprintf(sql_job_summary_maxmin_tail, "%s) GROUP BY jobid,time(1s)) GROUP BY jobid", inner_order_by);
    sprintf(sql_tail_apptype, "%s) GROUP BY jobid", inner_order_by);
    /*
     * exam SELECT stepcpu,stepmem FROM "Stepd" WHERE "jobid"='603537634' 
     * group by step  ORDER BY time DESC LIMIT 1
     */
    char sql_brief_head[]   = "select time,stepcpu,stepmem ";
    char sql_brief_tail[]   = " group by step,jobid ORDER BY time DESC LIMIT 1";

    /* end time is concatenated with the default as the current query timestamp */
    if(!sql_labels->end_label) {
        time_format(end, 0, true);
        sql_labels->end_label = true;
    } else {
        time_format(end, sql_time_sd->usage_end, false);
    }

    /* set query start time */
    if(sql_labels->start_label) {
        time_format(start, sql_time_sd->usage_start, false);
    }

    /* SQL for collecting job step information */
    if(query_send->params->level & INFLUXDB_STEPD) {
        sprintf(buffer_str,"%s WHERE time <= %s ",sql_step_head, end);
        c_string_append_str(sql_step, buffer_str);
        memset(buffer_str, 0, 2048);

    } 

    /* SQL for job summary */
    if(query_send->params->level & INFLUXDB_JOB_SUMMARY) {

        sprintf(buffer_total_str,"%s WHERE time <= %s ",sql_job_summary_total_head, end);
        c_string_append_str(sql_job_summary_total, buffer_total_str);
        memset(buffer_total_str, 0, 2048);
        
        sprintf(buffer_maxmin_str,"%s WHERE time <= %s ",sql_job_summary_maxmin_head, end);
        c_string_append_str(sql_job_summary_maxmin, buffer_maxmin_str);
        memset(buffer_maxmin_str, 0, 2048);
    }


    /* concatenate SQL for custom exception */
    if(query_send->params->level & INFLUXDB_DISPLAY) {
        sprintf(buffer_brief_str,"%s from %s where time <= %s ",sql_brief_head, sheet_step, end);
        c_string_append_str(sql_brief_step, buffer_brief_str);
        memset(buffer_brief_str, 0, 2048);
        if(query_send->params->level & INFLUXDB_DISPLAY)  
            rc = 3;  
    } 
     
    /*
     * the reason we bitwise 0x0110 instead of 0x0010 is that the exception event overview 
     * data is counted at the same time as the exception event overview data, so when -O is 
     * specified, the -A flow is still executed, but the -A message is not output
     */
    if((query_send->params->level & INFLUXDB_OVERALL) || (query_send->params->level & INFLUXDB_EVENT)) {
        sprintf(buffer_str,"%s WHERE time <= %s ",sql_event_head, end);
        c_string_append_str(sql_event, buffer_str);
        memset(buffer_str, 0, 2048);
    }

    if(query_send->params->level & INFLUXDB_APPTYPE) {
        sprintf(buffer_str,"%s WHERE time <= %s ",sql_apptype_head, end);
        c_string_append_str(sql_apptype, buffer_str);
        memset(buffer_str, 0, 2048);
    }

    /* specified job ID */
    if(sql_labels->jobid_out) {
        if(query_send->params->level & INFLUXDB_STEPD) {
            rc = strcat_field(sql_step, sql_time_sd->jobids, JOBID);
            if(rc == -1) goto fail;
        }
        if((query_send->params->level & INFLUXDB_EVENT) ||
             (query_send->params->level & INFLUXDB_OVERALL)) {
            rc = strcat_field(sql_event, sql_time_sd->jobids, JOBID);
            if(rc == -1) goto fail;
        }
        if(query_send->params->level & INFLUXDB_APPTYPE) {
            rc = strcat_field(sql_apptype, sql_time_sd->jobids, JOBID);
            if(rc == -1) goto fail;
        }
        if(query_send->params->level & INFLUXDB_DISPLAY) {
            rc = strcat_field(sql_brief_step, sql_time_sd->jobids, JOBID);
            if(rc == -1) goto fail;
        }
        if(query_send->params->level & INFLUXDB_JOB_SUMMARY) {
            rc = strcat_field(sql_job_summary_total, sql_time_sd->jobids, JOBID);
            if(rc == -1) goto fail;
            rc = strcat_field(sql_job_summary_maxmin, sql_time_sd->jobids, JOBID);
            if(rc == -1) goto fail;
        }
    }

    /* query the specified job step */
    if(sql_labels->step_out) {

        if(query_send->params->level & INFLUXDB_STEPD) {
            rc = strcat_field(sql_step, sql_time_sd->steps, STEP);
            if(rc == -1) goto fail;
        }
        if((query_send->params->level & INFLUXDB_EVENT) || 
            (query_send->params->level & INFLUXDB_OVERALL)) {
            rc = strcat_field(sql_event, sql_time_sd->steps, STEP);
            if(rc == -1) goto fail;
        }
        if(query_send->params->level & INFLUXDB_DISPLAY) {
            rc = strcat_field(sql_brief_step, sql_time_sd->steps, STEP);
            if(rc == -1) goto fail;
        }
    }

    /* query common anomalies for a job */
    if(query_send->params->level & INFLUXDB_EVENT_FLAG) {
        
        bool splicing = false;
        const char and[] = " and ";
        const char or[] = " or ";
        const char prefix[] = "type = ";
        const char delimiters[] = ",";
        char *event = NULL;
        char *events_copy = NULL;
        bool first =  false;
        c_string_t *tmp_events = c_string_create();

        if(sql_time_sd->events == NULL) {
            sql_time_sd->events = xmalloc(strlen(deauft_events)+1);
            strcpy(sql_time_sd->events, deauft_events);
        }
        events_copy = xstrdup(sql_time_sd->events);
        event = strtok(events_copy, delimiters);
        while (event != NULL) { 
            if(!first) {
                c_string_append_str(tmp_events, and);
                c_string_append_str(tmp_events, "(");
                first = true;
            } else
                c_string_append_str(tmp_events, or);
            if (strcasecmp(event, "CPUUSA") == 0) { 
                c_string_append_str(tmp_events, prefix);
                c_string_append_str(tmp_events, "'cpu'");
            } else if (strcasecmp(event, "PidSA") == 0) {
                c_string_append_str(tmp_events, prefix);
                c_string_append_str(tmp_events, "'process'");
            } else if (strcasecmp(event, "NodeSA") == 0) {
                c_string_append_str(tmp_events, prefix);
                c_string_append_str(tmp_events, "'node'");
            } else if (strcasecmp(event, "GPUUSA") == 0) {
                c_string_append_str(tmp_events, prefix);
                c_string_append_str(tmp_events, "'gres'");
            } else 
                splicing = true;
            event = strtok(NULL, delimiters);
        }   

        if(first) 
            c_string_append_str(tmp_events, ")");
        if(!splicing) {
            c_string_append_str(sql_event, c_string_peek(tmp_events));
            c_string_destroy(tmp_events);
            xfree(events_copy);
        } else {
            printf("Please enter a valid event field -e \n");
            c_string_destroy(tmp_events);
            xfree(events_copy);
            rc = -1;
            goto fail;
        }
  
    }

    if(query_send->params->level & INFLUXDB_STEPD) {
        rc =  stract_time(sql_labels->start_label, sql_labels->jobid_out, sql_time_sd->usage_start,
                            sql_time_sd->usage_end, start, sql_step);
        if(rc == -1)
            goto fail;
    }

    if((query_send->params->level & INFLUXDB_EVENT) || (query_send->params->level & INFLUXDB_OVERALL)) {
        rc = stract_time(sql_labels->start_label, sql_labels->jobid_out, sql_time_sd->usage_start,
                            sql_time_sd->usage_end, start, sql_event);
        if(rc == -1)
          goto fail;
    }

    if(query_send->params->level & INFLUXDB_APPTYPE) {
        rc =  stract_time(sql_labels->start_label, sql_labels->jobid_out, sql_time_sd->usage_start,
                            sql_time_sd->usage_end, start, sql_apptype);
        if(rc == -1)
            goto fail;
    }

    if(query_send->params->level & INFLUXDB_DISPLAY) {
        rc =  stract_time(sql_labels->start_label, sql_labels->jobid_out, sql_time_sd->usage_start,
                            sql_time_sd->usage_end, start, sql_brief_step);
        if(rc == -1)
            goto fail;
    }
    if(query_send->params->level & INFLUXDB_JOB_SUMMARY) {

        rc = stract_time(sql_labels->start_label, sql_labels->jobid_out, sql_time_sd->usage_start,
                        sql_time_sd->usage_end, start, sql_job_summary_total);
        if(rc == -1) goto fail;
        rc = stract_time(sql_labels->start_label, sql_labels->jobid_out, sql_time_sd->usage_start,
                            sql_time_sd->usage_end, start, sql_job_summary_maxmin);
        if(rc == -1) goto fail;
    }

    if(query_send->params->opt_uid != 0) {
        if(query_send->params->level & INFLUXDB_STEPD) {
            char *user = xmalloc(strlen(query_send->pw->pw_name) + 25); // 多出的字符给 SQL 语法
            sprintf(user, " and username = '%s' ", query_send->pw->pw_name);
            c_string_append_str(sql_step, user);
            xfree(user);
        }
        if(query_send->params->level & INFLUXDB_JOB_SUMMARY) {
            char *user = xmalloc(strlen(query_send->pw->pw_name) + 25);
            sprintf(user, " and username = '%s' ", query_send->pw->pw_name);
            c_string_append_str(sql_job_summary_total, user);
            c_string_append_str(sql_job_summary_maxmin, user);
            xfree(user);
        }
        if((query_send->params->level & INFLUXDB_EVENT) || (query_send->params->level & INFLUXDB_OVERALL)) {
            char *user = xmalloc(strlen(query_send->pw->pw_name) + 25); // 多出的字符给 SQL 语法
            sprintf(user, " and username = '%s' ", query_send->pw->pw_name);
            c_string_append_str(sql_event, user);
            xfree(user);
        }
        if(query_send->params->level & INFLUXDB_APPTYPE) {
            char *user = xmalloc(strlen(query_send->pw->pw_name) + 25); // 多出的字符给 SQL 语法
            sprintf(user, " and username = '%s' ", query_send->pw->pw_name);       
            c_string_append_str(sql_apptype, user);
            xfree(user);
        }
        if(query_send->params->level & INFLUXDB_DISPLAY) {
            char *user = xmalloc(strlen(query_send->pw->pw_name) + 25); // 多出的字符给 SQL 语法
            sprintf(user, " and username = '%s' ", query_send->pw->pw_name);
            c_string_append_str(sql_brief_step, user);
            xfree(user);
        }
    } else if(sql_labels->user_label) {
        if(query_send->params->level & INFLUXDB_STEPD) {
            rc = strcat_field(sql_step, sql_time_sd->user, USERNAME);
            if(rc == -1)
                goto fail; 
        }
        if((query_send->params->level & INFLUXDB_EVENT) || (query_send->params->level & INFLUXDB_OVERALL)) {
            rc = strcat_field(sql_event, sql_time_sd->user, USERNAME);
            if(rc == -1)
                goto fail;  
        }
        if(query_send->params->level & INFLUXDB_APPTYPE) {
            rc = strcat_field(sql_apptype, sql_time_sd->user, USERNAME);
            if(rc == -1)
            goto fail; 
        }
        if(query_send->params->level & INFLUXDB_DISPLAY) {
            rc = strcat_field(sql_brief_step, sql_time_sd->user, USERNAME);
            if(rc == -1)
            goto fail; 
        }
        if(query_send->params->level & INFLUXDB_JOB_SUMMARY) {

            rc = strcat_field(sql_job_summary_total, sql_time_sd->user, USERNAME);
            if(rc == -1) goto fail;
            rc = strcat_field(sql_job_summary_maxmin, sql_time_sd->user, USERNAME);
            if(rc == -1) goto fail;
        }

    }

    if(query_send->params->level & INFLUXDB_DISPLAY) {
        c_string_append_str(sql_brief_step, sql_brief_tail);
        char * response = influxdb_connect(query_send->data, c_string_peek(sql_brief_step), 
                                    STEPDRP, query_send->params->display);
        /* debug */
        if(response) {
            query_send->flag = UNIT_BRIEF;
            query_send->current_time = 0;
            parse_json(response, query_send);
        } else {
            rc = -1;
            goto fail;
        }
        if(response)
            xfree(response);

    } 

    if(query_send->params->level & INFLUXDB_STEPD) {
        c_string_append_str(sql_step, sql_tail);
        /* debug */
        char * response = influxdb_connect(query_send->data, c_string_peek(sql_step), STEPDRP, query_send->params->display);
        if(response) {
            query_send->flag = UNIT_STEP;
            query_send->current_time = 0;
            parse_json(response, query_send);
        } else {
            rc = -1;
            goto fail;
        }
        if(response)
            xfree(response);
    } 

    if(query_send->params->level & INFLUXDB_JOB_SUMMARY) {

        c_string_append_str(sql_job_summary_total, sql_job_summary_total_tail);
        char *response_total = influxdb_connect(query_send->data, c_string_peek(sql_job_summary_total), STEPDRP, query_send->params->display);
        if(response_total) {
            query_send->flag = UNIT_JOB_SUMMARY_TOTAL;
            query_send->current_time = 0;
            parse_json(response_total, query_send);
            xfree(response_total);
        } else {
            rc = -1;
            goto fail;
        }


        c_string_append_str(sql_job_summary_maxmin, sql_job_summary_maxmin_tail);
        char *response_maxmin = influxdb_connect(query_send->data, c_string_peek(sql_job_summary_maxmin), STEPDRP, query_send->params->display);
        if(response_maxmin) {
            query_send->flag = UNIT_JOB_SUMMARY_MAXMIN;
            query_send->current_time = 0;
            parse_json(response_maxmin, query_send);
            xfree(response_maxmin);
        } else {
            rc = -1;
            goto fail;
        }

    } 

    if(query_send->params->level & INFLUXDB_APPTYPE) {
        c_string_append_str(sql_apptype, sql_tail_apptype);
        /* debug */
        char * response = influxdb_connect(query_send->data, c_string_peek(sql_apptype), APPTYPERP, query_send->params->display);
        if(response) {
            query_send->flag = UNIT_APPTYPE;
            query_send->current_time = 0;
            parse_json(response, query_send);
        } else {
            rc = -1;
            goto fail;
        }
        if(response)
            xfree(response);    
    } 
    if((query_send->params->level & INFLUXDB_EVENT) || (query_send->params->level & INFLUXDB_OVERALL)) {
        c_string_append_str(sql_event, sql_tail);
        /* debug */
        char * response = influxdb_connect(query_send->data, c_string_peek(sql_event), EVENTRP, query_send->params->display);
        if(response) {
            query_send->flag = UNIT_EVENT;
            query_send->current_time = 0;
            parse_json(response, query_send);
        } else {
            rc = -1;
            goto fail;
        }
        if(response)
            xfree(response);    
    } 
fail:
    xfree(buffer_str);
    xfree(buffer_brief_str);
    xfree(buffer_total_str);
    xfree(buffer_maxmin_str);
    xfree(start);
    xfree(end); 

    if(sql_step)
        c_string_destroy(sql_step);
    if(sql_event)
        c_string_destroy(sql_event);
    if(sql_brief_step)
        c_string_destroy(sql_brief_step);
    // if(sql_runjob)
    //     c_string_destroy(sql_runjob);
    if(sql_apptype)
        c_string_destroy(sql_apptype); 
    if(sql_job_summary_total)
        c_string_destroy(sql_job_summary_total);
    if(sql_job_summary_maxmin)
        c_string_destroy(sql_job_summary_maxmin);

    return rc;
}

/* query custom anomaly SQL */
int query_spost(query_job_record_t* query_send, bool jobid_out, char *jobids)
{   
    int rc = SLURM_SUCCESS;
    if(!query_send) {
        rc = SLURM_ERROR;
        return rc;
    }

    char* sql_query       = NULL;
    char sql_query_head[] = "select * from ";
    char sheet_query[]    = "Spost";
    sql_query             = xmalloc(8192*sizeof(char));

    if(jobid_out) {
        if(query_send->params->opt_uid != 0 ) {
            sprintf(sql_query,"%s %s where uid=\'%d\' and  username=\'%s\'", sql_query_head, sheet_query, query_send->params->opt_uid, query_send->pw->pw_name);
            rc = strcat_jobid_tag(sql_query, jobids, true);
        } else {
            sprintf(sql_query,"%s %s where ", sql_query_head, sheet_query);
            rc = strcat_jobid_tag(sql_query, jobids, false);                
        }
        if(rc == SLURM_ERROR) {
            xfree(sql_query);
            return rc;
        }
           
        /* Add ORDER BY clause based on desc_set parameter */
        const char *order_by = query_send->params->desc_set ? " ORDER BY time DESC" : " ORDER BY time ASC";
        xstrcat(sql_query, order_by);
    } else
        printf("Please specify the job ID using -j.\n");
    /*  */
    char *response = influxdb_connect(query_send->data, sql_query, RPCNT, query_send->params->display);
    if(response)
        parse_json_tag(response, query_send->print_query_value_list);
    else {
        xfree(response); 
        xfree(sql_query);
        rc = SLURM_ERROR;
        return rc;
    }
    xfree(response); 
    xfree(sql_query);
    rc = 2;
    return rc;

}

