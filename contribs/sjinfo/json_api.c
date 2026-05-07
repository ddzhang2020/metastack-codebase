/*****************************************************************************\
 *  json_api.c - implementation-independent job of influxdb info
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
#include <stdint.h>
#include <jansson.h>
#include "list2.h"
#include "sjinfo.h"
#include "json_api.h"
#include "xstring2.h"
#include <pwd.h>

void free_interface_sjinfo(interface_sjinfo_t* iinfo) {
    if (iinfo == NULL)
        return;
    xfree(iinfo->username);
    xfree(iinfo->type);
    xfree(iinfo->apptype_cli);
    xfree(iinfo->apptype_step);
    xfree(iinfo->apptype);
    xfree(iinfo);
}

extern void destroy_brief_key_pair(void *object)
{
	sacct_entry_t *key_brief_ptr = (sacct_entry_t *)object;

	if (key_brief_ptr) { 
		xfree(key_brief_ptr);
	}
}
// 解析单行字符串到结构体
static int parse_sacct_line(const char *line, int count, List print_head_list) {
    int result = -1;
    sacct_entry_t *sacct_field = NULL;
    sacct_field = xmalloc(sizeof(sacct_entry_t));
    if(count == 1) {
        long long int field1 = 0; 
        long long int field2 = 0;
        long long int field3 = 0;      
        result = sscanf(line, "%lld %lld %lld", &field1, &field2, &field3);
        if (result == 3) {
            sacct_field->jobid     = field1;
            sacct_field->alloc_cpu = field2;
            sacct_field->stepdid   = -1; //作业默认为-1
            sacct_field->reqmem    = field3; 
            list_append(print_head_list, sacct_field); 
        } else {
            xfree(sacct_field);
            return -1;
        }
        
    } else {
        sacct_field->stepdid   = -2; //数字作业步默认为-2
        long long int field1   = 0; 
        char field2[50]        = {'\0'};
        long long int field3   = 0;    
        result = sscanf(line, "%lld %49s %lld", &field1, field2, &field3);
        /*成功读取3个变量*/
        if (result == 3) {
            
            sacct_field->jobid = field1;
           
            if (strstr(field2, "batch")) {
                sacct_field->stepdid = -5; //batch作业对应的数字作业步为-5
            } else {
                if (field2[0] == '.') {
                    int value = atoi(field2 + 1);  // 提取整数
                    sacct_field->stepdid = value;
                } else {
                    printf("Extracted integer: %s failed \n", field2);
                    xfree(sacct_field);
                    return -1;
                }
            }
            sacct_field->alloc_cpu = field3;
            list_append(print_head_list, sacct_field);           
        } else {
            xfree(sacct_field);
            return -1;
        }       
    }
    return result;
}

static int _find_sacct_by_stepid(void *x, void *key)
{
    sacct_entry_t *entry = (sacct_entry_t *)x;
    long long int target_stepid = *(long long int *)key;

    if (!entry)
        return 0;

    return (entry->stepdid == target_stepid);
}

static int sacct_get(List print_head_list, long int job_tran) 
{
    FILE *fp;
    int count = 0;
    int rc = 0;
    char cmd[2048] = {'\0'};
    char line[4096] = {'\0'};  // 用更大的buffer存结果行（比如4096字节）

    sprintf(cmd, "sacct -j %ld -o  jobid%%24,AllocC%%24,reqmem%%36 --unit=m --noheader|grep -v extern", job_tran);
    // 执行命令并打开管道
    fp = popen(cmd, "r");
    if (fp == NULL) {
        perror("popen failed");
        rc = -1;
        return rc;
    }

    // 读取命令输出
    while (fgets(line, sizeof(line), fp) != NULL) {
        count++;
        rc = parse_sacct_line(line, count, print_head_list);
        if(rc == -1) 
            break;
    }
    // 关闭管道
    pclose(fp);
    return rc;
}


/**
 * @brief Parse the response string in JSON format and fill the extracted data into the corresponding global 
 *        linked list for the display of job resource usage information.
 *
 * @param response     JSON strings, typically from InfluxDB query results.
 * @param username     The username of the current task owner, used to fill the `username` field in the result structure.
 * @param flag         Control the parsing mode, and the following values are supported:
 *                     - UNIT_STEP：Resource consumption information for job step
 *                     - UNIT_BRIEF：Brief information, combined with the fields of `sacct`.
 *                     - UNIT_APPTYPE：Extract Application type data.
 *                     - UNIT_EVENT：Extract Event type data.
 * @param current_time Current timestamp.
 *
 */
extern void parse_json(char *response, query_job_record_t* query_send) {
    json_t *root = NULL;
    json_error_t error;
    size_t r = 0;
    List print_head_list = NULL;
    long long int reqmem = 0;     

    if (!response || !query_send || !query_send->pw || !query_send->pw->pw_name || !query_send->params) {
        return;
    }  
    root = json_loads(response, 0, &error);
    if (!root) {
        fprintf(stderr, "error: on line %d: %s\n", error.line, error.text);
        return;
    }

    if(query_send->flag == UNIT_BRIEF) {
        print_head_list = list_create(destroy_brief_key_pair);
        sacct_get(print_head_list, query_send->jobid_tran);
        long long int defaut_step = -1; //sacct 查询出的 jobid那一栏，作业步赋值是-1
        sacct_entry_t *sacct_field_tmp = list_find_first(print_head_list, _find_sacct_by_stepid, &defaut_step);
        if(sacct_field_tmp)
            reqmem = sacct_field_tmp->reqmem;
        else
            reqmem = NOT_FIND;
     }
    /*Get the "results" array*/ 
    json_t *results = json_object_get(root, "results");
    size_t num_results = json_array_size(results);
    /*Iterate through each result*/ 
    for ( r = 0; r < num_results; ++r) {
        size_t s = 0;
        json_t *result = json_array_get(results, r);
        json_t *series = json_object_get(result, "series");
        size_t num_series = json_array_size(series);
        s = 0;  /* SQL already handles ordering via ORDER BY, so always start from beginning */
        while(s < num_series) {
            interface_sjinfo_t *iinfo_overall = NULL;
            json_t *series_element = json_array_get(series, s);
            json_t *tags = json_object_get(series_element, "tags");
            const char *step = json_string_value(json_object_get(tags, "step"));
            const char* jobid = json_string_value(json_object_get(tags, "jobid"));
            json_t *columns = json_object_get(series_element, "columns");
            json_t *values = json_object_get(series_element, "values");
            iinfo_overall = xmalloc(sizeof(*iinfo_overall));
            iinfo_overall->username = xmalloc(strlen(query_send->pw->pw_name) + 1);
            if(jobid)
                iinfo_overall->jobid = atoi(jobid);
            if(step)
                iinfo_overall->stepid = atoi(step);
            iinfo_overall->sum_cpu = 0;
            iinfo_overall->sum_pid = 0;
            iinfo_overall->sum_node = 0;
            iinfo_overall->sum_gpu = 0;
            strcpy(iinfo_overall->username, query_send->pw->pw_name);
            
            if(query_send->flag == UNIT_STEP) {
                /* Analyze the data of Stepd measurement */
                size_t i =0, j = 0, num_rows = json_array_size(values);
                i = 0;  /* SQL already handles ordering via ORDER BY, so always start from beginning */
                while(i < num_rows){
                    json_t *row = json_array_get(values, i);
                    interface_sjinfo_t *iinfo = xmalloc(sizeof(*iinfo));  
                    iinfo->username = xmalloc(strlen(query_send->pw->pw_name) + 1);
                    if(jobid)
                        iinfo->jobid = atoi(jobid);
                    if(step)
                        iinfo->stepid = atoi(step);
                    strcpy(iinfo->username, query_send->pw->pw_name);

                    // 初始化 DCU 字段为默认值
                    iinfo->stepdcu = 0.0;
                    iinfo->stepdcumem = 0.0;
                    iinfo->stepdcumax = 0.0;
                    iinfo->stepdcumin = 0.0;
                    iinfo->stepdcumemmax = 0.0;
                    iinfo->stepdcumemmin = 0.0;

                    for (j = 0; j < json_array_size(columns); ++j) {
                        json_t *value = json_array_get(row, j);
                        if (value == NULL || value->type == JSON_NULL)
                            continue;
                        const char * tmp_name = json_string_value(json_array_get(columns, j));
                        if ((xstrcmp(tmp_name, "last_stepavecpu") == 0)) {
                            iinfo->stepcpuave = json_real_value(value);
                        } else if (xstrcmp(tmp_name, "last_stepcpu") == 0) {
                            iinfo->stepcpu = json_real_value(value);
                        } else if (xstrcmp(tmp_name, "last_stepmem") == 0) {
                            iinfo->stepmem = (long long)json_integer_value(value);
                        } else if (xstrcmp(tmp_name, "last_stepvmem") == 0) {
                            iinfo->stepvmem = (long long)json_integer_value(value);
                        } else if (xstrcmp(tmp_name, "last_steppages") == 0) {
                            iinfo->steppages = (long long)json_integer_value(value);
                        }  else if (xstrcmp(tmp_name, "max_stepcpu") == 0) {
                            iinfo->stepcpumax = json_real_value(value);
                        } else if (xstrcmp(tmp_name, "min_stepcpu") == 0) {
                            iinfo->stepcpumin = json_real_value(value);
                        } else if (xstrcmp(tmp_name, "max_stepmem") == 0) {
                            iinfo->stepmemmax = json_integer_value(value);
                        } else if (xstrcmp(tmp_name, "min_stepmem") == 0) {
                            iinfo->stepmemmin = json_integer_value(value);
                        } else if (xstrcmp(tmp_name, "max_stepvmem") == 0) {
                            iinfo->stepvmemmax = json_integer_value(value);
                        } else if (xstrcmp(tmp_name, "min_stepvmem") == 0) {
                            iinfo->stepvmemmin = json_integer_value(value);
                        } else if (xstrcmp(tmp_name, "last_stepdcu") == 0) {
                            if (json_is_integer(value)) {
                                iinfo->stepdcu = (double)json_integer_value(value);
                            } else {
                                iinfo->stepdcu = json_real_value(value);
                            }
                        } else if (xstrcmp(tmp_name, "last_stepdcumem") == 0) {
                            if (json_is_integer(value)) {
                                iinfo->stepdcumem = (double)json_integer_value(value);
                            } else {
                                iinfo->stepdcumem = json_real_value(value);
                            }
                        } else if (xstrcmp(tmp_name, "max_stepdcu") == 0) {
                            if (json_is_integer(value)) {
                                iinfo->stepdcumax = (double)json_integer_value(value);
                            } else {
                                iinfo->stepdcumax = json_real_value(value);
                            }
                        } else if (xstrcmp(tmp_name, "min_stepdcu") == 0) {
                            if (json_is_integer(value)) {
                                iinfo->stepdcumin = (double)json_integer_value(value);
                            } else {
                                iinfo->stepdcumin = json_real_value(value);
                            }
                        } else if (xstrcmp(tmp_name, "max_stepdcumem") == 0) {
                            if (json_is_integer(value)) {
                                iinfo->stepdcumemmax = (double)json_integer_value(value);
                            } else {
                                iinfo->stepdcumemmax = json_real_value(value);
                            }
                        } else if (xstrcmp(tmp_name, "min_stepdcumem") == 0) {
                            if (json_is_integer(value)) {
                                iinfo->stepdcumemmin = (double)json_integer_value(value);
                            } else {
                                iinfo->stepdcumemmin = json_real_value(value);
                            }
                        }   
                    }
                    list_append(query_send->print_value_list, iinfo);
                    ++i; 
                }
            } else if (query_send->flag == UNIT_BRIEF && print_head_list && (list_count(print_head_list)>0)) { 
                size_t i = 0, j = 0, num_rows = json_array_size(values);
                i = 0; 
                while(i < num_rows){
                    json_t *row = json_array_get(values, i);
                    interface_sjinfo_t *iinfo = xmalloc(sizeof(*iinfo)); 
                    iinfo->username = xmalloc(strlen(query_send->pw->pw_name) + 1);
                    if(jobid)
                        iinfo->jobid = atoi(jobid);
                    if(step)
                        iinfo->stepid = atoi(step);
                    strcpy(iinfo->username, query_send->pw->pw_name);
                    iinfo->req_mem = reqmem;
                    for (j = 0; j < json_array_size(columns); ++j) {
                        json_t *value = json_array_get(row, j);
                        if (value == NULL || value->type == JSON_NULL)
                            continue;
                        const char * tmp_name = json_string_value(json_array_get(columns, j));
                        if (xstrcmp(tmp_name, "time") == 0) {
                            iinfo->time = (time_t)json_integer_value(value);
                        } else if ((xstrcmp(tmp_name, "stepcpu") == 0)) {
                            iinfo->stepcpu = json_real_value(value);
                        } else if (xstrcmp(tmp_name, "stepmem") == 0) {
                            iinfo->stepmem = json_integer_value(value);
                        } 
                    }
                    long long int tmp = iinfo->stepid;
                    sacct_entry_t *sacct_field_tmp = list_find_first(print_head_list, _find_sacct_by_stepid, &tmp);
                    if(!sacct_field_tmp) {
                        ++i; 
                        free_interface_sjinfo(iinfo);
                        continue; 
                    }
                    iinfo->alloc_cpu = sacct_field_tmp->alloc_cpu;
                    list_append(query_send->print_display_list, iinfo);
                    ++i;
                }
               
            } else if (query_send->flag == UNIT_APPTYPE) {
                /* Analyze the data of Apptype measurement */
                size_t i = 0, j = 0, num_rows = json_array_size(values);
                uint64_t max_cputime = 0; /* Keep the maximum value of cputime */
                char* max_username = NULL, *max_apptype = NULL;  /* The process name and user name corresponding to the maximum value of cputime */

                interface_sjinfo_t *iinfo_job = xmalloc(sizeof(*iinfo_job));
                iinfo_job->username = xmalloc(strlen(query_send->pw->pw_name) + 1);

                if(jobid)
                    iinfo_job->jobid = atoi(jobid);
                if(step)
                    iinfo_job->stepid = atoi(step);
                strcpy(iinfo_job->username, query_send->pw->pw_name);

                i = 0; 
                while(i < num_rows){
                    json_t *row = json_array_get(values, i);
                    interface_sjinfo_t *iinfo = xmalloc(sizeof(*iinfo));  

                    if(jobid)
                        iinfo->jobid = atoi(jobid);
                    if(step)
                        iinfo->stepid = atoi(step);

                    for (j = 0; j < json_array_size(columns); ++j) {
                        json_t *value = json_array_get(row, j);
                        if (value == NULL || value->type == JSON_NULL)
                            continue;
                        const char * tmp_name = json_string_value(json_array_get(columns, j));
                        if ((xstrcmp(tmp_name, "apptype_cli")) == 0) {
                            iinfo->apptype_cli = xstrdup(json_string_value(value));
                        } else if (xstrcmp(tmp_name, "apptype_step") == 0) {
                            iinfo->apptype_step = xstrdup(json_string_value(value));
                        } else if (xstrcmp(tmp_name, "cputime") == 0) {
                            iinfo->cputime = (uint64_t)strtoull(json_string_value(value), NULL, 10);
                        } else if (xstrcmp(tmp_name, "step") == 0) {
                            iinfo->stepid = (int)strtol(json_string_value(value), NULL, 10);
                        } else if (xstrcmp(tmp_name, "username") == 0) {
                            xfree(iinfo->username);
                            iinfo->username = xstrdup(json_string_value(value));
                        }
                    }
                    if (iinfo && iinfo->cputime >= max_cputime) {
                        max_cputime = iinfo->cputime;
                        xfree(max_apptype);
                        max_apptype = xstrdup(iinfo->apptype_step);
                        xfree(max_username);
                        max_username = xstrdup(iinfo->username);
                    }
                    if ((query_send->params->level & INFLUXDB_APPTYPE) && query_send->params->show_jobstep_apptype) {
                        list_append(query_send->print_apptype_value_list, iinfo);
                    } else {
                        free_interface_sjinfo(iinfo);
                    }
                    ++i;
                }
                if (!query_send->params->show_jobstep_apptype && query_send->params->level & INFLUXDB_APPTYPE) {
                    xfree(iinfo_job->apptype);
                    iinfo_job->apptype = xstrdup(max_apptype);
                    xfree(iinfo_job->username);
                    iinfo_job->username = xstrdup(max_username);
                    list_append(query_send->print_apptype_job_value_list, iinfo_job);
                } else {
                    free_interface_sjinfo(iinfo_job);
                }
                xfree(max_apptype);
                xfree(max_username);
            } else if (query_send->flag == UNIT_EVENT) {
                /* Analyze the data of Event measurement */
                size_t i = 0, j = 0, num_rows = json_array_size(values);
                i = 0;
                while(i < num_rows){
                    json_t *row = json_array_get(values, i);
                    interface_sjinfo_t *iinfo = xmalloc(sizeof(*iinfo));
                    iinfo->username = xmalloc(strlen(query_send->pw->pw_name) + 1);
                    if(jobid)
                        iinfo->jobid = atoi(jobid);
                    if(step)
                        iinfo->stepid = atoi(step);
                    strcpy(iinfo->username, query_send->pw->pw_name);

                    // 初始化 DCU 字段
                    iinfo->stepdcu = 0.0;
                    iinfo->stepdcumem = 0.0;
                    for (j = 0; j < json_array_size(columns); ++j) {
                        json_t *value = json_array_get(row, j);
                        if (value == NULL || value->type == JSON_NULL)
                            continue;
                        const char * tmp_name = json_string_value(json_array_get(columns, j));
                        if (xstrcmp(tmp_name, "stepcpu") == 0) {
                            iinfo->stepcpu = json_real_value(value);
                        } else if (xstrcmp(tmp_name, "stepmem") == 0) {
                            iinfo->stepmem = (long long)json_integer_value(value);
                        } else if (xstrcmp(tmp_name, "stepvmem") == 0) {
                            iinfo->stepvmem = (long long)json_integer_value(value);
                        } else if (xstrcmp(tmp_name, "steppages") == 0) {
                            iinfo->steppages = (long long)json_integer_value(value);
                        } else if (xstrcmp(tmp_name, "cputhreshold") == 0) {
                            iinfo->cputhreshold = (long long)json_integer_value(value);
                        } else if (xstrcmp(tmp_name, "gresthreshold") == 0) {
                            iinfo->gresthreshold = (long long)json_integer_value(value);
                        } else if (xstrcmp(tmp_name, "start") == 0) {
                            iinfo->start = (time_t)json_integer_value(value);
                            iinfo_overall->start_last = MAX(iinfo->start, iinfo_overall->start_last);
                        } else if (xstrcmp(tmp_name, "end") == 0) {
                            iinfo->end = (time_t)json_integer_value(value);
                            iinfo_overall->end_last = MAX(iinfo->end, iinfo_overall->end_last);
                        } else if (xstrcmp(tmp_name, "stepdcu") == 0) {
                            if (json_is_integer(value)) {
                                iinfo->stepdcu = (double)json_integer_value(value);
                            } else {
                                iinfo->stepdcu = json_real_value(value);
                            }
                        } else if (xstrcmp(tmp_name, "stepdcumem") == 0) {
                            if (json_is_integer(value)) {
                                iinfo->stepdcumem = (double)json_integer_value(value);
                            } else {
                                iinfo->stepdcumem = json_real_value(value);
                            }
                        }else if (xstrcmp(tmp_name, "tag_type1") == 0) {
                            const char *type1 = json_string_value(value);
                            if (type1)
                                iinfo->type1 = atoi(type1);
                        } else if (xstrcmp(tmp_name, "tag_type2") == 0) {
                            const char *type2 = json_string_value(value);
                            if (type2) 
                                iinfo->type2 = atoi(type2);
                        } else if (xstrcmp(tmp_name, "tag_type3") == 0) {
                            const char *type3 = json_string_value(value);
                            if (type3)
                                iinfo->type3 = atoi(type3);
                        } else if (xstrcmp(tmp_name, "tag_type4") == 0) {
                            const char *type4 = json_string_value(value);
                            if (type4)
                                iinfo->type4 = atoi(type4);
                        } else if (xstrcmp(tmp_name, "field_type1") == 0) {
                            iinfo->type1 = json_integer_value(value);
                        } else if (xstrcmp(tmp_name, "field_type2") == 0) {
                            iinfo->type2 = json_integer_value(value);
                        } else if (xstrcmp(tmp_name, "field_type3") == 0) {
                            iinfo->type3 = json_integer_value(value);
                        } else if (xstrcmp(tmp_name, "field_type4") == 0) {
                            iinfo->type4 = json_integer_value(value); 
                        } else if (xstrcmp(tmp_name, "type") == 0) {
                            const char *type = json_string_value(value);
                            if (type)
                                iinfo->type = xstrdup(type);
                        }

                    }
                     if ((iinfo->type1 || iinfo->type2 || iinfo->type3 || iinfo->type4 || iinfo->type != NULL) && 
                        ((query_send->params->level & INFLUXDB_EVENT) || query_send->params->level & INFLUXDB_OVERALL)) {
                        if (iinfo->type1) 
                            iinfo_overall->sum_cpu++;
                        if (iinfo->type2) 
                            iinfo_overall->sum_pid++;
                        if (iinfo->type3) 
                            iinfo_overall->sum_node++;
                        if (iinfo->type4)
                            iinfo_overall->sum_gpu++;
                        if(xstrcmp(iinfo->type, CPU_ABNORMAL_FLAG) == 0) {
                            iinfo_overall->sum_cpu++;
                        } else if (xstrcmp(iinfo->type, PROCESS_ABNORMAL_FLAG) == 0) {
                            iinfo_overall->sum_pid++;
                        } else if (xstrcmp(iinfo->type, NODE_ABNORMAL_FLAG) == 0) {
                            iinfo_overall->sum_node++;
                        } else if (xstrcmp(iinfo->type, GPU_ABNORMAL_FLAG) == 0) {
                            iinfo_overall->sum_gpu++;
                        }
                        list_append(query_send->print_events_value_list, iinfo);
                    } else {
                        free_interface_sjinfo(iinfo);
                    }

                    ++i;
                }

            } else if (query_send->flag == UNIT_RUNJOB) {
                /* According to the Stepd measurement, obtain the list of running jobs */
                size_t i = 0, j = 0, num_rows = json_array_size(values);
                i = 0;  
                while(i < num_rows) {
                    json_t *row = json_array_get(values, i);
                    time_t interval_time = 0, ctime = 0, diff = 0;
                    for (j = 0; j < json_array_size(columns); ++j) {
                        json_t *value = json_array_get(row, j);
                        if (value == NULL || value->type == JSON_NULL)
                            continue;
                        const char * tmp_name = json_string_value(json_array_get(columns, j));
                        if (xstrcmp(tmp_name, "ctime") == 0) {
                            ctime = (time_t)json_integer_value(value);
                        } else if (xstrcmp(tmp_name, "interval_time") == 0) {
                            if (json_is_integer(value)) {
                                interval_time = (time_t)json_integer_value(value);
                            } else {
                                fprintf(stderr, "Error: interval_time is not an integer at row %zu\n", i);
                                continue;
                            }
                        }
                    }
                    diff = labs(query_send->current_time - ctime);
                    if (diff <= interval_time) {
                        if (c_string_len(query_send->job_list) != 0) {
                            c_string_append_str(query_send->job_list, ",");
                        }
                        c_string_append_str(query_send->job_list, jobid);
                    }
                    ++i; 
                }
            } else if (query_send->flag == UNIT_JOB_SUMMARY_TOTAL) {
                
                size_t i = 0, j = 0, num_rows = json_array_size(values);
                i = 0; 
                
                while(i < num_rows){
                    json_t *row = json_array_get(values, i);
                    interface_sjinfo_t *iinfo = xmalloc(sizeof(*iinfo));  
                    iinfo->username = xmalloc(strlen(query_send->pw->pw_name) + 1);
                    
                    iinfo->type = NULL;
                    iinfo->apptype_cli = NULL;
                    iinfo->apptype_step = NULL;
                    iinfo->apptype = NULL;
                    
                    if(jobid)
                        iinfo->jobid = atoi(jobid);
                    iinfo->stepid = -1; // 作业级别
                    strcpy(iinfo->username, query_send->pw->pw_name);
                    
                    iinfo->total_cpu = 0.0;
                    iinfo->total_mem = 0;
                    iinfo->total_vmem = 0;
                    iinfo->total_pages = 0;
                    iinfo->total_dcu = 0.0;
                    iinfo->total_dcumem = 0.0;
                    iinfo->max_cpu = 0.0;
                    iinfo->min_cpu = 0.0;
                    iinfo->max_mem = 0;
                    iinfo->min_mem = 0;
                    iinfo->max_vmem = 0;
                    iinfo->min_vmem = 0;
                    iinfo->max_dcu = 0.0;
                    iinfo->min_dcu = 0.0;
                    iinfo->max_dcumem = 0.0;
                    iinfo->min_dcumem = 0.0;

                    for (j = 0; j < json_array_size(columns); ++j) {
                        json_t *value = json_array_get(row, j);
                        if (value == NULL || value->type == JSON_NULL)
                            continue;
                            
                        const char * tmp_name = json_string_value(json_array_get(columns, j));
                        
                        if (xstrcmp(tmp_name, "total_cpu") == 0) {
                            iinfo->total_cpu = json_real_value(value);
                        } else if (xstrcmp(tmp_name, "total_mem") == 0) {
                            iinfo->total_mem = json_integer_value(value);
                        } else if (xstrcmp(tmp_name, "total_vmem") == 0) {
                            iinfo->total_vmem = json_integer_value(value);
                        } else if (xstrcmp(tmp_name, "total_pages") == 0) {
                            iinfo->total_pages = json_integer_value(value);
                        } else if (xstrcmp(tmp_name, "total_dcu") == 0) {
                            if (json_is_integer(value)) {
                                iinfo->total_dcu = (double)json_integer_value(value);
                            } else {
                                iinfo->total_dcu = json_real_value(value);
                            }
                        } else if (xstrcmp(tmp_name, "total_dcumem") == 0) {
                            if (json_is_integer(value)) {
                                iinfo->total_dcumem = (double)json_integer_value(value);
                            } else {
                                iinfo->total_dcumem = json_real_value(value);
                            }
                        }   
                    }
                    list_append(query_send->print_job_summary_value_list, iinfo);
                    ++i;
                }

            } else if (query_send->flag == UNIT_JOB_SUMMARY_MAXMIN) {

                size_t i = 0, j = 0, num_rows = json_array_size(values);
                i = 0;
                
                while(i < num_rows){
                    json_t *row = json_array_get(values, i);
                    
                    if(jobid) {
                        unsigned long current_jobid = atoi(jobid);
                        list_itr_t *itr = list_iterator_create(query_send->print_job_summary_value_list);
                        interface_sjinfo_t *existing_iinfo = NULL;
                        
                        while ((existing_iinfo = list_next(itr))) {
                            if (existing_iinfo->jobid == current_jobid) {
                                for (j = 0; j < json_array_size(columns); ++j) {
                                    json_t *value = json_array_get(row, j);
                                    if (value == NULL || value->type == JSON_NULL)
                                        continue;
                                        
                                    const char * tmp_name = json_string_value(json_array_get(columns, j));
                                    
                                    if (xstrcmp(tmp_name, "max_cpu") == 0) {
                                        existing_iinfo->max_cpu = json_real_value(value);
                                    } else if (xstrcmp(tmp_name, "min_cpu") == 0) {
                                        existing_iinfo->min_cpu = json_real_value(value);
                                    } else if (xstrcmp(tmp_name, "max_mem") == 0) {
                                        existing_iinfo->max_mem = json_integer_value(value);
                                    } else if (xstrcmp(tmp_name, "min_mem") == 0) {
                                        existing_iinfo->min_mem = json_integer_value(value);
                                    } else if (xstrcmp(tmp_name, "max_vmem") == 0) {
                                        existing_iinfo->max_vmem = json_integer_value(value);
                                    } else if (xstrcmp(tmp_name, "min_vmem") == 0) {
                                        existing_iinfo->min_vmem = json_integer_value(value);
                                    } else if (xstrcmp(tmp_name, "max_dcu") == 0) {
                                        if (json_is_integer(value)) {
                                            existing_iinfo->max_dcu = (double)json_integer_value(value);
                                        } else {
                                            existing_iinfo->max_dcu = json_real_value(value);
                                        }
                                    } else if (xstrcmp(tmp_name, "min_dcu") == 0) {
                                        if (json_is_integer(value)) {
                                            existing_iinfo->min_dcu = (double)json_integer_value(value);
                                        } else {
                                            existing_iinfo->min_dcu = json_real_value(value);
                                        }
                                    } else if (xstrcmp(tmp_name, "max_dcumem") == 0) {
                                        if (json_is_integer(value)) {
                                            existing_iinfo->max_dcumem = (double)json_integer_value(value);
                                        } else {
                                            existing_iinfo->max_dcumem = json_real_value(value);
                                        }
                                    } else if (xstrcmp(tmp_name, "min_dcumem") == 0) {
                                        if (json_is_integer(value)) {
                                            existing_iinfo->min_dcumem = (double)json_integer_value(value);
                                        } else {
                                            existing_iinfo->min_dcumem = json_real_value(value);
                                        }
                                    }   
                                }
                                break;
                            }
                        }
                        list_iterator_destroy(itr);
                    }
                    ++i;
                }
            }

            ++s;
            if ((query_send->params->level & INFLUXDB_OVERALL) && (query_send->flag == UNIT_EVENT)) {
                list_append(query_send->print_overall_value_list, iinfo_overall);
            } else {
                free_interface_sjinfo(iinfo_overall);
            }
        }
    }
    
    if (query_send->flag == UNIT_BRIEF) {
        FREE_NULL_LIST(print_head_list);
    }
    json_decref(root);
}

/* parse fields returned by the query for custom exception handling */
void parse_json_tag(const char *response, list_t *print_query_value_list) {
    json_t *root = NULL;
    json_error_t error;
    /* Loading JSON */
    //printf("response =%s  /n",response);
    root = json_loads(response, 0, &error);
    if (!root) {
        fprintf(stderr, "Error: on line %d: %s\n", error.line, error.text);
        return;
    }

    /* Extract results */
    json_t *results = json_object_get(root, "results");
    if (!results || !json_is_array(results)) {
        fprintf(stderr, "Error: The specified job number may not have saved custom information\n");
        json_decref(root);
        return;
    }
    size_t num_results = json_array_size(results);

   /* Iterate over the results array */
    size_t i = 0;
    for ( i = 0; i < num_results; ++i ) {
        json_t *series = json_object_get(json_array_get(results, i), "series");
        if (!series || !json_is_array(series)) {
            fprintf(stderr, "debug: There is no content in the specified jobid.\n");
            continue;// Skip this result if there's no valid series
        }
        size_t num_series = json_array_size(series);
        /* Iterating over series */
        size_t j = 0;
        for ( j = 0; j < num_series; ++j ) {

            json_t *series_element = json_array_get(series, j);
            json_t *columns = json_object_get(series_element, "columns");
            json_t *values = json_object_get(series_element, "values");

            if ( !columns || !json_is_array(columns) || !values || !json_is_array(values) ) {
                fprintf(stderr, "Error: 'columns' or 'values' is not a valid array\n");
                continue;
            }
            size_t num_columns = json_array_size(columns);
            size_t num_values = json_array_size(values);

            /* Iterate through the values array*/
            size_t k  = 0;
            for ( k = 0; k < num_values; ++k) {
                json_t *row = json_array_get(values, k);
                if (!row || !json_is_array(row)) {
                    fprintf(stderr, "Error: 'row' is not an array\n");
                    continue; 
                }
                /* Skip invalid lines */ 
                spost_record_t *record = xmalloc(sizeof(spost_record_t));
                if (!record) {
                    fprintf(stderr, "Memory allocation failed for record\n");
                    continue; //Skip current record
                }
                /* Iterate over columns and rows */
                size_t m = 0;
                for ( m = 0; m < num_columns; ++m ) {

                    const char *column_name = json_string_value(json_array_get(columns, m));
                    json_t *value = json_array_get(row, m);

                    /* Skip invalid columns or values */
                    if (!column_name || !value) 
                        continue; 

                    if (xstrcmp(column_name, "record_time") == 0) {

                        record->record_time = json_integer_value(value);

                    } else if (xstrcmp(column_name, "data") == 0) {
                        /* 最长保存 4096 字节，超出部分被截断 */
                        const char *data = json_string_value(value);
                        const char *trunc_suffix = " [TRUNCATED]";
                        size_t max_len = 4096;
                        size_t suffix_len = strlen(trunc_suffix);
                        size_t data_len = 0;
                        size_t copy_len = 0;
                        bool truncated = false;

                        if (data == NULL)
                            data = "";

                        data_len = strlen(data);
                        if (data_len > max_len) {
                            truncated = true;
                            copy_len = max_len - suffix_len;
                        } else {
                            copy_len = data_len;
                        }

                        record->data = xmalloc(max_len + 1);
                        memcpy(record->data, data, copy_len);
                        if (truncated)
                            memcpy(record->data + copy_len, trunc_suffix, suffix_len);
                        else
                            suffix_len = 0;
                        record->data[copy_len + suffix_len] = '\0';

                    } else if (xstrcmp(column_name, "jobid") == 0) {

                        record->jobid = json_integer_value(value);

                    } else if (xstrcmp(column_name, "uid") == 0) {

                        const char *uid = json_string_value(value);
                        record->uid = xstrdup(uid ? uid : "0");

                    } else if (xstrcmp(column_name, "username") == 0) {

                        const char *username = json_string_value(value);
                        record->username = xstrdup(username ? username : "unknown");

                    } else if (xstrcmp(column_name, "step") == 0) {

                        const char *step =json_string_value(value);
                        if(step)
                            record->stepid = atoi(step);

                    } else if (xstrcmp(column_name, "sendnode") == 0) {

                        const char *node =json_string_value(value);
                        if(node)
                            record->nodename = xstrdup(node ? node : "0");
                    
                    }

                } 

                if( record->jobid> 0 )                                                             
                    list_append(print_query_value_list, record);
            }

        }
    }

    json_decref(root);
}