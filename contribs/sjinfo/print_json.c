/*****************************************************************************\
 *  print_json.c - implementation-independent job of influxdb info
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
#include <time.h>
#include <string.h>
#include <inttypes.h>
#include <stdlib.h>
#include "print_json.h"
#include "xstring2.h"
#include "list2.h"
#include "sjinfo.h"
#include "time_format.h"

#define LINE_WIDTH 103
char outbuf[FORMAT_STRING_SIZE];
char *fields_delimiter = NULL;
int print_fields_parsable_print = 0;
int print_fields_have_header = 1;

print_field_t fields[] = {
    {12, "JobID",           print_fields_str,   PRINT_JOBID},
    {6,  "StepID",          print_fields_str,   PRINT_STEPID},
    {10, "StepAVECPU",      print_fields_str,   PRINT_STEPAVECPU},
    {10, "StepCPU",         print_fields_str,   PRINT_STEPCPU},
    {10, "StepMEM",         print_fields_str,   PRINT_STEPMEM},
    {10, "StepVMEM",        print_fields_str,   PRINT_STEPVMEM},
    {10, "StepPages",       print_fields_str,   PRINT_STEPPAGES},
    {10, "MaxStepCPU",      print_fields_str,   PRINT_MAXSTEPCPU},
    {10, "MinStepCPU",      print_fields_str,   PRINT_MINSTEPCPU},
    {10, "MaxStepMEM",      print_fields_str,   PRINT_MAXSTEPMEM},
    {10, "MinStepMEM",      print_fields_str,   PRINT_MINSTEPMEM},
    {12, "MaxStepVMEM",     print_fields_str,   PRINT_MAXSTEPVMEM},
    {12, "MinStepVMEM",     print_fields_str,   PRINT_MINSTEPVMEM},
    {10, "StepDCU",         print_fields_str,   PRINT_STEPDCU},
    {15, "StepGPU(DCU)MEM", print_fields_str,   PRINT_STEPDCUMEM},
    {15, "MaxStepGPU(DCU)", print_fields_str,   PRINT_MAXSTEPDCU},
    {15, "MinStepGPU(DCU)", print_fields_str,   PRINT_MINSTEPDCU},
    {18, "MaxStepGPU(DCU)MEM",   print_fields_str,   PRINT_MAXSTEPDCUMEM},
    {18, "MinStepGPU(DCU)MEM",   print_fields_str,   PRINT_MINSTEPDCUMEM},
    {0,  NULL,              NULL,               0}
};

print_field_t field_event[] = {
    {12, "JobID",           print_fields_str,   PRINT_JOBID},
    {6,  "StepID",          print_fields_str,   PRINT_STEPID},       
    {12, "StepCPU",         print_fields_str,   PRINT_STEPCPU},
    {12, "StepMEM",         print_fields_str,   PRINT_STEPMEM},      
    {12, "StepVMEM",        print_fields_str,   PRINT_STEPVMEM},
    {17, "StepGPU(DCU)",    print_fields_str,   PRINT_STEPDCU},
    {17, "StepGPU(DCU)MEM", print_fields_str,   PRINT_STEPDCUMEM},
    {12, "StepPages",       print_fields_str,   PRINT_STEPPAGES},
    {12, "CPUthreshold",    print_fields_str,   PRINT_CPUTHRESHOLD},
    {13, "GRESthreshold",   print_fields_str,   PRINT_GRESTHRESHOLD},
    {22, "Start",           print_fields_str,   PRINT_START}, 
    {22, "End",             print_fields_str,   PRINT_END},
    {31, "Type",            print_fields_str,   PRINT_TYPE}, 
    {0,  NULL,              NULL,               0}
};

print_field_t field_overall[] = {
    {12, "JobID",       print_fields_str,   PRINT_JOBID},
    {6,  "StepID",      print_fields_str,   PRINT_STEPID},       
    {22, "Last_start",  print_fields_str,   PRINT_LASTSTART},
    {22, "Last_end",    print_fields_str,   PRINT_LASTEND},      
    {16, "CPU_Abnormal_CNT",     print_fields_str,   PRINT_SUMCPU},
    {17, "PROC_Abnormal_CNT",     print_fields_str,   PRINT_SUMPID},
    {17, "NODE_Abnormal_CNT",    print_fields_str,   PRINT_SUMNODE},
    {17, "GPU_Abnormal_CNT",     print_fields_str,   PRINT_SUMGPU},
    {0,  NULL,          NULL,               0}
};

print_field_t field_apptype[] = {
    {12, "JobID",       print_fields_str,   PRINT_JOBID},
    {6,  "StepID",      print_fields_str,   PRINT_STEPID},
    {8,  "Username",    print_fields_str,   PRINT_USERNAME},
    {15, "CpuTime",     print_fields_str,   PRINT_CPUTIME},
    {12, "Apptype_CLI", print_fields_str,   PRINT_APPTYPE_CLI},
    {12, "Apptype_STEP",print_fields_str,   PRINT_APPTYPE_STEP},
    {0,  NULL,          NULL,               0}
};

print_field_t field_apptype_job[] = {
    {12, "JobID",       print_fields_str,   PRINT_JOBID},
    {12, "Apptype",     print_fields_str,   PRINT_APPTYPE},
    {8,  "Username",    print_fields_str,   PRINT_USERNAME},
    {0,  NULL,          NULL,               0}
};


print_field_t fields_job_summary[] = {
    {12, "JobID",           print_fields_str,   PRINT_JOBID},
    {12, "TotalCPU",        print_fields_str,   PRINT_TOTALCPU},
    {12, "TotalMEM",        print_fields_str,   PRINT_TOTALMEM},
    {12, "TotalVMEM",       print_fields_str,   PRINT_TOTALVMEM},
    {12, "TotalPages",      print_fields_str,   PRINT_TOTALPAGES},
    {12, "MaxCPU",          print_fields_str,   PRINT_MAXCPU},
    {12, "MinCPU",          print_fields_str,   PRINT_MINCPU},
    {12, "MaxMEM",          print_fields_str,   PRINT_MAXMEM},
    {12, "MinMEM",          print_fields_str,   PRINT_MINMEM},
    {12, "MaxVMEM",         print_fields_str,   PRINT_MAXVMEM},
    {12, "MinVMEM",         print_fields_str,   PRINT_MINVMEM},
    {17, "TotalGPU(DCU)",   print_fields_str,   PRINT_TOTALDCU},
    {17, "TotalDCUMEM",     print_fields_str,   PRINT_TOTALDCUMEM},
    {17, "MaxGPU(DCU)",     print_fields_str,   PRINT_MAXDCU},
    {17, "MinGPU(DCU)",     print_fields_str,   PRINT_MINDCU},
    {17, "MaxGPU(DCU)MEM",  print_fields_str,   PRINT_MAXDCUMEM},
    {17, "MinGPU(DCU)MEM",  print_fields_str,   PRINT_MINDCUMEM},
    {0,  NULL,              NULL,               0}
};


extern void print_query_options(query_job_record_t *query_send)
{
        int field_count = list_count(query_send->print_fields_list);

        /*Print title*/
        print_fields_header(query_send->print_fields_list);
        print_field_t *field = NULL;

        spost_record_t * sjinfo_print = NULL;
        int curr_inx = 1;
        char tmp_char[4097] = {'0'};
        char buffer[180]= {'0'};
        //int tmp_int = NO_VAL;
        //struct tm *timeinfo = 0;
        time_t raw_time = 0;

        list_itr_t *itr = NULL;
        itr = list_iterator_create(query_send->print_query_value_list);
		while ((sjinfo_print = list_next(itr))) {
            list_itr_t *print_query_itr = list_iterator_create(query_send->print_fields_list);
            curr_inx = 1;
            while ((field = list_next(print_query_itr))) {
                raw_time = sjinfo_print->record_time;
                struct tm *local_time = localtime(&raw_time);
                if(local_time)
                     strftime(buffer, sizeof(buffer), "%Y-%m-%d %H:%M:%S", local_time);
                memset(tmp_char,'\0',sizeof(tmp_char));
                switch (field->type) {
                    case PRINT_JOBID:
                        sprintf(tmp_char, "%d", sjinfo_print->jobid);
                        field->print_routine(field,
                        tmp_char,
                        (curr_inx == field_count));
                    break;   
                    case PRINT_USERNAME:
                        if(sjinfo_print->username != NULL)
                            sprintf(tmp_char, "%s", sjinfo_print->username);
                        else
                            sprintf(tmp_char, "no username");
                        field->print_routine(field,
                        tmp_char,
                        (curr_inx == field_count));
                    break;                   
                    case PRINT_UID:
                        if (sjinfo_print->uid != NULL)
                            sprintf(tmp_char, "%s", sjinfo_print->uid);
                        else
                            sprintf(tmp_char, "0");
                        field->print_routine(field,
                        tmp_char,
                        (curr_inx == field_count));
                    break;  
                    case PRINT_RECORD:
                        if (sjinfo_print->data != NULL)
                            sprintf(tmp_char, "%s", sjinfo_print->data);
                        else
                            tmp_char[0] = '\0';
                        field->print_routine(field,
                        tmp_char,
                        (curr_inx == field_count));
                    break;     
                    case PRINT_STEPID:
                        if(sjinfo_print->stepid == -5)
                            sprintf(tmp_char, "batch");
                        else if(sjinfo_print->stepid == -4)
                            sprintf(tmp_char, "extern");
                        else if (sjinfo_print->stepid == 0x7FFFFFFF)
                            sprintf(tmp_char, "JSNS");
                        else
                            sprintf(tmp_char, "%d", sjinfo_print->stepid);
                        field->print_routine(field,
                        tmp_char,
                        (curr_inx == field_count));
                    break; 
                    case PRINT_TIME:
                        sprintf(tmp_char, "%s", buffer);
                        field->print_routine(field,
                        tmp_char,
                        (curr_inx == field_count));
                    break;    
                    case PRINT_SENDNODE:
                        if (sjinfo_print->nodename != NULL)
                            sprintf(tmp_char, "%s", sjinfo_print->nodename);
                        else
                            sprintf(tmp_char, "unknown");
                        field->print_routine(field,
                        tmp_char,
                        (curr_inx == field_count));
                    break;  

                }
                curr_inx++;
            }
            list_iterator_destroy(print_query_itr);    
            printf("\n");
        }
		list_iterator_destroy(itr);    
}

extern void print_query(query_job_record_t *query_send, sjinfo_parameters_t *params)
{
    char *opt_query_list = xmalloc(160);
    char base_query_field[] = "Jobid,StepID,SendNode,UserName,Uid,Message,PostTime"; 
    print_field_t fields[] = {
    	{12, "JobID",               print_fields_str, PRINT_JOBID},
        {12, "StepID",              print_fields_str, PRINT_STEPID},
        {8,  "Username",            print_fields_str, PRINT_USERNAME},
        {8,  "Sendnode",            print_fields_str, PRINT_SENDNODE},
        {12, "Uid",                 print_fields_str, PRINT_UID},        
        {24, "Message",             print_fields_str, PRINT_RECORD},
        {24, "PostTime",            print_fields_str, PRINT_TIME},
        {0,   NULL,                 NULL,             0}
    };

    if(!params->opt_field_list) {
        /*Consider the scenario where only one side of the field has it*/
        strcpy(opt_query_list, base_query_field);
        field_split(opt_query_list, fields, query_send->print_fields_list);
    } else {
        field_split(params->opt_field_list, fields, query_send->print_fields_list);
    }


    if(list_count(query_send->print_fields_list) > 0) {
        printf("*************************************************************************************************\n");
        printf("******                            Display User-defined exception                           ******\n");
        printf("*************************************************************************************************\n");
        printf("\n");
        
        print_query_options(query_send);
        printf("\n");
    }
    xfree(opt_query_list);

}

extern void print_fields_str(print_field_t *field, char *value, int last)
{

	int abs_len = abs(field->len);
	char temp_char[abs_len+1];
	char *print_this = NULL;



	if (!value) {
		if (print_fields_parsable_print)
			print_this = "";
		else
			print_this = " ";
	} else
		print_this = value;

	if (print_fields_parsable_print == PRINT_FIELDS_PARSABLE_NO_ENDING
	   && last)
		printf("%s", print_this);
	else if (print_fields_parsable_print && !fields_delimiter)
		printf("%s|", print_this);
	else if (print_fields_parsable_print && fields_delimiter)
		printf("%s%s", print_this, fields_delimiter);
	else {
		if (value) {
			int len = strlen(value);
			memcpy(&temp_char, value, MIN(len, abs_len) + 1);
			if (len > abs_len)
				temp_char[abs_len-1] = '+';
			print_this = temp_char;
		}

		if (field->len == abs_len)
			printf("%*.*s ", abs_len, abs_len, print_this);
		else
			printf("%-*.*s ", abs_len, abs_len, print_this);
	}
}

void print_fields_header(list_t *print_fields_list)
{
	list_itr_t *itr = NULL;
	print_field_t *field = NULL;
	int curr_inx = 1;
	int field_count = 0;

	if (!print_fields_list || !print_fields_have_header)
		return;

	field_count = list_count(print_fields_list);

	itr = list_iterator_create(print_fields_list);
	while ((field = list_next(itr))) {
		if (print_fields_parsable_print
		   == PRINT_FIELDS_PARSABLE_NO_ENDING
		   && (curr_inx == field_count))
			printf("%s", field->name);
		else if (print_fields_parsable_print
			 && fields_delimiter) {
			printf("%s%s", field->name, fields_delimiter);
		} else if (print_fields_parsable_print
			 && !fields_delimiter) {
			printf("%s|", field->name);

		} else {
			int abs_len = abs(field->len);
			printf("%*.*s ", field->len, abs_len, field->name);
		}
		curr_inx++;
	}
	list_iterator_reset(itr);
	printf("\n");

	if (print_fields_parsable_print) {
		list_iterator_destroy(itr);
		return;
	}

	while ((field = list_next(itr))) {
        int i;
		int abs_len = abs(field->len);
		for (i = 0; i < abs_len; i++)
			putchar('-');
		putchar(' ');
	}
	list_iterator_destroy(itr);
	printf("\n");
}


void print_options(list_t *print_list, list_t *value_list, list_itr_t *print_itr, sjinfo_parameters_t *params)
{
        int field_count = list_count(print_list);

        /*Print title*/
        print_fields_header(print_list);
        print_field_t *field = NULL;

        interface_sjinfo_t * sjinfo_print = NULL;
        int curr_inx = 1;
        uint64_t tmp_uint64 = NO_VAL64;
        char tmp_char[200] = {'0'};
        char tmp_batch[] = "batch";
        char tmp_extern[] = "extern";
        //int tmp_int = NO_VAL;
        struct tm *timeinfo;

        list_itr_t * print_value_itr = NULL;
        print_value_itr = list_iterator_create(value_list);

		while ((sjinfo_print = list_next(print_value_itr))) {
            list_iterator_reset(print_itr);
            while ((field = list_next(print_itr))) {
                    memset(tmp_char,'\0',sizeof(tmp_char));
                    switch (field->type) {
                    case PRINT_JOBID:
                        sprintf(tmp_char, "%lu", sjinfo_print->jobid);
                        field->print_routine(field,
                        tmp_char,
                        (curr_inx == field_count));
                    break;
                    case PRINT_APPTYPE_CLI:
                        if (sjinfo_print->apptype_cli != NULL)
                            sprintf(tmp_char, "%s", sjinfo_print->apptype_cli);
                        else
                            sprintf(tmp_char, "unknown");
                        field->print_routine(field,
                        tmp_char,
                        (curr_inx == field_count));
                        xfree(sjinfo_print->apptype_cli);
                    break;
                    case PRINT_APPTYPE_STEP:
                        if (sjinfo_print->apptype_step != NULL)
                            sprintf(tmp_char, "%s", sjinfo_print->apptype_step);
                        else
                            sprintf(tmp_char, "unknown");
                        field->print_routine(field,
                        tmp_char,
                        (curr_inx == field_count));
                        xfree(sjinfo_print->apptype_step);
                    break;
                    case PRINT_APPTYPE:
                        if (sjinfo_print->apptype != NULL)
                            sprintf(tmp_char, "%s", sjinfo_print->apptype);
                        else
                            sprintf(tmp_char, "unknown");
                        field->print_routine(field,
                        tmp_char,
                        (curr_inx == field_count));
                        xfree(sjinfo_print->apptype);
                    break;
                    case PRINT_CPUTIME:
                        if(sjinfo_print->cputime == UINT64_MAX)
                            sprintf(tmp_char, "%s", "SUCCESS MAPPING");
                        else
                            sprintf(tmp_char, "%"PRIu64"", sjinfo_print->cputime);
                        field->print_routine(field,
                        tmp_char,
                        (curr_inx == field_count));    
                    break; 
                    case PRINT_USERNAME:
                        if(sjinfo_print->username != NULL)
                            sprintf(tmp_char, "%s", sjinfo_print->username);
                        else
                            sprintf(tmp_char, "no username");
                        field->print_routine(field,
                        tmp_char,
                        (curr_inx == field_count));
                    break;
                    case PRINT_STEPID:
                        if((sjinfo_print->stepid == -5))
                            sprintf(tmp_char, "%s", tmp_batch);
                        else if((sjinfo_print->stepid == -4))
                            sprintf(tmp_char, "%s", tmp_extern);
                        else
                            sprintf(tmp_char, "%d", sjinfo_print->stepid);
                        field->print_routine(field,
                        tmp_char,
                        (curr_inx == field_count));
                        break;
                    case PRINT_STEPAVECPU:
                        sprintf(tmp_char, "%.2f", sjinfo_print->stepcpuave);
                        field->print_routine(field,
                        tmp_char,
                        (curr_inx == field_count));                            
                        break;
                    case PRINT_STEPCPU:
                        sprintf(tmp_char, "%.2f", sjinfo_print->stepcpu);
                        field->print_routine(field,
                        tmp_char,
                        (curr_inx == field_count));     
                        break;
                    case PRINT_STEPMEM:
                        tmp_uint64 = sjinfo_print->stepmem;
                        if (tmp_uint64 != NO_VAL64)
                            convert_num_unit((double)tmp_uint64, outbuf,
                                    sizeof(outbuf), UNIT_KILO,
                                    params->units,
                                    params->convert_flags);
                        field->print_routine(field,
                                    outbuf,
                                    (curr_inx == field_count));    
                        break;   
                    case PRINT_STEPVMEM:
                        tmp_uint64 = sjinfo_print->stepvmem;
                        if (tmp_uint64 != NO_VAL64)
                            convert_num_unit((double)tmp_uint64, outbuf,
                                    sizeof(outbuf), UNIT_KILO,
                                    params->units,
                                    params->convert_flags);
                        field->print_routine(field,
                                    outbuf,
                                    (curr_inx == field_count));    
                        break; 
                    case PRINT_STEPPAGES:
                        sprintf(tmp_char, "%lu", sjinfo_print->steppages);
                        field->print_routine(field,
                        tmp_char,
                        (curr_inx == field_count));     
                        break; 
                    case PRINT_MAXSTEPCPU:
                        sprintf(tmp_char, "%.2f", sjinfo_print->stepcpumax);
                        field->print_routine(field,
                        tmp_char,
                        (curr_inx == field_count));     
                        break;
                    case PRINT_MINSTEPCPU:
                        sprintf(tmp_char, "%.2f", sjinfo_print->stepcpumin);
                        field->print_routine(field,
                        tmp_char,
                        (curr_inx == field_count));     
                        break;      
                    case PRINT_MAXSTEPMEM:
                        tmp_uint64 = sjinfo_print->stepmemmax;
                        if (tmp_uint64 != NO_VAL64)
                            convert_num_unit((double)tmp_uint64, outbuf,
                                    sizeof(outbuf), UNIT_KILO,
                                    params->units,
                                    params->convert_flags);
                        field->print_routine(field,
                                    outbuf,
                                    (curr_inx == field_count));  
                        break; 
                    case PRINT_MINSTEPMEM:
                        tmp_uint64 = sjinfo_print->stepmemmin;
                        if (tmp_uint64 != NO_VAL64)
                            convert_num_unit((double)tmp_uint64, outbuf,
                                    sizeof(outbuf), UNIT_KILO,
                                    params->units,
                                    params->convert_flags);
                        field->print_routine(field,
                                    outbuf,
                                    (curr_inx == field_count));  
                        break; 
                    case PRINT_MAXSTEPVMEM:
                        tmp_uint64 = sjinfo_print->stepvmemmax;
                        if (tmp_uint64 != NO_VAL64)
                            convert_num_unit((double)tmp_uint64, outbuf,
                                    sizeof(outbuf), UNIT_KILO,
                                    params->units,
                                    params->convert_flags);
                        field->print_routine(field,
                                    outbuf,
                                    (curr_inx == field_count));    
                        break;
                    case PRINT_MINSTEPVMEM:
                        //sprintf(tmp_char, "%lld", sjinfo_print->stepvmemmin);
                        tmp_uint64 = sjinfo_print->stepvmemmin;
                        if (tmp_uint64 != NO_VAL64)
                            convert_num_unit((double)tmp_uint64, outbuf,
                                    sizeof(outbuf), UNIT_KILO,
                                    params->units,
                                    params->convert_flags);
                        field->print_routine(field,
                                    outbuf,
                                    (curr_inx == field_count));      
                        break;   
                    
                    case PRINT_STEPDCU:
                        sprintf(tmp_char, "%.2f", sjinfo_print->stepdcu);
                        field->print_routine(field,
                        tmp_char,
                        (curr_inx == field_count));     
                        break;
                    case PRINT_STEPDCUMEM:
                        tmp_uint64 = sjinfo_print->stepdcumem;
                        if (tmp_uint64 != NO_VAL64) {
                            uint32_t flags = params->convert_flags;
                            /* 如果用户没有指定单位，禁用自动转换 */
                            if ((uint32_t)params->units == NO_VAL)
                                flags |= CONVERT_NUM_UNIT_NO;
                            convert_num_unit((double)tmp_uint64, outbuf,
                                    sizeof(outbuf), UNIT_KILO,
                                    params->units,
                                    flags);
                        }
                        field->print_routine(field,
                                    outbuf,
                                    (curr_inx == field_count));    
                        break;
                    case PRINT_MAXSTEPDCU:
                        sprintf(tmp_char, "%.2f", sjinfo_print->stepdcumax);
                        field->print_routine(field,
                        tmp_char,
                        (curr_inx == field_count));     
                        break;
                    case PRINT_MINSTEPDCU:
                        sprintf(tmp_char, "%.2f", sjinfo_print->stepdcumin);
                        field->print_routine(field,
                        tmp_char,
                        (curr_inx == field_count));     
                        break;
                    case PRINT_MAXSTEPDCUMEM:
                        tmp_uint64 = sjinfo_print->stepdcumemmax;
                        if (tmp_uint64 != NO_VAL64) {
                            uint32_t flags = params->convert_flags;
                            /* 如果用户没有指定单位，禁用自动转换 */
                            if ((uint32_t)params->units == NO_VAL)
                                flags |= CONVERT_NUM_UNIT_NO;
                            convert_num_unit((double)tmp_uint64, outbuf,
                                    sizeof(outbuf), UNIT_KILO,
                                    params->units,
                                    flags);
                        }
                        field->print_routine(field,
                                    outbuf,
                                    (curr_inx == field_count));    
                        break;
                    case PRINT_MINSTEPDCUMEM:
                        tmp_uint64 = sjinfo_print->stepdcumemmin;
                        if (tmp_uint64 != NO_VAL64) {
                            uint32_t flags = params->convert_flags;
                            /* 如果用户没有指定单位，禁用自动转换 */
                            if ((uint32_t)params->units == NO_VAL)
                                flags |= CONVERT_NUM_UNIT_NO;
                            convert_num_unit((double)tmp_uint64, outbuf,
                                    sizeof(outbuf), UNIT_KILO,
                                    params->units,
                                    flags);
                        }
                        field->print_routine(field,
                                    outbuf,
                                    (curr_inx == field_count));    
                        break;

                    case PRINT_TOTALCPU:
                        sprintf(tmp_char, "%.2f", sjinfo_print->total_cpu);
                        field->print_routine(field, tmp_char, (curr_inx == field_count));
                        break;
                    case PRINT_TOTALMEM:
                        tmp_uint64 = sjinfo_print->total_mem;
                        if (tmp_uint64 != NO_VAL64)
                            convert_num_unit((double)tmp_uint64, outbuf, sizeof(outbuf), 
                                            UNIT_KILO, params->units, params->convert_flags);
                        field->print_routine(field, outbuf, (curr_inx == field_count));
                        break;
                    case PRINT_TOTALVMEM:
                        tmp_uint64 = sjinfo_print->total_vmem;
                        if (tmp_uint64 != NO_VAL64)
                            convert_num_unit((double)tmp_uint64, outbuf, sizeof(outbuf), 
                                            UNIT_KILO, params->units, params->convert_flags);
                        field->print_routine(field, outbuf, (curr_inx == field_count));
                        break;
                    case PRINT_TOTALPAGES:
                        sprintf(tmp_char, "%lu", sjinfo_print->total_pages);
                        field->print_routine(field, tmp_char, (curr_inx == field_count));
                        break;
                    case PRINT_MAXCPU:
                        sprintf(tmp_char, "%.2f", sjinfo_print->max_cpu);
                        field->print_routine(field, tmp_char, (curr_inx == field_count));
                        break;
                    case PRINT_MINCPU:
                        sprintf(tmp_char, "%.2f", sjinfo_print->min_cpu);
                        field->print_routine(field, tmp_char, (curr_inx == field_count));
                        break;
                    case PRINT_MAXMEM:
                        tmp_uint64 = sjinfo_print->max_mem;
                        if (tmp_uint64 != NO_VAL64)
                            convert_num_unit((double)tmp_uint64, outbuf, sizeof(outbuf), 
                                            UNIT_KILO, params->units, params->convert_flags);
                        field->print_routine(field, outbuf, (curr_inx == field_count));
                        break;
                    case PRINT_MINMEM:
                        tmp_uint64 = sjinfo_print->min_mem;
                        if (tmp_uint64 != NO_VAL64)
                            convert_num_unit((double)tmp_uint64, outbuf, sizeof(outbuf), 
                                            UNIT_KILO, params->units, params->convert_flags);
                        field->print_routine(field, outbuf, (curr_inx == field_count));
                        break;
                    case PRINT_MAXVMEM:
                        tmp_uint64 = sjinfo_print->max_vmem;
                        if (tmp_uint64 != NO_VAL64)
                            convert_num_unit((double)tmp_uint64, outbuf, sizeof(outbuf), 
                                            UNIT_KILO, params->units, params->convert_flags);
                        field->print_routine(field, outbuf, (curr_inx == field_count));
                        break;
                    case PRINT_MINVMEM:
                        tmp_uint64 = sjinfo_print->min_vmem;
                        if (tmp_uint64 != NO_VAL64)
                            convert_num_unit((double)tmp_uint64, outbuf, sizeof(outbuf), 
                                            UNIT_KILO, params->units, params->convert_flags);
                        field->print_routine(field, outbuf, (curr_inx == field_count));
                        break;
                    case PRINT_TOTALDCU:
                        sprintf(tmp_char, "%.2f", sjinfo_print->total_dcu);
                        field->print_routine(field, tmp_char, (curr_inx == field_count));
                        break;
                    case PRINT_TOTALDCUMEM:
                        tmp_uint64 = sjinfo_print->total_dcumem;
                        if (tmp_uint64 != NO_VAL64) {
                            uint32_t flags = params->convert_flags;
                            /* 如果用户没有指定单位，禁用自动转换 */
                            if ((uint32_t)params->units == NO_VAL)
                                flags |= CONVERT_NUM_UNIT_NO;
                            convert_num_unit((double)tmp_uint64, outbuf, sizeof(outbuf), 
                                            UNIT_KILO, params->units, flags);
                        }
                        field->print_routine(field, outbuf, (curr_inx == field_count));
                        break;
                    case PRINT_MAXDCU:
                        sprintf(tmp_char, "%.2f", sjinfo_print->max_dcu);
                        field->print_routine(field, tmp_char, (curr_inx == field_count));
                        break;
                    case PRINT_MINDCU:
                        sprintf(tmp_char, "%.2f", sjinfo_print->min_dcu);
                        field->print_routine(field, tmp_char, (curr_inx == field_count));
                        break;
                    case PRINT_MAXDCUMEM:
                        tmp_uint64 = sjinfo_print->max_dcumem;
                        if (tmp_uint64 != NO_VAL64) {
                            uint32_t flags = params->convert_flags;
                            /* 如果用户没有指定单位，禁用自动转换 */
                            if ((uint32_t)params->units == NO_VAL)
                                flags |= CONVERT_NUM_UNIT_NO;
                            convert_num_unit((double)tmp_uint64, outbuf, sizeof(outbuf), 
                                            UNIT_KILO, params->units, flags);
                        }
                        field->print_routine(field, outbuf, (curr_inx == field_count));
                        break;
                    case PRINT_MINDCUMEM:
                        tmp_uint64 = sjinfo_print->min_dcumem;
                        if (tmp_uint64 != NO_VAL64) {
                            uint32_t flags = params->convert_flags;
                            /* 如果用户没有指定单位，禁用自动转换 */
                            if ((uint32_t)params->units == NO_VAL)
                                flags |= CONVERT_NUM_UNIT_NO;
                            convert_num_unit((double)tmp_uint64, outbuf, sizeof(outbuf), 
                                            UNIT_KILO, params->units, flags);
                        }
                        field->print_routine(field, outbuf, (curr_inx == field_count));
                        break;

                    case PRINT_CPUTHRESHOLD:
                        sprintf(tmp_char, "%lu%%", sjinfo_print->cputhreshold);
                        field->print_routine(field,
                        tmp_char,
                        (curr_inx == field_count));     
                        break; 

                    case PRINT_GRESTHRESHOLD:
                        sprintf(tmp_char, "%lu%%", sjinfo_print->gresthreshold);
                        field->print_routine(field,
                        tmp_char,
                        (curr_inx == field_count));     
                        break; 

                    case PRINT_START:
                        timeinfo = localtime((const time_t *)&sjinfo_print->start);
                        if(timeinfo){
                            strftime(tmp_char, sizeof(tmp_char), "%Y-%m-%dT%H:%M:%S", timeinfo);
                            field->print_routine(field,
                            tmp_char,
                            (curr_inx == field_count));
                        }else{
                            fprintf(stderr, "Error: Failed to convert time from start. Value: %lu\n", sjinfo_print->start);
                        }
                        break;  

                    case PRINT_END:
                        timeinfo = localtime((const time_t *)&sjinfo_print->end);
                        if(timeinfo){
                            strftime(tmp_char, sizeof(tmp_char), "%Y-%m-%dT%H:%M:%S", timeinfo);
                            field->print_routine(field,
                            tmp_char,
                            (curr_inx == field_count));
                        }else{
                            fprintf(stderr, "Error: Failed to convert time from end. Value: %lu\n", sjinfo_print->end);
                        }
                        
                        break; 
                    
                    case PRINT_TYPE:
                        if(sjinfo_print->type1 || xstrcmp(sjinfo_print->type, CPU_ABNORMAL_FLAG) == 0) {
                            if(tmp_char != NULL && tmp_char[0] != '\0')
                                strcat(tmp_char, ",");
                            strcat(tmp_char, CPU_ABNORMAL_FLAG_DESC);
                        } 
                        if(sjinfo_print->type2 || xstrcmp(sjinfo_print->type, PROCESS_ABNORMAL_FLAG) == 0) {
                            if(tmp_char != NULL && tmp_char[0] != '\0')
                                strcat(tmp_char, ",");
                            strcat(tmp_char, PROCESS_ABNORMAL_FLAG_DESC);
                        }
                        if(sjinfo_print->type3 || xstrcmp(sjinfo_print->type, NODE_ABNORMAL_FLAG) == 0) {
                            if(tmp_char != NULL && tmp_char[0] != '\0')
                                strcat(tmp_char, ",");
                            strcat(tmp_char, NODE_ABNORMAL_FLAG_DESC);
                        }
                        if(sjinfo_print->type4 || xstrcmp(sjinfo_print->type, GPU_ABNORMAL_FLAG) == 0) {
                            if(tmp_char != NULL && tmp_char[0] != '\0')
                                strcat(tmp_char, ",");
                            strcat(tmp_char, GPU_ABNORMAL_FLAG_DESC);
                        }

                        field->print_routine(field,
                        tmp_char,
                        (curr_inx == field_count));     
                        break;   

                    case PRINT_SUMCPU:
                        sprintf(tmp_char, "%lu", sjinfo_print->sum_cpu);
                        field->print_routine(field,
                        tmp_char,
                        (curr_inx == field_count));
                        break;
                    case PRINT_SUMGPU:
                        sprintf(tmp_char, "%lu", sjinfo_print->sum_gpu);
                        field->print_routine(field,
                        tmp_char,
                        (curr_inx == field_count));
                        break;

                    case PRINT_SUMPID:
                        sprintf(tmp_char, "%lu", sjinfo_print->sum_pid);
                        field->print_routine(field,
                        tmp_char,
                        (curr_inx == field_count));
                        break;

                    case PRINT_SUMNODE:
                        sprintf(tmp_char, "%lu", sjinfo_print->sum_node);
                        field->print_routine(field,
                        tmp_char,
                        (curr_inx == field_count));
                        break;

                    case PRINT_LASTSTART:
                        timeinfo = localtime((const time_t *)&sjinfo_print->start_last);
                        if(timeinfo){
                            strftime(tmp_char, sizeof(tmp_char), "%Y-%m-%dT%H:%M:%S", timeinfo);
                            field->print_routine(field,
                            tmp_char,
                            (curr_inx == field_count));
                        }else{
                            fprintf(stderr, "Error: Failed to convert time from start_last. Value: %lu\n", sjinfo_print->start_last);
                        }
                        break;

                    case PRINT_LASTEND:
                        timeinfo = localtime((const time_t *)&sjinfo_print->end_last);
                        if(timeinfo){
                            strftime(tmp_char, sizeof(tmp_char), "%Y-%m-%dT%H:%M:%S", timeinfo);
                            field->print_routine(field,
                            tmp_char,
                            (curr_inx == field_count));  
                        }else{
                            fprintf(stderr, "Error: Failed to convert time from end_last. Value: %lu\n", sjinfo_print->end_last);
                        }
                        break; 
                    }
                    curr_inx++;
            }
            printf("\n");
        }

        if (print_value_itr)
		    list_iterator_destroy(print_value_itr);    
}

extern void field_split(char *field_str, print_field_t* fields_tmp, list_t *sj_list)
{
    int i = 0;
    char *end = NULL, *start = NULL;
    char *field_copy = xmalloc(strlen(field_str) + 10);
    strcpy(field_copy, field_str);
    start = field_copy;
    strcat(start,",");
    while ((end = strstr(start, ","))) {
        char *tmp_char = NULL;
        int command_len = 0;
        int newlen = 0;
        bool newlen_set = false;

        *end = 0;
        while (isspace(*start))
            start++;	/* discard whitespace */
        if (!(int)*start)
            continue;

        if ((tmp_char = strstr(start, "\%"))) {
            newlen_set = true;
            newlen = atoi(tmp_char + 1);
            tmp_char[0] = '\0';
        }

        command_len = strlen(start);
            
        if (!strncasecmp("ALL", start, command_len)) {
            for (i = 0; fields_tmp[i].name; i++) {
                if (newlen_set)
                    fields_tmp[i].len = newlen;
                list_append(sj_list, &fields_tmp[i]);
                start = end + 1;
            }
            start = end + 1;
            continue;
        }
            
        for (i = 0; fields_tmp[i].name; i++) {
            if (!strncasecmp(fields_tmp[i].name, start, command_len))
                goto foundfield;
        }
        continue;
foundfield:
        if (newlen_set)
            fields_tmp[i].len = newlen;

        list_append(sj_list, &fields_tmp[i]);
        start = end + 1;

    }
    if(field_copy)
        xfree(field_copy);

}


extern void print_star_line(const char *content) {
    size_t content_len = strlen(content);
    size_t max_content_len = LINE_WIDTH - 2;
    size_t copy_len = (content_len < max_content_len) ? content_len : max_content_len;
    //int pad = LINE_WIDTH - 2 - content_len;

    char line[LINE_WIDTH + 1];
    memset(line, ' ', LINE_WIDTH);
    line[0] = ' ';
    line[LINE_WIDTH - 1] = ' ';
    line[LINE_WIDTH] = '\0';

    memcpy(line + 1, content, copy_len); // 左对齐内容
    printf("%s\n", line);
}

extern void print_efficiency(interface_sjinfo_t *sjinfo_print, const char *time_str) {

    char info_buf[LINE_WIDTH] = {'\0'};
    char stepid_buf[32] = {'\0'};
    char m[6] = " MB   ";
    char g[6] = " GB   ";
    char t[6] = " TB   ";
    char p[6] = " PB   ";
    char cpu_unit_buf[6] = "CORES";
    char at[3] = "at";
    char stepid_mem[64] = {'\0'};
    char stepid_cpu[64] = {'\0'};
    char jobstep_buf[32] = {'\0'};
    char stepid_mem_aligned[27] = {'\0'};
    char stepid_cpu_aligned[27] = {'\0'};
     
    /*test*/
    // sjinfo_print->alloc_cpu =10000;
    // sjinfo_print->stepmem =123113LL*1024;
    // sjinfo_print->req_mem = 14313213;
    //sjinfo_print->jobid = 1;
    if (sjinfo_print->stepid == -5) {
        snprintf(stepid_buf, sizeof(stepid_buf), "batch");
    } else {
        snprintf(stepid_buf, sizeof(stepid_buf), "%d", sjinfo_print->stepid);
    }
   
    snprintf(jobstep_buf, sizeof(jobstep_buf), "%ld.%.*s", sjinfo_print->jobid, 8, stepid_buf);
    /* mem字符串转化 */

    // int count = num_digits(sjinfo_print->req_mem);

    // if(count == -1)
    //     return;
    double result = (double)(sjinfo_print->stepmem * 100.0/1024/sjinfo_print->req_mem); 
    if (sjinfo_print->req_mem > 1024*1024 && sjinfo_print->req_mem <= 1024*1024*1024) {
        snprintf(stepid_mem, sizeof(stepid_mem), "%6.2f of %10lld %6s", result, sjinfo_print->req_mem/1024, g);
    } else if (sjinfo_print->req_mem > 1024*1024*1024 && sjinfo_print->req_mem <= (long int)(1024L*1024L*1024L*1024L)) {
        snprintf(stepid_mem, sizeof(stepid_mem), "%6.2f of %10lld %6s", result, sjinfo_print->req_mem/1024/1024, t);
    } else if (sjinfo_print->req_mem > (long int)(1024L*1024L*1024L*1024L)) {
        snprintf(stepid_mem, sizeof(stepid_mem), "%6.2f of %10lld %6s", result, sjinfo_print->req_mem/1024/1024/1024, p);
    } else {
        snprintf(stepid_mem, sizeof(stepid_mem), "%6.2f of %10lld %6s", result, sjinfo_print->req_mem, m);
    }

    /* 对 stepid_mem 对齐以避免 warning */
    snprintf(stepid_mem_aligned, sizeof(stepid_mem_aligned), "%26.26s", stepid_mem);

    /* cpu字段字符串转化 */
    snprintf(stepid_cpu, sizeof(stepid_cpu), "%6.2f of %10lld %6s", (double)(sjinfo_print->stepcpu),sjinfo_print->alloc_cpu,cpu_unit_buf);
    snprintf(stepid_cpu_aligned, sizeof(stepid_cpu_aligned), "%26.26s", stepid_cpu);

    /* CPU Efficiency 行 */
    snprintf(info_buf, sizeof(info_buf),
             "    %-20.20s %-18.18s %26.26s %3.3s %-20.20s",
             jobstep_buf, "CPU Efficiency(%)", stepid_cpu_aligned,at,time_str);
    print_star_line(info_buf);

    /* MEM Efficiency 行 */
    snprintf(info_buf, sizeof(info_buf),
             "    %-20.20s %-18.18s %26.26s %3.3s %-20.20s",
             jobstep_buf, "MEM Efficiency(%)", stepid_mem_aligned,at,time_str);
    print_star_line(info_buf);
}

extern void job_brief(query_job_record_t *query_send, sjinfo_parameters_t *params)
{
    list_itr_t *print_display_itr = NULL;
    interface_sjinfo_t  * sjinfo_print = NULL;
    char beijing_buf[64] = {'\0'};
    printf("******************************************************************************************************* \n");
    printf("*                                Display brief information of job steps                               *\n");
    print_display_itr = list_iterator_create(query_send->print_display_list);
    while ((sjinfo_print = list_next(print_display_itr))) {
        int rc =  parse_utc_time_to_local(sjinfo_print->time, beijing_buf, sizeof(beijing_buf));
        if( rc == -1 || sjinfo_print->req_mem == NOT_FIND || sjinfo_print->req_mem == 0) {
            printf("*                                No request information available                                     *\n");
        } else {
            print_efficiency(sjinfo_print, beijing_buf);
        }
    }
    list_iterator_destroy(print_display_itr);
    printf("******************************************************************************************************* \n");
    printf("\n");
    print_options(query_send->print_fields_list, query_send->print_value_list, query_send->print_fields_itr, params);
    printf("\n");
}

extern void print_field(query_job_record_t *query_send, sjinfo_parameters_t *params)
{

    char *opt_step_list           = NULL;
    char *opt_event_list          = NULL;
    char *opt_overall_list        = NULL;
    char *opt_apptype_list        = NULL;
    char *opt_apptype_job_list    = NULL;
    char *opt_job_summary_list    = NULL;
    // char *opt_display             = xmalloc(160);
    char base_step_field[]        = "JobID,StepID,StepCPU,"
                "StepAVECPU,StepMEM,StepVMEM,StepPages,MaxStepCPU,"
                "MinStepCPU,MaxStepMEM,MinStepMEM,MaxStepVMEM,MinStepVMEM,"
                "StepGPU(DCU),StepGPU(DCU)MaxStepGPU(DCU),MinSteGPU(DCU),MaxStepGPU(DCU)MEM,MinStepGPU(DCU)MEM,";
  char base_event_field[]       = "JobID,StepID,StepCPU,"
                "StepMEM,StepVMEM,StepPages,StepGPU(DCU),StepGPU(DCU)MEM,CPUthreshold,GRESthreshold,Start,End,Type,";
    char base_overall_field[]     = "JobID,StepID,Last_start,"
                "Last_end,CPU_Abnormal_CNT,PROC_Abnormal_CNT,NODE_Abnormal_CNT,GPU_Abnormal_CNT";
    char base_apptype_field[]     = "JobID,StepID,Apptype_CLI,Apptype_STEP,UserName";
    char base_apptype_job_field[] = "JobID,Apptype,UserName";
    char base_display_field[]     = "JobID,StepID,"
                "StepAVECPU,MaxStepCPU,"
                "MinStepCPU,MaxStepMEM,MinStepMEM,";
    char base_job_summary_field[] = "JobID,TotalCPU,TotalMEM,TotalVMEM,TotalPages,"
                                   "MaxCPU,MinCPU,MaxMEM,MinMEM,MaxVMEM,MinVMEM,"
                                   "TotalGPU(DCU),TotalGPU(DCU)MEM,MaxGPU(DCU),MinGPU(DCU),MaxGPU(DCU)MEM,MinGPU(DCU)MEM";
    opt_step_list           = xmalloc(sizeof(base_step_field));
    opt_event_list          = xmalloc(sizeof(base_event_field));
    opt_overall_list        = xmalloc(sizeof(base_overall_field));
    opt_apptype_list        = xmalloc(sizeof(base_apptype_field));
    opt_apptype_job_list    = xmalloc(sizeof(base_apptype_job_field));
    opt_job_summary_list    = xmalloc(sizeof(base_job_summary_field));
    if(!params->opt_field_list) {
        if(params->level & INFLUXDB_DISPLAY) {
            strcpy(opt_step_list, base_display_field);
            field_split(opt_step_list, fields, query_send->print_fields_list);
            job_brief(query_send, params);
            goto endit;
            return;
        }else {
            /*Consider the scenario where only one side of the field has it*/
            if(params->level & INFLUXDB_STEPD) {
                strcpy(opt_step_list, base_step_field);
                field_split(opt_step_list, fields, query_send->print_fields_list);
            } 

            if(params->level & INFLUXDB_EVENT)  {
                strcpy(opt_event_list,base_event_field);
                field_split(opt_event_list, field_event, query_send->print_events_list);
            }

            if(params->level & INFLUXDB_OVERALL) {
                strcpy(opt_overall_list,base_overall_field);
                field_split(opt_overall_list, field_overall, query_send->print_overall_list);
            }

            if(params->level & INFLUXDB_APPTYPE) {
                if(params->show_jobstep_apptype){
                    strcpy(opt_apptype_list, base_apptype_field);
                    field_split(opt_apptype_list, field_apptype, query_send->print_apptype_list);
                }else{
                    strcpy(opt_apptype_job_list, base_apptype_job_field);
                    field_split(opt_apptype_job_list, field_apptype_job, query_send->print_apptype_job_list);
                }
            }

            if(params->level & INFLUXDB_JOB_SUMMARY) {
                strcpy(opt_job_summary_list, base_job_summary_field);
                field_split(opt_job_summary_list, fields_job_summary, query_send->print_job_summary_list);
            }

        }
    } else {
        if(params->level & INFLUXDB_STEPD) 
            field_split(params->opt_field_list, fields, query_send->print_fields_list);
        if(params->level & INFLUXDB_EVENT)
            field_split(params->opt_field_list, field_event, query_send->print_events_list);
        if(params->level & INFLUXDB_OVERALL) 
            field_split(params->opt_field_list, field_overall, query_send->print_overall_list);
        if(params->level & INFLUXDB_APPTYPE) {
            if(params->show_jobstep_apptype)
                field_split(params->opt_field_list, field_apptype, query_send->print_apptype_list);
            else
                field_split(params->opt_field_list, field_apptype_job, query_send->print_apptype_job_list);
        }
        if(params->level & INFLUXDB_DISPLAY) {
            printf("The -o option is no longer supported when using the -D flag.\n");
        }
        if(params->level & INFLUXDB_JOB_SUMMARY) {
            field_split(params->opt_field_list, fields_job_summary, query_send->print_fields_list);
        }
    }


    if(list_count(query_send->print_fields_list) > 0){
        printf("***************************************************************************** \n");
        printf("******       Display resource consumption information of job steps    *******\n");
        printf("***************************************************************************** \n");
        printf("\n");
        print_options(query_send->print_fields_list, query_send->print_value_list, query_send->print_fields_itr, params);
        printf("\n");
    }


    if(list_count(query_send->print_events_list) > 0) {
        printf("***************************************************************************** \n");
        printf("******       Display job step exception event information            ******** \n");
        printf("***************************************************************************** \n");
        printf("\n");
        print_options(query_send->print_events_list, query_send->print_events_value_list, query_send->print_events_itr, params);
    }
       
    if(list_count(query_send->print_overall_list) > 0) {
        printf("***************************************************************************** \n");
        printf("*******     Display job step exception event overall information      ******* \n");
        printf("***************************************************************************** \n");
        printf("\n");
        print_options(query_send->print_overall_list, query_send->print_overall_value_list, query_send->print_overall_itr, params);
    }

    if(list_count(query_send->print_apptype_list) > 0) {
        printf("***************************************************************************** \n");
        printf("*******             Display job step apptype information              ******* \n");
        printf("***************************************************************************** \n");
        printf("\n");
        print_options(query_send->print_apptype_list, query_send->print_apptype_value_list, query_send->print_apptype_itr, params);
    }

    if(list_count(query_send->print_apptype_job_list) > 0) {
        printf("***************************************************************************** \n");
        printf("*******                Display job apptype information                ******* \n");
        printf("***************************************************************************** \n");
        printf("\n");
        print_options(query_send->print_apptype_job_list, query_send->print_apptype_job_value_list, query_send->print_apptype_job_itr, params);
    }

    if(list_count(query_send->print_job_summary_list) > 0 && (params->level & INFLUXDB_JOB_SUMMARY)) {
        printf("*****************************************************************************\n");
        printf("******       Display Job-Level Resource Consumption Information     *********\n");
        printf("*****************************************************************************\n");
        printf("\n");
        print_options(query_send->print_job_summary_list, query_send->print_job_summary_value_list, 
                     query_send->print_job_summary_itr, params);
        printf("\n");
    }
endit:
    xfree(opt_step_list);
    xfree(opt_event_list);
    xfree(opt_overall_list);
    xfree(opt_apptype_list);
    xfree(opt_apptype_job_list);
    xfree(opt_job_summary_list); 
}