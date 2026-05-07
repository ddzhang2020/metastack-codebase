/*****************************************************************************
 *
 *  Copyright (C) 2007-2008 Lawrence Livermore National Security, LLC.
 *  Produced at Lawrence Livermore National Laboratory.
 *  Written by Mark Grondona <mgrondona@llnl.gov>.
 *
 *  UCRL-CODE-235358
 * 
 *  This file is part of chaos-spankings, a set of spank plugins for SLURM.
 * 
 *  This is free software; you can redistribute it and/or modify it
 *  under the terms of the GNU General Public License as published by
 *  the Free Software Foundation; either version 2 of the License, or
 *  (at your option) any later version.
 *
 *  This is distributed in the hope that it will be useful, but WITHOUT
 *  ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 *  FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 *  for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program.  If not, see <http://www.gnu.org/licenses/>.
 ****************************************************************************/
#ifndef _SJINFO_H
#define _SJINFO_H

#include <stdio.h>
#include "list2.h"
#include "xstring2.h"
#include "influx_src.h"

#define CPU_ABNORMAL_FLAG           "cpu"
#define PROCESS_ABNORMAL_FLAG       "process"
#define NODE_ABNORMAL_FLAG          "node"
#define GPU_ABNORMAL_FLAG           "gres"
#define CPU_ABNORMAL_FLAG_DESC      "CPU utilization below threshold"
#define GPU_ABNORMAL_FLAG_DESC      "GPU(DCU) utilization below threshold"
#define PROCESS_ABNORMAL_FLAG_DESC  "Operational process anomalies"
#define NODE_ABNORMAL_FLAG_DESC     "Node communication exception"
#define FORMAT_STRING_SIZE          34
#define NOT_FIND                    -1

#define SACCT_MAX_ENTRIES           200
#define INFLUXDB_NONE               0x0000000000000000
#define INFLUXDB_STEPD              0x0000000000000001
#define INFLUXDB_EVENT              0x0000000000000010
#define INFLUXDB_OVERALL            0x0000000000000100
#define INFLUXDB_EVENT_FLAG         0x0000000000001000
#define INFLUXDB_APPTYPE            0X0000000000010000
#define INFLUXDB_DISPLAY            0x0000000000100000
#define INFLUXDB_JOB_SUMMARY        0x0000000001000000
#define INFLUXDB_ALL                0x0000000001010111
#define MAX_POLICY_NAME_LENGTH      256	/* Set the maximum length of the reservation policy name */
//#define JOB_SUMMARY_STEPID          -100
#ifndef MAX
#  define MAX(a,b) ((a) > (b) ? (a) : (b))
#endif

#ifndef MIN
#  define MIN(a,b) ((a) < (b) ? (a) : (b))
#endif

enum {
	UNIT_STEP,
	UNIT_EVENT,
    UNIT_OVERALL,
    UNIT_APPTYPE,
    UNIT_RUNJOB,
    UNIT_BRIEF,
    UNIT_JOB_SUMMARY,
    UNIT_JOB_SUMMARY_TOTAL,   
    UNIT_JOB_SUMMARY_MAXMIN,
};

typedef struct print_field {
	int len;  /* what is the width of the print */
	char *name;  /* name to be printed in header */
	void (*print_routine) (); /* what is the function to print with  */
	uint16_t type; /* defined in the local function */
} print_field_t;

typedef struct sacct_entry{
    long long int jobid;
    long long int stepdid;
    long long int reqmem;
    long long int alloc_cpu;

} sacct_entry_t;

typedef struct {
	int opt_gid;		/* running persons gid */
	int opt_uid;		/* running persons uid */
	int units;		/* --units*/
	uint32_t convert_flags;	/* --noconvert */    
	char *opt_field_list;	/* --fields= */ 
    uint64_t level;
    bool desc_set;      /* output data in reverse order*/
    // bool only_run_job;  /* querying for running jobs*/
    bool show_jobstep_apptype;  /* Displays the apptype information for each job step */
    bool display;        /*user show job */ 
} sjinfo_parameters_t;

typedef struct {
   char *  username;
   /*stepd*/ 
   time_t time;
   unsigned long jobid;
   int stepid;
   double stepcpu; 
   double stepcpumin; 
   double stepcpumax;  
   double stepcpuave;
   unsigned long int stepmem;
   unsigned long int stepmemmin;
   unsigned long int stepmemmax;
   unsigned long int stepvmem;
   unsigned long int stepvmemmin;
   unsigned long int stepvmemmax; 
   unsigned long int steppages;
   /* DCU */
   double stepdcu;         
   double stepdcumem;     
   double stepdcumax;     
   double stepdcumin;       
   double stepdcumemmax;    
   double stepdcumemmin;    
   /*event*/
   unsigned long cputhreshold;
   unsigned long gresthreshold;
   time_t start;
   time_t end;
   int type1;   // Marking cpu frequency anomalies
   int type2;   // identify process anomalies
   int type3;   // identifies node communication anomalies
   int type4;   // identifies GPU/DCU utilization anomalies
   char *type;   //dcu 
   /* overall */
   time_t end_last;
   time_t start_last;
   unsigned long sum_cpu;
   unsigned long sum_pid;
   unsigned long sum_node;
   unsigned long sum_gpu;
   /* apptype */
   char *apptype_cli;
   char *apptype_step;
   char *apptype;
   unsigned long cputime;
   long long int req_mem;
   long long int alloc_cpu;
   /* 作业汇总字段 */
   double total_cpu;          
   unsigned long int total_mem; 
   unsigned long int total_vmem; 
   unsigned long int total_pages; 
   double max_cpu;            
   double min_cpu;        
   unsigned long int max_mem;  
   unsigned long int min_mem;  
   unsigned long int max_vmem;  
   unsigned long int min_vmem;  
   double total_dcu;           
   double total_dcumem;      
   double max_dcu;          
   double min_dcu;             
   double max_dcumem;          
   double min_dcumem;  
} interface_sjinfo_t;

typedef struct {
    int jobid;
    int stepid;
    time_t record_time;
    char *uid;
    char *username;
    char *data;
    char *nodename;
} spost_record_t;

typedef struct {
    char *username;
    int flag;
    long int jobid_tran;
    time_t current_time;
    /*display table*/
    list_t *print_display_list;

    /*stepd table*/
    list_t *print_fields_list;
    list_t *print_value_list;
    list_itr_t *print_fields_itr;

    list_t *print_query_value_list;

    /*event table*/
    list_t *print_events_list;
    list_t *print_events_value_list;
    list_itr_t *print_events_itr;

    /*overall table*/
    list_t *print_overall_list;
    list_t *print_overall_value_list;
    list_itr_t *print_overall_itr;

    /*apptype table*/
    list_t *print_apptype_list;
    list_t *print_apptype_value_list;
    list_itr_t *print_apptype_itr;

    /*apptype table only job*/
    list_t *print_apptype_job_list;
    list_t *print_apptype_job_value_list;
    list_itr_t *print_apptype_job_itr;

    /* job summary table */
    list_t *print_job_summary_list;        
    list_t *print_job_summary_value_list;  
    list_itr_t *print_job_summary_itr; 
    

    c_string_t *job_list;
    sjinfo_parameters_t *params;
    slurm_influxdb *data;
    struct passwd *pw;
    int type;
} query_job_record_t;

typedef enum {
    PRINT_JOBID,
    PRINT_STEPID,
    PRINT_STEPAVECPU,
    PRINT_STEPCPU,
    PRINT_STEPMEM,
    PRINT_STEPVMEM,
    PRINT_STEPPAGES,
    PRINT_MAXSTEPCPU,
    PRINT_MINSTEPCPU,
    PRINT_MAXSTEPMEM,
    PRINT_MINSTEPMEM,
    PRINT_MAXSTEPVMEM,
    PRINT_MINSTEPVMEM,
    /* DCU 相关打印类型 */
    PRINT_STEPDCU,
    PRINT_STEPDCUMEM,
    PRINT_MAXSTEPDCU,
    PRINT_MINSTEPDCU,
    PRINT_MAXSTEPDCUMEM,
    PRINT_MINSTEPDCUMEM,
    PRINT_CPUTHRESHOLD,
    PRINT_GRESTHRESHOLD,
    PRINT_USERNAME,
    PRINT_UID,  
    PRINT_RECORD, 
    PRINT_TIME, 
    PRINT_START,
    PRINT_END,
    PRINT_LASTSTART,
    PRINT_LASTEND,
    PRINT_SUMCPU,
    PRINT_SUMPID,
    PRINT_SUMNODE,
    PRINT_SENDNODE,
    PRINT_SUMGPU,
    PRINT_TYPE,
    PRINT_APPTYPE_CLI,
    PRINT_APPTYPE_STEP,
    PRINT_APPTYPE,
    PRINT_TOTALCPU,
    PRINT_TOTALMEM,
    PRINT_TOTALVMEM,
    PRINT_TOTALPAGES,
    PRINT_MAXCPU,
    PRINT_MINCPU,
    PRINT_MAXMEM,
    PRINT_MINMEM,
    PRINT_MAXVMEM,
    PRINT_MINVMEM,
    PRINT_TOTALDCU,
    PRINT_TOTALDCUMEM,
    PRINT_MAXDCU,
    PRINT_MINDCU,
    PRINT_MAXDCUMEM,
    PRINT_MINDCUMEM,
    PRINT_CPUTIME
} sjinfo_print_types_t;

enum {
    PRINT_FIELDS_PARSABLE_NOT = 0,
    PRINT_FIELDS_PARSABLE_ENDING,
    PRINT_FIELDS_PARSABLE_NO_ENDING
};


/* global variable */
/*display table*/
extern  List print_display_list;

/*stepd table*/
extern List print_fields_list;
extern List print_value_list;
extern list_itr_t *print_fields_itr;

extern List print_query_value_list;

/*event table*/
extern List print_events_list;
extern List print_events_value_list;
extern list_itr_t *print_events_itr;

/*overall table*/
extern List print_overall_list;
extern List print_overall_value_list;
extern list_itr_t *print_overall_itr;

/*apptype table*/
extern List print_apptype_list;
extern List print_apptype_value_list;
extern list_itr_t *print_apptype_itr;

/*apptype table only job*/
extern List print_apptype_job_list;
extern List print_apptype_job_value_list;
extern list_itr_t *print_apptype_job_itr;
// /* Names for the values of the `has_arg' field of `struct option'.  */
extern void print_fields_str(print_field_t *field, char *value, int last);
extern int print_fields_parsable_print;
#define KEYDIR "/opt/gridview/slurm"
#endif /* !_SJINFO_H */

