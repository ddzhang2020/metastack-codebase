#ifndef __QACCT_H
#define __QACCT_H

#include "common_util.h"
#include <grp.h>
#include <pwd.h>
#include <inttypes.h>

extern char *xstrdup_printf(const char *fmt, ...)
  __attribute__ ((format (printf, 1, 2)));

/* err msg */
#define SFNMAX 256
#define SGE_EVENT sge_event
static char sge_event[SFNMAX];
#define MSG_PARSE_NODATE              "No date specified."
#define MSG_PARSE_STARTTIMETOOLONG    "Starttime specifier field length exceeds maximum"
#define MSG_PARSE_INVALIDSECONDS      "Invalid format of seconds field."
#define MSG_PARSE_INVALIDHOURMIN      "Invalid format of date/hour-minute field."
#define MSG_PARSE_INVALIDMONTH        "Invalid month specification."
#define MSG_PARSE_INVALIDDAY          "Invalid day specification."
#define MSG_PARSE_INVALIDHOUR         "Invalid hour specification."
#define MSG_PARSE_INVALIDMINUTE       "Invalid minute specification."
#define MSG_PARSE_INVALIDSECOND       "Invalid seconds specification."
#define MSG_PARSE_NODATEFROMINPUT     "Couldn't generate date from input. Perhaps a date before 1970 was specified."

typedef enum {
	TRES_CPU = 1,
	TRES_MEM,
	TRES_ENERGY,
	TRES_NODE,
	TRES_BILLING,
	TRES_FS_DISK,
	TRES_VMEM,
	TRES_PAGES,
	TRES_STATIC_CNT
} tres_types_t;

typedef struct Option {
    bool is_set;
    char *value;
} Option;


typedef struct Qacct_args {
    Option job_list;
    Option user_list;
    Option start_time;
    Option end_time;
    Option is_help;
    Option is_version;
} Qacct_args;

/** Structure to hold user summary statistics. */
typedef struct {
    char *owner;        /**< Username. */
    double wallclock;   /**< Total wallclock time (seconds). */
    double utime;       /**< User CPU time (seconds). */
    double stime;       /**< System CPU time (seconds). */
    double cpu;         /**< Total CPU time (seconds). */
    double memory;      /**< Memory usage (GB-seconds). */
    double io;          /**< I/O usage (GB). */
    double iow;         /**< I/O wait time (seconds). */
} user_summary_t;

uint32_t sge_time_format_convert_to_slurm(const char *value);
void xfree_ptr(void *ptr);

// extern void add_handler(CommandParser* parser, CommandHandler handler);
// extern int parse_command(CommandParser* parser, int argc, char* argv[]);
// extern void destroy_parser(CommandParser* parser);
extern uint64_t slurmdb_find_tres_count_in_string(char *tres_str_in, int id);
extern void slurmdb_job_cond_def_start_end(slurmdb_job_cond_t *job_cond);


#endif