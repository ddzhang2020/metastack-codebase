#ifndef __QSTAT_H
#define __QSTAT_H

#include "common_util.h"

/* Resource key definitions */
#define KEY_MEM_FREE    "mem_free"
#define KEY_NUM_PROC    "num_proc"
#define KEY_HOSTNAME    "hostname"
#define KEY_TIME_LIMIT  "h_rt"       // Format: hh:mm:ss
#define KEY_GPU         "GPU"        // Format: GPU=3

/** Structure to hold command-line arguments. */
typedef struct {
    char *job_list;     /**< Comma-separated list of job IDs. */
    char *user_list;    /**< Comma-separated list of usernames. */
    bool is_help;       /**< Help flag. */
    bool is_version;    /**< Version flag. */
} qsub_args_t;

void add_handler(CommandParser *parser, CommandHandler handler);
int parse_command(CommandParser *parser, int argc, char *argv[]);
void destroy_parser(CommandParser *parser);

#endif