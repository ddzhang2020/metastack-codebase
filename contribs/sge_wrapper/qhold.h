#ifndef __QHOLD_H
#define __QHOLD_H

#include "common_util.h"

typedef struct {
    char *user_list;
    char *job_list;
    char *hold_type;
    bool is_help;
    bool is_version;
} qhold_args_t;

#endif