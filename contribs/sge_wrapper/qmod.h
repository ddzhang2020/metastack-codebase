#ifndef __QACCT_H
#define __QACCT_H

#include "common_util.h"

typedef struct {
    char *job_ids;         /**< Comma-separated job IDs or ranges. */
    bool is_version;       /**< Flag for version display. */
    bool is_suspend;       /**< Flag for suspend operation. */
    bool is_resume;        /**< Flag for resume operation. */
    bool is_help;
} qmod_args_t;

#endif