/*****************************************************************************\
 *  sql_api.h - implementation-independent job of influxdb info
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
#ifndef _INTERFACES_SQL_API_H
#define _INTERFACES_SQL_API_H
#include <stdio.h>
#include <sys/time.h>
#include <stdbool.h>
#include <stdint.h>
#include <ctype.h>
#include <strings.h>
#include <time.h>
#include "xstring2.h"
#include "list2.h"
#include "sjinfo.h"
#include "influx_src.h"
#include <pwd.h>


enum {
    JOBID,
    STEP,
    USERNAME,
};
extern int strcat_stepd(const char* jobids, uint32_t* job_id, int* stepd);
extern int strcat_jobid_tag(char *sql, const char *str, bool flag);
extern char* reassemble_job_ids(const char *input);
extern int strcat_field(c_string_t* sql, const char *str, int field);
extern int stract_time(bool start_label, bool jobid_out, time_t usage_start,
                         time_t usage_end, char *start, c_string_t *sql_str);
extern int query_step_event(label_flags_t* sql_labels, join_sql_t *sql_time_sd, query_job_record_t *query_step_event);
int query_spost(query_job_record_t* query_step_event_send, bool jobid_out, char *jobids);
#endif