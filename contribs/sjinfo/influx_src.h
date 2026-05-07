/*****************************************************************************\
 *  acct_gather_profile.h - implementation-independent job profile
 *  accounting plugin definitions
 *  Copyright (C) 2013 Bull S. A. S.
 *		Bull, Rue Jean Jaures, B.P.68, 78340, Les Clayes-sous-Bois.
 *
 *  Written by Rod Schultz <rod.schultz@bull.com>
 *
 *  This file is part of Slurm, a resource management program.
 *  For details, see <https://slurm.schedmd.com>.
 *  Please also read the included file: DISCLAIMER.
 *
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

#ifndef _INTERFACES_INFLUXSRC_H
#define _INTERFACES_INFLUXSRC_H
#include <stdbool.h>
#include <stdint.h>
/* general return codes */
#define SLURM_SUCCESS   0
#define ESPANK_SUCCESS 0
#define SLURM_ERROR    -1

typedef enum {
	NATIVERP,
	STEPDRP,
	EVENTRP,
	APPTYPERP,
	RPCNT
} RPType;

typedef struct {
    char *length;
	char *password;
	char *username;
    char *database;
    char *host;
    char *policy;
} slurm_influxdb;

/* Type for handling HTTP responses */
struct http_response {
	char *message;
	size_t size;
};

typedef struct {
	int opt_gid;		/* running persons gid */
	int opt_uid;		/* running persons uid */
	int units;		/* --units*/
	uint32_t convert_flags;	/* --noconvert */    
    uint64_t level;
	char *opt_field_list;	/* --fields= */ 
} spost_parameters_t;

typedef struct {
    bool start_label;
    bool end_label;
    bool user_label;

    bool jobid_out;
    bool step_out;
    bool no_step;
    bool no_jobid;

    bool query_label;  
} label_flags_t;

typedef struct {
	time_t usage_start;
	time_t usage_end;
	char* jobids;
	char* user;
	char* steps;
	char* events;
} join_sql_t;

extern char* influxdb_connect(slurm_influxdb *data, const char* sql, int type, bool display);
extern char* _parse_rt_policy(const char *rt_policy, RPType type);
extern int _send_data2(slurm_influxdb *influxdb_conf, char *datastr2);
/*Read a hexadecimal string and convert it to a uint8_t type array*/
extern int read_hex_bytes_from_file(char *path_tmp, const uint8_t *key, slurm_influxdb *data);
#endif