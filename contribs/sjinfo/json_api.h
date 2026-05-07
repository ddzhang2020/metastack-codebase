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
#ifndef _INTERFACES_JSON_API_H
#define _INTERFACES_JSON_API_H
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/time.h>
extern void destroy_brief_key_pair(void *object);
extern void parse_json(char *response, query_job_record_t* query_send);
void parse_json_tag(const char *response, list_t *print_query_value_list);
extern void free_interface_sjinfo(interface_sjinfo_t* iinfo);
#endif