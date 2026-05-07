/*****************************************************************************\
 *  print_json.h - implementation-independent job of influxdb info
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
#ifndef _PRINT_JSON_H
#define _PRINT_JSON_H
#include "sjinfo.h"
extern void print_query_options(query_job_record_t *query_send);
extern void print_field(query_job_record_t *query_send, sjinfo_parameters_t *params);
extern void print_query(query_job_record_t *query_send, sjinfo_parameters_t *params);
void print_options(list_t *list_tprint_list, list_t *value_list, list_itr_t *print_itr, sjinfo_parameters_t *params);
extern void print_fields_str(print_field_t *field, char *value, int last);
void print_fields_header(list_t *print_fields_list);
extern void field_split(char *field_str, print_field_t* fields_tmp, list_t *sj_list);
extern void print_star_line(const char *content);
extern void print_efficiency(interface_sjinfo_t *sjinfo_print, const char *time_str);
extern void job_brief(query_job_record_t *query_send, sjinfo_parameters_t *params);
#endif

