/*****************************************************************************\
 *  time_format.h - implementation-independent job of influxdb info
 *  functions
 *****************************************************************************
 *  Copyright (C) 2005 Hewlett-Packard Development Company, L.P.

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

/*****************************************************************************\
 *  Modification history
 *  
\*****************************************************************************/
#ifndef _INTERFACES_TIME_FORMAT_H
#define _INTERFACES_TIME_FORMAT_H
#include <stdio.h>
#include <sys/time.h>
#include <stdbool.h>
#include <stdint.h>
#include <ctype.h>
#include <strings.h>
#include <time.h>
#define CONVERT_NUM_UNIT_EXACT 0x00000001
#define CONVERT_NUM_UNIT_NO    0x00000002
#define CONVERT_NUM_UNIT_RAW   0x00000004
#define NO_VAL     (0xfffffffe)
#define NO_VAL64   (0xfffffffffffffffe)
#define DEF_TIMERS	struct timeval tv1, tv2; char tv_str[20] = ""; long delta_t;
#define START_TIMER	gettimeofday(&tv1, NULL)
#define END_TIMER do {							\
	gettimeofday(&tv2, NULL);					\
	slurm_diff_tv_str(&tv1, &tv2, tv_str, 20, NULL, 0, &delta_t);	\
} while (0)
#define END_TIMER2(from) do {						\
	gettimeofday(&tv2, NULL);					\
	slurm_diff_tv_str(&tv1, &tv2, tv_str, 20, from, 0, &delta_t);	\
} while (0)
#define END_TIMER3(from, limit) do {					\
	gettimeofday(&tv2, NULL);					\
	slurm_diff_tv_str(&tv1, &tv2, tv_str, 20, from, limit, &delta_t); \
} while (0)
#define DELTA_TIMER	delta_t
#define TIME_STR 	tv_str
enum {
	UNIT_NONE,
	UNIT_KILO,
	UNIT_MEGA,
	UNIT_GIGA,
	UNIT_TERA,
	UNIT_PETA,
	UNIT_UNKNOWN
};
void time_format(char *time_go, time_t tran_time, bool now);
extern time_t slurm_mktime(struct tm *tp);
extern time_t parse_time(const char *time_str, int past);
extern void convert_num_unit2(double num, char *buf, int buf_size,
			      int orig_type, int spec_type, int divisor,
			      uint32_t flags);
void convert_num_unit(double num, char *buf, int buf_size,
			     int orig_type, int spec_type, uint32_t flags); 
extern void slurm_diff_tv_str(struct timeval *tv1, struct timeval *tv2,
			      char *tv_str, int len_tv_str, const char *from,
			      long limit, long *delta_t);    
extern int contains_non_digit(const char *str);     
extern int parse_utc_time_to_local(time_t utc_time , char *out_buf, size_t buf_len);        
#endif