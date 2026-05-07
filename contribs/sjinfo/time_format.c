/*****************************************************************************\
 *  time_format.c - implementation-independent job of influxdb info
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

#include <stdio.h>
#include <time.h>
#include <unistd.h>
#include <limits.h>
#include "time_format.h"
#include "xstring2.h"
#include "list2.h"
#include "influx_src.h" 
#include <inttypes.h>
#include <string.h>
#include <stdlib.h>


// #define xstrfmtcat(__p, __fmt, args...)	_xstrfmtcat(&(__p), __fmt, ## args)
//# define PRIu64		__PRI64_PREFIX "u"
/* unit types */


typedef struct unit_names {
	char *name;
	int name_len;
	int multiplier;
} unit_names_t;

static unit_names_t un[] = {
	{"seconds",	7,	1},
	{"second",	6,	1},
	{"minutes",	7,	60},
	{"minute",	6,	60},
	{"hours",	5,	(60*60)},
	{"hour",	4,	(60*60)},
	{"days",	4,	(24*60*60)},
	{"day",		3,	(24*60*60)},
	{"weeks",	5,	(7*24*60*60)},
	{"week",	4,	(7*24*60*60)},
	{NULL,		0,	0}
};

void time_format(char *time_go, time_t tran_time, bool now) 
{
   
    time_t rawtime = time(NULL);
    
    if (now) {
        struct tm *timeinfo = gmtime(&rawtime);
        char time_go1[21];
        strftime(time_go1, sizeof(time_go1), "%Y-%m-%dT%H:%M:%SZ", timeinfo);
        sprintf(time_go,"'%s'",time_go1);
    } else {
        struct tm *timeinfo = gmtime(&tran_time);
        /*ISO 8601 format*/
        if(tran_time !=0 ) {
            char time_go1[21]; 
            strftime(time_go1, sizeof(time_go1), "%Y-%m-%dT%H:%M:%SZ", timeinfo);
            sprintf(time_go,"'%s'",time_go1);
        } else {
            /* Getting the local time */
            struct tm timeinfo = *localtime(&rawtime);
            /* Gets the start time of the date on the day of the local time */
            timeinfo.tm_hour = 0;
            timeinfo.tm_min = 0;
            timeinfo.tm_sec = 0;

            time_t local_start_of_today = mktime(&timeinfo);
            /* Convert to utc time */
            struct tm *utc_timeinfo = gmtime(&local_start_of_today);

            char time_go1[21];
            strftime(time_go1, sizeof(time_go1), "%Y-%m-%dT%H:%M:%SZ", utc_timeinfo);
            sprintf(time_go, "'%s'", time_go1);
        }
    }
    
}

extern time_t slurm_mktime(struct tm *tp)
{
	/* Force tm_isdt to -1. */
	tp->tm_isdst = -1;
	return mktime(tp);
}

/* convert "HH:MM[:SS] [AM|PM]" string to numeric values
 * time_str (in): string to parse
 * pos (in/out): position of parse start/end
 * hour, minute, second (out): numberic values
 * RET: -1 on error, 0 otherwise
 */
static int _get_time(const char *time_str, int *pos, int *hour, int *minute,
		     int *second)
{
	int hr, min, sec;
	int offset = *pos;

	/* get hour */
	if ((time_str[offset] < '0') || (time_str[offset] > '9'))
		goto prob;
	hr = time_str[offset++] - '0';
	if (time_str[offset] != ':') {
		if ((time_str[offset] < '0') || (time_str[offset] > '9'))
			goto prob;
		hr = (hr * 10) + time_str[offset++] - '0';
	}
	if (hr > 23) {
		offset -= 2;
		goto prob;
	}
	if (time_str[offset] != ':')
		goto prob;
	offset++;

	/* get minute */
	if ((time_str[offset] < '0') || (time_str[offset] > '9'))
                goto prob;
	min = time_str[offset++] - '0';
	if ((time_str[offset] < '0') || (time_str[offset] > '9'))
		goto prob;
	min = (min * 10)  + time_str[offset++] - '0';
	if (min > 59) {
		offset -= 2;
		goto prob;
	}

	/* get optional second */
	if (time_str[offset] == ':') {
		offset++;
		if ((time_str[offset] < '0') || (time_str[offset] > '9'))
			goto prob;
		sec = time_str[offset++] - '0';
		if ((time_str[offset] < '0') || (time_str[offset] > '9'))
			goto prob;
		sec = (sec * 10)  + time_str[offset++] - '0';
		if (sec > 59) {
			offset -= 2;
			goto prob;
		}
	} else
		sec = 0;

	while (isspace((int)time_str[offset])) {
		offset++;
	}
	if (strncasecmp(time_str + offset, "pm", 2)== 0) {
		hr += 12;
		if (hr > 23) {
			if (hr == 24)
				hr = 12;
			else
				goto prob;
		}
		offset += 2;
	} else if (strncasecmp(time_str + offset, "am", 2) == 0) {
		if (hr > 11) {
			if (hr == 12)
				hr = 0;
			else
				goto prob;
		}
		offset += 2;
	}

	*pos = offset - 1;
	*hour   = hr;
	*minute = min;
	*second = sec;
	return 0;

 prob:	*pos = offset;
	return -1;
}

/* convert "MMDDYY" "MM.DD.YY" or "MM/DD/YY" string to numeric values
 * or "YYYY-MM-DD string to numeric values
* time_str (in): string to parse
 * pos (in/out): position of parse start/end
 * month, mday, year (out): numberic values
 * RET: -1 on error, 0 otherwise
 */
static int _get_date(const char *time_str, int *pos, int *month, int *mday,
		     int *year)
{
	int mon, day, yr;
	int offset = *pos;
	int len;

	if (!time_str)
		goto prob;

	len = strlen(time_str);

	if ((len >= (offset+7)) && (time_str[offset+4] == '-')
	    && (time_str[offset+7] == '-')) {
		/* get year */
		if ((time_str[offset] < '0') || (time_str[offset] > '9'))
			goto prob;
		yr = time_str[offset++] - '0';

		if ((time_str[offset] < '0') || (time_str[offset] > '9'))
			goto prob;
		yr = (yr * 10) + time_str[offset++] - '0';

		if ((time_str[offset] < '0') || (time_str[offset] > '9'))
			goto prob;
		yr = (yr * 10) + time_str[offset++] - '0';

		if ((time_str[offset] < '0') || (time_str[offset] > '9'))
			goto prob;
		yr = (yr * 10) + time_str[offset++] - '0';

		offset++; // for the -

		/* get month */
		mon = time_str[offset++] - '0';
		if ((time_str[offset] >= '0') && (time_str[offset] <= '9'))
			mon = (mon * 10) + time_str[offset++] - '0';
		if ((mon < 1) || (mon > 12)) {
			offset -= 2;
			goto prob;
		}

		offset++; // for the -

		/* get day */
		if ((time_str[offset] < '0') || (time_str[offset] > '9'))
			goto prob;
		day = time_str[offset++] - '0';
		if ((time_str[offset] >= '0') && (time_str[offset] <= '9'))
			day = (day * 10) + time_str[offset++] - '0';
		if ((day < 1) || (day > 31)) {
			offset -= 2;
			goto prob;
		}

		*pos = offset - 1;
		*month = mon - 1;	/* zero origin */
		*mday  = day;
		*year  = yr - 1900;     /* need to make it slurm_mktime
					   happy 1900 == "00" */
		return 0;
	}

	/* get month */
	mon = time_str[offset++] - '0';
	if ((time_str[offset] >= '0') && (time_str[offset] <= '9'))
		mon = (mon * 10) + time_str[offset++] - '0';
       	if ((mon < 1) || (mon > 12)) {
		offset -= 2;
		goto prob;
	}
	if ((time_str[offset] == '.') || (time_str[offset] == '/'))
		offset++;

	/* get day */
	if ((time_str[offset] < '0') || (time_str[offset] > '9'))
		goto prob;
	day = time_str[offset++] - '0';
	if ((time_str[offset] >= '0') && (time_str[offset] <= '9'))
		day = (day * 10) + time_str[offset++] - '0';
	if ((day < 1) || (day > 31)) {
		offset -= 2;
		goto prob;
	}
	if ((time_str[offset] == '.') || (time_str[offset] == '/'))
		offset++;

	/* get optional year */
	if ((time_str[offset] >= '0') && (time_str[offset] <= '9')) {
		yr = time_str[offset++] - '0';
		if ((time_str[offset] < '0') || (time_str[offset] > '9'))
			goto prob;
		yr = (yr * 10) + time_str[offset++] - '0';
	} else
		yr = 0;

	*pos = offset - 1;
	*month = mon - 1;	/* zero origin */
	*mday  = day;
	if (yr)
		*year  = yr + 100;	/* 1900 == "00" */
	return 0;

 prob:	*pos = offset;
	return -1;
}


int contains_non_digit(const char *str) {
    /* Check if the string is empty */
    if (str == NULL) {
        return SLURM_ERROR;
    }
    /* Check if the first character is a negative sign */
    if (*str == '-') {
        str++; // Skip the negative sign and check the rest of the string
    }
	if (*str == '\0') 
		return SLURM_ERROR;
    /* Traverse the string until encountering the null terminator '\0' */
    while (*str) {
        /* if the current character is not a digit */
        if (!isdigit((unsigned char)*str)) { 
            /* non-digit character exists*/
            return SLURM_ERROR; 
        }
        str++;
    }
    /*all characters are digits*/ 
    return SLURM_SUCCESS; 
}
/* convert time differential string into a number of seconds
 * time_str (in): string to parse
 * pos (in/out): position of parse start/end
 * delta (out): delta in seconds
 * RET: -1 on error, 0 otherwise
 */
static int _get_delta(const char *time_str, int *pos, long *delta)
{
	int i, offset;
	long cnt = 0;
	int digit = 0;

	for (offset = (*pos) + 1;
	     ((time_str[offset] != '\0') && (time_str[offset] != '\n'));
	     offset++) {
		if (isspace((int)time_str[offset]))
			continue;
		for (i=0; un[i].name; i++) {
			if (!strncasecmp((time_str + offset),
					 un[i].name, un[i].name_len)) {
				offset += un[i].name_len;
				cnt    *= un[i].multiplier;
				break;
			}
		}
		if (un[i].name)
			break;	/* processed unit name */
		if ((time_str[offset] >= '0') && (time_str[offset] <= '9')) {
			cnt = (cnt * 10) + (time_str[offset] - '0');
			digit++;
			continue;
		}
		goto prob;
	}

	if (!digit)	/* No numbers after the '=' */
		return -1;

	*pos = offset - 1;
	*delta = cnt;
	return 0;

 prob:	*pos = offset - 1;
	return -1;
}

/* Convert string to equivalent time value
 * input formats:
 *   today or tomorrow
 *   midnight, noon, fika (3 PM), teatime (4 PM)
 *   HH:MM[:SS] [AM|PM]
 *   MMDD[YY] or MM/DD[/YY] or MM.DD[.YY]
 *   MM/DD[/YY]-HH:MM[:SS]
 *   YYYY-MM-DD[THH:MM[:SS]]
 *   now[{+|-}count[seconds(default)|minutes|hours|days|weeks]]
 *
 * Invalid input results in message to stderr and return value of zero
 * NOTE: not thread safe
 * NOTE: by default this will look into the future for the next time.
 * if you want to look in the past set the past flag.
 */
extern time_t parse_time(const char *time_str, int past)
{
	time_t time_now;
	struct tm time_now_tm;
	int    hour = -1, minute = -1, second = 0;
	int    month = -1, mday = -1, year = -1;
	int    pos = 0;
	struct tm res_tm;
	time_t ret_time;

	if (strncasecmp(time_str, "uts", 3) == 0) {
		char *last = NULL;
		long uts = strtol(time_str+3, &last, 10);
		if ((uts < 1000000) || (uts == LONG_MAX) ||
		    (last == NULL) || (last[0] != '\0'))
			goto prob;
		return (time_t) uts;
	}

	time_now = time(NULL);
	localtime_r(&time_now, &time_now_tm);

	for (pos=0; ((time_str[pos] != '\0') && (time_str[pos] != '\n'));
	     pos++) {
		if (isblank((int)time_str[pos]) ||
		    (time_str[pos] == '-') || (time_str[pos] == 'T'))
			continue;
		if (strncasecmp(time_str+pos, "today", 5) == 0) {
			month = time_now_tm.tm_mon;
			mday = time_now_tm.tm_mday;
			year = time_now_tm.tm_year;
			pos += 4;
			continue;
		}
		if (strncasecmp(time_str+pos, "tomorrow", 8) == 0) {
			time_t later = time_now + (24 * 60 * 60);
			struct tm later_tm;
			localtime_r(&later, &later_tm);
			month = later_tm.tm_mon;
			mday = later_tm.tm_mday;
			year = later_tm.tm_year;
			pos += 7;
			continue;
		}
		if (strncasecmp(time_str+pos, "midnight", 8) == 0) {
			hour   = 0;
			minute = 0;
			second = 0;
			pos += 7;
			continue;
		}
		if (strncasecmp(time_str+pos, "noon", 4) == 0) {
			hour   = 12;
			minute = 0;
			second = 0;
			pos += 3;
			continue;
		}
		if (strncasecmp(time_str+pos, "fika", 4) == 0) {
			hour   = 15;
			minute = 0;
			second = 0;
			pos += 3;
			continue;
		}
		if (strncasecmp(time_str+pos, "teatime", 7) == 0) {
			hour   = 16;
			minute = 0;
			second = 0;
			pos += 6;
			continue;
		}
		if (strncasecmp(time_str+pos, "now", 3) == 0) {
			int i;
			long delta = 0;
			time_t later;
			struct tm later_tm;
			for (i=(pos+3); ; i++) {
				if (time_str[i] == '+') {
					pos += i;
					if (_get_delta(time_str, &pos, &delta))
						goto prob;
					break;
				}
				if (time_str[i] == '-') {
					pos += i;
					if (_get_delta(time_str, &pos, &delta))
						goto prob;
					delta = -delta;
					break;
				}
				if (isblank((int)time_str[i]))
					continue;
				if ((time_str[i] == '\0')
				    || (time_str[i] == '\n')) {
					pos += (i-1);
					break;
				}
				pos += i;
				goto prob;
			}
			later    = time_now + delta;
			localtime_r(&later, &later_tm);
			month = later_tm.tm_mon;
			mday = later_tm.tm_mday;
			year = later_tm.tm_year;
			hour = later_tm.tm_hour;
			minute = later_tm.tm_min;
			second = later_tm.tm_sec;
			continue;
		}

		if ((time_str[pos] < '0') || (time_str[pos] > '9'))
			/* invalid */
			goto prob;
		/* We have some numeric value to process */
		if ((time_str[pos+1] == ':') || (time_str[pos+2] == ':')) {
			/* Parse the time stamp */
			if (_get_time(time_str, &pos, &hour, &minute, &second))
				goto prob;
			continue;
		}

		if (_get_date(time_str, &pos, &month, &mday, &year))
			goto prob;
	}
/* 	printf("%d/%d/%d %d:%d\n",month+1,mday,year,hour+1,minute);  */


	if ((hour == -1) && (month == -1))		/* nothing specified, time=0 */
		return (time_t) 0;
	else if ((hour == -1) && (month != -1)) {	/* date, no time implies 00:00 */
		hour = 0;
		minute = 0;
	}
	else if ((hour != -1) && (month == -1)) {
		/* time, no date implies soonest day */
		if (past || (hour >  time_now_tm.tm_hour)
		    ||  ((hour == time_now_tm.tm_hour)
			 && (minute > time_now_tm.tm_min))) {
			/* today */
			month = time_now_tm.tm_mon;
			mday = time_now_tm.tm_mday;
			year = time_now_tm.tm_year;
		} else {/* tomorrow */
			time_t later = time_now + (24 * 60 * 60);
			struct tm later_tm;
			localtime_r(&later, &later_tm);
			month = later_tm.tm_mon;
			mday = later_tm.tm_mday;
			year = later_tm.tm_year;
		}
	}
	if (year == -1) {
		if (past) {
			if (month > time_now_tm.tm_mon) {
				/* last year */
				year = time_now_tm.tm_year - 1;
			} else  {
				/* this year */
				year = time_now_tm.tm_year;
			}
		} else if ((month  >  time_now_tm.tm_mon)
			   ||  ((month == time_now_tm.tm_mon)
				&& (mday > time_now_tm.tm_mday))
			   ||  ((month == time_now_tm.tm_mon)
				&& (mday == time_now_tm.tm_mday)
				&& (hour >  time_now_tm.tm_hour))
			   ||  ((month == time_now_tm.tm_mon)
				&& (mday == time_now_tm.tm_mday)
				&& (hour == time_now_tm.tm_hour)
				&& (minute > time_now_tm.tm_min))) {
			/* this year */
			year = time_now_tm.tm_year;
		} else {
			/* next year */
			year = time_now_tm.tm_year + 1;
		}
	}

	/* convert the time into time_t format */
	memset(&res_tm, 0, sizeof(res_tm));
	res_tm.tm_sec   = second;
	res_tm.tm_min   = minute;
	res_tm.tm_hour  = hour;
	res_tm.tm_mday  = mday;
	res_tm.tm_mon   = month;
	res_tm.tm_year  = year;

/* 	printf("%d/%d/%d %d:%d\n",month+1,mday,year,hour,minute); */
	if ((ret_time = slurm_mktime(&res_tm)) != -1)
		return ret_time;

 prob:	fprintf(stderr, "Invalid time specification (pos=%d): %s\n", pos, time_str);
	//errno = ESLURM_INVALID_TIME_VALUE;
	return (time_t) 0;
}


/*
 * Convert number from one unit to another.
 * By default, Will convert num to largest divisible unit.
 * Appends unit type suffix -- if applicable.
 *
 * IN num: number to convert.
 * OUT buf: buffer to copy converted number into.
 * IN buf_size: size of buffer.
 * IN orig_type: The original type of num.
 * IN spec_type: Type to convert num to. If specified, num will be converted up
 * or down to this unit type.
 * IN divisor: size of type
 * IN flags: flags to control whether to convert exactly or not at all.
 */
extern void convert_num_unit2(double num, char *buf, int buf_size,
			      int orig_type, int spec_type, int divisor,
			      uint32_t flags)
{
	char *unit = "\0KMGTP?";
	uint64_t i;

	if ((int64_t)num == 0) {
		snprintf(buf, buf_size, "0");
		return;
	}

	if ((unsigned int)spec_type != NO_VAL) {
		/* spec_type overrides all flags */
		if (spec_type < orig_type) {
			while (spec_type < orig_type) {
				num *= divisor;
				orig_type--;
			}
		} else if (spec_type > orig_type) {
			while (spec_type > orig_type) {
				num /= divisor;
				orig_type++;
			}
		}
	} else if (flags & CONVERT_NUM_UNIT_RAW) {
		orig_type = UNIT_NONE;
	} else if (flags & CONVERT_NUM_UNIT_NO) {
		/* no op */
	} else if (flags & CONVERT_NUM_UNIT_EXACT) {
		/* convert until we would loose precision */
		/* half values  (e.g., 2.5G) are still considered precise */

		while (num >= divisor
		       && ((uint64_t)num % (divisor / 2) == 0)) {
			num /= divisor;
			orig_type++;
		}
	} else {
		/* aggressively convert values */
		while (num >= divisor) {
			num /= divisor;
			orig_type++;
		}
	}

	if (orig_type < UNIT_NONE || orig_type > UNIT_PETA)
		orig_type = UNIT_UNKNOWN;
	i = (uint64_t)num;
	/* Here we are checking to see if these numbers are the same,
	 * meaning the float has not floating point.  If we do have
	 * floating point print as a float.
	*/
	if ((double)i == num)
		snprintf(buf, buf_size, "%"PRIu64"%c", i, unit[orig_type]);
	else
		snprintf(buf, buf_size, "%.2f%c", num, unit[orig_type]);
}

void convert_num_unit(double num, char *buf, int buf_size,
			     int orig_type, int spec_type, uint32_t flags)
{
	convert_num_unit2(num, buf, buf_size, orig_type, spec_type, 1024,
			  flags);
}

/*
 * slurm_diff_tv_str - build a string showing the time difference between two
 *		       times
 * IN tv1 - start of event
 * IN tv2 - end of event
 * OUT tv_str - place to put delta time in format "usec=%ld"
 * IN len_tv_str - size of tv_str in bytes
 * IN from - where the function was called form
 */
extern void slurm_diff_tv_str(struct timeval *tv1, struct timeval *tv2,
			      char *tv_str, int len_tv_str, const char *from,
			      long limit, long *delta_t)
{
	char p[64] = "";
	struct tm tm;
	int debug_limit = limit;

	(*delta_t)  = (tv2->tv_sec - tv1->tv_sec) * 1000000;
	(*delta_t) += tv2->tv_usec;
	(*delta_t) -= tv1->tv_usec;
	snprintf(tv_str, len_tv_str, "usec=%ld", *delta_t);
	if (from) {
		if (!limit) {
			/* NOTE: The slurmctld scheduler's default run time
			 * limit is 4 seconds, but that would not typically
			 * be reached. See "max_sched_time=" logic in
			 * src/slurmctld/job_scheduler.c */
			limit = 3000000;
			debug_limit = 1000000;
		}
		if ((*delta_t > debug_limit) || (*delta_t > limit)) {
			if (!localtime_r(&tv1->tv_sec, &tm))
				printf("localtime_r(): %m");
			if (strftime(p, sizeof(p), "%T", &tm) == 0)
				printf("strftime(): %m");
			if (*delta_t > limit) {
				printf("Warning: Note very large processing "
					"time from %s: %s began=%s.%3.3d",
					from, tv_str, p,
					(int)(tv1->tv_usec / 1000));
			} else {	/* Log anything over 1 second here */
				printf("Note large processing time from %s: "
				      "%s began=%s.%3.3d",
				      from, tv_str, p,
				      (int)(tv1->tv_usec / 1000));
			}
		}
	}
}


extern int parse_utc_time_to_local(time_t utc_time , char *out_buf, size_t buf_len) {

    if (utc_time == (time_t) -1) {
        return -1;
    }

    // 转换为本地时间（会自动根据当前时区调整）
    struct tm *local_tm = localtime(&utc_time);
    if (!local_tm) {
        return -1;
    }
    // 格式化为本地时间字符串
    if (strftime(out_buf, buf_len, "%Y-%m-%d %H:%M:%S", local_tm) == 0) {
        return -1;
    }

    return 0;
}