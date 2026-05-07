/*****************************************************************************\
 *  spost.c - implementation-independent job of influxdb info
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
#include <stdio.h>
#include <time.h>
#include <string.h>
#include <strings.h>
#include <stdint.h>
#include <stdlib.h>
#include <stddef.h>
#include <fcntl.h>
#include <stdbool.h>
#include <curl/curl.h>
#include <stdarg.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <jansson.h>
#include <getopt.h>
#include <unistd.h>
#include <pwd.h>
#include <assert.h>
#include <ctype.h>
#include <inttypes.h>
#include <errno.h>
#include "sjinfo.h"
#include "aes.h"
#include "xstring2.h"
#include "influx_src.h" 
#include "time_format.h"
#include "sql_api.h"
#include "json_api.h"
#define MAX_SQL_STR   1024*4
/* create_directory() return code: user hit 10-second request rate limit. */
#define SPOST_RC_RATE_LIMITED 2


 /*AES-128 packet length is 16 bytes*/
#define BLOCKSIZE 16 



#define CONVERT_NUM_UNIT_EXACT 0x00000001
#define CONVERT_NUM_UNIT_NO    0x00000002
#define CONVERT_NUM_UNIT_RAW   0x00000004

#define NO_VAL     (0xfffffffe)
#define NO_VAL64   (0xfffffffffffffffe)

spost_parameters_t spost_params;

#define MAX_STRING_LENGTH 2000


void spost_init(slurm_influxdb *influxdb_data)
{
    /*given sufficient length*/
    influxdb_data->username = xmalloc(320) ;
    memset(influxdb_data->username, 0, 320);
    influxdb_data->password = xmalloc(320);
    memset(influxdb_data->password, 0, 320);
    influxdb_data->database = xmalloc(640) ;
    memset(influxdb_data->database, 0, 640);
    influxdb_data->host = xmalloc(640);
    memset(influxdb_data->host, 0, 320);
    influxdb_data->policy = xmalloc(640);
    memset(influxdb_data->policy, 0, 640);
}


void spost_fini(slurm_influxdb * influxdb_data)
{
    xfree(influxdb_data->username);
    xfree(influxdb_data->password);
    xfree(influxdb_data->database);
    xfree(influxdb_data->host);
    xfree(influxdb_data->policy);
    xfree(influxdb_data);
}

typedef enum {
	PROFILE_FIELD_NOT_SET,
	PROFILE_FIELD_UINT64,
	PROFILE_FIELD_DOUBLE
} acct_gather_profile_field_type_t;

typedef struct {
	char *name;
	acct_gather_profile_field_type_t type;
} acct_gather_profile_dataset_t;


typedef struct {
	char *name;
	acct_gather_profile_field_type_t type;
} send_string_t;

/* "Generate a nanosecond-level timestamp" */ 
uint64_t get_nanotime() {
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return (ts.tv_sec * 1000000000LL) + ts.tv_nsec;  
}

static char* _record_table(char * data, uint32_t jobid, int stepd, uid_t uid, char *username)
{
    if (data == NULL || username == NULL) {
        perror("data or user is NULL");
        return NULL;
    }
	time_t ct;
    int tmp = 0x7FFFFFFF;
    char dataset[10] = "Spost"; /* dataset associated to this task when profiling */
    ct = time(NULL);
    char *str = NULL;
    uint64_t timestamp = get_nanotime(); 
    char hostbuffer[2048] = {'0'};
    int hostname = gethostname(hostbuffer, sizeof(hostbuffer));
    if(hostname == -1) {
        memset(hostbuffer, 0, sizeof(hostbuffer));
        strcpy(hostbuffer,"no_host");
    }

    if(stepd != 0x7FFFFFFF) {
        xstrfmtcat(str, "%s,uid=%"PRIu64",step=%d,sendnode=%s jobid=%d,data=\"%s\",username=\"%s\",record_time=%"PRIu64" %"PRIu64"\n", 
                                dataset, uid, stepd, hostbuffer, jobid, data, username, (uint64_t)ct, timestamp);        
    }
    else
        xstrfmtcat(str, "%s,uid=%"PRIu64",step=%d,sendnode=%s jobid=%d,data=\"%s\",username=\"%s\",record_time=%"PRIu64" %"PRIu64"\n", 
                                dataset, uid, tmp, hostbuffer, jobid, data, username, (uint64_t)ct, timestamp);
    return str;
}
void print_spost_help(void)
{
    printf(
"spost [<OPTION>]                                                          \n"
"    Ordinary users can only query their own jobs.                          \n"
"    The flow limit for each user on the same node is 30 times in 10 seconds.\n"
"    Valid <OPTION> values are:                                            \n"
"     -j, --jobs:                                                          \n"
"        Specify the job ID,                                               \n"
"        and you can also specify the job step ID simultaneously,          \n"
"          e.g., spost -j 1001.batch.                                      \n"
"     -p, --post:                                                          \n"
"         Send customized messages,                                        \n"
"         Maximum support for sending 4k data in a single transmission.   \n" 
"\n");
}


struct flock lock;
int lock_file_write(int fd)
{
    if (fd < 0)
        return SLURM_ERROR;
    /* Lock file */
    lock.l_type = F_WRLCK;    // Write lock
    lock.l_whence = SEEK_SET; // The lock starts from the beginning of the file.
    lock.l_start = 0;         // The lock starts from the file.
    lock.l_len = 0;           // Lock the entire file.
    if (fcntl(fd, F_SETLKW, &lock) == -1) {
        perror("Error locking file");
        return SLURM_ERROR;
    }
    return SLURM_SUCCESS;
}
int unlock_file_write(int fd)
{
    if (fd < 0)
        return SLURM_ERROR;
    /* Unlock the file. */ 
    lock.l_type = F_UNLCK;   //Unlock
    if (fcntl(fd, F_SETLK, &lock) == -1) {
        perror("Error unlocking file");
        return SLURM_ERROR;
    }
    return SLURM_SUCCESS;
}

int create_directory(const char *path, char *script, uid_t uid, bool *flag) 
{
    int rc = SLURM_SUCCESS;
    if((path == NULL) ||(script == NULL)) {
        perror("path or script is NULL");
        return SLURM_ERROR;
    }
    struct stat st;
    char* full_path = NULL;
    xstrfmtcat(full_path, "%s/%s",path, script);
    int retry_cnt = 0;
    time_t timestamp = time(NULL);
    int count = 1;
    if (stat(path, &st) == 0) {
        if (S_ISDIR(st.st_mode)) {
            if (access(full_path, F_OK) == 0) {

                if (access(full_path, R_OK|W_OK) != 0) {
                    if(chmod(full_path, 0755) < 0) {
                         xfree(full_path);
                         return SLURM_ERROR;
                    }
                }
                uid_t uid_read;
                time_t timestamp_read;
                int count_read;
                char buffer[800];  
                int fd = -1;
                ssize_t bytes_read = -1;
                do {
                    retry_cnt++;
                    rc = SLURM_SUCCESS;
                    fd = open(full_path, O_RDWR  | O_CREAT , 0644);  
                    if (fd == -1) {
                        //perror("Error opening file, retrying");
                        rc =  SLURM_ERROR;
                        usleep(1000); 
                        continue;
                    }
                    if (lock_file_write(fd) != SLURM_SUCCESS) {
                        close(fd);
                        rc = SLURM_ERROR;
                        usleep(1000);
                        continue;
                    }

                    bytes_read = read(fd, buffer, sizeof(buffer) - 1);  
                    if (bytes_read == -1) {
                        //perror("Error reading file, retrying");
                        //printf("Error code: %d\n", errno);  
                        (void) unlock_file_write(fd);
                        *flag = true;
                        close(fd);
                        rc =  SLURM_ERROR;
                        usleep(1000); 
                        continue;
                    }
        
                    /* Use sscanf to parse what you read */ 
                    int parsed_count = sscanf(buffer, "%x,%lx,%x\n", &uid_read, &timestamp_read, &count_read);

                    if (parsed_count != 3) {
                        //printf("Error reading data, retrying.\n");
                        (void) unlock_file_write(fd);
                        close(fd);
                        *flag = true;
                        rc = SLURM_ERROR;
                        usleep(1000); 
                        continue;
                    }
                    (void) unlock_file_write(fd);
                    close(fd); 
                } while(rc == SLURM_ERROR && retry_cnt < 10);

               
                if(rc == SLURM_ERROR) {
                    perror("Error reading data error");
                    xfree(full_path);
                    return rc;
                }
                    
                //printf("UID: %d, Timestamp: %ld Count:%d\n", uid_read, timestamp_read, count_read);
                retry_cnt = 0;
                do {
                    retry_cnt++;
                    /* Reopen the file to clear its contents. `O_TRUNC` will clear the file contents. */ 
                    fd = open(full_path, O_WRONLY | O_TRUNC);  
                    if (fd == -1) {
                        //perror("Error opening file for writing");
                        rc =  SLURM_ERROR;
                        usleep(1000); 
                        continue;                       
                    }
                    if (lock_file_write(fd) != SLURM_SUCCESS) {
                        close(fd);
                        rc = SLURM_ERROR;
                        usleep(1000);
                        continue;
                    }
                    double tmp = difftime(timestamp, timestamp_read);
                    if(tmp <= 10 ) {
                        if(count_read < 30 && count_read >= 0) {
                            count = count_read + 1; 
                            char buffer[2560] = {'0'};
                            int len = snprintf(buffer, sizeof(buffer), "%x,%lx,%x\n",uid, timestamp_read, count);
                            ssize_t bytes_written = write(fd, buffer, len);
                            if (bytes_written == -1) {
                                //perror("Error writing to file, retrying");
                                (void) unlock_file_write(fd);
                                close(fd);
                                rc = SLURM_ERROR;
                                usleep(1000); 
                                continue;
                            }              
                            *flag = false;    
                        } else {
                            if(count_read < 0 ) {
                                count = 30;
                            }
                            int len = snprintf(buffer, sizeof(buffer), "%x,%lx,%x\n", uid, timestamp_read, count_read);
                            ssize_t bytes_written = write(fd, buffer, len);
                            if (bytes_written == -1) {
                                //perror("Error writing to file, retrying");
                                (void) unlock_file_write(fd);
                                close(fd);
                                rc = SLURM_ERROR;
                                usleep(1000); 
                                continue;
                            }  
                            *flag = true;
                            rc = SPOST_RC_RATE_LIMITED;
                            printf("Your user has triggered too many requests within 10 seconds." 
                                    "Please try again later. Currently, each user is allowed to execute" 
                                    " a maximum of 30 requests per node within 10 seconds.\n");

                        }
                
                    } else if( tmp > 10 ) {
                        *flag = false;  
                        count = 1;
                        int len = snprintf(buffer, sizeof(buffer), "%x,%lx,%x\n", uid, timestamp, count);
                        ssize_t bytes_written = write(fd, buffer, len);
                        if (bytes_written == -1) {
                            //perror("Error writing to file, retrying");
                            (void) unlock_file_write(fd);
                            close(fd);
                            rc = SLURM_ERROR;
                            usleep(1000); 
                            continue;
                        }  
                    } 
                    (void) unlock_file_write(fd);
                    close(fd);
                }while(rc == SLURM_ERROR && retry_cnt < 5);

                if(rc == SLURM_ERROR) {
                    perror("Error write data error");
                    xfree(full_path);
                    return rc;
                }
 
            } else {
                int fd = open(full_path, O_RDWR | O_CREAT, S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);  // Permissions: rw-r--r--
                if (fd == -1) {
                    xfree(full_path);
                    return SLURM_ERROR;
                }
                if (lock_file_write(fd) != SLURM_SUCCESS) {
                    close(fd);
                    xfree(full_path);
                    return SLURM_ERROR;
                }
                char buffer[2560] = {'0'};
                /* Format the string, convert the numeric part to hexadecimal, and separate it with commas */ 
                int len = snprintf(buffer, sizeof(buffer), "%x,%lx,%x\n", uid, timestamp, count);
                ssize_t bytes_written = write(fd, buffer, len);
                if (bytes_written == -1) {
                    perror("Error writing to file");
                    (void) unlock_file_write(fd);
                    close(fd);
                    xfree(full_path);
                    return SLURM_ERROR;
                }
                *flag = true;
                (void) unlock_file_write(fd);
                close(fd);
            }
        } else {
            fprintf(stderr, "'%s' exists but is not a directory.\n", path);
            xfree(full_path);
            return SLURM_ERROR;
        }

    } else {
        mode_t old_umask;
        old_umask = umask(0);
        if (mkdir(path, 0777) == -1) {
            perror("mkdir failed");
            xfree(full_path);
            return SLURM_ERROR;
        } 
        umask(old_umask); 
        printf("Directory '%s' created successfully.\n", path);
        int fd = open(full_path, O_WRONLY | O_CREAT | O_APPEND, 0644);
        if (fd == -1) {
            xfree(full_path);
            return SLURM_ERROR;
        }
        if (lock_file_write(fd) != SLURM_SUCCESS) {
            close(fd);
            xfree(full_path);
            return SLURM_ERROR;
        }
        *flag = true; 
        char buffer[2560] = {'0'};
        int len = snprintf(buffer, sizeof(buffer), "%x,%lx,%x\n", uid, timestamp, count);
        ssize_t bytes_written = write(fd, buffer, len);
        if (bytes_written == -1) {
            perror("Error writing to file");
            (void) unlock_file_write(fd);
            close(fd);
            xfree(full_path);
            return SLURM_ERROR;
        }
        (void) unlock_file_write(fd);
        close(fd);
    }
    xfree(full_path);
    return rc;
}

int rpc_limit()
{   
    int rc = SLURM_SUCCESS;
    struct passwd *pw;
    pw =   getpwuid(getuid());
    bool sleep_ms = false;
    char *path = NULL;
    char *script = NULL;
    char *rm_script  = NULL;
    xstrfmtcat(path,"/tmp/uid_post_flush");
    xstrfmtcat(script,"limit_uid_%d",pw->pw_uid);   
    xstrfmtcat(rm_script,"%s/%s",path,script);
    rc = create_directory(path, script, pw->pw_uid, &sleep_ms);
    if(rc == SLURM_ERROR)
        remove(rm_script);
    if(sleep_ms)
        usleep(300000); 
    if(path)
        xfree(path);
    if(script)
        xfree(script);
    if(rm_script)
        xfree(rm_script);
    return rc;
}

static int parse_command_and_send(int argc, char **argv, slurm_influxdb *data, char *post_str) 
{
    struct passwd *pw;
    int c = -1, optionIndex = 0, rc = SLURM_SUCCESS ;
    //bool no_jobid          = false; 
    spost_params.convert_flags   = CONVERT_NUM_UNIT_EXACT;
	spost_params.units           = NO_VAL;
	spost_params.opt_uid         = getuid();
	spost_params.opt_gid         = getgid();
    spost_params.level           = 0x001;
    bool jobid_label       = false;
    bool post_lable        = false; 
    char* jobids           = NULL;
    char* send             = NULL;
    int   stepd            = 0x7FFFFFFF;
    uint32_t job_id        = 0;
    char *job              = NULL; 
    char *stepids          = NULL;    
    if (!data || !post_str)
        return SLURM_ERROR;
    assert(spost_params.opt_uid != -1);
    
    pw = getpwuid(spost_params.opt_uid);
    if (!pw) {
        rc = SLURM_ERROR;
        goto fail;
    }
	static struct option long_options[] = {
                {"jobid",      required_argument,        0,    'j'},
                {"post",       required_argument,        0,    'p'},
                {"help",       no_argument,        0,    'h'},
                {0,            0,		                 0,     0}};
    while (1) {		/* now cycle through the command line */
		c = getopt_long(argc, argv,
				"p:j:h",
				long_options, &optionIndex);      
        if (c == -1) {
            //no_jobid = true;
            break;
        }
        switch (c) {
        	case 'j':
                jobid_label = true;
                jobids = xmalloc(strlen(optarg)+1);
                sprintf(jobids,"%s",optarg);
                break;
        	case 'p':
                post_lable = true;
                sprintf(post_str,"%s",optarg);
                break;
        	case 'h':
                print_spost_help();
                exit(1);
    		case '?':	/* getopt() has explained it */
			    exit(1);
            default:
                break;
        }
        // if(no_jobid)
        //     break;     
    }

    if(jobid_label) {
        if(strlen(jobids)> 400) {
            printf("The job ID being sent is too long. \n");
            rc = SLURM_ERROR;
            goto fail;          
        }

        char *p  = strchr(jobids, '.');
        if(p != NULL && *p !='\0') {
            stepids = xmalloc(sizeof(char) * 400);
            job = xmalloc(sizeof(char) * 400);

            strncpy(job, jobids, p - jobids); 
            job[p - jobids] = '\0';
            strcpy(stepids, p + 1); 
        }

        if(job != NULL) {
            if(contains_non_digit(job) != 0) {
                printf("There is an error in the job ID format specification "
                                    "(other: only one job can be specified).\n");
                rc = SLURM_ERROR;
                goto fail;
            } 
            job_id = (uint32_t) atol(job);
        } else {
            if(contains_non_digit(jobids) != 0) {
                printf("There is an error in the job ID format specification "
                                    "(other: only one job can be specified).\n");
                rc = SLURM_ERROR;
                goto fail;
            } 
            job_id = (uint32_t) atol(jobids);
        }

        if(stepids) {
            
            if(strncmp(stepids, "batch", 5) == 0) {
                stepd = -5;
            } else if(strncmp(stepids, "extern", 6) == 0){
                stepd = -4;
            } else if(contains_non_digit(stepids) == 0) {
                stepd = atol(stepids);
            } else {
                printf("Invalid characters have been specified. (other: only one job can be specified). \n");
                rc = SLURM_ERROR;
                goto fail;
            }
        }

    } else {
        printf("Please specify a single job ID using -j.\n");
        rc = SLURM_ERROR;
        goto fail;
    }

    if(post_lable) {
        /*max send 4k*/
       if(strlen(post_str) >= MAX_SQL_STR) {
         rc = SLURM_ERROR;
         printf("The specified string length is too long. "
                         "Please reduce the length of the custom string.\n");
         goto fail;
       } 
    } else{
         printf("Please specify the push string using -p.\n");
         rc = SLURM_ERROR;
         goto fail;
    }

    send = _record_table(post_str, job_id, stepd, spost_params.opt_uid, pw->pw_name);
    if(send)
        _send_data2(data, send);
    else
        rc = SLURM_ERROR;
fail:
    if(jobids)
        xfree(jobids);
    if(job)
        xfree(job);
    if(stepids)
        xfree(stepids);
    if(send)
        xfree(send);
   return rc;
}

int main(int argc ,char** argv) {
     
    slurm_influxdb* influxdb_data = xmalloc(sizeof(slurm_influxdb));
    char* configpath = NULL;
    char* post_str = NULL;

    int rc = SLURM_SUCCESS ;
    char tmp_conf[] = "/etc/acct_gather.conf.key";
    
    rc = rpc_limit();
    if (rc == SLURM_ERROR || rc == SPOST_RC_RATE_LIMITED) {
        xfree(influxdb_data);
        return SLURM_SUCCESS;   
    }
    post_str = xcalloc(MAX_SQL_STR, sizeof(char));
    memset(post_str, '\0', MAX_SQL_STR *sizeof(char) );
    if(strcmp(KEYDIR, "NONE") == 0) {
         configpath = xstrdup("/etc/slurm/acct_gather.conf.key");
    } else {
        char* def_conf = NULL;
        def_conf = xmalloc(strlen(KEYDIR)+strlen(tmp_conf)+2);
        sprintf(def_conf, "%s%s",KEYDIR,tmp_conf);
        configpath = xstrdup(def_conf);
        xfree(def_conf);
    }
   
    struct stat statbuf;
    if(stat(configpath, &statbuf) != 0){
        printf("File location does not exist of %s \n", configpath);
        goto file_fail1;
    }

    spost_init(influxdb_data);
    /*16-bit encryption key*/
    const uint8_t key[]="fcad715bd73b5cb0";
    rc = read_hex_bytes_from_file(configpath, key, influxdb_data);
    if(rc == SLURM_ERROR)
         goto file_fail;
    rc =  parse_command_and_send(argc, argv, influxdb_data, post_str);
    if(rc == SLURM_ERROR)
         goto file_fail;    

file_fail:
    spost_fini(influxdb_data);
file_fail1:
    xfree(post_str);
    if (configpath)
        xfree(configpath);
    return SLURM_SUCCESS;
}