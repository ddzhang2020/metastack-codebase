#ifndef __QDEL_H
#define __QDEL_H

#include "common_util.h"
#include <grp.h>
#include <pwd.h>
#include <inttypes.h>
#include <pthread.h>

extern void fatal(const char *, ...)
	__attribute__((format (printf, 1, 2))) __attribute__((noreturn));

extern char *xstrdup_printf(const char *fmt, ...)
  __attribute__ ((format (printf, 1, 2)));

extern void _xstrfmtcat(char **str, const char *fmt, ...)
	__attribute__((format(printf, 2, 3)));
extern void slurm_diff_tv_str(struct timeval *tv1,struct timeval *tv2,
			      char *tv_str, int len_tv_str, const char *from,
			      long limit, long *delta_t);

#define xstrfmtcat(__p, __fmt, args...)	_xstrfmtcat(&(__p), __fmt, ## args)
#define DELTA_TIMER	delta_t
#define MAX_CANCEL_RETRY 10
#define MAX_THREADS 10
#define USEC_IN_SEC 1000000
#define DEF_TIMERS	struct timeval tv1, tv2; char tv_str[20] = ""; long delta_t;
#define START_TIMER	gettimeofday(&tv1, NULL)
#define END_TIMER do {							\
	gettimeofday(&tv2, NULL);					\
	slurm_diff_tv_str(&tv1, &tv2, tv_str, 20, NULL, 0, &delta_t);	\
} while (0)

typedef struct Option {
    bool is_set;
    char *value;
} Option;

typedef struct qsub_args_t {
    // Option job_list;
    Option user_list;
    Option force;
    Option help;
    Option version;
	bool debug;
	bool job_list_set;
	char **job_list;
	uint32_t job_list_count;
} qsub_args_t;

typedef struct scancel_options {
	// char *account;		/* --account=n, -a		*/
	// bool batch;		/* --batch, -b			*/
	// char *sibling;		/* --sibling=<sib_name>		*/
	// bool ctld;		/* --ctld			*/
	// List clusters;          /* --cluster=cluster_name -Mcluster-name */
	// bool full;		/* --full, -f			*/
	// bool hurry;		/* --hurry, -H			*/
	// bool interactive;	/* --interactive, -i		*/
	char *job_name;		/* --name=n, -nn		*/
	// char *partition;	/* --partition=n, -pn		*/
	// char *qos;		/* --qos=n, -qn			*/
	// char *reservation;	/* --reservation=n, -Rn		*/
	uint16_t signal;	/* --signal=n, -sn		*/
	// uint32_t state;		/* --state=n, -tn		*/
	uid_t user_id;		/* derived from user_name	*/
	char *user_name;	/* --user=n, -un		*/
	// int verbose;		/* --verbose, -v		*/
	// char *wckey;		/* --wckey			*/
	// char *nodelist;		/* --nodelist, -w		*/

	char **job_list;        /* job ID input, NULL termated
				 * Expanded in to arrays below	*/

	uint16_t job_cnt;	/* count of job_id's specified	*/
	uint32_t *job_id;	/* list of job ID's		*/
	uint32_t *array_id;	/* list of job array task IDs	*/
	// uint32_t *step_id;	/* list of job step ID's	*/
	bool *job_found;	/* Set if the job record is found */
	bool *job_pend;		/* Set fi job is pending	*/
	// 新增
	uid_t *uids_array;
	int uids_count;
	int uids_array_capacity;

} opt_t;

typedef struct job_cancel_info {
	uint32_t array_job_id;
	uint32_t array_task_id;
	bool     array_flag;
/* Note: Either set job_id_str OR job_id */
	char *   job_id_str;
	uint32_t job_id;
	uint32_t step_id;
	uint16_t sig;
	int    * rc;
	int             *num_active_threads;
	pthread_mutex_t *num_active_threads_lock;
	pthread_cond_t  *num_active_threads_cond;
} job_cancel_info_t;

void xfree_ptr(void *ptr);

// extern void add_handler(CommandParser* parser, CommandHandler handler);
// extern int parse_command(CommandParser* parser, int argc, char* argv[]);
// extern void destroy_parser(CommandParser* parser);
extern uint64_t slurmdb_find_tres_count_in_string(char *tres_str_in, int id);


static	int num_active_threads = 0;
static	pthread_mutex_t  num_active_threads_lock;
static	pthread_cond_t   num_active_threads_cond;
static	pthread_mutex_t  max_delay_lock;
static	int max_resp_time = 0;
static	int request_count = 0;

#define slurm_attr_destroy(attr)					\
	do {								\
		int err = pthread_attr_destroy(attr);			\
		if (err) {						\
			errno = err;					\
			error("pthread_attr_destroy failed, "		\
				"possible memory leak!: %m");		\
		}							\
	} while (0)
#define slurm_cond_init(cond, cont_attr)				\
	do {								\
		int err = pthread_cond_init(cond, cont_attr);		\
		if (err) {						\
			errno = err;					\
			fatal("%s:%d %s: pthread_cond_init(): %m",	\
				__FILE__, __LINE__, __func__);		\
			abort();					\
		}							\
	} while (0)
#define slurm_mutex_init(mutex)						\
	do {								\
		int err = pthread_mutex_init(mutex, NULL);		\
		if (err) {						\
			errno = err;					\
			fatal("%s:%d %s: pthread_mutex_init(): %m",	\
				__FILE__, __LINE__, __func__);		\
			abort();					\
		}							\
	} while (0)

#define slurm_mutex_destroy(mutex)					\
	do {								\
		int err = pthread_mutex_destroy(mutex);			\
		if (err) {						\
			errno = err;					\
			fatal("%s:%d %s: pthread_mutex_destroy(): %m",	\
				__FILE__, __LINE__, __func__);		\
			abort();					\
		}							\
	} while (0)

#define slurm_mutex_lock(mutex)					\
	do {								\
		int err = pthread_mutex_lock(mutex);			\
		if (err) {						\
			errno = err;					\
			fatal("%s:%d %s: pthread_mutex_lock(): %m",	\
				__FILE__, __LINE__, __func__);		\
			abort();					\
		}							\
	} while (0)

#define slurm_mutex_unlock(mutex)					\
	do {								\
		int err = pthread_mutex_unlock(mutex);			\
		if (err) {						\
			errno = err;					\
			fatal("%s:%d %s: pthread_mutex_unlock(): %m",	\
				__FILE__, __LINE__, __func__);		\
			abort();					\
		}							\
	} while (0)

#define slurm_cond_wait(cond, mutex)					\
	do {								\
		int err = pthread_cond_wait(cond, mutex);		\
		if (err) {						\
			errno = err;					\
			error("%s:%d %s: pthread_cond_wait(): %m",	\
				__FILE__, __LINE__, __func__);		\
		}							\
	} while (0)
#define slurm_cond_destroy(cond)					\
	do {								\
		int err = pthread_cond_destroy(cond);			\
		if (err) {						\
			errno = err;					\
			error("%s:%d %s: pthread_cond_destroy(): %m",	\
				__FILE__, __LINE__, __func__);		\
		}							\
	} while (0)
#define slurm_cond_signal(cond)					\
	do {								\
		int err = pthread_cond_signal(cond);			\
		if (err) {						\
			errno = err;					\
			error("%s:%d %s: pthread_cond_signal(): %m",	\
				__FILE__, __LINE__, __func__);		\
		}							\
	} while (0)
#ifdef PTHREAD_SCOPE_SYSTEM
#  define slurm_attr_init(attr)						\
	do {								\
		int err = pthread_attr_init(attr);			\
		if (err) {						\
			errno = err;					\
			fatal("pthread_attr_init: %m");			\
		}							\
		/* we want 1:1 threads if there is a choice */		\
		err = pthread_attr_setscope(attr, PTHREAD_SCOPE_SYSTEM);\
		if (err) {						\
			errno = err;					\
			error("pthread_attr_setscope: %m");		\
		}							\
		err = pthread_attr_setstacksize(attr, 1024*1024);	\
		if (err) {						\
			errno = err;					\
			error("pthread_attr_setstacksize: %m");		\
		}							\
	 } while (0)
#else
#  define slurm_attr_init(attr)						\
	do {								\
		int err = pthread_attr_init(attr);			\
		if (err) {						\
			errno = err;					\
			fatal("pthread_attr_init: %m");			\
		}							\
		err = pthread_attr_setstacksize(attr, 1024*1024);	\
		if (err) {						\
			errno = err;					\
			error("pthread_attr_setstacksize: %m");		\
		}							\
	} while (0)
#endif
#define slurm_thread_create_detached(id, func, arg)			\
	do {								\
		pthread_t *id_ptr, id_local;				\
		pthread_attr_t attr;					\
		int err;						\
		id_ptr = (id != (pthread_t *) NULL) ? id : &id_local;	\
		slurm_attr_init(&attr);					\
		err = pthread_attr_setdetachstate(&attr,		\
						  PTHREAD_CREATE_DETACHED); \
		if (err) {						\
			errno = err;					\
			fatal("%s: pthread_attr_setdetachstate %m",	\
			      __func__);				\
		}							\
		err = pthread_create(id_ptr, &attr, func, arg);		\
		if (err) {						\
			errno = err;					\
			fatal("%s: pthread_create error %m", __func__);	\
		}							\
		slurm_attr_destroy(&attr);				\
	} while (0)


#endif