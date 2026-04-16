#ifndef __SUGON_UTIL_H
#define __SUGON_UTIL_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include <sys/stat.h>
#include <grp.h>
#include <time.h>
#include <math.h>
#include <unistd.h>
#include <sys/resource.h> /* for RLIMIT_NOFILE */
#include <sys/param.h>
#include <sys/syslog.h>
#include <stdint.h>
#include <slurm/slurm.h>
#include <slurm/slurm_errno.h>
#include <slurm/slurm_version.h>

/** Program version string. */
#define PROGRAM_VERSION "3.1.0"
/** Program release date. */
#define RELEASE_DATE "2025-10-31"

typedef enum {
    OPTION_TYPE_NONE,    // 无参数
    OPTION_TYPE_REQUIRED, // 必选参数
    OPTION_TYPE_OPTIONAL, // 可选参数
    OPTION_TYPE_MULTIPLE  // 多个参数
}	OptionType;

typedef enum {
	SGE_WRAPPER_SUCCESS,	// 成功
	SGE_WRAPPER_FINISH,		// 需要提前退出
	SGE_WRAPPER_ERROR		// 错误
}	SgeWrapperType;

extern char *xstrdup_printf(const char *fmt, ...)
  __attribute__ ((format (printf, 1, 2)));
extern bitoff_t bit_size(bitstr_t *b);
extern int bit_test(bitstr_t *b, bitoff_t bit);
extern int sig_name2num(const char *signal_name);
extern int uid_from_string(const char *name, uid_t *uidp);
extern int xstrcasecmp(const char *s1, const char *s2);
extern char *xstrtolower(char *str);
extern uint32_t job_state_num(const char *state_name);
extern void slurm_error (const char *format, ...)
        __attribute__ ((format (printf, 1, 2)));
extern slurm_conf_t slurm_conf;
extern int slurm_conf_init(const char *file_name);
extern void * slurm_xrecalloc(void **item, size_t count, size_t size,
			      bool clear, bool try, const char *file,
			      int line, const char *func);
extern void *slurm_xcalloc(size_t count, size_t size, bool clear, bool try, const char *file, int line, const char *func);
extern char *xstrdup(const char *str);
extern void slurm_xfree(void **item);
extern void _xstrcat(char **str1, const char *str2);
extern char *xstrchr(const char *s1, int c);
extern int xstrncmp(const char *s1, const char *s2, size_t n);
extern int xstrcmp(const char *s1, const char *s2);
// extern char *basename (const char *__filename) __THROW __nonnull ((1));
extern char *xbasename(char *path);
extern int slurm_addto_step_list(List step_list, char *names);
extern int slurm_addto_char_list(List char_list, char *names);
extern void slurm_make_time_str (time_t *time, char *string, int size);
extern void list_destroy(List l);
extern List list_create (ListDelF f);
extern int list_count(List l);
// extern void *slurm_list_append(list_t *l, void *x);
extern int slurm_addto_id_char_list(List char_list, char *names, bool gid);
extern int list_delete_item(list_itr_t *i);

#define xstrcat(__p, __q)		_xstrcat(&(__p), __q)
#define xfree(__p) slurm_xfree((void **)&(__p))
#define xmalloc(__sz) \
	slurm_xcalloc(1, __sz, true, false, __FILE__, __LINE__, __func__)
#define xrealloc(__p, __sz) \
        slurm_xrecalloc((void **)&(__p), 1, __sz, true, false, __FILE__, __LINE__, __func__)
#define debug(fmt, ...)		\
	do {			\
	format_print(LOG_LEVEL_DEBUG, fmt, ##__VA_ARGS__);\
	} while (0)
#define IS_JOB_FINISHED(_X)		\
	((_X->job_state & JOB_STATE_BASE) >  JOB_SUSPENDED)
#define IS_JOB_PENDING(_X)		\
	((_X->job_state & JOB_STATE_BASE) == JOB_PENDING)
#define IS_JOB_RUNNING(_X)		\
	((_X->job_state & JOB_STATE_BASE) == JOB_RUNNING)
#define IS_JOB_SUSPENDED(_X)		\
	((_X->job_state & JOB_STATE_BASE) == JOB_SUSPENDED)
#define	error			slurm_error
#define	xstrdup_printf		slurm_xstrdup_printf

#define START_TIME_BUFFER_SIZE 20       /* 202503271200.12 */

#define list_iterator_create    slurm_list_iterator_create
#define list_next       slurm_list_next
#define list_iterator_destroy   slurm_list_iterator_destroy
#define	list_append		slurm_list_append
#define	list_sort		slurm_list_sort
#define	hostlist_create		slurm_hostlist_create
#define	hostlist_shift		slurm_hostlist_shift
#define	hostlist_destroy	slurm_hostlist_destroy
#define xstrfmtcat(__p, __fmt, args...)	_xstrfmtcat(&(__p), __fmt, ## args)

#define FREE_NULL_LIST(_X)			\
	do {					\
		if (_X) list_destroy (_X);	\
		_X	= NULL; 		\
	} while (0)


typedef struct CommandHandler {
    const char* command;                    // 命令名称
    int require_param;                      // 是否需要参数
    int (*execute)(const char* param);     // 执行函数指针
}	CommandHandler;

typedef struct CommandParser CommandParser;

// 命令解析器结构体
struct CommandParser {
    CommandHandler* handlers;               // 命令处理器数组
    int handler_count;                      // 处理器数量
    void (*add_handler)(CommandParser* parser, CommandHandler handler);    // 添加处理器方法
    int (*parse)(CommandParser* parser, int argc, char* argv[]);          // 解析方法
    void (*destroy)(CommandParser* parser);                               // 销毁方法
	char **remain_args;
	int remain_count;
};

// void add_handler(CommandParser* parser, CommandHandler handler);
// int parse_command(CommandParser* parser, int argc, char* argv[]);
// void destroy_parser(CommandParser* parser);

#endif
