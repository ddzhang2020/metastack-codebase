/*****************************************************************************\
 *  xstring2.h - implementation-independent job of influxdb info
 *   *  Modification history
 *  
 *****************************************************************************/
#ifndef XSTRING2_H_INCLUDED
#define XSTRING2_H_INCLUDED
#include <assert.h>
#include <stdio.h> 
#include <stdbool.h>
#include <stdint.h> 
#include <stdarg.h>

#define XMALLOC_MAGIC 0x42
#define C_STRING_INIT_SIZE 2048

#define xmalloc(__sz) \
		slurm_xcalloc(1, __sz, true, false, __FILE__, __LINE__)
#define xrecalloc(__p, __cnt, __sz) \
        slurm_xrecalloc((void **)&(__p), __cnt, __sz, true, false, __FILE__, __LINE__)
#define xrealloc(__p, __sz) \
        slurm_xrecalloc((void **)&(__p), 1, __sz, true, false, __FILE__, __LINE__)
#define xfree(__p) slurm_xfree((void **)&(__p))
#define xcalloc(__cnt, __sz) \
		slurm_xcalloc(__cnt, __sz, true, false, __FILE__, __LINE__)
#define xstrfmtcat(__p, __fmt, args...)	_xstrfmtcat(&(__p), __fmt, ## args)
#define xstrcat(__p, __q)		_xstrcat(&(__p), __q)
typedef struct c_string {
    char *str;
    size_t alloced;
    size_t len;
} c_string_t;
/*
 * Free which takes a pointer to object to free, which it turns into a null
 * object.
 *   item (IN/OUT)	double-pointer to allocated space
 */
void slurm_xfree(void **item);
/*
 * "Safe" version of malloc().
 *   size (IN)	number of bytes to malloc
 *   clear (IN) initialize to zero
 *   RETURN	pointer to allocate heap space
 */
void *slurm_xcalloc(size_t count, size_t size, bool clear, bool try,
		    const char *file, int line);
void * slurm_xrecalloc(void **item, size_t count, size_t size,
			      bool clear, bool try, const char *file,
			      int line);
/*
 * safe strchr (handles NULL values)
 */
char *xstrchr(const char *s1, int c); 

/*
 * safe strstr (handles NULL values)
 */
char *xstrstr(const char *haystack, const char *needle);
// /*
//  * Give me a copy of the string as if it were printf.
//  * This is stdarg-compatible routine, so vararg-compatible
//  * functions can do va_start() and invoke this function.
//  *
//  *   fmt (IN)		format of string and args if any
//  *   RETURN		copy of formated string
//  */
// static size_t _xstrdup_vprintf(char **str, const char *fmt, va_list ap);
/*
 * append formatted string with printf-style args to buf, expanding
 * buf as needed
 */
void _xstrfmtcat(char **str, const char *fmt, ...);
/*
 * Duplicate a string.
 *   str (IN)		string to duplicate
 *   RETURN		copy of string
 */
char *xstrdup(const char *str);
int xstrcmp(const char *s1, const char *s2);
void log_oom(const char *file, int line);
size_t safe_strlen(const char *str);
/**
 * Creates and initializes a new c_string_t object.
 *
 * @return A pointer to the newly created c_string_t object.
 */
c_string_t *c_string_create(void);

/**
 * Destroys and frees the memory associated with the specified c_string_t object.
 *
 * @param cs A pointer to the c_string_t object to be destroyed.
 */
void c_string_destroy(c_string_t *cs);

/**
 * Appends a given C-style string to the end of the c_string_t object.
 *
 * @param cs  A pointer to the c_string_t object.
 * @param str The C-style string (null-terminated) to append.
 */
void c_string_append_str(c_string_t *cs, const char *str);

/**
 * Prepends a given C-style string to the beginning of the c_string_t object.
 *
 * @param cs  A pointer to the c_string_t object.
 * @param str The C-style string (null-terminated) to prepend.
 */
void c_string_front_str(c_string_t *cs, const char *str);

/**
 * Returns the length of the string stored in the c_string_t object (excluding the null terminator).
 *
 * @param cs A pointer to the c_string_t object.
 * @return   The length of the string as a size_t value.
 */
size_t c_string_len(const c_string_t *cs);

/**
 * Retrieves a pointer to the internal C-style string stored in the c_string_t object.
 *
 * @param cs A pointer to the c_string_t object.
 * @return   A const char* pointer to the internal string.
 */
const char *c_string_peek(const c_string_t *cs);

/* safe strrchr */
char *xstrrchr(const char *s1, int c);
/* safe strncmp */
int xstrncmp(const char *s1, const char *s2, size_t n);
/* safe strcasecmp */
int xstrcasecmp(const char *s1, const char *s2);
/* safe strncasecmp */
int xstrncasecmp(const char *s1, const char *s2, size_t n);

char *xstrcasestr(const char *haystack, const char *needle);
size_t xsize(void *item);
/*
** cat str2 onto str1, expanding str1 as necessary
*/
void _xstrcat(char **str1, const char *str2);
#endif /* XSTRING2_H_INCLUDED */