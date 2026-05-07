/*****************************************************************************\
 *  xstring2.c - string and memory helper functions for sjinfo
 *****************************************************************************/
/*****************************************************************************\
 *  Modification history
\*****************************************************************************/
#include "xstring2.h"
#include <assert.h>
#include <stdio.h> 
#include <stdbool.h>
#include <stdint.h> 
#include <stdarg.h>
#include <string.h>
#include <stdlib.h>
#include <ctype.h>
#include <stdlib.h>

#define XFGETS_CHUNKSIZE 64
/*
 * Duplicate a string.
 *   str (IN)		string to duplicate
 *   RETURN		copy of string
 */
char *xstrdup(const char *str)
{
	size_t siz;
	char *result;

	if (!str)
		return NULL;

	siz = strlen(str) + 1;
	result = xmalloc(siz);

	/* includes terminating NUL from source string */
	(void) memcpy(result, str, siz);

	return result;
}

/*
 * Free which takes a pointer to object to free, which it turns into a null
 * object.
 *   item (IN/OUT)	double-pointer to allocated space
 */
void slurm_xfree(void **item)
{
	if (*item != NULL) {
		size_t *p = (size_t *)*item - 2;
		/* magic cookie still there? */
		assert(p[0] == XMALLOC_MAGIC);
		p[0] = 0;	/* make sure xfree isn't called twice */
		free(p);
		*item = NULL;
	}
}

/*
 * "Safe" version of malloc().
 *   size (IN)	number of bytes to malloc
 *   clear (IN) initialize to zero
 *   RETURN	pointer to allocate heap space
 */
void *slurm_xcalloc(size_t count, size_t size, bool clear, bool try,
		    const char *file, int line)
{
	size_t total_size;
	size_t count_size;
	size_t *p;

	if (!size || !count)
		return NULL;

	/*
	 * Detect overflow of the size calculation and abort().
	 * Ensure there is sufficient space for the two header words used to
	 * store the magic value and the allocation length by dividing by two,
	 * and because on 32-bit systems, if a 2GB allocation request isn't
	 * sufficient (which would attempt to allocate 2GB + 8Bytes),
	 * then we're going to run into other problems anyways.
	 * (And on 64-bit, if a 2EB + 16Bytes request isn't sufficient...)
	 */
	if ((count != 1) && (count > SIZE_MAX / size / 4)) {
		if (try)
			return NULL;
		log_oom(file, line);
		abort();
	}

	count_size = count * size;
	total_size = count_size + 2 * sizeof(size_t);

	if (clear)
		p = calloc(1, total_size);
	else
		p = malloc(total_size);

	if (!p && try) {
		return NULL;
	} else if (!p) {
		/* out of memory */
		log_oom(file, line);
		abort();
	}
	p[0] = XMALLOC_MAGIC;	/* add "secret" magic cookie */
	p[1] = count_size;	/* store size in buffer */

	return &p[2];
}

/*
 * "Safe" version of realloc() / reallocarray().
 * Args are different: pass in a pointer to the object to be
 * realloced instead of the object itself.
 *   item (IN/OUT)	double-pointer to allocated space
 *   newcount (IN)	requested count
 *   newsize (IN)	requested size
 *   clear (IN)		initialize to zero
 */
void * slurm_xrecalloc(void **item, size_t count, size_t size,
			      bool clear, bool try, const char *file,
			      int line)
{
	size_t total_size;
	size_t count_size;
	size_t *p;

	if (!size || !count)
		return NULL;

	/*
	 * Detect overflow of the size calculation and abort().
	 * Ensure there is sufficient space for the two header words used to
	 * store the magic value and the allocation length by dividing by two,
	 * and because on 32-bit systems, if a 2GB allocation request isn't
	 * sufficient (which would attempt to allocate 2GB + 8Bytes),
	 * then we're going to run into other problems anyways.
	 * (And on 64-bit, if a 2EB + 16Bytes request isn't sufficient...)
	 */
	if ((count != 1) && (count > SIZE_MAX / size / 4))
		goto error;

	count_size = count * size;
	total_size = count_size + 2 * sizeof(size_t);

	if (*item != NULL) {
		size_t old_size;
		p = (size_t *)*item - 2;

		/* magic cookie still there? */
		assert(p[0] == XMALLOC_MAGIC);
		old_size = p[1];

		p = realloc(p, total_size);
		if (p == NULL)
			goto error;

		if (old_size < count_size) {
			char *p_new = (char *)(&p[2]) + old_size;
			if (clear)
				memset(p_new, 0, (count_size - old_size));
		}
		assert(p[0] == XMALLOC_MAGIC);
	} else {
		/* Initalize new memory */
		if (clear)
			p = calloc(1, total_size);
		else
			p = malloc(total_size);
		if (p == NULL)
			goto error;
		p[0] = XMALLOC_MAGIC;
	}

	p[1] = count_size;
	*item = &p[2];
	return *item;

error:
	if (try)
		return NULL;
	log_oom(file, line);
	abort();
}

/* safe strchr */
char *xstrchr(const char *s1, int c)
{
	return s1 ? strchr(s1, c) : NULL;
}

/*
 * Give me a copy of the string as if it were printf.
 * This is stdarg-compatible routine, so vararg-compatible
 * functions can do va_start() and invoke this function.
 *
 *   fmt (IN)		format of string and args if any
 *   RETURN		copy of formated string
 */
static size_t _xstrdup_vprintf(char **str, const char *fmt, va_list ap)
{
	/* Start out with a size of 100 bytes. */
	int n, size = 100;
	va_list our_ap;
	char *p = xmalloc(size);

	while (1) {
		/* Try to print in the allocated space. */
		va_copy(our_ap, ap);
		n = vsnprintf(p, size, fmt, our_ap);
		va_end(our_ap);
		/* If that worked, return the string. */
		if (n > -1 && n < size) {
			*str = p;
			return n;
		}
		/* Else try again with more space. */
		if (n > -1)               /* glibc 2.1 */
			size = n + 1;           /* precisely what is needed */
		else                      /* glibc 2.0 */
			size *= 2;              /* twice the old size */
		p = xrealloc(p, size);
	}
	/* NOTREACHED */
}

/*
 * append formatted string with printf-style args to buf, expanding
 * buf as needed
 */
void _xstrfmtcat(char **str, const char *fmt, ...)
{
	char *p = NULL;
	va_list ap;

	va_start(ap, fmt);
	_xstrdup_vprintf(&p, fmt, ap);
	va_end(ap);

	if (!p)
		return;

	/* If str does not exist yet, just give it back p directly */
	if (!*str) {
		*str = p;
		return;
	}

	strcat(*str, p);
	xfree(p);
}

/* Log out of memory without message buffering */
void log_oom(const char *file, int line)
{
	printf("%s %d malloc failed\n",file, line);
}


/*
    ##########  Custom string types for dynamic scaling  ##########
*/

/*Get the length of a string, taking into account the case where the string is empty*/
size_t safe_strlen(const char *str) {
    return str ? strlen(str) : 0;
}

/*
 *##########  Custom string types for dynamic scaling  ##########
 */
c_string_t *c_string_create(void) {
    c_string_t *cs;
    cs = xcalloc(1, sizeof(c_string_t));
    cs->str = xmalloc(C_STRING_INIT_SIZE);
    cs->str[0] = '\0';

    cs->alloced = C_STRING_INIT_SIZE;
    cs->len = 0;

    return cs;
}

void c_string_destroy(c_string_t *cs){
    if(cs == NULL) 
        return;
    xfree(cs->str);
    xfree(cs);
}

static void c_string_ensure_space(c_string_t *cs, size_t add_len) {
    if (cs == NULL || add_len == 0) return;

    if(cs->alloced >= cs->len + add_len + 1) return;

    while(cs->alloced < cs->len + add_len + 1) {
        cs->alloced <<= 1;
        if(cs->alloced == 0) {
            cs->alloced--;
        }
    }
    cs->str = xrealloc(cs->str, cs->alloced);
}

void c_string_append_str(c_string_t *cs, const char *str) {
    if(cs == NULL || str == NULL || *str == '\0') return;
    // if(len == 0) len = strlen(str);
    size_t len = strlen(str);

    c_string_ensure_space(cs, len);
    memmove(cs->str + cs->len, str, len);
    cs->len += len;
    cs->str[cs->len] = '\0';
}

void c_string_front_str(c_string_t *cs, const char *str) {
    if (cs == NULL || str == NULL || *str == '\0') return;

    size_t len = strlen(str);

    c_string_ensure_space(cs, len);
    memmove(cs->str + len, cs->str, cs->len);
    memmove(cs->str, str, len);
    cs->len += len;
    cs->str[cs->len] = '\0';
}

size_t c_string_len(const c_string_t *cs) {
    if (cs == NULL) return 0;
    return cs->len;
}

const char *c_string_peek(const c_string_t *cs) {
    if (cs == NULL) return NULL;
    return cs->str;
}

/* safe strrchr */
char *xstrrchr(const char *s1, int c)
{
	return s1 ? strrchr(s1, c) : NULL;
}

/* safe strcmp */
int xstrcmp(const char *s1, const char *s2)
{
	if (!s1 && !s2)
		return 0;
	else if (!s1)
		return -1;
	else if (!s2)
		return 1;
	else
		return strcmp(s1, s2);
}

/* safe strncmp */
int xstrncmp(const char *s1, const char *s2, size_t n)
{
	if (!s1 && !s2)
		return 0;
	else if (!s1)
		return -1;
	else if (!s2)
		return 1;
	else
		return strncmp(s1, s2, n);
}

/* safe strcasecmp */
int xstrcasecmp(const char *s1, const char *s2)
{
	if (!s1 && !s2)
		return 0;
	else if (!s1)
		return -1;
	else if (!s2)
		return 1;
	else
		return strcasecmp(s1, s2);
}

/* safe strncasecmp */
int xstrncasecmp(const char *s1, const char *s2, size_t n)
{
	if (!s1 && !s2)
		return 0;
	else if (!s1)
		return -1;
	else if (!s2)
		return 1;
	else
		return strncasecmp(s1, s2, n);
}

/* safe xstrstr */
char *xstrstr(const char *haystack, const char *needle)
{
	if (!haystack || !needle)
		return NULL;

	return strstr(haystack, needle);
}

char *xstrcasestr(const char *haystack, const char *needle)
{
	int hay_inx, hay_size, need_inx, need_size;
	char *hay_ptr = (char *) haystack;

	if (haystack == NULL || needle == NULL)
		return NULL;

	hay_size = strlen(haystack);
	need_size = strlen(needle);

	for (hay_inx=0; hay_inx<hay_size; hay_inx++) {
		for (need_inx=0; need_inx<need_size; need_inx++) {
			if (tolower((int) hay_ptr[need_inx]) !=
			    tolower((int) needle [need_inx]))
				break;		/* mis-match */
		}

		if (need_inx == need_size)	/* it matched */
			return hay_ptr;
		else				/* keep looking */
			hay_ptr++;
	}

	return NULL;	/* no match anywhere in string */
}

/*
 * Return the size of a buffer.
 *   item (IN)		pointer to allocated space
 */
size_t xsize(void *item)
{
	size_t *p = (size_t *)item - 2;
	assert(item != NULL);
	assert(p[0] == XMALLOC_MAGIC); /* CLANG false positive here */
	return p[1];
}

/*
 * Ensure that a string has enough space to add 'needed' characters.
 * If the string is uninitialized, it should be NULL.
 * str (IN/OUT)		str
 * str_len(IN)		current string length, if known. -1 otherwise
 * needed (IN)		additional space needed
 */
static void _makespace(char **str, int str_len, int needed)
{
	if (*str == NULL)
		*str = xmalloc(needed + 1);
	else {
		int actual_size;
		int used = (str_len < 0) ? strlen(*str) + 1 : (size_t)str_len + 1;
		int min_new_size = used + needed;
		int cur_size = xsize(*str);
		if (min_new_size > cur_size) {
			int new_size = min_new_size;
			if (new_size < (cur_size + XFGETS_CHUNKSIZE))
				new_size = cur_size + XFGETS_CHUNKSIZE;
			if (new_size < (cur_size * 2))
				new_size = cur_size * 2;

			xrealloc(*str, new_size);
			actual_size = xsize(*str);
			if (actual_size)
				assert(actual_size == new_size);
		}
	}
}

/*
 * Concatenate str2 onto str1, expanding str1 as needed.
 *   str1 (IN/OUT)	target string (pointer to in case of expansion)
 *   str2 (IN)		source string
 */
void _xstrcat(char **str1, const char *str2)
{
	if (str2 == NULL)
		str2 = "(null)";

	_makespace(str1, -1, strlen(str2));
	strcat(*str1, str2);
}