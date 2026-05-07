/*****************************************************************************
 *  list2.c
 *****************************************************************************
 *  Copyright (C) 2001-2002 The Regents of the University of California.
 *  Copyright (C) 2021 NVIDIA Corporation.
 *  Produced at Lawrence Livermore National Laboratory (cf, DISCLAIMER).
 *  Written by Chris Dunlap <cdunlap@llnl.gov>.
 *
 *  This file is from LSD-Tools, the LLNL Software Development Toolbox.
 *
 *  LSD-Tools is free software; you can redistribute it and/or modify it under
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
 *  LSD-Tools is distributed in the hope that it will be useful, but WITHOUT
 *  ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 *  FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for
 *  more details.
 *
 *  You should have received a copy of the GNU General Public License along
 *  with LSD-Tools; if not, write to the Free Software Foundation, Inc.,
 *  51 Franklin Street, Fifth Floor, Boston, MA 02110-1301  USA.
 *****************************************************************************
 *  Refer to "list.h" for documentation on public functions.
 *****************************************************************************/
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "xstring2.h"
#include "list2.h"

#define LIST_MAGIC 0xDEADBEEF
#define LIST_ITR_MAGIC 0xDEADBEFF

#define list_iterator_free(_i) xfree(_i)
#define c(_p) xfree(_p)
#define list_free(_l) xfree(_l)
#define list_node_alloc() xmalloc(sizeof(struct listNode))
#define list_iterator_alloc() xmalloc(sizeof(struct listIterator))
#define list_alloc() xmalloc(sizeof(struct xlist))


/*
 *  List Iterator opaque data type.
 */
typedef struct listNode {
	void                 *data;         /* node's data                       */
	struct listNode      *next;         /* next node in list                 */
} list_node_t;

struct listIterator {
	unsigned int          magic;        /* sentinel for asserting validity   */
	struct xlist         *list;         /* the list being iterated           */
	struct listNode      *pos;          /* the next node to be iterated      */
	struct listNode     **prev;         /* addr of 'next' ptr to prv It node */
	struct listIterator  *iNext;        /* iterator chain for list_destroy() */
};
struct xlist {
	unsigned int          magic;        /* sentinel for asserting validity   */
	struct listNode      *head;         /* head of the list                  */
	struct listNode     **tail;         /* addr of last node's 'next' ptr    */
	struct listIterator  *iNext;        /* iterator chain for list_destroy() */
	ListDelF              fDel;         /* function to delete node data      */
	int                   count;        /* number of nodes in list           */
};
static void *_list_find_first_locked(list_t *l, ListFindF f, void *key);
static void _list_node_create(list_t *l, list_node_t **pp, void *x);
static void *_list_node_destroy(list_t *l, list_node_t **pp);
static void *_list_pop_locked(list_t *l);
/* list_create()
 */
extern list_t *list_create(ListDelF f)
{
	list_t *l = xmalloc(sizeof(*l));

	l->magic = LIST_MAGIC;
	l->head = NULL;
	l->tail = &l->head;
	l->iNext = NULL;
	l->fDel = f;
	l->count = 0;
	//slurm_rwlock_init(&l->mutex);

	return l;
}

/* list_destroy()
 */
extern void list_destroy(list_t *l)
{
	list_itr_t *i = NULL, *iTmp = NULL;
	list_node_t *p = NULL, *pTmp = NULL;

	assert(l != NULL);
	assert(l->magic == LIST_MAGIC);
	//slurm_rwlock_wrlock(&l->mutex);

	i = l->iNext;
	while (i) {
		assert(i->magic == LIST_ITR_MAGIC);
		i->magic = ~LIST_ITR_MAGIC;
		iTmp = i->iNext;
		xfree(i);
		i = iTmp;
	}
	p = l->head;
	while (p) {
		pTmp = p->next;
		if (p->data && l->fDel)
			l->fDel(p->data);
		xfree(p);
		p = pTmp;
	}
	l->magic = ~LIST_MAGIC;
	// slurm_rwlock_unlock(&l->mutex);
	// slurm_rwlock_destroy(&l->mutex);
	xfree(l);
}

/* list_is_empty()
 */
extern int list_is_empty(list_t *l)
{
	int n = 0;

	assert(l != NULL);
	assert(l->magic == LIST_MAGIC);
	//slurm_rwlock_rdlock(&l->mutex);
	n = l->count;
	//slurm_rwlock_unlock(&l->mutex);

	return (n == 0);
}

/*
 * Return the number of items in list [l].
 * If [l] is NULL, return 0.
 */
extern int list_count(list_t *l)
{
	int n = 0;

	if (!l)
		return 0;

	assert(l->magic == LIST_MAGIC);
	//slurm_rwlock_rdlock(&l->mutex);
	n = l->count;
	//slurm_rwlock_unlock(&l->mutex);

	return n;
}

/*
 * Inserts data pointed to by [x] into list [l] after [pp],
 * the address of the previous node's "next" ptr.
 * Returns a ptr to data [x], or NULL if insertion fails.
 * This routine assumes the list is already locked upon entry.
 */
static void _list_node_create(list_t *l, list_node_t **pp, void *x)
{
	list_node_t *p = NULL;
	list_itr_t *i = NULL;

	assert(l != NULL);
	assert(l->magic == LIST_MAGIC);
	//assert(_list_mutex_is_locked(&l->mutex));
	assert(pp != NULL);
	assert(x != NULL);

	p = xmalloc(sizeof(list_node_t));

	p->data = x;
	if (!(p->next = *pp))
		l->tail = &p->next;
	*pp = p;
	l->count++;

	for (i = l->iNext; i; i = i->iNext) {
		assert(i->magic == LIST_ITR_MAGIC);
		if (i->prev == pp)
			i->prev = &p->next;
		else if (i->pos == p->next)
			i->pos = p;
		assert((i->pos == *i->prev) ||
		       ((*i->prev) && (i->pos == (*i->prev)->next)));
	}
}


extern int slurm_find_char_in_list(void *x, void *key)
{
	char *char1 = (char *)x;
	char *char2 = (char *)key;

	if (!xstrcasecmp(char1, char2))
		return 1;

	return 0;
}

static void *_list_next_locked(list_itr_t *i)
{
	list_node_t *p = NULL;

	if ((p = i->pos))
		i->pos = p->next;
	if (*i->prev != p)
		i->prev = &(*i->prev)->next;

	return (p ? p->data : NULL);
}

static void *_list_find_first_locked(list_t *l, ListFindF f, void *key)
{
	list_node_t *p = NULL;
    for (p = l->head; p; p = p->next) {
		if (f(p->data, key))
			return p->data;
	}

	return NULL;
}


static void *_list_find_first_lock(list_t *l, ListFindF f, void *key)
{
	void *v = NULL;

	assert(l != NULL);
	assert(f != NULL);
	assert(l->magic == LIST_MAGIC);
	// if (write_lock)
	// 	slurm_rwlock_wrlock(&l->mutex);
	// else
	// 	slurm_rwlock_rdlock(&l->mutex);

	v = _list_find_first_locked(l, f, key);

	// slurm_rwlock_unlock(&l->mutex);

	return v;
}
/*
 * list_find_first()
 */
extern void *list_find_first(list_t *l, ListFindF f, void *key)
{
	return _list_find_first_lock(l, f, key);
}

/* list_append()
 */
extern void list_append(list_t *l, void *x)
{
	assert(l != NULL);
	assert(x != NULL);
	assert(l->magic == LIST_MAGIC);
	//slurm_rwlock_wrlock(&l->mutex);
	_list_node_create(l, l->tail, x);
	//slurm_rwlock_unlock(&l->mutex);
}
/* list_append_list()
 */
extern int list_append_list(list_t *l, list_t *sub)
{
	int n = 0;
	list_node_t *p;

	assert(l != NULL);
	assert(l->magic == LIST_MAGIC);
	assert(l->fDel == NULL);
	assert(sub != NULL);
	assert(sub->magic == LIST_MAGIC);

	// slurm_rwlock_wrlock(&l->mutex);
	// slurm_rwlock_wrlock(&sub->mutex);
	p = sub->head;
	while (p) {
		_list_node_create(l, l->tail, p->data);
		n++;
		p = p->next;
	}

	// slurm_rwlock_unlock(&sub->mutex);
	// slurm_rwlock_unlock(&l->mutex);

	return n;
}

extern list_t *list_shallow_copy(list_t *l)
{
	list_t *m = list_create(NULL);

	(void) list_append_list(m, l);

	return m;
}

/*
 * Removes the node pointed to by [*pp] from from list [l],
 * where [pp] is the address of the previous node's "next" ptr.
 * Returns the data ptr associated with list item being removed,
 * or NULL if [*pp] points to the NULL element.
 * This routine assumes the list is already locked upon entry.
 */
static void *_list_node_destroy(list_t *l, list_node_t **pp)
{
	void *v = NULL;
	list_node_t *p = NULL;
	list_itr_t *i = NULL;

	assert(l != NULL);
	assert(l->magic == LIST_MAGIC);
	//assert(_list_mutex_is_locked(&l->mutex));
	assert(pp != NULL);

	if (!(p = *pp))
		return NULL;

	v = p->data;
	if (!(*pp = p->next))
		l->tail = pp;
	l->count--;

	for (i = l->iNext; i; i = i->iNext) {
		assert(i->magic == LIST_ITR_MAGIC);
		if (i->pos == p)
			i->pos = p->next, i->prev = pp;
		else if (i->prev == &p->next)
			i->prev = pp;
		assert((i->pos == *i->prev) ||
		       ((*i->prev) && (i->pos == (*i->prev)->next)));
	}
	xfree(p);

	return v;
}


/* _list_pop_locked
 *
 * Pop an item from the list assuming the
 * the list is already locked.
 */
static void *_list_pop_locked(list_t *l)
{
	void *v = NULL;

	v = _list_node_destroy(l, &l->head);

	return v;
}

/* list_pop()
 */
extern void *list_pop(list_t *l)
{
	void *v = NULL;

	assert(l != NULL);
	assert(l->magic == LIST_MAGIC);
	//slurm_rwlock_wrlock(&l->mutex);

	v = _list_pop_locked(l);
	//slurm_rwlock_unlock(&l->mutex);

	return v;
}

/* list_push()
 */
extern void list_push(list_t *l, void *x)
{
	assert(l != NULL);
	assert(x != NULL);
	assert(l->magic == LIST_MAGIC);
	//slurm_rwlock_wrlock(&l->mutex);
	_list_node_create(l, &l->head, x);
	//slurm_rwlock_unlock(&l->mutex);
}


/* list_remove()
 */
extern void *list_remove(list_itr_t *i)
{
	void *v = NULL;

	assert(i != NULL);
	assert(i->magic == LIST_ITR_MAGIC);
	assert(i->list->magic == LIST_MAGIC);
	//slurm_rwlock_wrlock(&i->list->mutex);

	if (*i->prev != i->pos)
		v = _list_node_destroy(i->list, i->prev);
	//slurm_rwlock_unlock(&i->list->mutex);

	return v;
}

/* list_delete_item()
 */
extern int list_delete_item(list_itr_t *i)
{
	void *v = NULL;

	assert(i != NULL);
	assert(i->magic == LIST_ITR_MAGIC);

	if ((v = list_remove(i))) {
		if (i->list->fDel)
			i->list->fDel(v);
		return 1;
	}

	return 0;
}


/* list_iterator_create()
 */
extern list_itr_t *list_iterator_create(list_t *l)
{
	list_itr_t *i = xmalloc(sizeof(*i));

	assert(l != NULL);

	i->magic = LIST_ITR_MAGIC;
	i->list = l;
	assert(l->magic == LIST_MAGIC);
	//slurm_rwlock_wrlock(&l->mutex);

	i->pos = l->head;
	i->prev = &l->head;
	i->iNext = l->iNext;
	l->iNext = i;

	//slurm_rwlock_unlock(&l->mutex);

	return i;
}
/* list_next()
 */
extern void *list_next(list_itr_t *i)
{
	void *rc = NULL;

	assert(i != NULL);
	assert(i->magic == LIST_ITR_MAGIC);
	assert(i->list->magic == LIST_MAGIC);
	//slurm_rwlock_wrlock(&i->list->mutex);

	rc = _list_next_locked(i);

	//slurm_rwlock_unlock(&i->list->mutex);

	return rc;
}

/* list_iterator_destroy()
 */
extern void list_iterator_destroy(list_itr_t *i)
{
	list_itr_t **pi = NULL;

	assert(i != NULL);
	assert(i->magic == LIST_ITR_MAGIC);
	assert(i->list->magic == LIST_MAGIC);
	//slurm_rwlock_wrlock(&i->list->mutex);

	for (pi = &i->list->iNext; *pi; pi = &(*pi)->iNext) {
		//xassert((*pi)->magic == LIST_ITR_MAGIC);
		if (*pi == i) {
			*pi = (*pi)->iNext;
			break;
		}
	}
	//slurm_rwlock_unlock(&i->list->mutex);

	i->magic = ~LIST_ITR_MAGIC;
	xfree(i);
}

/* list_iterator_reset()
 */
extern void list_iterator_reset(list_itr_t *i)
{
	assert(i != NULL);
	assert(i->magic == LIST_ITR_MAGIC);
    assert(i->list->magic == LIST_MAGIC);
	//slurm_rwlock_wrlock(&i->list->mutex);

	i->pos = i->list->head;
	i->prev = &i->list->head;

	//slurm_rwlock_unlock(&i->list->mutex);
}
