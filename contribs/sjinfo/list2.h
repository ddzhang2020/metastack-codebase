/*****************************************************************************
 *  list2.h
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
 *****************************************************************************/

#ifndef LSD_LIST2_H
#define LSD_LIST2_H

#define FREE_NULL_LIST(_X)			\
	do {					\
		if (_X) list_destroy (_X);	\
		_X	= NULL; 		\
	} while (0)

typedef struct xlist *List;
typedef struct xlist list_t;
/***************
 *  Constants  *
 ***************/



typedef struct listNode * ListNode;
typedef struct listIterator list_itr_t;
typedef void (*ListDelF) (void *x);
typedef int (*ListFindF) (void *x, void *key);
/*
 *  Creates and returns a new empty list.
 *  The deletion function [f] is used to deallocate memory used by items
 *    in the list; if this is NULL, memory associated with these items
 *    will not be freed when the list is destroyed.
 *  Note: Abandoning a list without calling list_destroy() will result
 *    in a memory leak.
 */
extern list_t *list_create(ListDelF f);

/*
 *  Destroys list [l], freeing memory used for list iterators and the
 *    list itself; if a deletion function was specified when the list
 *    was created, it will be called for each item in the list.
 */
extern void list_destroy(list_t *l);

/*
 *  Returns non-zero if list [l] is empty; o/w returns zero.
 */
extern int list_is_empty(list_t *l);

/*
 * Return the number of items in list [l].
 * If [l] is NULL, return 0.
 */
extern int list_count(list_t *l);


extern int slurm_find_char_in_list(void *x, void *key);

/*
 *  Traverses list [l] using [f] to match each item with [key].
 *  Returns a ptr to the first item for which the function [f]
 *    returns non-zero, or NULL if no such item is found.
 *  Note: This function differs from list_find() in that it does not require
 *    a list iterator; it should only be used when all list items are known
 *    to be unique (according to the function [f]).
 */
extern void *list_find_first(list_t *l, ListFindF f, void *key);

/*
 *  Inserts data [x] at the end of list [l].
 */
extern void list_append(list_t *l, void *x);

/*
 *  Inserts list [sub] at the end of list [l].
 *  Note: list [l] must have a destroy function of NULL.
 *  Returns a count of the number of items added to list [l].
 */
extern int list_append_list(list_t *l, List sub);

/*
 *  Create new shallow copy of list [l] pointers, without destructor.
 *
 *  The list created is intended to allow manipulation of the list without
 *  affecting the real list (such as sorting).
 *
 *  Warning: destruction of this list will not free members of [l].
 *  Warning: This list is only valid while [l] is unchanged.
 */
extern list_t *list_shallow_copy(list_t *l);

/*
 *  Pops the data item at the top of the stack [l].
 *  Returns the data's ptr, or NULL if the stack is empty.
 */
extern void *list_pop(list_t *l);

/****************************
 *  Stack Access Functions  *
 ****************************/

/*
 *  Pushes data [x] onto the top of stack [l].
 */
extern void list_push(list_t *l, void *x);

/*
 *  Removes from the list the last item returned via list iterator [i]
 *    and returns the data's ptr.
 *  Note: The client is responsible for freeing the returned data.
 */
extern void *list_remove(list_itr_t *i);

/*
 *  Removes from the list the last item returned via list iterator [i];
 *    if a deletion function was specified when the list was created,
 *    it will be called to deallocate the item being removed.
 *  Returns a count of the number of items removed from the list
 *    (ie, '1' if the item was removed, and '0' otherwise).
 */
extern int list_delete_item(list_itr_t *i);

/*
 *  Creates and returns a list iterator for non-destructively traversing
 *    list [l].
 */
extern list_itr_t *list_iterator_create(list_t *l);

/*
 *  Returns a ptr to the next item's data,
 *    or NULL once the end of the list is reached.
 *  Example: i=list_iterator_create(i); while ((x=list_next(i))) {...}
 */
extern void *list_next(list_itr_t *i);

/*
 *  Destroys the list iterator [i]; list iterators not explicitly destroyed
 *    in this manner will be destroyed when the list is deallocated via
 *    list_destroy().
 */
extern void list_iterator_destroy(list_itr_t *i);

/*
 *  Resets the list iterator [i] to start traversal at the beginning
 *    of the list.
 */
extern void list_iterator_reset(list_itr_t *i);
#endif /* !LSD_LIST_H */