/*
 * This file or a portion of this file is licensed under the terms of
 * the Globus Toolkit Public License, found in file GTPL, or at
 * http://www.globus.org/toolkit/download/license.html. This notice must
 * appear in redistributions of this file, with or without modification.
 *
 * Redistributions of this Software, with or without modification, must
 * reproduce the GTPL in: (1) the Software, or (2) the Documentation or
 * some other similar material which is provided with the Software (if
 * any).
 *
 * Copyright 1999-2004 University of Chicago and The University of
 * Southern California. All rights reserved.
 */
#ifndef _MYLIST_H
#define _MYLIST_H

#include <sys/types.h>

typedef struct mylist_item_tag {
    unsigned long magic;
    const char* pfn;
    const char* lfn;
    struct mylist_item_tag* next;
} mylist_item_t, *mylist_item_p;

extern int mylist_item_init(mylist_item_p item, const char* data);
extern int mylist_item_done(mylist_item_p item);

typedef struct mylist_tag {
    unsigned long magic;
    struct mylist_item_tag* head;
    struct mylist_item_tag* tail;
    size_t count;
} mylist_t, *mylist_p;

extern int mylist_init(mylist_p list);
extern int mylist_add(mylist_p list, const char* data);
extern int mylist_done(mylist_p list);
extern int mylist_fill(mylist_p list, const char* fn);

#endif /* _MYLIST_H */
