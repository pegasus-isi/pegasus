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
#include "mylist.h"
#include <ctype.h>
#include <errno.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

static const char* RCS_ID =
"$Id: mylist.c,v 1.2 2005/08/15 16:45:38 griphyn Exp $";

#ifndef ITEM_MAGIC
#define ITEM_MAGIC 0xcd82bd08
#endif

#ifndef LIST_MAGIC
#define LIST_MAGIC 0xbae21bdb
#endif

int 
mylist_item_init( mylist_item_p item, const char* data )
/* purpose: initial a data item.
 * paramtr: item (OUT): item pointer to initialize
 *          data (IN): string to copy into item
 * returns: 0 on success,
 *          EINVAL if arguments are NULL
 *          ENOMEM if allocation failed
 */
{
  char* s;

  /* sanity check */
  if ( item == NULL || data == NULL ) return EINVAL;

  memset( item, 0, sizeof(mylist_item_t) );
  item->magic = ITEM_MAGIC;
  if ( (item->pfn = strdup(data)) == NULL ) return ENOMEM;
  if ( (s = strchr(item->pfn, '=')) ) {
    *s++ = '\0';
    item->lfn = item->pfn;
    item->pfn = s;
  } else {
    item->lfn = NULL;
  }
  return 0;
}

int
mylist_item_done( mylist_item_p item )
/* purpose: free allocated space of an item
 * paramtr: item (IO): area to free
 * returns: 0 on success,
 *          EINVAL if the magic failed, or NULL argument
 */
{
  /* sanity check */
  if ( item == NULL || item->magic != ITEM_MAGIC ) return EINVAL;

  /* free item */
  if ( item->lfn ) free((void*) item->lfn);
  else if ( item->pfn ) free((void*) item->pfn);
  memset( item, 0, sizeof(mylist_item_t) );
  return 0;
}

int
mylist_init( mylist_p list )
{
  /* sanity check */
  if ( list == NULL ) return EINVAL;

  memset( list, 0, sizeof(mylist_t) );
  list->magic = LIST_MAGIC;
  return 0;
}

int
mylist_add( mylist_p list, const char* data )
{
  int status;
  mylist_item_p temp;

  /* sanity check */
  if ( list == NULL || list->magic != LIST_MAGIC ) return EINVAL;

  /* allocate item space */
  if ( (temp = malloc( sizeof(mylist_item_t) )) == NULL ) return ENOMEM;
  if ( (status = mylist_item_init( temp, data )) != 0 ) {
    free((void*) temp);
    return status;
  }

  /* add item to list */
  if ( list->count ) {
    list->tail->next = temp;
    list->tail = temp;
  } else {
    list->head = list->tail = temp;
  }

  list->count++;
  return 0;
}

int
mylist_done( mylist_p list )
{
  /* sanity check */
  if ( list == NULL || list->magic != LIST_MAGIC ) return EINVAL;

  if ( list->count ) {
    /* traverse list */
    int status;
    mylist_item_p temp;
    while ( (temp = list->head) != NULL ) {
      list->head = list->head->next;
      if ( (status=mylist_item_done( temp )) != 0 ) return status;
      free((void*) temp);
    }
  }

  memset( list, 0, sizeof(mylist_t) );
  return 0;
}

int
mylist_fill( mylist_p list, const char* fn )
/* purpose: Add each line in the specified file to the list
 * paramtr: list (IO): list to modify
 *          fn (IN): name of the file to read
 * returns: 0 on success,
 */
{
  FILE* file;
  char* line, *s;
  int result = 0;
  size_t size = getpagesize();

  /* sanity check */
  if ( list == NULL || list->magic != LIST_MAGIC ) return EINVAL;

  /* try to open file */
  if ( (file=fopen( fn, "r" )) == NULL ) return errno;

  /* allocate line buffer */
  if ( (line=(char*)malloc(size)) == NULL ) {
    fclose(file);
    return ENOMEM;
  }

  /* read lines from file */
  while ( fgets( line, size, file ) ) {
    /* FIXME: unhandled overly long lines */

    /* comments */
    if ( (s = strchr( line, '#' )) ) *s-- = '\0';
    else s = line + strlen(line) - 1;

    /* chomp */
    while ( s > line && isspace(*s) ) *s-- = '\0';

    /* skip empty lines */
    if ( *line == 0 ) continue;

    if ( (result=mylist_add( list, line )) != 0 ) break;
  }

  /* done with file */
  fclose(file);
  free((void*) line);
  return result;
}
