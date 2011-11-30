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
#ifndef _MEMINFO_H
#define _MEMINFO_H

#include <sys/types.h>
#include <time.h>
#include <sys/types.h>
#include <sys/time.h>

typedef struct {
  /* private */
  struct timeval stamp;      /* point of time that app was started */
  unsigned long  n;          /* running average helper */

  /* the following info is in pages */
  unsigned long  size;       /* memory (virtual) size */
  unsigned long  rss;        /* resident set (present) size */
  unsigned long  share;      /* amount of sharable memory */
  unsigned long  trs;        /* executable (and lib) size */
  unsigned long  lrs;        /* local (usually zero) size */
  unsigned long  drs;        /* data PLUS stack size */
  unsigned long  dirty;      /* dirty (need to write) pages */
} MemInfo;

extern
void
initMemInfo( MemInfo* mem, pid_t pid );
/* purpose: initialize the data structure from process status
 * paramtr: mem (OUT): initialized memory block
 *          pid (IN): process id to use for initialization.
 */

extern
void
maxMemInfo( MemInfo* max, const MemInfo* add );
/* purpose: keeps the maximum found for the memory info.
 * paramtr: max (IO): adjusted to the maximum
 *          add (IN): look for maxima here
 */

extern
void
avgMemInfo( MemInfo* avg, const MemInfo* add );
/* purpose: keeps a running average
 * paramtr: max (IO): keeping the running average
 *          avg (IN): new values to add to average
 */

extern
int
printXMLMemInfo( char* buffer, size_t size, size_t* len, size_t indent,
		 const char* tag, const MemInfo* mem );
/* purpose: format the status information into the given buffer as XML.
 * paramtr: buffer (IO): area to store the output in
 *          size (IN): capacity of character area
 *          len (IO): current position within area, will be adjusted
 *          indent (IN): indentation level
 *          tag (IN): name to use for element tags.
 *          mem (IN): job status info to xml format.
 * returns: number of characters put into buffer (buffer length)
 */

extern
void
deleteMemInfo( MemInfo* mem );
/* purpose: destructor
 * paramtr: mem (IO): valid MemInfo structure to destroy. 
 */

#endif /* _MEMINFO_H */
