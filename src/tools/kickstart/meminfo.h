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

#include <time.h>
#include <sys/types.h>
#include <sys/time.h>

typedef struct {
  /* private */
  struct timeval stamp;      /* point of time that app was started */
  
} JobInfo;

extern
void
initMemInfo( MemInfo* meminfo, pid_t pid );
/* purpose: initialize the data structure from process status
 * paramtr: meminfo (OUT): initialized memory block
 *          pid (IN): process id to use for initialization.
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
deleteMemInfo( MemInfo* meminfo );
/* purpose: destructor
 * paramtr: meminfo (IO): valid MemInfo structure to destroy. 
 */

#endif /* _APPINFO_H */
