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
#ifndef _JOBINFO_H
#define _JOBINFO_H

#include <time.h>
#include <sys/types.h>
#include <sys/time.h>
#include <sys/resource.h>
#include "statinfo.h"

#ifdef USE_MEMINFO
#include "meminfo.h"
#endif /* USE_MEMINFO */

typedef struct {
  /* private */
  int           isValid;     /* 0: uninitialized, 1:valid, 2:app not found */
  char*         copy;        /* buffer for argument separation */

  /* public */
  char* const*  argv;        /* application executable and arguments */
  int           argc;        /* application CLI number of arguments */
  StatInfo      executable;  /* stat() info for executable, if available */

  struct timeval start;      /* point of time that app was started */
  struct timeval finish;     /* point of time that app was reaped */

  pid_t         child;       /* pid of process that ran application */
  int           status;      /* raw exit status of application */
  int           saverr;      /* errno for status < 0 */
  char*         prefix;      /* prefix to error message for status < 0 */
  struct rusage use;         /* rusage record from reaping application status */
#ifdef USE_MEMINFO
  MemInfo       peakmem;     /* maximum memory usage during lifetime */
#endif /* USE_MEMINFO */
} JobInfo;

extern
void
initJobInfo( JobInfo* jobinfo, int argc, char* const* argv );
/* purpose: initialize the data structure with defaults.
 * paramtr: appinfo (OUT): initialized memory block
 *          argc (IN): adjusted argument count
 *          argv (IN): adjusted argument vector to point to app.
 */

extern
void
initJobInfoFromString( JobInfo* jobinfo, const char* commandline );
/* purpose: initialize the data structure with default
 * paramtr: jobinfo (OUT): initialized memory block
 *          commandline (IN): commandline concatenated string to separate
 */

extern
int
printXMLJobInfo( char* buffer, size_t size, size_t* len, size_t indent,
		 const char* tag, const JobInfo* job );
/* purpose: format the job information into the given buffer as XML.
 * paramtr: buffer (IO): area to store the output in
 *          size (IN): capacity of character area
 *          len (IO): current position within area, will be adjusted
 *          indent (IN): indentation level
 *          tag (IN): name to use for element tags.
 *          job (IN): job info to print.
 * returns: number of characters put into buffer (buffer length)
 */

extern
void
deleteJobInfo( JobInfo* jobinfo );
/* purpose: destructor
 * paramtr: runinfo (IO): valid JobInfo structure to destroy. 
 */

#endif /* _JOBINFO_H */
