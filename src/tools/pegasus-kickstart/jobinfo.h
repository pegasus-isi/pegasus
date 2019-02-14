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
#include "procinfo.h"

typedef struct {
  int            isValid;     /* 0: uninitialized, 1:valid, 2:app not found */
  char*          copy;        /* buffer for argument separation */

  char* const*   argv;        /* application executable and arguments */
  int            argc;        /* application CLI number of arguments */
  StatInfo       executable;  /* stat() info for executable, if available */

  struct timeval start;       /* point of time that app was started */
  struct timeval finish;      /* point of time that app was reaped */

  pid_t          child;       /* pid of process that ran application */
  int            status;      /* raw exit status of application */
  int            saverr;      /* errno for status < 0 */
  char*          prefix;      /* prefix to error message for status < 0 */
  struct rusage  use;         /* rusage record from reaping application status */

  ProcInfo *     children;    /* per-process memory, I/O and CPU usage */
} JobInfo;

/* if set to 1, make the application executable, no matter what. */
extern int make_application_executable;

extern void initJobInfo(JobInfo* jobinfo, int argc, char* const* argv, const char *wrapper);
extern void initJobInfoFromString(JobInfo* jobinfo, const char* commandline);
extern int printYAMLJobInfo(FILE *out, int indent, const char* tag, const JobInfo* job);
extern void deleteJobInfo(JobInfo* jobinfo);

#endif /* _JOBINFO_H */
