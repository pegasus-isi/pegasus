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
#ifndef _APPINFO_H
#define _APPINFO_H

#include <time.h>
#include <sys/types.h>
#include <sys/time.h>
#include <sys/resource.h>
#include <sys/utsname.h>
#include "statinfo.h"
#include "jobinfo.h"
#include "limitinfo.h"

#ifndef SYS_NMLN
#ifdef _SYS_NAMELEN /* DARWIN */
#define SYS_NMLN 65
#else
#error "No SYS_NMLN nor _SYS_NAMELEN: check <sys/utsname.h>"
#endif /* _SYS_NAMELEN */
#endif /* SYS_NMLN */

typedef struct {
  struct timeval start;      /* point of time that app was started */
  struct timeval finish;     /* point of time that app was reaped */
  int            isPrinted;  /* flag to set after successful print op */
  int            noHeader;   /* avoid <?xml ?> premable */

  char* const*   argv;       /* application executable and arguments */
  int            argc;       /* application CLI number of arguments */
  char* const*   envp;       /* snapshot of environment */
  size_t         envc;       /* size of the environment vector envp */

  char           ipv4[16];   /* host address of primary interface */
  char*          xformation; /* chosen VDC TR fqdn for this invocation */
  char*          derivation; /* chosen VDC DV fqdn for this invocation */
  char*          sitehandle; /* resource handle for the this site */
  char*          wf_label;   /* label of workflow this job belongs to */
  char*          wf_stamp;   /* time stamp of workflow this job belongs to */
  char*          workdir;    /* CWD at point of execution */
  pid_t          child;      /* pid of gridstart itself */

  JobInfo        setup;      /* optional set-up application to run */
  JobInfo        prejob;     /* optional pre-job application to run */
  JobInfo        application;/* the application itself that was run */
  JobInfo        postjob;    /* optional post-job application to run */
  JobInfo        cleanup;    /* optional clean-up application to run */

  StatInfo       input;      /* stat() info for "input", if available */
  StatInfo       output;     /* stat() info for "output", if available */
  StatInfo       error;      /* stat() info for "error", if available */
  StatInfo       logfile;    /* stat() info for "logfile", if available */
  StatInfo       gridstart;  /* stat() info for this program, if available */
  StatInfo       channel;    /* stat() on app channel FIFO, if avail. */

  StatInfo*      initial;    /* stat() info for user-specified files. */
  size_t         icount;     /* size of initial array, may be 0 */
  StatInfo*      final;      /* stat() info for user-specified files. */
  size_t         fcount;     /* size of final array, may be 0 */
  mode_t         umask;      /* currently active umask */

  struct rusage  usage;      /* rusage record for myself */
  struct utsname uname;      /* system environment */
  char   archmode[SYS_NMLN]; /* IA32, IA64, ILP32, LP64, ... */
  LimitInfo      limits;     /* hard- and soft limits */
} AppInfo;

extern
void
initAppInfo( AppInfo* appinfo, int argc, char* const* argv );
/* purpose: initialize the data structure with defaults.
 * This will also parse the CLI arguments and assemble the app call CLI.
 * paramtr: appinfo (OUT): initialized memory block
 *          argc (IN): from main()
 *          argv (IN): from main()
 * except.: Will exit with code 1 on empty commandline
 */

extern
int
printAppInfo( const AppInfo* runinfo );
/* purpose: output the given app info onto the given fd
 * paramtr: appinfo (IN): is the collective information about the run
 * returns: the number of characters actually written (as of write() call).
 * sidekck: will update the self resource usage record before print.
 */

extern
void
envIntoAppInfo( AppInfo* runinfo, char* envp[] );
/* purpose: save a deep copy of the current environment
 * paramtr: runinfo (IO): place to store the deep copy
 *          envp (IN): current environment pointer
 */

extern
void
deleteAppInfo( AppInfo* runinfo );
/* purpose: destructor
 * paramtr: runinfo (IO): valid AppInfo structure to destroy. 
 */

#endif /* _APPINFO_H */
