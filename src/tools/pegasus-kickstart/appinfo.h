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
#include <unistd.h>

#include "statinfo.h"
#include "jobinfo.h"
#include "limitinfo.h"
#include "machine.h"

typedef struct {
    struct timeval start;          /* point of time that app was started */
    struct timeval finish;         /* point of time that app was reaped */
    int            isPrinted;      /* flag to set after successful print op */
    int            noHeader;       /* exclude <?xml ?> premable and <machine> */
    int            fullInfo;       /* include <statcall>, <environment> and <resource> */
    int            enableTracing;  /* Enable resource usage tracing */
    int            enableSysTrace; /* Enable system call tracing */
    int            omitData;       /* Omit <data> for stdout and stderr if job succeeds */
    int            enableLibTrace; /* Enable library tracing */
    int            termTimeout;    /* Time to allow job to run before sending sigterm */
    int            killTimeout;    /* Time to allow job to handle sigterm before sending sigkill */
    pid_t          currentChild;   /* The current child process (setup, pre, main, post, cleanup) */
    int            nextSignal;     /* The next signal to deliver */
    int            useCDATA;       /* Use CDATA instead of quoting <data> */

    char* const*   argv;           /* application executable and arguments */
    int            argc;           /* application CLI number of arguments */

    char           ipv4[16];       /* host address of primary interface */
    char           prif[16];       /* name of primary interface NIC */ 
    char*          xformation;     /* chosen VDC TR fqdn for this invocation */
    char*          derivation;     /* chosen VDC DV fqdn for this invocation */
    char*          sitehandle;     /* resource handle for the this site */
    char*          wf_label;       /* label of workflow this job belongs to */
    char*          wf_stamp;       /* time stamp of workflow this job belongs to */
    char*          workdir;        /* CWD at point of execution */
    pid_t          child;          /* pid of kickstart itself */

    JobInfo        setup;          /* optional set-up application to run */
    JobInfo        prejob;         /* optional pre-job application to run */
    JobInfo        application;    /* the application itself that was run */
    JobInfo        postjob;        /* optional post-job application to run */
    JobInfo        cleanup;        /* optional clean-up application to run */

    StatInfo       input;          /* stat() info for "input", if available */
    StatInfo       output;         /* stat() info for "output", if available */
    StatInfo       error;          /* stat() info for "error", if available */
    StatInfo       logfile;        /* stat() info for "logfile", if available */
    StatInfo       kickstart;      /* stat() info for this program, if available */
    StatInfo       metadata;       /* stat() info for "metadata", if available */
    StatInfo       integritydata;  /* stat() info for "integritydata", if available */

    StatInfo*      initial;        /* stat() info for user-specified files. */
    size_t         icount;         /* size of initial array, may be 0 */
    StatInfo*      final;          /* stat() info for user-specified files. */
    size_t         fcount;         /* size of final array, may be 0 */
    mode_t         umask;          /* currently active umask */

    struct rusage  usage;          /* rusage record for myself */
    LimitInfo      limits;         /* hard- and soft limits */
    MachineInfo    machine;        /* more system information */

    int            status;         /* The final status of the job */
} AppInfo;

extern int initAppInfo(AppInfo* appinfo, int argc, char* const* argv);
extern int printAppInfo(AppInfo* runinfo);
extern void deleteAppInfo(AppInfo* runinfo);

#endif /* _APPINFO_H */
