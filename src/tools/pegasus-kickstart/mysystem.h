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
#ifndef _MYSYSTEM_H
#define _MYSYSTEM_H

#include "appinfo.h"
#include "statinfo.h"
#include "jobinfo.h"

extern 
int 
mysystem( AppInfo* appinfo, JobInfo* jobinfo, char* envp[] );
/* purpose: emulate the system() libc call, but save utilization data. 
 * paramtr: appinfo (IO): shared record of information
 *                        isPrinted (IO): only to reset isPrinted in child process!
 *                        input (IN): connect to stdin or share
 *                        output (IN): connect to stdout or share
 *                        error (IN): connect to stderr or share
 *          jobinfo (IO): updated record of job-specific information
 *                        argv (IN): assembled commandline
 *                        child (OUT): pid of child process
 *                        status (OUT): also returned as function result
 *                        saverr (OUT): will be set to value of errno
 *                        start (OUT): will be set to startup time
 *                        final (OUT): will be set to finish time after reap
 *                        use (OUT): rusage record from application call
 *          envp (IN): vector with the parent's environment
 * returns:   -1: failure in mysystem processing, check errno
 *           126: connecting child to its new stdout failed
 *           127: execve() call failed
 *          else: status of child
 */

#endif /* _MYSYSTEM_H */
