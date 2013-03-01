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
#include <sys/types.h>
#include <errno.h>
#include <sys/resource.h>
#include <sys/wait.h>
#include <signal.h>
#include <unistd.h>
#include <string.h>
#include <stdlib.h>
#include <fcntl.h>
#include <stdio.h>

#include "debug.h"
#include "tools.h"
#include "appinfo.h"
#include "statinfo.h"
#include "mysystem.h"
#include "mysignal.h"
#include "procinfo.h"

int mysystem(AppInfo* appinfo, JobInfo* jobinfo, char* envp[])
/* purpose: emulate the system() libc call, but save utilization data. 
 * paramtr: appinfo (IO): shared record of information
 *                        isPrinted (IO): reset isPrinted in child process!
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
 *          input (IN): connect to stdin or share
 *          output (IN): connect to stdout or share
 *          error (IN): connect to stderr or share
 *          envp (IN): vector with the parent's environment
 * returns:   -1: failure in mysystem processing, check errno
 *           126: connecting child to its new stdout failed
 *           127: execve() call failed
 *          else: status of child
 */
{
  struct sigaction ignore, saveintr, savequit;

  /* sanity checks first */
  if ( ! jobinfo->isValid ) {
    errno = ENOEXEC; /* no executable */
    return -1;
  }

  memset( &ignore, 0, sizeof(ignore) );
  ignore.sa_handler = SIG_IGN;
  sigemptyset( &ignore.sa_mask );
  ignore.sa_flags = 0;
  if ( sigaction( SIGINT, &ignore, &saveintr ) < 0 )
    return -1;
  if ( sigaction( SIGQUIT, &ignore, &savequit ) < 0 )
    return -1;

  /* start wall-clock */
  now( &(jobinfo->start) );

  if ( (jobinfo->child=fork()) < 0 ) {
    /* no more process table space */
    jobinfo->status = -1;
  } else if ( jobinfo->child == 0 ) {
    /* child */
    appinfo->isPrinted=1;

    /* connect jobs stdio */
    if ( forcefd( &appinfo->input, STDIN_FILENO ) ) _exit(126);
    if ( forcefd( &appinfo->output, STDOUT_FILENO ) ) _exit(126);
    if ( forcefd( &appinfo->error, STDERR_FILENO ) ) _exit(126);

    /* undo signal handlers */
    sigaction( SIGINT, &saveintr, NULL );
    sigaction( SIGQUIT, &savequit, NULL );

    /* If we are tracing, then hand over control to the proc module */
    if (1) {
      if ( procChild() ) _exit(126);
    }
    
    execve( jobinfo->argv[0], (char* const*) jobinfo->argv, envp );
    _exit(127); /* executed in child process */
  } else {
    /* parent */
    if (1) {
      procParentTrace(jobinfo->child, &jobinfo->status, &jobinfo->use, &(jobinfo->children));
    } else {
      procParentWait(jobinfo->child, &jobinfo->status, &jobinfo->use, &(jobinfo->children));
    }
    
    /* sanity check */
    if ( kill( jobinfo->child, 0 ) == 0 ) {
      debugmsg( "ERROR: job %d is still running!\n", jobinfo->child );
      if ( ! errno ) errno = EINPROGRESS;
    }
  }
  
  /* save any errors before anybody overwrites this */
  jobinfo->saverr = errno;
  
  /* stop wall-clock */
  now( &(jobinfo->finish) );
  
  /* ignore errors on these, too. */
  sigaction( SIGINT, &saveintr, NULL );
  sigaction( SIGQUIT, &savequit, NULL );
  
  /* finalize */
  return jobinfo->status;
}

