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

#include "tools.h"
#include "appinfo.h"
#include "statinfo.h"
#include "event.h"
#include "mysystem.h"

static const char* RCS_ID =
"$Id$";

#include "mysignal.h"

typedef struct {
  volatile sig_atomic_t count;   /* OUT: number of signals seen */
  volatile sig_atomic_t done;    /* OUT: 0: to be done, 1: child reaped */
  volatile int          error;   /* OUT: errno when something went bad */
  JobInfo*       	job;     /*  IO: data repository */
} SignalHandlerCommunication;

static SignalHandlerCommunication child;


#ifdef DEBUG_WAIT
static
pid_t
mywait4( pid_t wpid, int* status, int options, struct rusage* rusage )
{
  pid_t result = wait4( wpid, status, options, rusage );
  fprintf( stderr, "# wait4(%d,%p=%d,%d,%p) = %d\n",
	   wpid, status, *status, options, rusage, result );
  return result;
}
#else
#define mywait4(a,b,c,d) wait4(a,b,c,d)
#endif /* DEBUG_WAIT */

static
SIGRETTYPE
sig_child( SIGPARAM signo )
{
  int rc = -1;

  ++child.count;
  /* There have been known cases where Linux delivers signals twice
   * that may have been sent only once, grrrr. 
   */
#ifdef DEBUG_WAIT
  fprintf( stderr, "# child.count == %d\n", child.count );
#endif /* DEBUG_WAIT */

  if ( child.job != NULL ) {
    int saverr = errno;
    errno = 0;
#ifdef DEBUG_WAIT
    fputs( "# child.job != NULL\n", stderr );
#endif /* DEBUG_WAIT */

    /* WARN: wait4 is not POSIX.1 reentrant safe */
    while ( (rc=mywait4( child.job->child, &child.job->status, 
			 WNOHANG, &child.job->use )) < 0 ) {
      if ( errno != EINTR ) {
	child.error = errno;
	child.job->status = -42;
	break;
      }
    }
    errno = saverr;
  }

#ifdef DEBUG_WAIT
  fprintf( stderr, "# child.done := (%d != 0) => %d\n", rc, (rc != 0) );
#endif /* DEBUG_WAIT */

  /* once set, never reset */
  if ( ! child.done ) child.done = ( rc != 0 );
}

static
SIGRETTYPE
sig_propagate( SIGPARAM signo )
/* purpose: propagate the signal to active children */
{
  if ( child.job != NULL ) kill( child.job->child, signo );
}

int
mysystem( AppInfo* appinfo, JobInfo* jobinfo, char* envp[] )
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
  struct sigaction new_child, old_child;

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

  /* install SIGCHLD handler */
  memset( &child, 0, sizeof(child) );
  child.job  = jobinfo;

  memset( &new_child, 0, sizeof(new_child) );
  new_child.sa_handler = sig_child;
  sigemptyset( &new_child.sa_mask );
  new_child.sa_flags = SA_NOCLDSTOP;
#ifdef SA_INTERRUPT
  new_child.sa_flags |= SA_INTERRUPT; /* SunOS, obsoleted by POSIX */
#endif
  if ( sigaction( SIGCHLD, &new_child, &old_child ) < 0 )
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
    sigaction( SIGCHLD, &old_child, NULL );

    execve( jobinfo->argv[0], (char* const*) jobinfo->argv, envp );
    _exit(127); /* executed in child process */
  } else {
    /* parent */
    int saverr;
    errno = 0;

    /* insert event loop here */
    while ( ! child.done )
      eventLoop( STDERR_FILENO, &appinfo->channel, &child.done );

    /* sanity check */
    saverr = errno;
    if ( kill( jobinfo->child, 0 ) == 0 ) {
      fprintf( stderr, "ERROR: job %d is still running!\n", jobinfo->child );
      if ( ! child.error ) child.error = EINPROGRESS;
    }
    errno = child.error ? child.error : saverr;
  }

  /* save any errors before anybody overwrites this */
  jobinfo->saverr = errno;

  /* move closer towards signal occurance -- ward off further signals */
  sigaction( SIGCHLD, &old_child, NULL );

  /* stop wall-clock */
  now( &(jobinfo->finish) );

  /* ignore errors on these, too. */
  sigaction( SIGINT, &saveintr, NULL );
  sigaction( SIGQUIT, &savequit, NULL );

  /* only after handler was deactivated */
  if ( child.count != 1 || child.error ) {
    char temp[256];
    snprintf( temp, sizeof(temp), "%d x SIGCHLD; %d: %s",
	      child.count, child.error, strerror(child.error) );
    send_message( STDERR_FILENO, temp, strlen(temp), 0 );
  }

  /* finalize */
  return jobinfo->status;
}
