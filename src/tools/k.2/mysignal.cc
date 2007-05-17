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
#include <string.h>
#include <memory.h>
#include <stdio.h>
#include <unistd.h>
#include <sys/wait.h>
#include <signal.h>

#include "mysignal.hh"

static const char* RCS_ID = 
"$Id: mysignal.cc,v 1.2 2004/02/23 20:21:53 griphyn Exp $";

SigFunc*
mysignal( int signo, SigFunc* newhandler, bool doInterrupt )
  // purpose: install reliable signals
  // paramtr: signo (IN): signal for which a handler is to be installed
  //          newhandler (IN): function pointer to the signal handler
  //          doInterrupt (IN): interrupted system calls wanted!
  // returns: the old signal handler, or SIG_ERR in case of error.
{
  struct sigaction action, old;

  memset( &old, 0, sizeof(old) );
  memset( &action, 0, sizeof(action) );

#if 1
  action.sa_handler = newhandler; // I HATE TYPE-OVERCORRECT NAGGING
#else
  memmove( &action.sa_handler, &newhandler, sizeof(SigFunc*) );
#endif
  sigemptyset( &action.sa_mask );

  if ( signo == SIGCHLD ) {
    action.sa_flags |= SA_NOCLDSTOP;

#ifdef SA_NODEFER
    action.sa_flags |= SA_NODEFER;   // SYSV: don't block current signal
#endif
  }

  if ( signo == SIGALRM || doInterrupt ) {
#ifdef SA_INTERRUPT
    action.sa_flags |= SA_INTERRUPT; // SunOS, obsoleted by POSIX
#endif
  } else {
#ifdef SA_RESTART
    action.sa_flags |= SA_RESTART;   // BSD, SVR4
#endif
  }

  return ( sigaction( signo, &action, &old ) < 0 ) ? 
    (SigFunc*) SIG_ERR : 
    (SigFunc*) old.sa_handler;
}
