/**
 *  Copyright 2007-2010 University Of Southern California
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
#include <sys/types.h>
#include <sys/wait.h>
#include <errno.h>
#include <stdio.h>
#include <string.h>
#include <signal.h>
#include <unistd.h>
#include <fcntl.h>

#include "tools.h"
#include "report.h"
#include "mysystem.h"

static const char* RCS_ID =
  "$Id$";

extern int debug; 
extern int progress; 
extern char* application; 

int
mysystem( char* argv[], char* envp[], const char* special )
/* purpose: implement system(3c) call w/o calling the shell
 * paramtr: argv (IN): NULL terminated argument vector
 *          envp (IN): NULL terminated environment vector
 *          special (IN): set for setup/cleanup jobs
 * returns: exit status from wait() family 
 */
{
  char date[32];
  struct rusage usage;
  struct sigaction ignore, saveintr, savequit;
  sigset_t childmask, savemask;
  pid_t child;
  time_t when, then;
  double diff, start = now(&when);
  int saverr = 0;
  int status = -1;

  ignore.sa_handler = SIG_IGN;
  sigemptyset( &ignore.sa_mask );
  ignore.sa_flags = 0;
  
  if ( sigaction( SIGINT, &ignore, &saveintr ) < 0 )
    return -1;
  if ( sigaction( SIGQUIT, &ignore, &savequit ) < 0 )
    return -1;

  sigemptyset( &childmask );
  sigaddset( &childmask, SIGCHLD );
  memset( &usage, 0, sizeof(usage) );
  if ( sigprocmask( SIG_BLOCK, &childmask, &savemask ) < 0 )
    return -1;

  if ( (child=fork()) < 0 ) {
    /* no more process table space */
    return -1;
  } else if ( child == (pid_t) 0 ) {
    /* child */
    int null = open( "/dev/null", O_RDONLY );
    if ( null != -1 ) { 
      if ( dup2( null, STDIN_FILENO ) == -1 && debug )
	showerr( "%s: dup2 stdin: %d: %s\n",
		 application, errno, strerror(errno) );
    } else {
      if ( debug ) 
	showerr( "%s: open /dev/null: %d: %s\n", 
		 application, errno, strerror(errno) );
    }

    /* undo signal handlers */
    sigaction( SIGINT, &saveintr, NULL );
    sigaction( SIGQUIT, &savequit, NULL );
    sigprocmask( SIG_SETMASK, &savemask, NULL );

    execve( argv[0], (char* const*) argv, envp );
    showerr( "%s: exec %s: %d: %s\n", 
	     application, argv[0], errno, strerror(errno) );
    _exit(127); /* never reached unless error */
  } else {
    /* parent */
    
    /* wait for child */
    while ( wait4( child, &status, 0, &usage ) < 0 ) {
      if ( errno != EINTR ) {
	status = -1;
	break;
      }
    }
    
    /* remember why/how we did exit */
    saverr = errno;
    
    /* sanity check */
    if ( kill( child, 0 ) == 0 )
      showerr( "Warning: job %d (%s) is still running!\n", 
	       child, argv[0] );
  }

  /* ignore errors on these, too. */
  sigaction( SIGINT, &saveintr, NULL );
  sigaction( SIGQUIT, &savequit, NULL );
  sigprocmask( SIG_SETMASK, &savemask, NULL );

  /* say hi */
  diff = now(&then) - start;
  if ( debug > 1 ) {
    printf( "<job app=\"%s\" start=\"%s\" duration=\"%.3f\" status=\"%d\"/>\n",
	    special ? special : argv[0], 
	    isodate(when,date,sizeof(date)), diff, status );
  }

  /* progress report finish */
  if ( progress != -1 ) 
    report( progress, when, diff, status, argv, &usage, special );

  errno = saverr;
  return status;
}
