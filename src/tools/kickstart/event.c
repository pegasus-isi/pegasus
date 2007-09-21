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
#include <sys/time.h>
#include <sys/poll.h>
#include <fcntl.h>
#include <errno.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <stdio.h>

#include "rwio.h"
#include "tools.h"
#include "event.h"
#include "mysignal.h"

static const char* RCS_ID =
"$Id$";

static sig_atomic_t seen_sigpipe; /* defaults to 0 */

static
SIGRETTYPE
sig_pipe( int signo )
{
  seen_sigpipe = 1;
}

extern int isExtended;
extern int isLocal;

ssize_t
send_message( int outfd, char* msg, ssize_t msize, unsigned channel )
/* purpose: sends a XML-encoded message chunk back to the application
 * paramtr: outfd (IN): output file descriptor, writable (STDERR_FILENO)
 *          msg (IN): pointer to message
 *          msize (IN): length of message content
 *          channel (IN): which channel to send upon (0 - app)
 */
{
  int locked;
  size_t i, len = 0;
  size_t size = msize + 256;
  char* buffer = (char*) malloc(size);
  struct timeval t;

  if ( buffer == NULL ) {
    errno = ENOMEM;
    return -1;
  }

  now( &t );
  myprint( buffer, size, &len, "<chunk channel=\"%u\" size=\"%ld\" start=\"",
	   channel, msize );
  mydatetime( buffer, size, &len, isLocal, isExtended, t.tv_sec, t.tv_usec );
  append( buffer, size, &len, "\"><![CDATA[" );
#if 1
  full_append( buffer, size, &len, msg, msize );
#else
  for ( i=0; i<msize; ++i ) {
    switch ( msg[i] ) {
    case '\'':
      append( buffer, size, &len, "&apos;" );
      break;
    case '"':
      append( buffer, size, &len, "&quot;" );
      break;
    case '>':
      append( buffer, size, &len, "&gt;" );
      break;
    case '&':
      append( buffer, size, &len, "&amp;" );
      break;
    case '<':
      append( buffer, size, &len, "&lt;" );
      break;
    default:
      if ( len < size ) {
	buffer[len++] = msg[i];
	buffer[len] = 0;
      }
    }
  }
#endif
  append( buffer, size, &len, "]]></chunk>\n" );

  /* atomic write, bracketted by POSIX locks (also forces NFS updates) */
  locked = mytrylock(outfd);
  msize = writen( outfd, buffer, len, 3 );
  if ( locked==1 ) lockit( outfd, F_SETLK, F_UNLCK );

  free( (void*) buffer );
  return msize;
}

#ifdef MUST_USE_SELECT_NOT_POLL
int
poll_via_select( struct pollfd* fds, unsigned nfds, long timeout )
/* purpose: emulate poll() through select() <yikes!>
 * warning: this is an incomplete and very simplified emulation!
 * paramtr: see poll() arguments -- however, this only handles read events!
 * returns: return value from select()
 */
{
  struct timeval tv = { timeout / 1000, (timeout % 1000) * 1000 };
  fd_set rfds, efds;
  unsigned i, status;
  int max = 0;

  FD_ZERO( &rfds );
  FD_ZERO( &efds );
  for ( i = 0; i < nfds; ++i ) {
    if ( fds[i].events & ( POLLIN | POLLRDNORM ) &&
	 fds[i].fd != -1 ) { 
      FD_SET( fds[i].fd, &rfds );
      FD_SET( fds[i].fd, &efds );
      if ( fds[i].fd >= max ) max = fds[i].fd+1;
      fds[i].revents = 0;
    }
  }

  if ( (status = select( max, &rfds, NULL, NULL, &tv )) > 0 ) {
    for ( i = 0; i < nfds; ++i ) {
      if ( fds[i].fd != -1 ) {
	if ( FD_ISSET( fds[i].fd, &rfds ) ) fds[i].revents |= POLLIN;
	if ( FD_ISSET( fds[i].fd, &efds ) ) fds[i].revents |= POLLERR;
      }
    }
  }
  
  return status;
}
#endif /* MUST_USE_SELECT_NOT_POLL */

int
eventLoop( int outfd, StatInfo* fifo, volatile sig_atomic_t* terminate )
/* purpose: copy from input file(s) to output fd while not interrupted.
 * paramtr: outfd (IN): output file descriptor, ready for writing.
 *          fifo (IO): contains input fd, and maintains statistics.
 *          terminate (IN): volatile flag, set in signal handlers.
 * returns: -1 in case of error, 0 for o.k.
 *          -3 for a severe interruption of poll()
 */
{
  size_t count, bufsize = getpagesize();
  int timeout = 30000;
  int result = 0;
  int saverr, status = 0;
  int mask = POLLIN | POLLERR | POLLHUP | POLLNVAL;
  char* rbuffer;
  struct pollfd pfds;
  struct sigaction old_pipe, new_pipe;

  /* sanity checks first */
  if ( outfd == -1 || fifo->source != IS_FIFO ) return 0;

  /* prepare poll fds */
  pfds.fd = fifo->file.descriptor;
  pfds.events = POLLIN;

  /* become aware of SIGPIPE for write failures */
  memset( &new_pipe, 0, sizeof(new_pipe) );
  memset( &old_pipe, 0, sizeof(old_pipe) );
  new_pipe.sa_handler = sig_pipe;
  sigemptyset( &new_pipe.sa_mask );
#ifdef SA_INTERRUPT
  new_pipe.sa_flags |= SA_INTERRUPT; /* SunOS, obsoleted by POSIX */
#endif
  seen_sigpipe = 0; /* ATLAS 20050331: clear previous failures */
  if ( sigaction( SIGPIPE, &new_pipe, &old_pipe ) < 0 )
    return -1;

  /* allocate read buffer */
  if ( (rbuffer = (char*) malloc( bufsize )) == NULL )
    return -1;

#ifdef DEBUG_EVENTLOOP
  fputs( "# starting event loop\n", stderr );
#endif /* DEBUG_EVENTLOOP */

  /* poll (may have been interrupted by SIGCHLD) */
  for ( count=0; 1; count++ ) {
    /* race condition possible, thus we MUST time out */
    /* However, we MUST transfer everything that is waiting */
    if ( *terminate || seen_sigpipe ) {
      timeout = 0;
    } else if ( count < 5 ) {
      timeout = 200; 
    } else if ( count < 15 ) {
      timeout = 1000;
    } else {
      timeout = 30000;
    }
    pfds.revents = 0;

#ifdef DEBUG_EVENTLOOP
    fprintf( stderr, "# tm=%d, s_sp=%d, calling poll([%d:%x:%x],%d,%d)\n", 
	     *terminate, seen_sigpipe, 
	     pfds.fd, pfds.events, pfds.revents, 1, timeout );
#endif /* DEBUG_EVENTLOOP */

    errno = 0;
#ifdef MUST_USE_SELECT_NOT_POLL
    status = poll_via_select( &pfds, 1, timeout );
#else
    status = poll( &pfds, 1, timeout );
#endif /* MUST_USE_SELECT_NOT_POLL */
    saverr = errno;

#ifdef DEBUG_EVENTLOOP
    fprintf( stderr, "# poll() returned %d [errno=%d: %s] [%d:%x:%x]\n", 
	     status, saverr, strerror(saverr),
	     pfds.fd, pfds.events, pfds.revents );
#endif /* DEBUG_EVENTLOOP */

    errno = saverr;
    if ( status == -1 ) {
      /* poll ERR */
      if ( errno != EINTR ) {
	/* not an interruption */
	result = -3;
	break;
      }
    } else if ( status == 0 ) {
      /* timeout -- only exit, if we were wrapping up anyway! */
      if ( timeout == 0 ) break;
    } else if ( status > 0 ) {
      /* poll OK */
      if ( (pfds.revents & mask) > 0 ) {
	ssize_t rsize = read( pfds.fd, rbuffer, bufsize-1 );
	if ( rsize == -1 ) {
	  /* ERR */
	  if ( errno != EINTR ) {
	    result = -1;
	    break;
	  }
	} else if ( rsize == 0 ) {
	  /* EOF */
	  result = 0;
	  break;
	} else {
	  /* data */
	  ssize_t wsize;
	  rbuffer[rsize] = '\0';	  
	  if ( (wsize = send_message( outfd, rbuffer, rsize, 1 )) == -1 ) {
	    /* we'll be unable to send anything further */
	    result = -1;
	    break;
	  } else {
	    /* update statistics */
	    fifo->client.fifo.count++;
	    fifo->client.fifo.rsize += rsize;
	    fifo->client.fifo.wsize += wsize;
	  }
	}
      } /* if pfds mask */
    } /* if status > 0 */
  } /* forever */

  sigaction( SIGPIPE, &old_pipe, NULL );
  free( (void*) rbuffer );
  return result;
}
