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
#include "zio.h"

#include <errno.h>
#include <string.h>
#include <stdio.h>

#include <sys/wait.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdlib.h>

static pid_t* childpid = NULL;

int 
zopen( const char* pathname, int flags, mode_t mode )
/* purpose: open a file, but put gzip into the io-path
 * paramtr: pathname (IN): file to read or create
 *          flags (IN): if O_RDONLY, use gunzip on file
 *                      if O_WRONLY, use gzip on file
 *          mode (IN): file mode, see open(2)
 * returns: -1 in case of error, or an open file descriptor
 */
{
  int    pfd[2];
  pid_t  pid;
  long   maxfd = sysconf( _SC_OPEN_MAX );
  
  if ( maxfd == -1 ) maxfd = _POSIX_OPEN_MAX;

  if ( (flags & 3) != O_RDONLY && (flags & 3) != O_WRONLY ) {
    errno = EINVAL;
    return -1;
  }

  if ( childpid == NULL ) {
    if ( (childpid = calloc(maxfd,sizeof(pid_t))) == NULL ) return -1;
  }

  if ( pipe(pfd) < 0 ) return -1;
  if ( (pid=fork()) < 0 ) return -1;
  else if ( pid == 0 ) {
    /* child code */
    char* argv[3];
    int fd = open( pathname, flags, mode );

    argv[0] = strdup( GZIP_PATH );
    argv[2] = NULL;
    if ( fd == -1 ) _exit(126);

    if ( (flags & 3) == O_RDONLY ) {
      close(pfd[0]);
      if ( pfd[1] != STDOUT_FILENO ) {
	dup2( pfd[1], STDOUT_FILENO );
	close(pfd[1]);
      }

      if ( fd != STDIN_FILENO ) {
	dup2( fd, STDIN_FILENO );
	close(fd);
      }

      argv[1] = "-cd";
    } else {
      close(pfd[1]);
      if ( pfd[0] != STDIN_FILENO ) {
	dup2( pfd[0], STDIN_FILENO );
	close(pfd[0]);
      }

      if ( fd != STDOUT_FILENO ) {
	dup2( fd, STDOUT_FILENO );
	close(fd);
      }

      argv[1] = "-cf";
    }

    /* close descriptors in childpid for gzip */
    for ( fd=0; fd<maxfd; ++fd ) 
      if ( childpid[fd] > 0 ) close(childpid[fd]);

    execv( GZIP_PATH, argv );
    _exit(127);
  } else {
    /* parent code */
    int keep = -1;
    if ( (flags & 3) == O_RDONLY ) {
      close(pfd[1]);
      keep = pfd[0];
    } else {
      close(pfd[0]);
      keep = pfd[1];
    }
    
    childpid[keep] = pid;
    return keep;
  }
}

int
zclose( int fd )
/* purpose: close a file that has a gzip in its io path
 * returns: process status from gzip
 */
{
  int status;
  pid_t pid;

  if ( childpid == NULL ) {
    errno = EBADF;
    return -1;
  }

  if ( (pid = childpid[fd]) == 0 ) {
    errno = EBADF;
    return -1;
  }

  childpid[fd] = 0;
  if ( close(fd) == -1 ) return -1;

  while ( waitpid( pid, &status, 0 ) < 0 ) {
    if ( errno != EINTR && errno != EAGAIN ) 
      return -1;
  }

  return status;
}
