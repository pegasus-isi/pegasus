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
#include <fcntl.h>
#include <utime.h>
#include <sys/poll.h>
#include <errno.h>
#include <stdio.h>
#include <time.h>
#include <unistd.h>
#ifdef sun
#include <memory.h>
#endif
#include <string.h>

#include "rwio.h"

static const char* RCS_ID =
"$Id: rwio.c,v 1.5 2007/01/23 23:55:34 voeckler Exp $";

ssize_t
writen( int fd, const char* buffer, ssize_t n, unsigned restart )
/* purpose: write all n bytes in buffer, if possible at all
 * paramtr: fd (IN): filedescriptor open for writing
 *          buffer (IN): bytes to write (must be at least n byte long)
 *          n (IN): number of bytes to write 
 *          restart (IN): if true, try to restart write at max that often
 * returns: n, if everything was written, or
 *          [0..n-1], if some bytes were written, but then failed,
 *          < 0, if some error occurred.
 */
{
  int start = 0;
  while ( start < n ) {
    int size = write( fd, buffer+start, n-start );
    if ( size < 0 ) {
      if ( restart && errno == EINTR ) { restart--; continue; }
      return size;
    } else {
      start += size;
    }
  }
  return n;
}

int
lockit( int fd, int cmd, int type )
/* purpose: fill in POSIX lock structure and attempt lock or unlock
 * paramtr: fd (IN): which file descriptor to lock
 *          cmd (IN): F_SETLK, F_GETLK, F_SETLKW
 *          type (IN): F_WRLCK, F_RDLCK, F_UNLCK
 * warning: always locks full file ( offset=0, whence=SEEK_SET, len=0 )
 * returns: result from fcntl call
 */
{
  struct flock lock;

  /* empty all -- even non-POSIX data fields */
  memset( &lock, 0, sizeof(lock) );
  lock.l_type   = type;

  /* full file */
  lock.l_whence = SEEK_SET;
  lock.l_start  = 0;
  lock.l_len    = 0;

  return ( fcntl( fd, cmd, &lock ) );
}

int
mytrylock( int fd )
/* purpose: Try to lock the file
 * paramtr: fd (IN): open file descriptor
 * returns: -1: fatal error while locking the file, file not locked
 *           0: all backoff attempts failed, file is not locked
 *           1: file is locked
 */
{
  int backoff = 50; /* milliseconds, increasing */
  int retries = 10; /* 2.2 seconds total */

  while ( lockit( fd, F_SETLK, F_WRLCK ) == -1 ) {
    if ( errno != EACCES && errno != EAGAIN ) return -1;
    if ( --retries == 0 ) return 0;
    backoff += 50;
    poll( NULL, 0, backoff );
  }

  return 1;
}

int
nfs_sync( int fd, unsigned idle )
/* purpose: tries to force NFS to update the given file descriptor
 * paramtr: fd (IN): descriptor of an open file
 *          idle (IN): how many milliseconds between lock and unlock
 * seelaso: DEFAULT_SYNC_IDLE as suggested argument for idle
 * returns: 0 is ok, -1 for failure
 */
{
  /* lock file */
  if ( lockit( fd, F_SETLK, F_WRLCK ) == -1 )
    return -1;

  /* wait $idle ms */
  if ( idle > 0 ) poll( NULL, 0, idle );

  /* unlock file */
  return lockit( fd, F_SETLK, F_UNLCK );
}


/*
 * old code
 */
#if 0
int
nfs_sync( int fd, unsigned idle )
/* purpose: tries to force NFS to update the given file descriptor
 * paramtr: fd (IN): descriptor of an open file
 *          idle (IN): how many milliseconds between lock and unlock
 * returns: 0 is ok, -1 for failure
 */
{
#ifndef LINUX
  /* lock file */
  if ( lockit( fd, F_SETLK, F_WRLCK ) == -1 )
    return -1;

  /* wait 100 ms */
  if ( idle > 0 ) poll( NULL, 0, idle );

  /* unlock file */
  return lockit( fd, F_SETLK, F_UNLCK );
#else /* is LINUX */
  /* how I loathe eternally broken NFS locking on Linux */
  char src[32];
  char dst[4096];
  struct utimbuf utb;

  /* which FD to translate */
  snprintf( src, sizeof(src), "/proc/%d/fd/%d", getpid(), fd );

  /* read symlink information */
  if ( readlink( src, dst, sizeof(dst) ) == -1 )
    return -1;

  /* attempt an utime */
  utb.actime = utb.modtime = time(NULL);
  return utime( dst, &utb ); 
#endif /* LINUX */
}

#endif /* old code #if 0 */
