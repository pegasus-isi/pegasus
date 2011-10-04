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
#ifndef _RWIO_H
#define _RWIO_H

#include <sys/types.h>

#ifndef DEFAULT_SYNC_IDLE
#define DEFAULT_SYNC_IDLE 100
#endif

extern
ssize_t
writen( int fd, const char* buffer, ssize_t n, unsigned restart );
/* purpose: write all n bytes in buffer, if possible at all
 * paramtr: fd (IN): filedescriptor open for writing
 *          buffer (IN): bytes to write (must be at least n byte long)
 *          n (IN): number of bytes to write 
 *          restart (IN): if true, try to restart write at max that often
 * returns: n, if everything was written, or
 *          [0..n-1], if some bytes were written, but then failed,
 *          < 0, if some error occurred.
 */

extern
int
lockit( int fd, int cmd, int type );
/* purpose: fill in POSIX lock structure and attempt lock or unlock
 * paramtr: fd (IN): which file descriptor to lock
 *          cmd (IN): F_SETLK, F_GETLK, F_SETLKW
 *          type (IN): F_WRLCK, F_RDLCK, F_UNLCK
 * warning: always locks full file ( offset=0, whence=SEEK_SET, len=0 )
 * returns: result from fcntl call
 */

extern
int
mytrylock( int fd );
/* purpose: Try to lock the file
 * paramtr: fd (IN): open file descriptor
 * returns: -1: fatal error while locking the file, file not locked
 *           0: all backoff attempts failed, file is not locked
 *           1: file is locked
 */

extern
int
nfs_sync( int fd, unsigned idle );
/* purpose: tries to force NFS to update the given file descriptor
 * paramtr: fd (IN): descriptor of an open file
 *          idle (IN): how many milliseconds between lock and unlock
 * seelaso: DEFAULT_SYNC_IDLE as suggested argument for idle
 * returns: 0 is ok, -1 for failure
 */

#endif /* _RWIO_H */
