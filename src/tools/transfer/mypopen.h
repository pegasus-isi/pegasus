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
#ifndef _MYPOPEN_H
#define _MYPOPEN_H

#include <sys/types.h>

typedef struct {
  pid_t child;	/* pid of process that runs things */
  int   readfd;	/* fd to read output from process */
} PipeCmd;

extern
PipeCmd*
mypopen( const char* tag, char* argv[], char* envp[] );
/* purpose: fork off a commend and capture its stderr and stdout. 
 * warning: does not use /bin/sh -c internally. 
 * paramtr: name (IN): some short tag to name the app
 *          argv (IN): the true argv[] vector for execve
 *          envp (IN): the true envp[] vector for execve
 * returns: a structure which contains information about the child process.
 *          it will return NULL on failure. 
 */

extern
int
mypclose( PipeCmd* po );
/* purpose: free the data structure and all associated resources.
 * paramtr: po (IO): is a valid pipe open structure.
 * returns: process exit status, or -1 for invalid po structure. 
 */

extern
int
pipe_out_cmd( const char* tag, char* argv[], char* envp[], 
	      char* buffer, size_t blen );
/* purpose: fork off a commend and capture its stderr and stdout
 * paramtr: name (IN): some short tag to name the app
 *          argv (IN): the true argv[] vector for execve
 *          envp (IN): the true envp[] vector for execve
 *          buffer (OUT): area to store output into. Will be cleared
 *          blen (IN): length of the area that is usable to us. 
 * returns: -1 for regular failure, exit code from application otherwise
 */

#endif /* _MYPOPEN_H */
