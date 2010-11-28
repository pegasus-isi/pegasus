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

extern int
exec_cmd( char* cmd, char* envp[], char* buffer, size_t blen );

extern int
exec_cmd2( char* cmd, char* buffer, size_t blen );

extern int
mysystem( const char* tag, char* argv[], char* envp[],
	  int fd_input, int fd_output, int fd_error );

#endif /* _MYPOPEN_H */
