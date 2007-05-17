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
#ifndef _INVOKE_H
#define _INVOKE_H

#include <sys/types.h>

extern
int
append_arg( char* data, char*** arg, size_t* index, size_t* capacity );
/* purpose: adds a string to a list of arguments
 *          This is a low-level function, use add_arg instead.
 * paramtr: data (IN): string to append
 *          arg (OUT): list of arguments as vector
 *          index (IO): index where a new data should be inserted into
 *          capacity (IO): capacity (extend) of vector
 * returns: 0 means ok, -1 means error, see errno
 * warning: Always creates a strdup of data
 */

extern
int
expand_arg( const char* fn, char*** arg, size_t* index, size_t* capacity, 
	    int level );
/* purpose: adds the contents of a file, line by line, to an argument vector
 *          This is a low-level function, use add_arg instead.
 * paramtr: fn (IN): name of file with contents to append
 *          arg (OUT): list of arguments as vector
 *          index (IO): index where a new data should be inserted into
 *          capacity (IO): capacity (extend) of vector
 *          level (IN): level of recursion
 * returns: 0 means ok, -1 means error, see errno
 */

extern
int
add_arg( char* data, char*** arg, size_t* index, size_t* capacity, 
	 int level );
/* purpose: sorts a given full argument string, whether to add or extend
 *          This is the high-level interface to previous functions.
 * paramtr: data (IN): string to append
 *          arg (OUT): list of arguments as vector
 *          index (IO): index where a new data should be inserted into
 *          capacity (IO): capacity (extend) of vector
 *          level (IN): level of recursion, use 1
 * returns: 0 means ok, -1 means error, see errno
 */

#endif /* _INVOKE_H */
