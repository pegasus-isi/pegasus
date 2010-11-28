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
#include <errno.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include "invoke.h"

static const char* RCS_ID =
"$Id$";

extern int debug;

int
append_arg( char* data, char*** arg, size_t* index, size_t* capacity )
/* purpose: adds a string to a list of arguments
 *          This is a low-level function, use add_arg instead.
 * paramtr: data (IN): string to append
 *          arg (OUT): list of arguments as vector
 *          index (IO): index where a new data should be inserted into
 *          capacity (IO): capacity (extend) of vector
 * returns: 0 means ok, -1 means error, see errno
 * warning: Always creates a strdup of data
 */ 
{
  if ( debug )
    fprintf( stderr, "# data=%p arg=%p index=%d cap=%d: \"%s\"\n", 
	     data, *arg, *index, *capacity, data );

  if ( *index >= *capacity ) {
    *capacity <<= 1;
    if ( debug > 1 ) 
      fputs( "# realloc\n", stderr );
    *arg = realloc( *arg, *capacity * sizeof(char*) );
    if ( *arg == NULL ) return -1;
    /* re-calloc: init new space with NULL */
    memset( *arg + *index, 0, sizeof(char*) * (*capacity - *index) );
  }
    
  (*arg)[(*index)++] = data ? strdup(data) : NULL;
  return 0;
}

static
char*
merge( char* s1, char* s2 )
/* purpose: merge two strings and return the result
 * paramtr: s1 (IN): first string, may be NULL
 *          s2 (IN): second string, must not be NULL
 * returns: merge of strings into newly allocated area.
 *          NULL, if the allocation failed. 
 */
{
  if ( s1 == NULL ) {
    return strdup(s2);
  } else {
    size_t len = strlen(s1) + strlen(s2) + 2;
    char* temp = (char*) malloc(len);
    if ( temp == NULL ) return NULL;

    strncpy( temp, s1, len );
    strncat( temp, " ", len );
    strncat( temp, s2, len );
    return temp;
  }
}

int
expand_arg( const char* fn, char*** arg, size_t* index, size_t* capacity, 
	    int level )
/* purpose: adds the contents of a file, line by line, to an argument vector
 *          This is a low-level function, use add_arg instead.
 * paramtr: fn (IN): name of file with contents to append
 *          arg (OUT): list of arguments as vector
 *          index (IO): index where a new data should be inserted into
 *          capacity (IO): capacity (extend) of vector
 *          level (IN): level of recursion
 * returns: 0 means ok, -1 means error, see errno
 */
{
  FILE*  f;
  char   line[4096];
  size_t len;
  char*  cmd, *save = NULL;
  unsigned long lineno = 0ul;

  if ( level >= 32 ) {
    fprintf( stderr, "ERROR: Nesting too deep (%d levels), "
	     "circuit breaker triggered!\n", level );
    errno = EMLINK;
    return -1;
  }

  if ( (f = fopen( fn, "r" )) == NULL ) {
    /* error while opening file for reading */
    return -1;
  }

  while ( fgets( line, sizeof(line), f ) ) {
    ++lineno;

    /* check for skippable line */
    if ( line[0] == 0 || line[0] == '\r' || line[0] == '\n' ) continue;

    /* check for unterminated line (larger than buffer) */
    len = strlen(line);
    if ( line[len-1] != '\r' && line[len-1] != '\n' ) {
      /* read buffer was too small, save and append */
      char* temp = merge( save, line );
      if ( temp == NULL ) {
	/* error while merging strings */
	int saverr = errno;
	fclose(f);
	if ( save != NULL ) free((void*) save);
	errno = saverr;
	return -1;
      }

      if ( save != NULL ) free((void*) save);
      save = temp;
      lineno--;
      continue;
    } else {
      /* remove terminating character(s) */
      while ( len > 0 && (line[len-1] == '\r' || line[len-1] == '\n') ) {
	line[len-1] = 0;
	len--;
      } 
    }

    /* final assembly of argument */
    if ( save != NULL ) {
      /* assemble merged line */
      cmd = merge( save, line );
      free((void*) save);
      save = NULL;

      if ( cmd == NULL ) {
	/* error while merging strings */
	int saverr = errno;
	fclose(f);
	errno = saverr;
	return -1;
      }
    } else {
      /* no overlong lines */
      cmd = line;
    }

    if ( debug ) {
      printf( "# %s:%lu: %s\n", fn, lineno, cmd );
    }

    if ( (len=strlen(cmd)) > 0 ) {
      /* recursion here */
      if ( add_arg( cmd, arg, index, capacity, level+1 ) == -1 ) {
	int saverr = errno;
	fclose(f);
	if ( cmd != line ) free((void*) cmd);
	errno = saverr;
	return -1;
      }
    }

    /* done with this argument */
    if ( cmd != line ) free((void*) cmd);
  }

  fclose(f);
  return 0;
}

int
add_arg( char* s, char*** arg, size_t* index, size_t* capacity, 
	 int level )
/* purpose: sorts a given full argument string, whether to add or extend
 *          This is the high-level interface to previous functions.
 * paramtr: s (IN): string to append
 *          arg (OUT): list of arguments as vector
 *          index (IO): index where a new data should be inserted into
 *          capacity (IO): capacity (extend) of vector
 *          level (IN): level of recursion, use 1
 * returns: 0 means ok, -1 means error, see errno
 */
{
  if ( s[0] == '@' && s[1] != 0 ) {
    if ( s[1] == '@' ) {
      return append_arg( s+1, arg, index, capacity );
    } else {
      return expand_arg( s+1, arg, index, capacity, level+1 );
    }
  } else {
    return append_arg( s, arg, index, capacity );
  }
}
