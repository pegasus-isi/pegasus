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
#include <ctype.h>
#include <sys/types.h>
#include <errno.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

#include "capabilities.h"
#include "mypopen.h"

static const char* RCS_ID =
"$Id$";

/*
 * state transition machinery to recognize options from guc -help output
 *
 * state|| NUL |HT,SP| LF  |  -  |alnum|other| remark
 * -----++-----+-----+-----+-----+-----+-----+-------
 *    0 || 3,- | 0,- | 0,- | 1,S | 2,- | 2,- | wait after start
 *    1 || 3,R | 2,R | 0,R | 1,A | 1,A | 2,R | collect word
 *    2 || 3,- | 2,- | 0,- | 2,- | 2,- | 2,- | skip remainder
 *
 * action 1: set start and end pointer
 * action 2: advance end pointer
 * action 3: recognize word between start and end pointer
 */
static const int action_map[3][6] = { { 0, 0, 0, 1, 0, 0 },
				      { 3, 3, 3, 2, 2, 3 },
				      { 0, 0, 0, 0, 0, 0 } };
static const int states_map[3][6] = { { 3, 0, 0, 1, 2, 2 },
				      { 3, 2, 0, 1, 1, 2 },
				      { 3, 2, 0, 2, 2, 2 } };

static 
int
xlate( char ch ) 
/* purpose: translate a character into the state table column.
 * paramtr: ch (IN): character to translate
 * returns: state table column for the character
 */
{
  if ( isalnum(ch) )
    return 4;
  else 
    switch ( ch ) {
    case '\0': return 0;
    case '\t': return 1;
    case ' ': return 1;
    case '\n': return 2;
    case '-': return 3;
    default: return 5;
  }
}

static
unsigned long
recognize( const char* t )
/* purpose: recognizes an option phrase
 * paramtr: t (IN): option word, NUL terminated, to recognize
 * returns: the capability associated with the option. 
 */
{
  unsigned long cap = 0ul;
#if 0
  fprintf( stderr, "# %s\n", t );
#endif

  switch ( *t ) {
  case 'b':
    if ( strcmp( t, "bs" ) == 0 ) cap |= GUC_BLOCKSIZE;
    break;

  case 'c':
    if ( strcmp( t, "c" ) == 0 ) cap |= GUC_CONTINUE;
    else if ( strcmp( t, "cd" ) == 0 ) cap |= GUC_CREATEDIR;
    break;

  case 'd':
    if ( strcmp( t, "dbg" ) == 0 ) cap |= GUC_DEBUG;
    break;

  case 'f':
    if ( strcmp( t, "f" ) == 0 ) cap |= GUC_FROMFILE;
    else if ( strcmp( t, "fast" ) == 0 ) cap |= GUC_FAST;
    break;

  case 'p':
    if ( strcmp( t, "p" ) == 0 ) cap |= GUC_PARALLEL;
    break;

  case 'r':
    if ( strcmp( t, "r" ) == 0 ) cap |= GUC_RECURSIVE;
    else if ( strcmp( t, "rst-interval" ) == 0 ) cap |= GUC_REST_IV;
    else if ( strcmp( t, "rst-timeout" ) == 0 ) cap |= GUC_REST_TO;
    else if ( strcmp( t, "rst" ) == 0 ) cap |= GUC_RESTART;
    break;

  case 's':
    if ( strcmp( t, "stripe" ) == 0 ) cap |= GUC_STRIPE;
    else if ( strcmp( t, "sbs" ) == 0 || 
	      strcmp( t, "striped-block-size" ) == 0 ) cap |= GUC_STRIPE_BS;
    break;

  case 't':
    if ( strcmp( t, "tcp-bs" ) == 0 ) cap |= GUC_TCP_BS;
    break;

  case 'v':
    if ( strcmp( t, "vb" ) == 0 ) cap |= GUC_PERFDATA;
    break;
  }

  return cap;
}

unsigned long
guc_capabilities( char* app, char* envp[] )
/* purpose: Obtains the capabilties of a given g-u-c
 * paramtr: app (IN): fully-qualified path name pointing to a guc
 *          envp (IN): environment pointer to pass to pipe_out_cmd
 * returns: capabilities of guc. Return value of 0 is suspicious of
 *          problems with the guc.
 */
{
  unsigned long result = 0ul;
  size_t bufsize = 49152; /* FIXME: Cross your fingers... */
  char* buffer = malloc( bufsize );
  char* args[3];
  int exitcode = 0;
  char* d, *f, *s;
  int state, cooked;

  /* init args */
  args[0] = app;
  args[1] = "-help";
  args[2] = NULL;
  exitcode = pipe_out_cmd( "guc", args, envp, buffer, bufsize );

  /* sanity check */
  if ( exitcode < 0 || (exitcode & 127) != 0 || exitcode > 512 ) {
    free((void*) buffer);
    return result;
  }

  /* state machine */
  d = f = s = buffer;
  for ( state = 0; state != 3; state = states_map[state][cooked] ) {
    cooked = xlate(*s++);
    switch ( action_map[state][cooked] ) {
    case 1:
      d = f = s;
      break;
    case 2:
      f = s;
      break;
    case 3: 
      *f = '\0';
      result |= recognize( d );
      break;
    }
  }

  free((void*) buffer);
  return result;
}

unsigned long
guc_versions( const char* prefix, char* app, char* envp[] )
/* purpose: Obtains the versions of a given g-u-c
 * paramtr: prefix (IN): prefix matching component to look for, 
 *                       or NULL to obtain g-u-c's own version number
 *          app (IN): fully-qualified path name pointing to a guc
 *          envp (IN): environment pointer to pass to pipe_out_cmd
 * returns: the version number of the sub component, as major*1000 + minor,
 *          or 0 to indicate that it was not found. 
 * warning: This will invoke g-u-c -versions only once, and cache output. 
 */
{
  static char buffer[10240];
  unsigned long result = 0ul;
  char* s;

  if ( buffer[0] == 0 ) {
    char* args[3];
    int exitcode = 0;

    /* init args */
    args[0] = app;
    args[1] = "-versions";
    args[2] = NULL;
    exitcode = pipe_out_cmd( "guc", args, envp, buffer, sizeof(buffer) );

    /* sanity check */
    if ( exitcode < 0 || (exitcode & 127) != 0 || exitcode > 512 ) {
      memset( buffer, ':', sizeof(buffer)-1 );
      buffer[sizeof(buffer)-1] = 0;
      return result;
    }
  }

  /* look for version */
  if ( prefix != NULL ) {
    while ( (s = strstr(buffer,prefix)) ) {
      if ( s == buffer || *(s-1) == '\n' ) {
	unsigned int major, minor;
	sscanf( s, "%*[^:]: %u.%u", &major, &minor );
	result = major * 1000 + minor;
	break;
      }
    }
  } else {
    /* just the version number of ourselves -- whatever we are called */
    unsigned int major, minor;
    sscanf( buffer, "%*[^:]: %u.%u", &major, &minor );
    result = major * 1000 + minor;
  }

  return result;
}
