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
#ifdef sun
#include <thread.h>
#include <memory.h>
#endif

#include "debug.h"
#include <string.h>
#include <stdarg.h>
#include <unistd.h>
#include <pthread.h>

static const char* RCS_ID =
"$Id$";

ssize_t
debug( const char* fmt, ... )
/* purpose: write a debug message, prefixed by the current thread id, 
 *          as atomic write operation onto stderr (no locking). 
 * warning: message total is limited to and cut off at 4096 bytes. 
 * paramtr: fmt (IN): printf format to be passed on
 * returns: whatever write returns. 
 */
{
  va_list ap;
  char buffer[4096];
  size_t len;

  memset( buffer, 0, sizeof(buffer) );
  sprintf( buffer, "## %#010x: ", pthread_self() );
  len = strlen(buffer);

  va_start( ap, fmt );
  vsnprintf( buffer+len, sizeof(buffer)-len-2, fmt, ap );
  va_end(ap);

  if ( buffer[strlen(buffer)-1] != '\n' ) strcat( buffer, "\n" );
  return write( STDERR_FILENO, buffer, strlen(buffer) );
}


void
hexdump( FILE* out, const char* prefix, 
	 void* area, size_t len, size_t offset )
/* purpose: hex dumps a given memory area into a given file.
 * paramtr: out (IO): file pointer to dump into
 *          prefix (IN): optional message to prefix lines with, NULL permitted
 *          area (IN): start of the memory area to dump
 *          len (IN): length of the memory area not to exceed
 *          offset (IN): offset into the memory area for partial prints
 */
{
  unsigned char* a = (unsigned char*) area;
  size_t i, j, k;

  for ( i=offset; i<len; i+=16 ) {
    if ( prefix && *prefix ) fputs( prefix, out );
    fprintf( out, "0x%08lX:", (i & 0xFFFFFFF0) );
    for ( j=0; j < (i & 15); ++j ) fputs( "   ", out );
    i &= 0xFFFFFFF0;
    for ( k=j; j<16 && j+i<len; ++j ) 
      fprintf( out, "%c%02X", (j==8?'-':' '), a[j+i] );
    for ( ; j<16; ++j ) fputs( "   ", out );
    fputs( "  ", out );
    for ( j=0; j<k; ++j ) fputc( ' ', out );
    for ( ; j<16 && j+i<len; ++j ) /* FIXME: what about 0x7F and 0xFF */
      fputc( ( (a[j+i]&127)>=32 ? a[j+i] : '.'), out );
    fputc( '\n', out );
  }
}
