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
#include "debug.h"

#include <errno.h>
#include <string.h>
#include <unistd.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <stdarg.h>

static const char* RCS_ID =
  "$Id$";

int
debugmsg( char* fmt, ... )
/* purpose: create a log line on stderr.
 * paramtr: fmt (IN): printf-style format string
 *          ... (IN): other arguments according to format
 * returns: number of bytes written to STDERR via write()
 */
{
  int result;
  va_list ap;
  char buffer[4096];
  int saverr = errno;

  va_start( ap, fmt );
  vsnprintf( buffer, sizeof(buffer), fmt, ap );
  va_end( ap );

  result = write( STDERR_FILENO, buffer, strlen(buffer) );
  errno = saverr;
  return result;
}

int 
hexdump( void* area, size_t size )
/* purpose: dump a memory area in old-DOS style hex chars and printable ASCII
 * paramtr: area (IN): pointer to area start
 *          size (IN): extent of area to print
 * returns: number of byte written 
 */ 
{
  static const char digit[16] = "0123456789ABCDEF";
  char a[82];
  unsigned char b[18];
  size_t i, j;
  unsigned char c;
  ssize_t result = 0;
  unsigned char* buffer = (unsigned char*) area; 

  for ( i=0; i<size; i+=16 ) {
    memset( a, 0, sizeof(a) );
    memset( b, 0, sizeof(b) );
    sprintf( a, "%04X: ", i );
    for ( j=0; j<16 && j+i<size; ++j ) {
      c = (unsigned char) buffer[i+j];

      a[6+j*3] = digit[ c >> 4 ];
      a[7+j*3] = digit[ c & 15 ];
      a[8+j*3] = ( j == 7 ? '-' : ' ' );
      b[j] = (char) (c < 32 || c >= 127 ? '.' : c);
    }
    for ( ; j<16; ++j ) {
      a[6+j*3] = a[7+j*3] = a[8+j*3] = b[j] = ' ';
    }
    strncat( a, (char*) b, sizeof(a) );
    strncat( a, "\n", sizeof(a) );
    result += write( STDERR_FILENO, a, strlen(a) );
  }

  return result;
}
