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
#include <stdio.h>
#include "util.h"

static const char* RCS_ID =
"$Id$";

int
main( int argc, char* argv[] )
{
  char line[1024];
  FILE* mem;
  unsigned megs;
  
  help( argc, argv, RCS_ID );

  if ( (mem = popen( "/usr/sbin/prtconf", "r" )) == NULL ) {
    perror( "open /usr/sbin/prtconf" );
    return 1;
  }

  while ( fgets( line, sizeof(line), mem ) != NULL ) {
    if ( sscanf( line, "Memory size: %u Megabytes", &megs ) > 0 ) break;
  }
  pclose(mem);

  /* Grrrrr!!!!! */
  if ( megs > 4095 ) megs = 4095;
  show( megs );
  return 0;
}
