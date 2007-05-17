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
#include <string.h>
#include "util.h"

static const char* RCS_ID =
"$Id: lnx-free.c,v 1.9 2005/03/02 17:24:21 griphyn Exp $";

int
main( int argc, char* argv[] )
{
  char line[1024];
  FILE* mem;
  unsigned megs = 0u;
  
  help( argc, argv, RCS_ID );

#ifndef DEBUG
  if ( (mem = popen( "/usr/bin/free -m", "r" )) == NULL ) {
    perror( "open /usr/bin/free" );
    return 1;
  }
#else
  if ( (mem = fopen( "free.txt", "r" )) == NULL ) {
    perror( "fopen free.txt" );
    return 1;
  }
#endif

  while ( fgets( line, sizeof(line), mem ) != NULL ) {
    if ( strncmp( line, "Mem", 3 ) == 0 ) {
      if ( sscanf( line, "%*s %u", &megs ) > 0 ) break;
    }
  }

#ifndef DEBUG
  pclose(mem);
#else
  fclose(mem);
#endif

  /* Grrrrr!!!!! */
  if ( megs > 1900 ) megs = 1024;
  show( megs );
  return 0;
}
