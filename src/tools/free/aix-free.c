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
"$Id: aix-free.c,v 1.2 2005/03/02 00:22:24 griphyn Exp $";

int
main( int argc, char* argv[] )
{
  char line[1024];
  FILE* mem;
  unsigned long kilos;
  unsigned megs;
  
  help( argc, argv, RCS_ID );

  /* lsattr -E -l sys0 -a realmem */
  /* realmem 3137536 Amount of usable physical memory in Kbytes False */
  if ( (mem = popen( "/usr/sbin/lsattr -E -l sys0 -a realmem", "r" )) == NULL ) {
    perror( "open /usr/sbin/lsattr" );
    return 1;
  }

  while ( fgets( line, sizeof(line), mem ) != NULL ) {
    if ( sscanf( line, "realmem %lu Amount", &kilos ) > 0 ) break;
  }
  pclose(mem);

  megs = kilos >> 10;
  show( megs );
  return 0;
}
