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
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include "util.h"

static const char* RCS_ID =
"$Id$";

void 
show( unsigned megs )
/* purpose: Create the necessary command line information on stdout.
 * paramtr: megs (IN): is the size in MB of true RAM of the host
 * environ: VDS_JAVA_HEAPMAX: maximum size of heap in MB, or 0 for don't set
 *          VDS_JAVA_HEADMIN: minimum size of heap in MB, or 0 for don't set
 */
{
  unsigned min = -1u;
  unsigned max = -1u;
  char* env_max = getenv("VDS_JAVA_HEAPMAX");
  char* env_min = getenv("VDS_JAVA_HEAPMIN");

  if ( env_min != NULL ) min = strtoul( env_min, 0, 0 );
  if ( env_max != NULL ) max = strtoul( env_max, 0, 0 );

  if ( min == -1u ) min = MAX( 64, megs >> 3 );
  if ( max == -1u ) max = MIN( megs, max );

  if ( min > 0 ) printf( " -Xms%dm", min );
  if ( max > 0 ) printf( " -Xmx%dm", max );
  putchar( '\n' );
}

void
help( int argc, char* argv[], const char* mainid )
/* purpose: Check for the presence of -h, -? or --help, and help.
 * paramtr: argc (IN): see main()
 *          argv (IN): see main()
 *          mainid (IN): main's RCS Id string
 * returns: only in the absence of options
 */
{
  if ( argc > 1 ) {
    if ( argv[1][0] == '-' ) {
      if ( ((argv[1][1] == 'h' || argv[1][1] == '?') && argv[1][2] == 0 ) ||
	   ( argv[1][1] == '-' && strcmp(argv[1]+2,"help") == 0 ) ) {
	puts( "Provide Java 1.4 with appropriate memory settings.\n" );
	puts( mainid );
	puts( RCS_ID );
      } else {
	fprintf( stderr, "Illegal option \"%s\"\n", argv[1] );
      }
    } else {
      fprintf( stderr, "Illegal argument \"%s\"\n", argv[1] );
    }
    exit(1);
  }
}
