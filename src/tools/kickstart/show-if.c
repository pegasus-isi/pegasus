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
#include <sys/types.h>
#include <ctype.h>
#include <errno.h>
#include <stdio.h>
#include <time.h>
#include <sys/time.h>
#include <sys/resource.h>
#include <sys/wait.h>
#include <signal.h>
#include <unistd.h>
#include <fcntl.h>
#include <string.h>
#include <stdlib.h>
#include "getif.h"

static const char* RCS_ID =
"$Id: show-if.c,v 1.2 2005/10/19 18:37:20 griphyn Exp $";

int
main( int argc, char* argv[] )
{
  int result = 0;
  char buffer[128];

  if ( argc > 1 ) {
    getif_debug = atoi(argv[1]);
  } else {
    getif_debug = -1;
  }

  whoami( buffer, sizeof(buffer) );
  printf( "primary interface: %s\n", buffer );
  return result;
}
