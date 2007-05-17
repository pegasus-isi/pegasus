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
#include <signal.h>
#include <unistd.h>
#include <stdlib.h>

static const char* RCS_ID =
"$Id: fail.c,v 1.3 2004/07/22 14:43:26 griphyn Exp $";

int
main( int argc, char* argv[] )
{
  /* no args, die right now */
  if ( argc < 2 ) return 1;

  /* send myself the given signal */
  kill( getpid(), atoi(argv[1]) );

  /* for all ignored signals */
  return 127;
}
