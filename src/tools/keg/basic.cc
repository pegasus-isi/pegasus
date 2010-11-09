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
#include "basic.hh"

static const char* RCS_ID =
  "$Id$";

#include <stdio.h>
#include <string.h>

void
smart_units( char* buffer, size_t capacity,
	     unsigned long long int value )
{
  if ( value < 8192ull ) {
    unsigned long t = (unsigned long) value; 
    snprintf( buffer, capacity, "%5luB", t );
  } else if ( value < 8388608ull ) {
    snprintf( buffer, capacity, "%5lukB", kils(value) ); 
  } else if ( value < 8589934592ull ) {
    snprintf( buffer, capacity, "%5luMB", megs(value) ); 
  } else if ( value < 8796093022208ull ) {
    snprintf( buffer, capacity, "%5luGB", gigs(value) ); 
  } else { 
    snprintf( buffer, capacity, "%5luTB", ters(value) ); 
  }
}
