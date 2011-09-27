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
#ifndef _PEGASUS_BASIC_HH
#define _PEGASUS_BASIC_HH

inline unsigned long int kils( unsigned long long int x ) 
{ return (x >> 10); }

inline unsigned long int megs( unsigned long long int x ) 
{ return (x >> 20); }

inline unsigned long int gigs( unsigned long long int x )
{ return (x >> 30); }

inline unsigned long int ters( unsigned long long int x )
{ return (x >> 40); }

#include <sys/types.h>

extern
void
smart_units( char* buffer, size_t capacity,
	     unsigned long long int value ); 
	     

#endif // _PEGASUS_BASIC_HH
