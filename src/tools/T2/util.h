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
#ifndef _UTIL_H
#define _UTIL_H

extern
void
double2timeval( struct timeval* tv, double interval );
/* purpose: Converts a double timestamp into a timeval 
 * paramtr: tv (OUT): destination to put the timeval into
 *          interval (IN): time in seconds to convert
 */

#define timeval2double(tv) (tv.tv_sec + tv.tv_usec/1E6) 
/* purpose: Converts a timeval into a fractional seconds double rep
 * paramtr: tv (IN): timeval to convert
 * returns: double representing the seconds with fraction.
 */

extern
double
now( void );
/* purpose: obtains an UTC timestamp with microsecond resolution.
 * returns: the timestamp, or -1.0 if it was completely impossible.
 */

extern
char*
check_link( void );
/* purpose: Obtains the path to system's symlink tool ln
 * returns: absolute path to ln, or NULL if not found nor accessible
 */

extern
char*
default_globus_url_copy( void );
/* purpose: Determines the default path to default g-u-c. No checks!
 * returns: absolute path to g-u-c, or NULL if environment mismatch
 */

extern
char*
alter_globus_url_copy( const char* argv0 );
/* purpose: Determines the alternative g-u-c. Simple check only!
 * paramtr: argv0 (IN): main's argv[0]
 * returns: absolute path to g-u-c, or NULL if environment mismatch
 */

extern
long
check_globus_url_copy( char* location, char* envp[] );
/* purpose: Obtains the version of a given globus-url-copy
 * parmatr: location (IN): location of an alternative g-u-c, or
 *                         NULL to use $GLOBUS_LOCATION/bin/globus-url-copy
 * paramtr: env (IN): environment pointer from main()
 * returns: The version number as major * 1000 + minor, 
 *          or -1 if troubles running the g-u-c
 */

extern
long
check_grid_proxy_info( char* envp[] );
/* purpose: Obtains the time remaining on the current user certificate proxy.
 * paramtr: env (IN): environment pointer from main()
 * returns: the time remaining on the certificate, 0 for expired, -1 error
 */

#endif /* _UTIL_H */
