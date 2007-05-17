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
int
full_symlink( const char* original, const char* destination, int force );
/* purpose: Recreate the "ln -s[f]" without starting externals.
 * paramtr: original (IN): location of the original file
 *          destination (IN): location of link file to create
 *          force (IN): bit#0: 0: fail if link fails EEXIST
 *                             1: remove destination (-f)
 * returns:  0 for success,
 *          -1 for symlink failure
 *          -2 (force) for failure to remove destination before linking
 *          -3 for unaccessibility of destination
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
char*
default_grid_proxy_info( void );
/* purpose: Determines the default path to default g-p-i. Minimal checks!
 * returns: absolute path to g-p-i, or NULL if somehow inaccessible.
 */

extern
long
check_grid_proxy_info( char* gpi, char* envp[] );
/* purpose: Obtains the time remaining on the current user certificate proxy.
 * paramtr: gpi (IN): absolute path to grid-proxy-info, no more checks
 *          envp (IN): environment pointer from main()
 * returns: the time remaining on the certificate, 0 for expired, -1 error
 * seealso: default_grid_proxy_init() determines location and executability
 */

extern
int
max_files( int request );
/* purpose: obtain the maximum filehandle possible in the current setting.
 * paramtr: request (IN): request number of handles
 * returns: maximum filehandle after adjustment attempts.
 */

extern
int
max_procs( int request );
/* purpose: obtain the maximum number of user processes possible.
 * paramtr: request (IN): request number of handles
 * returns: maximum filehandle after adjustment attempts.
 */


extern
int 
mkdirs( char* directory );
/* purpose: Recreates the "mkdir -p" functionality.
 *
 * paramtr: dir (IN): the pathname to the directory that needs to be created.
 * 
 * returns:  0 on success
 *          -3 for unaccesability of directory.
 *
 */


extern
char*
get_parent(const char* url);
/*
 * purpose: returns the pathname string to the URL's parent.
 * paramtr: url(IN): the URL whose parent needs to be returned.
 * return : the pathname string to the parent or NULL if invalid
 */



#endif /* _UTIL_H */
