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
#include <ctype.h>
#include <errno.h>
#include <math.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>

#include <sys/types.h>
#include <sys/time.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <sys/resource.h>
#include <unistd.h>
#include <fcntl.h>
#include <limits.h>

#include "util.h"
#include "mypopen.h"

static const char* RCS_ID =
"$Id: util.c,v 1.23 2006/06/29 18:33:55 voeckler Exp $";

void
double2timeval( struct timeval* tv, double interval )
/* purpose: Converts a double timestamp into a timeval 
 * paramtr: tv (OUT): destination to put the timeval into
 *          interval (IN): time in seconds to convert
 */
{
  double integral, fraction = modf(interval,&integral);
  tv->tv_sec = (long) integral;
  tv->tv_usec = (long) (1E6*fraction);
}

double
now( void )
/* purpose: obtains an UTC timestamp with microsecond resolution.
 * returns: the timestamp, or -1.0 if it was completely impossible.
 */
{
  int timeout = 0;
  struct timeval t = { -1, 0 };
  while ( gettimeofday( &t, NULL ) == -1 && timeout < 10 ) timeout++;
  return timeval2double(t);
}

int
full_symlink( const char* original, const char* destination, int force )
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
{
  int status; 
  struct stat st;

  /* check if the parent directory exists or create it*/
  char* parent = get_parent(destination);

  if( ((status = lstat( parent, &st)) == 0 ) && S_ISDIR(st.st_mode)){
    /* parent directory exists*/
  }
  else if( (status = mkdirs( parent )) ){
    /* creation of parent directory failed */
    free( (void*)parent);
    return status;
  }

  /* parent directory now exists*/
  errno = 0;
  free ( (void*)parent);

  // if ( (status=lstat(destination,&st)) && errno != ENOENT ) {
  //  /* unable to stat destination, but not due to non-existence */
  //  return -3;
  // }

  if ( status == 0 && (force & 1) == 1 ) {
    /* destination exists, removal requested */
    if ( unlink( destination ) && errno != ENOENT ) 
      return -2;
  }

  errno = 0;
  return symlink( original, destination );
}

int 
mkdirs( char* directory )
/* purpose: Recreates the "mkdir -p" functionality.
 *
 * paramtr: dir (IN): the pathname to the directory that needs to be created.
 * 
 * returns:  0 on success
 *          -3 for unaccesability of directory.
 *
 */
{

  /* set the mode to 755 */
  mode_t mode = S_IRWXU | S_IRGRP| S_IXGRP | S_IROTH | S_IXOTH;
      
  struct stat st;
  char *counter = directory;
  char tmp;
  int status; 
  
  /* duplicate the -p functionality. start from root to the parent dir */
  while( *counter ){
    
    /* skip over any file separators that maybe bunched up together */
    while( *counter && *counter == '/'){ counter ++;}
    
    /* get hold of a path component in between two separators */
    while( *counter && *counter != '/'){ counter ++;};
    
    /* get hold of the directory path that you want to try creating*/
    tmp = *counter;
    *counter = '\0';

    //fprintf( stderr, " parent directory now is %s\n", directory);

    /* check the directory for existence */
    if ( (status = stat( directory , &st)) ){
      if (errno != ENOENT || mkdir( directory, mode )){
	/*lstat failed for reason other than non existence or
	  creation of the directory failed */
	
	/* revert back the file separator */
	*counter = tmp;
	return -3;
      }
    }
    else if(!S_ISDIR(st.st_mode)){
      //  lstat succeeded but is not a directory 
      //  FIXME: Can i set an appropriate error number?
      *counter = tmp;
      return -3;
    }
    
    
    /* directory is created succesfully here or already existed */ 
    /* revert back the file separator or end of string*/
    *counter = tmp;

    /* increment the iterator */
    counter++;

  } /* end of while(*counter) */
      
  return 0;

}


char*
/*
 * purpose: returns the pathname string to the URL's parent.
 * paramtr: url(IN): the URL whose parent needs to be returned.
 * return : the pathname string to the parent or NULL if invalid
 */
get_parent(const char* url){
  int last_sep = 0;
  int i = 0;
  char* parent;
  
  /* take care of trailing slash at the url */
  int end = strlen(url) - 1;
  end = (*(url + end) == '/')? end - 1: end;
  
  /* sanity check */
  if( end < 0 ) return NULL;

  /* record the last location of file separator */
  for(i = 0; i <= end; i++){
    if ( *(url + i) == '/') last_sep = i;
  }

  //fprintf(stderr, " Last separator is %d", last_sep);
  if(last_sep == 0){

    if( *url == '/'){
      /*just return the root directory*/
      parent = (char*)malloc( 2 * sizeof(char));
      *(parent + 0) = *(url + 0);
      *(parent + 1) = '\0';
    }
    else{
      /* return NULL if just a basename is given e.g f.a */
      return NULL;
    }

  }
  else{
    /* return a parent path */
    parent = (char*)malloc( (last_sep + 1)* sizeof(char) );
    
    /* copy to parent */
    for(i = 0; i < last_sep; i++){  *(parent + i) = *(url + i);}

    *(parent + i) = '\0';
  }
  
  return parent;

}


char*
default_globus_url_copy( void )
/* purpose: Determines the default path to default g-u-c. No checks!
 * returns: absolute path to g-u-c, or NULL if environment mismatch
 */
{
  char* globus;
  char* guc;

  /* assemble default location */
  if ( (globus=getenv("GLOBUS_LOCATION")) == NULL ) {
    fputs( "ERROR: You need to set your GLOBUS_LOCATION\n", stderr );
    return NULL;
  }

  guc = (char*) malloc( strlen(globus) + 24 * sizeof(char) );
  strcpy( guc, globus );
  strcat( guc, "/bin/globus-url-copy" );
  return guc;
}

static
char*
jerry_globus_url_copy( const char* argv0 )
/* purpose: Determines the alternative g-u-c. Simple check only!
 * paramtr: argv0 (IN): main's argv[0]
 * returns: absolute path to g-u-c, or NULL if mismatch
 */
{
  size_t size = strlen(argv0) + 16;
  int fd;
  char* s;
  char* guc;

  /* This is not a fixed alternative, no messages */
  if ( argv0 == NULL ) return NULL;

  /* assemble default location */
  guc = (char*) calloc( sizeof(char), size );
  if ( guc == NULL ) return NULL;
  strncpy( guc, argv0, size-1 );

  /* look for path separator */
  if ( (s = strrchr( guc, '/' )) == NULL ) {
    s = guc;
    *s = '\0';
  } else { 
    s++;
  }
  strncat( s, "guc", size - (s-guc) );

  /* open() is faster than stat() for simple accessibility checks */
  if ( (fd = open( guc, O_RDONLY )) >= 0 ) {
    /* exists, and is good */
    close(fd);
    return guc;
  } else {
    /* does not exist, bad path */
    free((void*) guc);
    return NULL;
  }
}

char*
alter_globus_url_copy( const char* argv0 )
/* purpose: Determines the alternative g-u-c. Simple check only!
 * paramtr: argv0 (IN): main's argv[0]
 * returns: absolute path to g-u-c, or NULL if environment mismatch
 */
{
  size_t size;
  int fd;
  char* vds_home = getenv("PEGASUS_HOME");
  char* guc = jerry_globus_url_copy( argv0 );

  /* Jerry's request */
  if ( guc != NULL ) return guc;

  /* This is not a fixed alternative, no messages */
  if ( vds_home == NULL ) return NULL;

  /* assemble default location */
  size = strlen(vds_home) + 48;
  guc = (char*) calloc( sizeof(char), size );
  if ( guc == NULL ) return NULL;
  strncpy( guc, vds_home, size-1 );
  strncat( guc, "/bin/guc", size );

  /* open() is faster than stat() for simple accessibility checks */
  if ( (fd = open( guc, O_RDONLY )) >= 0 ) {
    /* exists, and is good */
    close(fd);
    return guc;
  } else {
    /* does not exist, bad path */
    free((void*) guc);
    return NULL;
  }
}

long
check_globus_url_copy( char* location, char* envp[] )
/* purpose: Obtains the version of a given globus-url-copy
 * parmatr: location (IN): location of an alternative g-u-c, or
 *                         NULL to use $GLOBUS_LOCATION/bin/globus-url-copy
 * paramtr: env (IN): environment pointer from main()
 * returns: The version number, as major*1000 + minor, 
 *          or -1 if troubles running the g-u-c
 */
{
  int   status;
  char* argv[3];
  char  line[1024];
  long  result = -1;
  unsigned major, minor;
  struct stat st;
  char* guc = ( location == NULL ) ? default_globus_url_copy() : location;

  /* sanity check, if default fails */
  if ( guc == NULL ) return result;

  /* check accessibility */
  if ( stat( guc, &st ) == 0 ) {
    if ( (geteuid() != st.st_uid || (st.st_mode & S_IXUSR) == 0) &&
	 (getegid() != st.st_gid || (st.st_mode & S_IXGRP) == 0) &&
	 ((st.st_mode & S_IXOTH) == 0) && ! S_ISREG(st.st_mode) ) {
      fprintf( stderr, "ERROR: Check execute permissions on %s\n", guc );
      return result;
    }
  } else {
    fprintf( stderr, "ERROR: Unable to access %s: %d: %s\n",
	     guc, errno, strerror(errno) );
    return result;
  }

  /* postcondition: We can access the g-u-c. Now let's dry-run it. */
  /* This should also catch errors due to missing shared libraries. */
  argv[0] = guc;
  argv[1] = "-version";
  argv[2] = NULL;

  /* g-u-c -version exits with exit-code of 1 -- for how much longer? */
  *line = '\0';
  status = pipe_out_cmd( "g-u-c", argv, envp, line, sizeof(line) );
  if ( *line ) { 
    sscanf( line, "%*s %u.%u\n", &major, &minor );
    result = major * 1000 + minor;
  }

  if ( status == -1 ) {
    result = -1;
    fprintf( stderr, "ERROR: While waiting for globus-url-copy: %s\n", 
	     strerror(errno) );
  } else if ( status != 256 && status != 0 ) {
    result = -1;
    if ( WIFEXITED(status) ) {
      fprintf( stderr, "ERROR: globus-url-copy termined with exit code %d\n", 
	       WEXITSTATUS(status) );
    } else if ( WIFSIGNALED(status) ) {
      fprintf( stderr, "ERROR: globus-url-copy terminated on signal %d\n",
	       WTERMSIG(status) );
    } else {
      fprintf( stderr, "ERROR: globus-url-copy died abnormally on an unspecified cause.\n" );
    }
  }

  return result;
}

char*
default_grid_proxy_info( void )
/* purpose: Determines the default path to default g-p-i. Minimal checks!
 * returns: absolute path to g-p-i, or NULL if somehow inaccessible.
 */
{
  char* globus;
  char* gpi;
  struct stat st;

  /* assemble default location */
  if ( (globus=getenv("GLOBUS_LOCATION")) == NULL ) {
    fputs( "ERROR: You need to set your GLOBUS_LOCATION\n", stderr );
    return NULL;
  }

  gpi = (char*) malloc( strlen(globus) + 24 * sizeof(char) );
  strcpy( gpi, globus );
  strcat( gpi, "/bin/grid-proxy-info" );

  if ( stat( gpi, &st ) == 0 ) {
    if ( (geteuid() != st.st_uid || (st.st_mode & S_IXUSR) == 0) &&
	 (getegid() != st.st_gid || (st.st_mode & S_IXGRP) == 0) &&
	 ((st.st_mode & S_IXOTH) == 0) && ! S_ISREG(st.st_mode) ) {
      fprintf( stderr, "ERROR: Check execute permissions on %s\n", gpi );
      free((void*) gpi);
      return NULL;
    }
  } else {
    fprintf( stderr, "ERROR: Unable to access %s: %d: %s\n",
	     gpi, errno, strerror(errno) );
    free((void*) gpi);
    return NULL;
  }

  return gpi;
}

long
check_grid_proxy_info( char* gpi, char* envp[] )
/* purpose: Obtains the time remaining on the current user certificate proxy.
 * paramtr: gpi (IN): absolute path to grid-proxy-info, no more checks
 *          envp (IN): environment pointer from main()
 * returns: the time remaining on the certificate, 0 for expired, -1 error
 * seealso: default_grid_proxy_init() determines location and executability
 */
{
  int status;
  long result = -1;
  char* argv[3];
  char line[256];

  argv[0] = gpi;
  argv[1] = "-timeleft";
  argv[2] = NULL;
  if ( (status=pipe_out_cmd( "g-p-i", argv, envp, line, sizeof(line) )) == 0 )
    sscanf( line, "%ld\n", &result );

  if ( status == -1 ) {
    fprintf( stderr, "ERROR: While waiting for grid-proxy-info: %s\n", 
	     strerror(errno) );
    result = -1;
  } else if ( status != 0 ) {
    result = -1;
    if ( WIFEXITED(status) ) {
      fprintf( stderr, "ERROR: grid-proxy-info termined with exit code %d\n", 
	       WEXITSTATUS(status) );
    } else if ( WIFSIGNALED(status) ) {
      fprintf( stderr, "ERROR: grid-proxy-info terminated on signal %d\n",
	       WTERMSIG(status) );
    } else {
      fprintf( stderr, "ERROR: grid-proxy-info died abnormally on an unspecified cause.\n" );
    }
  }

  return result;
}

static
int
my_ulimit( int resource, int sys_resource, int request )
/* purpose: fudges a request from sysconf and rlimits
 * paramtr: request (IN): request number of handles
 * returns: maximum after adjustment attempts.
 */
{
  struct rlimit current;
  int result = 0;

  /* getrlimit should not fail in well-formed code */
  if ( getrlimit( resource, &current ) ) {
    fprintf( stderr, "%s:%d getrlimit %d: %s\n",
	     __FILE__, __LINE__, resource, strerror(errno) );
    exit(1);
  }

  if ( (result = current.rlim_cur) != RLIM_INFINITY ) {
    /* limited resources - more checks */
    if ( request > current.rlim_cur && 
	 ( current.rlim_max == RLIM_INFINITY || 
	   current.rlim_max >= request ) ) {
      /* we have a chance to raise the limit to the request */
      current.rlim_cur = request;
      if ( setrlimit( resource, &current ) == 0 ) result = request;
    }
  } else {
    result = request;
  }

  return result;
}

int
max_files( int request )
/* purpose: obtain the maximum filehandle possible in the current setting.
 * paramtr: request (IN): request number of handles
 * returns: maximum filehandle after adjustment attempts.
 */
{
  int result = my_ulimit( RLIMIT_NOFILE, _SC_OPEN_MAX, request );
  if ( result < request ) 
    fprintf( stderr, 
	     "Warning: Unable to raise RLIMIT_NOFILE to %d, using %d\n", 
	     request, result );
  return request < result ? request : result;
}

int
max_procs( int request )
/* purpose: obtain the maximum number of user processes possible.
 * paramtr: request (IN): request number of handles
 * returns: maximum filehandle after adjustment attempts.
 */
{
#ifdef RLIMIT_NPROC
  int result = my_ulimit( RLIMIT_NPROC, _SC_CHILD_MAX, request );
#else
  int result = sysconf(_SC_CHILD_MAX);
#endif
  if ( result < request ) 
    fprintf( stderr, 
	     "Warning: Unable to raise RLIMIT_NPROC to %d, using %d\n", 
	     request, result );
  return request < result ? request : result;
}
