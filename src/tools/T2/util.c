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
#include <errno.h>
#include <math.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>

#include <sys/types.h>
#include <sys/time.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>
#include <fcntl.h>

#include "util.h"
#include "mypopen.h"

static const char* RCS_ID =
"$Id: util.c,v 1.8 2005/12/22 21:23:23 voeckler Exp $";

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
  return timeval2double(t); /* t.tv_sec + t.tv_usec / 1E6; */
}

char*
check_link( void )
/* purpose: Obtains the path to system's symlink tool ln
 * returns: absolute path to ln, or NULL if not found nor accessible
 */
{
  struct stat st;

#if 1
  char* link = strdup("/bin/ln");

#else
  /* FIXME: to be implemented post-SC */
  char* link, *s;
  char* path = NULL;
  char* temp = getenv("PATH");
  if ( temp == NULL || *temp == '\0' ) temp = "/bin:/usr/bin";
  if ( (path = strdup(temp)) == NULL ) return NULL;

  for ( s=strtok(path,":"); s; s=strtok(NULL,":") ) {
    
  }
#endif

  if ( stat( link, &st ) == 0 ) {
    if ( (geteuid() != st.st_uid || (st.st_mode & S_IXUSR) == 0) &&
	 (getegid() != st.st_gid || (st.st_mode & S_IXGRP) == 0) &&
	 ((st.st_mode & S_IXOTH) == 0) && ! S_ISREG(st.st_mode) ) {
      fprintf( stderr, "ERROR: Check execute permissions on %s\n", link );
      return NULL;
    }
  } else {
    fprintf( stderr, "ERROR: Unable to access %s: %d: %s\n",
	     link, errno, strerror(errno) );
    return NULL;
  }

  return link;
}

char*
default_globus_url_copy( void )
/* purpose: Determines the default path to default g-u-c. No checks!
 * returns: absolute path to g-u-c, or NULL if environment mismatch
 */
{
  char* globus = getenv("GLOBUS_LOCATION");
  char* guc;

  /* assemble default location */
  if ( globus == NULL ) {
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

long
check_grid_proxy_info( char* envp[] )
/* purpose: Obtains the time remaining on the current user certificate proxy.
 * paramtr: env (IN): environment pointer from main()
 * returns: the time remaining on the certificate, 0 for expired, -1 error
 */
{
  int status;
  char* gpi;
  char* globus;
  long result = -1;
  struct stat st;
  char* argv[3];
  char line[256];

  if ( (globus=getenv("GLOBUS_LOCATION")) == NULL ) {
    fputs( "ERROR: You need to set your GLOBUS_LOCATION\n", stderr );
    return result;
  }

  gpi = (char*) malloc( strlen(globus) + 36*sizeof(char) );
  strcpy( gpi, globus );
  strcat( gpi, "/bin/grid-proxy-info" );

  if ( stat( gpi, &st ) == 0 ) {
    if ( (geteuid() != st.st_uid || (st.st_mode & S_IXUSR) == 0) &&
	 (getegid() != st.st_gid || (st.st_mode & S_IXGRP) == 0) &&
	 ((st.st_mode & S_IXOTH) == 0) && ! S_ISREG(st.st_mode) ) {
      fprintf( stderr, "ERROR: Check execute permissions on %s\n", gpi );
      return result;
    }
  } else {
    fprintf( stderr, "ERROR: Unable to access %s: %d: %s\n",
	     gpi, errno, strerror(errno) );
    return result;
  }

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

  free((void*) gpi);
  return result;
}
