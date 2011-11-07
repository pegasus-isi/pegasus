/**
 *  Copyright 2007-2011 University Of Southern California
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
#include <sys/types.h>
#include <sys/stat.h>
#include <errno.h>
#include <paths.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include "statinfo.h"

static const char* RCS_ID =
"$Id$";

int
myaccess( const char* path )
/* purpose: check a given file for being accessible and executable
 *          under the currently effective user and group id. 
 * paramtr: path (IN): current path to check
 * returns: 0 if the file is accessible, -1 for not
 */
{
  /* sanity check */
  if ( path && *path ) {
    struct stat st;
    if ( stat(path,&st) == 0 && S_ISREG(st.st_mode) ) {
      /* stat on file succeeded, and it is a regular file */
      if ( ( st.st_uid == geteuid() && (S_IXUSR & st.st_mode) == S_IXUSR ) ||
	   ( st.st_gid == getegid() && (S_IXGRP & st.st_mode) == S_IXGRP ) ||
	   ( (S_IXOTH & st.st_mode) == S_IXOTH ) ) {
	/* all is well, app is executable and accessible */
	return 0;
      } else {
	return -1;
      }
    } else {
      /* stat call failed, or file is not a regular file */
      return -1;
    }
  } else {
    /* illegal filename string (empty or NULL) */
    return -1;
  }
}

char*
findApp( const char* fn )
/* purpose: check the executable filename and correct it if necessary
 * paramtr: fn (IN): current knowledge of filename
 * returns: newly allocated fqpn of exectuble, or NULL if not found
 */
{
  char* s, *path, *t = NULL;

  /* sanity check */
  if ( fn == NULL || *fn == '\0' ) return NULL;

  /* don't touch absolute paths */
  if ( *fn == '/' ) {
    if ( myaccess(fn) == 0 ) return strdup(fn);
    else return NULL;
  }

#if 0
  /* try reaching executable from CWD */
  if ( myaccess(fn) == 0 ) return strdup(fn);
#endif

  /* continue only if there is a PATH to check */
  if ( (s=getenv("PATH")) == NULL ) {
#ifdef _PATH_DEFPATH
    path = strdup(_PATH_DEFPATH); 
#else
    return NULL;
#endif /* _PATH_DEFPATH */ 
  } else {
    /* yes, there is a PATH variable */ 
    path = strdup(s);
  }

  /* tokenize to compare */
  for ( s=strtok(path,":"); s; s=strtok(NULL,":") ) {
    size_t len = strlen(fn) + strlen(s) + 2;
    t = (char*) malloc(len);
    strncpy( t, s, len );
    strncat( t, "/", len );
    strncat( t, fn, len );
    if ( myaccess(t) == 0 ) break;
    else {
      free((void*) t);
      t = NULL;
    }
  }

  /* some or no matches found */
  free((void*) path);
  return t;
}
