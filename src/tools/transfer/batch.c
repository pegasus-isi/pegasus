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
#include <sys/types.h>
#include <sys/uio.h>
#include <errno.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

#include "batch.h"

static const char* RCS_ID =
"$Id$";

void
init_batching( Batch* result, unsigned parallel )
/* purpose: Initializes a batch structure
 * paramtr: result (OUT): structure to initialize
 *          parallel (IN): size of structure from parallel gucs
 * returns: -
 */
{
  unsigned i;
  size_t size;
  char* template, *tempdir = getenv( "TMP" );
  if ( tempdir == NULL ) tempdir = getenv("TEMP");
  if ( tempdir == NULL ) tempdir = getenv("TMPDIR");
  if ( tempdir == NULL ) tempdir = "/tmp";
  
  size = strlen(tempdir) + 24;
  if ( (template = malloc(size)) == NULL ) {
    fputs( "ERROR: Out of memory\n", stderr );
    exit(10);
  } else {
    snprintf( template, size, "%s/tx-%d-XXXXXX", tempdir, getpid() );
  }

  /* init */
  result->size = parallel;
  result->batch = calloc( parallel, sizeof(FileStructure) );
  result->user = NULL;

  if ( result->batch == NULL ) {
    fputs( "ERROR: Out of memory\n", stderr );
    exit(10);
  } else {
    for ( i=0; i < parallel; ++i ) {
      if ( (result->batch[i].filename = strdup( template )) == NULL ) {
	fputs( "ERROR: Out of memory\n", stderr );
	exit(11);
      }
      if ( (result->batch[i].descriptor=mkstemp(result->batch[i].filename)) 
	   == -1 ) {
	fprintf( stderr, "ERROR: mkstemp failed: %d: %s\n",
		 errno, strerror(errno) );
	exit(11);
      }
    }
  }

  free((void*) template );
}

typedef struct iovec IOVector[4];

static
void
init_iovec( IOVector iov, const char* src, const char* dst )
{
  iov[0].iov_base = (void*) src;
  iov[0].iov_len  = strlen(src);
  iov[1].iov_base = (void*) " ";
  iov[1].iov_len  = 1;
  iov[2].iov_base = (void*) dst;
  iov[2].iov_len  = strlen(dst);
  iov[3].iov_base = (void*) "\n";
  iov[3].iov_len  = 1;
}

ssize_t
queue_batching_rr( Batch* batch, const char* src, const char* dst )
/* purpose: Adds a src-dst URL-pair to one of the temporary files.
 *          This implementation slots round-robin over all temp files.
 * paramtr: batch (IO): structure (self in OO)
 *          src (IN): src URL to add
 *          dst (IN): dst URL to add
 * returns: number of bytes written, see write and writev
 */
{
  static size_t i = 0;
  IOVector iov;

  /* sanity check */
  if ( batch == NULL || batch->batch == NULL ) {
    errno = EFAULT;
    return -1; 
  } else {
    init_iovec( iov, src, dst );
  }

  /* distribute round robin */
  return writev( batch->batch[ i++ % batch->size ].descriptor, iov, 4 );
}

static
unsigned long
hashpjw( const char* p )
/* purpose: Obtain a small hash value for the source host
 * paramtr: p (IN): URL except file: URLs
 * returns: a hash value between 0..4095
 */
{
  int state;
  unsigned long g, h = 0;
  for ( state=0; *p && state < 3; ++p ) {
    h = (h<<4) + (*p);
    if ( (g = h & 0xF0000000) != 0 ) h ^= g | (g >> 24);
    if ( *p == '/' ) ++state;
  }

  return ( (h & 0xFFF00) >> 8);
}

ssize_t
queue_batching_ss( Batch* batch, const char* src, const char* dst )
/* purpose: Adds a src-dst URL-pair to one of the temporary files.
 *          This implementation sorts by source host hash
 * paramtr: batch (IO): structure (self in OO)
 *          src (IN): src URL to add
 *          dst (IN): dst URL to add
 * returns: number of bytes written, see write and writev
 */
{
  struct iovec iov[4];

  /* sanity check */
  if ( batch == NULL || batch->batch == NULL ) {
    errno = EFAULT;
    return -1; 
  } else {
    init_iovec( iov, src, dst );
  }

  /* distribute source-sorted */
  return writev( batch->batch[hashpjw(src) % batch->size].descriptor, iov, 4 );
}

void
done_batching( Batch* batch )
/* purpose: Frees the file structure after all have been worked with.
 * paramtr: batch (IO): structure (self in OO)
 */
{
  unsigned i;

  /* sanity check */
  if ( batch == NULL || batch->batch == NULL ) return ; 

  for ( i=0; i < batch->size; ++i ) {
    if ( batch->batch[i].descriptor != -1 ) {
      close( batch->batch[i].descriptor );
      batch->batch[i].descriptor = -1;
    }

    unlink( batch->batch[i].filename );
    free((void*) batch->batch[i].filename );
    batch->batch[i].filename = 0;
  }

  free((void*) batch->batch );
  batch->batch = NULL;

  if ( batch->user != NULL ) free((void*) batch->user);
  batch->user = NULL;
}
