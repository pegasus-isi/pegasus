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
#ifndef _BATCH_H
#define _BATCH_H

#include <sys/types.h>

typedef struct {
  /* WARNING: If you close the descriptor outside of done_batching,
   * you MUST invalidate it by setting it to -1 after calling close() 
   */
  int   descriptor;	/* file descriptor returned by mkstemp */
  char* filename;       /* filename instance overwritten by mkstemp */
} FileStructure;

typedef struct {
  unsigned        size;    /* number of file structure elements */
  FileStructure*  batch;   /* array of file structures */
  void*           user;    /* user data section -- for queue algorithms */
} Batch;

typedef ssize_t BatchingFunction( Batch*, const char*, const char* );

extern
void
init_batching( Batch* result, unsigned parallel );
/* purpose: Initializes a batch structure
 * paramtr: result (OUT): structure to initialize
 *          parallel (IN): size of structure from parallel gucs
 */

extern
ssize_t
queue_batching_rr( Batch* batch, const char* src, const char* dst );
/* purpose: Adds a src-dst URL-pair to one of the temporary files.
 *          This implementation slots round-robin over all temp files.
 * paramtr: batch (IO): structure (self in OO)
 *          src (IN): src URL to add
 *          dst (IN): dst URL to add
 * returns: number of bytes written, see write and writev
 */

extern
ssize_t
queue_batching_ss( Batch* batch, const char* src, const char* dst );
/* purpose: Adds a src-dst URL-pair to one of the temporary files.
 *          This implementation sorts by source host hash
 * paramtr: batch (IO): structure (self in OO)
 *          src (IN): src URL to add
 *          dst (IN): dst URL to add
 * returns: number of bytes written, see write and writev
 */

extern
void
done_batching( Batch* batch );
/* purpose: Frees the file structure after all have been worked with.
 * paramtr: batch (IO): structure (self in OO)
 */

#endif /* _BATCH_H */
