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
#ifndef _LIMIT_INFO_H
#define _LIMIT_INFO_H

#include <sys/types.h>
#include <sys/resource.h>

typedef struct {
  int            resource;  /* which resource, e.g. RLIMIT_STACK */
  int            error;     /* errno after call to getrlimit */
  struct rlimit  limit;     /* resource limits acquired */
} SingleLimitInfo;

typedef struct {
  size_t            size;
  SingleLimitInfo*  limits;
} LimitInfo;

extern
void
initLimitInfo( LimitInfo* limits );
/* purpose: initializes the data structure
 * paramtr: limits (OUT): sufficiently large memory block
 */

extern
void
updateLimitInfo( LimitInfo* limits );
/* purpose: initializes the data with current limits
 * paramtr: limits (IO): sufficiently large memory block
 */

extern
void
deleteLimitInfo( LimitInfo* limits );
/* purpose: destructor
 * paramtr: limits (IO): valid LimitInfo structure to destroy. 
 */

extern
int
printXMLLimitInfo( char* buffer, size_t size, size_t* len, size_t indent,
		   const LimitInfo* limits );
/* purpose: format the rusage record into the given buffer as XML.
 * paramtr: buffer (IO): area to store the output in
 *          size (IN): capacity of character area
 *          len (IO): current position within area, will be adjusted
 *          indent (IN): indentation level
 *          limits (IN): observed resource limits
 * returns: number of characters put into buffer (buffer length)
 */

#endif /* _LIMIT_INFO_H */
