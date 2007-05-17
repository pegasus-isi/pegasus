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
#ifndef _USEINFO_H
#define _USEINFO_H

#include <sys/types.h>
#include <sys/resource.h>

#ifdef SOLARIS
#ifndef HAS_USAGE_MEM
#define HAS_USAGE_MEM 1
#endif
#ifndef HAS_USAGE_IO
#define HAS_USAGE_IO 1
#endif
#ifndef HAS_USAGE_MSG
#define HAS_USAGE_MSG 1
#endif
#endif /* SOLARIS */

extern
int
printXMLUseInfo( char* buffer, size_t size, size_t* len, size_t indent,
		 const char* id, const struct rusage* use );
/* purpose: format the rusage record into the given buffer as XML.
 * paramtr: buffer (IO): area to store the output in
 *          size (IN): capacity of character area
 *          len (IO): current position within area, will be adjusted
 *          indent (IN): indentation level
 *          id (IN): object identifier to use as element tag name.
 *          use (IN): struct rusage info
 * returns: number of characters put into buffer (buffer length)
 */

extern
void
addUseInfo( struct rusage* sum, const struct rusage* summand );
/* purpose: add a given rusage record to an existing one
 * paramtr: sum (IO): initialized rusage record to add to
 *          summand (IN): values to add to
 * returns: sum += summand;
 */
#endif /* _USEINFO_H */
