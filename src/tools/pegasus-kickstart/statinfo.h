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
#ifndef _VDS_STATINFO_H
#define _VDS_STATINFO_H

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

typedef enum { 
  IS_INVALID	= 0, 
  IS_FILE	= 1, 
  IS_HANDLE	= 2, 
  IS_TEMP	= 3,
  IS_FIFO	= 4
} StatSource;

typedef struct {
  StatSource      source;
  struct {
    int           descriptor;  /* IS_HANDLE, IS_TEMP|FIFO, openmode IS_FILE */
    const char*   name;        /* IS_FILE, IS_TEMP|FIFO */
  }               file;
  int             error;
  int             deferred;    /* IS_FILE: truncate was deferred */
  union {
    unsigned char header[16];  /* IS_FILE regular init */
    struct {
      size_t      count;       /* IS_FIFO msg count */
      size_t      rsize;       /* IS_FIFO input byte count */
      size_t      wsize;       /* IS_FIFO output byte count */
    }             fifo;
  }               client;
  struct stat     info;
  const char*     lfn;         /* from -s/-S option */
} StatInfo;

extern int make_application_executable;
/* if set to 1, make the application executable, no matter what. */

extern size_t data_section_size;
/* size of the <data> section returned for stdout and stderr. */

extern
int
myaccess( const char* path );
/* purpose: check a given file for being accessible and executable
 *          under the currently effective user and group id. 
 * paramtr: path (IN): current path to check
 * returns: 0 if the file is accessible, -1 for not
 */

extern
char*
findApp( const char* fn );
/* purpose: check the executable filename and correct it if necessary
 * paramtr: fn (IN): current knowledge of filename
 * returns: newly allocated fqpn of path to exectuble, or NULL if not found
 */

extern
int
forcefd( const StatInfo* info, int fd );
/* purpose: force open a file on a certain fd
 * paramtr: info (IN): is the StatInfo of the file to connect to (fn or fd)
 *          the mode for potential open() is determined from this, too.
 *          fd (IN): is the file descriptor to plug onto. If this fd is 
 *          the same as the descriptor in info, nothing will be done.
 * returns: 0 if all is well, or fn was NULL or empty.
 *          1 if opening a filename failed,
 *          2 if dup2 call failed
 */

extern
int
initStatInfoAsTemp( StatInfo* statinfo, char* pattern );
/* purpose: Initialize a stat info buffer with a temporary file
 * paramtr: statinfo (OUT): the newly initialized buffer
 *          pattern (IO): is the input pattern to mkstemp(), will be modified!
 * returns: a value of -1 indicates an error 
 */

extern
int
initStatInfoAsFifo( StatInfo* statinfo, char* pattern, const char* key );
/* purpose: Initialize a stat info buffer associated with a named pipe
 * paramtr: statinfo (OUT): the newly initialized buffer
 *          pattern (IO): is the input pattern to mkstemp(), will be modified!
 *          key (IN): is the environment key at which to store the filename
 * returns: a value of -1 indicates an error 
 */

extern
int
initStatInfoFromName( StatInfo* statinfo, const char* filename, int openmode,
		      int flag );
/* purpose: Initialize a stat info buffer with a filename to point to
 * paramtr: statinfo (OUT): the newly initialized buffer
 *          filename (IN): the filename to memorize (deep copy)
 *          openmode (IN): are the fcntl O_* flags to later open calls
 *          flag (IN): bit#0 truncate: whether to reset the file size to zero
 *                     bit#1 defer op: whether to defer opening the file for now
 *                     bit#2 preserve: whether to backup existing target file
 * returns: the result of the stat() system call on the provided file
 */

extern
int
initStatInfoFromHandle( StatInfo* statinfo, int descriptor );
/* purpose: Initialize a stat info buffer with a filename to point to
 * paramtr: statinfo (OUT): the newly initialized buffer
 *          descriptor (IN): the handle to attach to
 * returns: the result of the fstat() system call on the provided handle
 */

extern
int
updateStatInfo( StatInfo* statinfo );
/* purpose: update existing and initialized statinfo with latest info 
 * paramtr: statinfo (IO): stat info pointer to update
 * returns: the result of the stat() or fstat() system call.
 */

extern
int
addLFNToStatInfo( StatInfo* info, const char* lfn );
/* purpose: optionally replaces the LFN field with the specified LFN 
 * paramtr: statinfo (IO): stat info pointer to update
 *          lfn (IN): LFN to store, use NULL to free
 * returns: -1 in case of error, 0 if OK.
 */

extern
size_t
printXMLStatInfo( char* buffer, const size_t size, size_t* len, size_t indent,
		  const char* tag, const char* id, const StatInfo* info );
/* purpose: XML format a stat info record into a given buffer
 * paramtr: buffer (IO): area to store the output in
 *          size (IN): capacity of character area
 *          len (IO): current position within area, will be adjusted
 *          indent (IN): indentation level of tag
 *          tag (IN): name of element to generate
 *          id (IN): id attribute, use NULL to not generate
 *          info (IN): stat info to print.
 * returns: number of characters put into buffer (buffer length)
 */

extern
void
deleteStatInfo( StatInfo* statinfo );
/* purpose: clean up and invalidates structure after being done.
 * paramtr: statinfo (IO): clean up record.
 */

#endif /* _VDS_STATINFO_H */
