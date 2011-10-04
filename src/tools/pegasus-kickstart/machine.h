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
#ifndef _MACHINE_H
#define _MACHINE_H

#include <sys/types.h>

typedef struct {
  /* virtual method table */
  void* (*ctor)( void ); 
  int   (*show)( char*, size_t, size_t*, size_t, const char*, const void* ); 
  void  (*dtor)( void* ); 

  /* mutable object data */
  void*   data;
} MachineInfo; 

extern
void
initMachineInfo( MachineInfo* machine ); 
/* purpose: initialize the data structure. 
 * paramtr: machine (OUT): initialized MachineInfo structure. 
 */

extern
int
printXMLMachineInfo( char* buffer, size_t size, size_t* len, size_t indent,
                     const char* tag, const MachineInfo* machine );
/* purpose: format the job information into the given buffer as XML.
 * paramtr: buffer (IO): area to store the output in
 *          size (IN): capacity of character area
 *          len (IO): current position within area, will be adjusted
 *          indent (IN): indentation level
 *          tag (IN): name to use for element tags.
 *          machine (IN): machine info to print.
 * returns: number of characters put into buffer (buffer length)
 */


extern
void
deleteMachineInfo( MachineInfo* machine );
/* purpose: destructor
 * paramtr: machine (IO): valid MachineInfo structure to destroy. 
 */

#endif /* _MACHINE_H */
