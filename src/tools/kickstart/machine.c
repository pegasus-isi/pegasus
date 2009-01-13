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
#include "machine.h"

static const char* RCS_ID =
  "$Id$";

#ifdef LINUX
#define __MFLAG 1
#include "machine/linux.h"
#endif /* LINUX */

#ifdef DARWIN
#define __MFLAG 2
#include "machine/darwin.h"
#endif /* DARWIN */

#ifdef SUNOS
#define __MFLAG 3
#include "machine/sunos.h"
#include <memory.h>
#endif /* SUNOS */

#ifndef __MFLAG
#include "machine/basic.h"
#endif /* unknown */

#ifdef EXTRA_DEBUG
#include <stdio.h>
#endif /* EXTRA_DEBUG */

#include <string.h>
#include "debug.h"

void
initMachineInfo( MachineInfo* machine )
/* purpose: initialize the data structure. 
 * paramtr: machine (OUT): initialized MachineInfo structure. 
 */
{
  /* initialize virtual method table */
#ifdef __MFLAG
  machine->ctor = initMachine; 
  machine->show = printMachine;
  machine->dtor = deleteMachine;
#else
  machine->ctor = initBasicMachine; 
  machine->show = printBasicMachine;
  machine->dtor = deleteBasicMachine;
#endif /* __MFLAG */

  /* call constructor on data */ 
  machine->data = machine->ctor(); 
}

int
printXMLMachineInfo( char* buffer, size_t size, size_t* len, size_t indent,
                     const char* tag, const MachineInfo* machine )
/* purpose: format the job information into the given buffer as XML.
 * paramtr: buffer (IO): area to store the output in
 *          size (IN): capacity of character area
 *          len (IO): current position within area, will be adjusted
 *          indent (IN): indentation level
 *          tag (IN): name to use for element tags.
 *          machine (IN): machine info to print.
 * returns: number of characters put into buffer (buffer length)
 */
{
  /* sanity check */
  if ( machine && machine->show && machine->data )
    machine->show( buffer, size, len, indent, tag, machine->data ); 

  return *len; 
}

void
deleteMachineInfo( MachineInfo* machine )
/* purpose: destructor
 * paramtr: machine (IO): valid MachineInfo structure to destroy. 
 */
{
#ifdef EXTRA_DEBUG
  fprintf( stderr, "# deleteMachineInfo(%p)\n", machine );
#endif

  machine->dtor( machine->data ); 
  memset( machine, 0, sizeof(MachineInfo) ); 
}
