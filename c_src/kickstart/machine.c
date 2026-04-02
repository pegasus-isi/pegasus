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
#include <string.h>

#include "machine.h"

#ifdef LINUX
#define __MFLAG 1
#include "machine/linux.h"
#endif /* LINUX */

#ifdef DARWIN
#define __MFLAG 2
#include "machine/darwin.h"
#endif /* DARWIN */

#ifndef __MFLAG
#include "machine/basic.h"
#endif /* unknown */

void initMachineInfo(MachineInfo* machine) {
    /* purpose: initialize the data structure. 
     * paramtr: machine (OUT): initialized MachineInfo structure. 
     */
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

int printYAMLMachineInfo(FILE *out, int indent, const char* tag,
                        const MachineInfo* machine) {
    /* purpose: format the job information into the given stream as YAML.
     * paramtr: out (IO): The stream
     *          indent (IN): indentation level
     *          tag (IN): name to use for element tags.
     *          machine (IN): machine info to print.
     * returns: number of characters put into buffer (buffer length)
     */

    /* sanity check */
    if (machine && machine->show && machine->data) {
        machine->show(out, indent+2, tag, machine->data);
    }

    return 0;
}

void deleteMachineInfo(MachineInfo* machine) {
    /* purpose: destructor
     * paramtr: machine (IO): valid MachineInfo structure to destroy.
     */
    machine->dtor(machine->data);
    memset(machine, 0, sizeof(MachineInfo));
}

