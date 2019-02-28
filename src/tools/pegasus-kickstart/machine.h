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

#include <stdio.h>
#include <sys/types.h>

typedef struct {
    /* virtual method table */
    void* (*ctor)(void);
    int   (*show)(FILE*, int, const char*, const void*);
    void  (*dtor)(void*);

    /* mutable object data */
    void*   data;
} MachineInfo;

extern void initMachineInfo(MachineInfo* machine);
extern int printYAMLMachineInfo(FILE *out, int indent, const char* tag,
                               const MachineInfo* machine);
extern void deleteMachineInfo(MachineInfo* machine);

#endif /* _MACHINE_H */
