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
 * Copyright 1999-2008 University of Chicago and The University of
 * Southern California. All rights reserved.
 */
#ifndef _MACHINE_BASIC_H
#define _MACHINE_BASIC_H

#include <stdio.h>
#include <sys/types.h>
#include <sys/time.h>
#include <sys/utsname.h>
#include <unistd.h>

#ifndef SYS_NMLN
#ifdef _SYS_NAMELEN /* DARWIN */
#define SYS_NMLN 65
#else
#define SYS_NMLN 65
#define _SYS_NAMELEN 65
#endif /* _SYS_NAMELEN */
#endif /* SYS_NMLN */

typedef struct {
    /* common (shared) portion */
    const char*      provider;    /* name of this provider */
    struct timeval   stamp;   /* when was this snapshot taken */
    struct utsname   uname;   /* general system information */ 
    unsigned long    pagesize;    /* size of a page in bytes */

    /* fall-back provider-specific portion */
#ifdef _SC_PHYS_PAGES
    unsigned long long ram_total; 
#endif /* _SC_PHYS_PAGES */
#ifdef _SC_AVPHYS_PAGES
    unsigned long long ram_avail; 
#endif /* _SC_AVPHYS_PAGES */

#ifdef _SC_NPROCESSORS_CONF
    unsigned short   cpu_total; 
#endif /* _SC_NPROCESSORS_CONF */
#ifdef _SC_NPROCESSORS_ONLN
    unsigned short   cpu_online; 
#endif /* _SC_NPROCESSORS_ONLN */

} MachineBasicInfo;

extern void* initBasicMachine();
extern int startBasicMachine(FILE *out, int indent, const char* tag,
                             const MachineBasicInfo* machine);
extern int finalBasicMachine(FILE *out, int indent, const char* tag,
                             const MachineBasicInfo* machine);
extern int printBasicMachine(FILE *out, int indent, const char* tag,
                             const void* data);
extern void deleteBasicMachine(void* data);

#endif /* _MACHINE_BASIC_H */
