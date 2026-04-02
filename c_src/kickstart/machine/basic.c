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
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <time.h>
#include <errno.h>

#include "basic.h"
#include "../utils.h"
#include "../error.h"

static char* mytolower(char* s, size_t max) {
    /* array version */
    size_t i;
    for (i=0; i < max && s[i]; ++i) {
        s[i] = tolower(s[i]);
    }
    return s;
}

void* initBasicMachine(void) {
    /* purpose: initialize the data structure.
     * returns: initialized MachineBasicInfo structure.
     */
    long result;
    MachineBasicInfo* p = (MachineBasicInfo*) calloc(1, sizeof(MachineBasicInfo));

    /* extra sanity check */
    if (p == NULL) {
        printerr("calloc: %s\n", strerror(errno));
        return NULL;
    }

    /* name of this provider -- overwritten by importers */
    p->provider = "basic";

    /* start of data gathering */
    now(&p->stamp);

    if (uname(&p->uname) == -1) {
        memset(&p->uname, 0, sizeof(p->uname));
    } else {
        /* remove mixed case */
        mytolower(p->uname.sysname, SYS_NMLN);
        mytolower(p->uname.nodename, SYS_NMLN);
        mytolower(p->uname.machine, SYS_NMLN);
    }
    p->pagesize = getpagesize();

#ifdef _SC_PHYS_PAGES
    if ((result=sysconf(_SC_PHYS_PAGES)) != -1) {
        p->ram_total = result;
        p->ram_total *= p->pagesize;
    }
#endif /* _SC_PHYS_PAGES */

#ifdef _SC_AVPHYS_PAGES
    if ((result=sysconf(_SC_AVPHYS_PAGES)) != -1) {
        p->ram_avail = result;
        p->ram_avail *= p->pagesize;
    }
#endif /* _SC_AVPHYS_PAGES */

#ifdef _SC_NPROCESSORS_CONF
    if ((result=sysconf(_SC_NPROCESSORS_CONF)) != -1)
        p->cpu_total = result;
#endif /* _SCN_PROCESSORS_CONF */

#ifdef _SC_NPROCESSORS_ONLN
    if ((result=sysconf(_SC_NPROCESSORS_ONLN)) != -1)
        p->cpu_online = result;
#endif /* _SC_NPROCESSORS_ONLN */

    return p;
}

int startBasicMachine(FILE *out, int indent, const char* tag,
                      const MachineBasicInfo* machine) {
    /* purpose: start format the information into the given stream as XML.
     * paramtr: out (IO): the stream
     *          indent (IN): indentation level
     *          tag (IN): name to use for element tags.
     *          machine (IN): basic machine structure info to print.
     * returns: 0 if no error
     */
    /* sanity check */
    if (machine == NULL) {
        return 0;
    }

    /* <machine> open tag */
    fprintf(out, "%*s%s:\n", indent-2, "", tag);
    fprintf(out, "%*s  page-size: %lu\n", indent-2, "", 
                 machine->pagesize);

    /* <uname> */
    fprintf(out, "%*s  uname_system: %s\n", indent-2, "", machine->uname.sysname);
    fprintf(out, "%*s  uname_nodename: %s\n", indent-2, "", machine->uname.nodename);
    fprintf(out, "%*s  uname_release: %s\n", indent-2, "", machine->uname.release);
    fprintf(out, "%*s  uname_machine: %s\n", indent-2, "", machine->uname.machine);

    return 0;
}

int finalBasicMachine(FILE *out, int indent, const char* tag,
                      const MachineBasicInfo* machine) {
    /* purpose: finish format the information into the given stream as XML.
     * paramtr: out (IO): The stream
     *          indent (IN): indentation level
     *          tag (IN): name to use for element tags.
     *          machine (IN): basic machine structure info to print.
     * returns: 0 if no error
     */
    /* sanity check */
    if (machine == NULL) {
        return 0;
    }

    return 0;
}

int printBasicMachine(FILE *out, int indent, const char* tag,
                      const void* data) {
    /* purpose: format the machine information into the given stream as XML.
     * paramtr: out (IO): The stream
     *          indent (IN): indentation level
     *          tag (IN): name to use for element tags.
     *          data (IN): MachineBasicInfo info to print.
     */
    const MachineBasicInfo* ptr = (const MachineBasicInfo*) data;

    if (ptr) {
        startBasicMachine(out, indent+2, tag, ptr);

#if defined(_SC_PHYS_PAGES) || defined(_SC_AVPHYS_PAGES)
#ifdef _SC_PHYS_PAGES
        fprintf(out, "%*s  ram_total: %llu\n", indent-2, "", ptr->ram_total / 1024);
#endif /* _SC_PHYS_PAGES */
#ifdef _SC_AVPHYS_PAGES
        fprintf(out, "%*s  ram_avail: %llu\n", indent-2, "", ptr->ram_avail / 1024);
#endif /* _SC_AVPHYS_PAGES */
#endif /* _SC_PHYS_PAGES || _SC_AVPHYS_PAGES */

#if defined(_SC_NPROCESSORS_CONF) || defined(_SC_NPROCESSORS_ONLN)
#ifdef _SC_NPROCESSORS_CONF
        fprintf(out, "%*s  cpu_total: %hu\n", indent-2, "", ptr->cpu_total);
#endif /* _SCN_PROCESSORS_CONF */
#ifdef _SC_NPROCESSORS_ONLN
        fprintf(out, "%*s  cpu_online=\"%hu\"", indent-2, "", ptr->cpu_online);
#endif /* _SC_NPROCESSORS_ONLN */
#endif /* _SC_NPROCESSORS_CONF || _SC_NPROCESSORS_ONLN */

        finalBasicMachine(out, indent+2, tag, ptr);
    }

    return 0;
}

void deleteBasicMachine(void* data) {
    /* purpose: destructor
     * paramtr: data (IO): valid MachineInfo structure to destroy.
     */
    if (data) {
        free(data);
    }
}

