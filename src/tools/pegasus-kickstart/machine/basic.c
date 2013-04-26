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
#include "basic.h"
#include "../tools.h"

#include <ctype.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <time.h>

extern int isExtended; /* timestamp format concise or extended */
extern int isLocal;    /* timestamp time zone, UTC or local */

static
size_t
mystrlen(const char* s, size_t max)
{
  /* array version */
  size_t i = 0;
  while (i < max && s[i]) ++i;
  return i;
}

static
char*
mytolower(char* s, size_t max)
{
  /* array version */
  size_t i;
  for (i=0; i < max && s[i]; ++i) s[i] = tolower(s[i]);
  return s;
}

void*
initBasicMachine(void)
/* purpose: initialize the data structure.
 * returns: initialized MachineBasicInfo structure.
 */
{
  long result;
  MachineBasicInfo* p = (MachineBasicInfo*) malloc(sizeof(MachineBasicInfo));

  /* extra sanity check */
  if (p == NULL) {
    fputs("initBasicMachine c'tor failed\n", stderr);
    return NULL;
  } else memset(p, 0, sizeof(MachineBasicInfo));

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

int
startBasicMachine(FILE *out, int indent, const char* tag,
                  const MachineBasicInfo* machine)
/* purpose: start format the information into the given stream as XML.
 * paramtr: out (IO): the stream
 *          indent (IN): indentation level
 *          tag (IN): name to use for element tags.
 *          machine (IN): basic machine structure info to print.
 * returns: 0 if no error
 */
{
  /* sanity check */
  if (machine == NULL) return 0;

  /* <machine> open tag */
  fprintf(out, "%*s<%s page-size=\"%lu\">\n", indent-2, "", tag,
          machine->pagesize);

  /* <stamp> */
  fprintf(out, "%*s<stamp>%s</stamp>\n", indent, "",
          fmtisodate(isLocal, isExtended, machine->stamp.tv_sec,
                     machine->stamp.tv_usec));

  /* <uname> */
  fprintf(out, "%*s<uname", indent, "");
  fprintf(out, " system=\"%s\"", machine->uname.sysname);
  fprintf(out, " nodename=\"%s\"", machine->uname.nodename);
  fprintf(out, " release=\"%s\"", machine->uname.release);
  fprintf(out, " machine=\"%s\"", machine->uname.machine);
  fprintf(out, ">%s</uname>\n", machine->uname.version);

  /* <"provider"> grouping tag */
  fprintf(out, "%*s<%s>\n", indent-1, "", machine->provider);

  return 0;
}

int
finalBasicMachine(FILE *out, int indent, const char* tag,
                  const MachineBasicInfo* machine)
/* purpose: finish format the information into the given stream as XML.
 * paramtr: out (IO): The stream
 *          indent (IN): indentation level
 *          tag (IN): name to use for element tags.
 *          machine (IN): basic machine structure info to print.
 * returns: 0 if no error
 */
{
  /* sanity check */
  if (machine == NULL) return 0;

  /* </"provider"> close tag */
  fprintf(out, "%*s</%s>\n", indent-1, "", machine->provider);

  /* </machine> close tag */
  fprintf(out, "%*s</%s>\n", indent-2, "", tag);

  return 0;
}

int
printBasicMachine(FILE *out, int indent, const char* tag,
                  const void* data)
/* purpose: format the machine information into the given stream as XML.
 * paramtr: out (IO): The stream
 *          indent (IN): indentation level
 *          tag (IN): name to use for element tags.
 *          data (IN): MachineBasicInfo info to print.
 * returns: number of characters put into buffer (buffer length)
 */
{
  const MachineBasicInfo* ptr = (const MachineBasicInfo*) data;

  if (ptr) {
    char b[32];
    startBasicMachine(out, indent+2, tag, ptr);

#if defined(_SC_PHYS_PAGES) || defined(_SC_AVPHYS_PAGES)
    fprintf(out, "%*s<ram", indent, "");
#ifdef _SC_PHYS_PAGES
    fprintf(out, " total=\"%s\"",
            sizer(b, 32, sizeof(ptr->ram_total), &(ptr->ram_total)));
#endif /* _SC_PHYS_PAGES */
#ifdef _SC_AVPHYS_PAGES
    fprintf(out, " avail=\"%s\"",
            sizer(b, 32, sizeof(ptr->ram_avail), &(ptr->ram_avail)));
#endif /* _SC_AVPHYS_PAGES */
    fprintf(out, "/>\n");
#endif /* _SC_PHYS_PAGES || _SC_AVPHYS_PAGES */

#if defined(_SC_NPROCESSORS_CONF) || defined(_SC_NPROCESSORS_ONLN)
    fprintf(out, "%*s<cpu", indent, "");
#ifdef _SC_NPROCESSORS_CONF
    fprintf(out, " total=\"%s\"",
            sizer(b, 32, sizeof(ptr->cpu_total), &(ptr->cpu_total)));
#endif /* _SCN_PROCESSORS_CONF */
#ifdef _SC_NPROCESSORS_ONLN
    fprintf(out, " online=\"%s\"",
            sizer(b, 32, sizeof(ptr->cpu_online), &(ptr->cpu_online)));
#endif /* _SC_NPROCESSORS_ONLN */
    fprintf(out, "/>\n");
#endif /* _SC_NPROCESSORS_CONF || _SC_NPROCESSORS_ONLN */

    finalBasicMachine(out, indent+2, tag, ptr);
  }

  return 0;
}

void
deleteBasicMachine(void* data)
/* purpose: destructor
 * paramtr: data (IO): valid MachineInfo structure to destroy.
 */
{
#ifdef EXTRA_DEBUG
  fprintf(stderr, "# deleteBasicMachineInfo(%p)\n", data);
#endif

  if (data) free(data);
}
