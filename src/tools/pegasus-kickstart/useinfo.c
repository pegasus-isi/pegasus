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

#include "utils.h"
#include "useinfo.h"

int printYAMLUseInfo(FILE *out, int indent, const char* id,
                    const struct rusage* use) {
    /* purpose: format the rusage record into the given stream as XML.
     * paramtr: out (IO): the stream
     *          indent (IN): indentation level
     *          id (IN): object identifier to use as element tag name.
     *          use (IN): struct rusage info
     * returns: number of characters put into buffer (buffer length)
     */
    char b[4][32];

    /* <usage> */
    fprintf(out, "%*s%s:\n", indent, "", id);
    fprintf(out, "%*s  utime: %.3f\n", indent, "", doubletime(use->ru_utime));
    fprintf(out, "%*s  stime: %.3f\n", indent, "", doubletime(use->ru_stime));

    fprintf(out, "%*s  maxrss: %lu\n", indent, "", 
#ifdef DARWIN
            /* On Mac OS X this is in bytes */
            use->ru_maxrss / 1024
#else
            /* On linux it is in KB */
            use->ru_maxrss
#endif
    );

    fprintf(out, "%*s  minflt: %s\n%*s  majflt: %s\n%*s  nswap: %s\n",
            indent, "", sizer(b[0], 32, sizeof(use->ru_minflt), &(use->ru_minflt)),
            indent, "", sizer(b[1], 32, sizeof(use->ru_majflt), &(use->ru_majflt)),
            indent, "", sizer(b[2], 32, sizeof(use->ru_nswap), &(use->ru_nswap)));

    fprintf(out, "%*s  inblock: %s\n%*s  outblock: %s\n",
            indent, "", sizer(b[0], 32, sizeof(use->ru_inblock), &(use->ru_inblock)),
            indent, "", sizer(b[1], 32, sizeof(use->ru_oublock), &(use->ru_oublock)));

    fprintf(out, "%*s  msgsnd: %s\n%*s  msgrcv: %s\n",
            indent, "", sizer(b[2], 32, sizeof(use->ru_msgsnd), &(use->ru_msgsnd)),
            indent, "", sizer(b[3], 32, sizeof(use->ru_msgrcv), &(use->ru_msgrcv)));

    fprintf(out, "%*s  nsignals: %s\n%*s  nvcsw: %s\n%*s  nivcsw: %s\n",
            indent, "", sizer(b[0], 32, sizeof(use->ru_nsignals), &(use->ru_nsignals)),
            indent, "", sizer(b[1], 32, sizeof(use->ru_nvcsw), &(use->ru_nvcsw)),
            indent, "", sizer(b[2], 32, sizeof(use->ru_nivcsw), &(use->ru_nivcsw)));

    return 0;
}

static void add(struct timeval* sum, const struct timeval* summand) {
    sum->tv_usec += summand->tv_usec;
    sum->tv_sec += summand->tv_sec;
    if (sum->tv_usec >= 1000000) {
        sum->tv_sec++;
        sum->tv_usec -= 1000000;
    }
}

void addUseInfo(struct rusage* sum, const struct rusage* summand) {
    /* purpose: add a given rusage record to an existing one
     * paramtr: sum (IO): initialized rusage record to add to
     *          summand (IN): values to add to
     * returns: sum += summand; */

    /* Total amount of user time used. */
    add(&sum->ru_utime, &summand->ru_utime);

    /* Total amount of system time used.  */
    add(&sum->ru_stime, &summand->ru_stime);

    /* Maximum resident set size (in kilobytes).  */
    sum->ru_maxrss += summand->ru_maxrss;

    /* Amount of sharing of text segment memory
       with other processes (kilobyte-seconds).  */
    sum->ru_ixrss += summand->ru_ixrss;

    /* Amount of data segment memory used (kilobyte-seconds).  */
    sum->ru_idrss += summand->ru_idrss;

    /* Amount of stack memory used (kilobyte-seconds).  */
    sum->ru_isrss += summand->ru_isrss;

    /* Number of soft page faults (i.e. those serviced by reclaiming
       a page from the list of pages awaiting reallocation.  */
    sum->ru_minflt += summand->ru_minflt;

    /* Number of hard page faults (i.e. those that required I/O).  */
    sum->ru_majflt += summand->ru_majflt;

    /* Number of times a process was swapped out of physical memory.  */
    sum->ru_nswap += summand->ru_nswap;

    /* Number of input operations via the file system.  Note: This
       and `ru_oublock' do not include operations with the cache.  */
    sum->ru_inblock += summand->ru_inblock;

    /* Number of output operations via the file system.  */
    sum->ru_oublock += summand->ru_oublock;

    /* Number of IPC messages sent.  */
    sum->ru_msgsnd += summand->ru_msgsnd;

    /* Number of IPC messages received.  */
    sum->ru_msgrcv += summand->ru_msgrcv;

    /* Number of signals delivered.  */
    sum->ru_nsignals += summand->ru_nsignals;

    /* Number of voluntary context switches, i.e. because the process
       gave up the process before it had to (usually to wait for some
       resource to be available).  */
    sum->ru_nvcsw += summand->ru_nvcsw;

    /* Number of involuntary context switches, i.e. a higher priority process
       became runnable or the current process used up its time slice.  */
    sum->ru_nivcsw += summand->ru_nivcsw;
}

