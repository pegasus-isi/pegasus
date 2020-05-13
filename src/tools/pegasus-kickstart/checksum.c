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

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <libgen.h>

#ifdef DARWIN
#include <libproc.h>
#endif

#include "checksum.h"

#define BUFSIZE 4096


int pegasus_integrity_yaml(const char *fname, char *yaml) {
    /* purpose: calculate the checksum of a file
     * paramtr: fname: name of the file
     *          yaml: the buffer for the calculated checksum
     * returns: 1 on success
     */
    char buf[BUFSIZE];
    char cmd[4096];

    /* in case of failure */
    *yaml = '\0';

    cmd[0] = '\0';

    /* use the same location for pegasus-integrity as was
       used for pegasus-kickstart */
#ifdef LINUX
    if (readlink("/proc/self/exe", buf, BUFSIZE)) {
#endif
#ifdef DARWIN
    pid_t pid = getpid();
    if (proc_pidpath(pid, buf, sizeof(buf))) {
#endif
        strcat(cmd, dirname(buf));
        strcat(cmd, "/");
    }

    strcat(cmd, "pegasus-integrity --generate-yaml=");
    strcat(cmd, fname);
    strcat(cmd, " 2>/dev/null");

    FILE *p = popen(cmd, "r");
    if (p == NULL) {
        return 0;
    }
    while (fgets(buf, BUFSIZE, p) != NULL) {
        strcpy(yaml, buf);
        yaml += strlen(buf);
    }

    if (pclose(p) != 0) {
        return 0;
    } 

    return 1;
}

int print_pegasus_integrity_yaml_blob(FILE *out, const char *fname) {
    /* purpose: if exists, reads the integrity data and puts in the yaml
     * paramtr: out: output stream to print to
     * returns: 1 on success
     */
    char buf[BUFSIZE];
    int fd;
    int len;

    if ((fd = open(fname, O_RDONLY)) == -1 ) {
        /* missing file is ok */
        return 1;
    }
    while ((len = read(fd, buf, BUFSIZE))) {
        fprintf(out, "%.*s", len, buf);
    }
    close(fd);

    return 1;
}

