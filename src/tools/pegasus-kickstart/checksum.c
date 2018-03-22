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

#include "checksum.h"

int pegasus_integrity_xml(const char *fname, char *xml) {
    /* purpose: calculate the checksum of a file
     * paramtr: fname: name of the file
     *          xml: the buffer for the calculated checksum
     * returns: 1 on success
     */
    char buf[4096];
    char cmd[4096];

    strcpy(cmd, "pegasus-integrity --generate-xml=");
    strcat(cmd, fname);
    strcat(cmd, " 2>/dev/null");

    FILE *p = popen(cmd, "r");
    if (p == NULL) {
        return 0;
    }
    if (fgets(buf, sizeof(buf), p) != NULL) {
        /* make sure we got a full checksum */
        if (strlen(buf) > 0) {
            if(buf[strlen(buf) - 1] == '\n')
            {
                buf[strlen(buf) - 1] = '\0';
            }
        }
    }
    if (pclose(p) != 0) {
        return 0;
    } 

    /* all good, copy the buffer */
    strcpy(xml, buf);

    return 1;
}

int print_pegasus_integrity_xml_blob(FILE *out, const char *fname) {
    /* purpose: if exists, reads the integrity data and puts in the xml
     * paramtr: out: output stream to print to
     * returns: 1 on success
     */
    char buf[4096];
    int fd;
    int len;

    if ((fd = open(fname, O_RDONLY)) == -1 ) {
        /* missing file is ok */
        return 1;
    }
    while ((len = read(fd, buf, 4096))) {
        buf[len + 1] = '\0';
        fprintf(out, "%.*s", len, buf);
    }
    close(fd);

    return 1;
}

