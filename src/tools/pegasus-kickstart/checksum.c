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
#include <time.h>
#include <sys/time.h>
#include "sha2.h"

#include "checksum.h"

#define BUFSIZE 4096


int pegasus_integrity_yaml(const char *fname, char *yaml) {
    /* purpose: calculate the checksum of a file
     * paramtr: fname: name of the file
     *          yaml: the buffer for the calculated checksum
     * returns: 1 on success
     */
    FILE          *inf;
    char          buf[BUFSIZE];
    sha256_ctx    ctx[1];
    unsigned char hval[SHA256_DIGEST_SIZE];
    char          chksum_str[SHA256_DIGEST_SIZE * 2];
    char          *chksum_cur;
    int           i, len;
    double        start_ts, duration;

    /* in case of failure */
    *yaml = '\0';
    chksum_str[0] = '\0';

    start_ts = get_ts(); 
    if (!(inf = fopen(fname, "r"))) {
        return 0;
    }

    sha256_begin(ctx);
    len = 0;
    do
    {   
        len = (int)fread(buf, 1, BUFSIZE, inf);
        if (len) {
            sha256_hash((unsigned char*)buf, len, ctx);
        }
    }
    while (len);
    fclose(inf);
    sha256_end(hval, ctx);
    duration = get_ts() - start_ts;

    chksum_cur = chksum_str;
    for (i = 0; i < SHA256_DIGEST_SIZE; ++i) {
        sprintf(chksum_cur, "%02x", hval[i]);
        chksum_cur += 2;
    }
   
    sprintf(buf, "      sha256: %s\n", chksum_str);
    strcat(yaml, buf);
    sprintf(buf, "      checksum_timing: %0.2f\n", duration);
    strcat(yaml, buf);

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

double get_ts() {
    struct timeval time;
    if (gettimeofday(&time,NULL)){
        //  Handle error
        return 0;
    }
    return (double)time.tv_sec + (double)time.tv_usec * .000001;
}
