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
#include <errno.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>

#include "invoke.h"
#include "error.h"

static int append_arg(char* data, char*** arg, size_t* index, size_t* capacity) {
    /* purpose: adds a string to a list of arguments
     * paramtr: data (IN): string to append
     *          arg (OUT): list of arguments as vector
     *          index (IO): index where a new data should be inserted into
     *          capacity (IO): capacity (extend) of vector
     * returns: 0 means ok, -1 means error, see errno
     * warning: Always creates a strdup of data
     */ 

    if (*index >= *capacity) {
        *capacity <<= 1;
        *arg = realloc(*arg, *capacity * sizeof(char*));
        if (*arg == NULL) {
            printerr("realloc: %s\n", strerror(errno));
            return -1;
        }
        /* re-calloc: init new space with NULL */
        memset(*arg + *index, 0, sizeof(char*) * (*capacity - *index));
    }

    if (data == NULL) {
        (*arg)[(*index)++] = NULL;
    } else {
        char * temp = strdup(data);
        if (temp == NULL) {
            printerr("strdup: %s\n", strerror(errno));
            return -1;
        }
        (*arg)[(*index)++] = temp;
    }

    return 0;
}

static char* merge(char* s1, char* s2) {
    /* purpose: merge two strings and return the result
     * paramtr: s1 (IN): first string, may be NULL
     *          s2 (IN): second string, must not be NULL
     * returns: merge of strings into newly allocated area.
     *          NULL, if the allocation failed. 
     */
    char *temp;
    if (s1 == NULL) {
        temp = strdup(s2);
        if (temp == NULL) {
            printerr("strdup: %s\n", strerror(errno));
            return NULL;
        }
        return temp;
    } else {
        size_t len = strlen(s1) + strlen(s2) + 2;
        temp = (char*) malloc(len);
        if (temp == NULL) {
            printerr("malloc: %s\n", strerror(errno));
            return NULL;
        }
        strncpy(temp, s1, len);
        strncat(temp, s2, len);
        return temp;
    }
}

int expand_arg(const char* fn, char*** arg, size_t* index, size_t* capacity, int level) {
    /* purpose: adds the contents of a file, line by line, to an argument vector
     * paramtr: fn (IN): name of file with contents to append
     *          arg (OUT): list of arguments as vector
     *          index (IO): index where a new data should be inserted into
     *          capacity (IO): capacity (extend) of vector
     *          level (IN): level of recursion
     * returns: 0 means ok, -1 means error, see errno
     */
    FILE* f;
    char line[4096];
    size_t len;
    char* cmd = NULL;
    char* save = NULL;
    unsigned long lineno = 0ul;

    if (level >= 32) {
        printerr("ERROR: Nesting too deep (%d levels), "
                "circuit breaker triggered!\n", level);
        errno = EMLINK;
        return -1;
    }

    if ((f = fopen(fn, "r")) == NULL) {
        /* error while opening file for reading */
        return -1;
    }

    while (fgets(line, sizeof(line), f)) {
        ++lineno;

        /* check for skippable line */
        if (line[0] == 0 || line[0] == '\r' || line[0] == '\n') continue;

        /* check for unterminated line (larger than buffer) */
        len = strlen(line);
        if (line[len-1] != '\r' && line[len-1] != '\n') {
            /* read buffer was too small, save and append */
            char* temp = merge(save, line);
            if (temp == NULL) {
                /* error while merging strings */
                int saverr = errno;
                fclose(f);
                if (save != NULL) free((void*) save);
                errno = saverr;
                return -1;
            }

            if (save != NULL) free((void*) save);
            save = temp;
            lineno--;
            continue;
        } else {
            /* remove terminating character(s) */
            while (len > 0 && (line[len-1] == '\r' || line[len-1] == '\n')) {
                line[len-1] = 0;
                len--;
            } 
        }

        /* final assembly of argument */
        if (save != NULL) {
            /* assemble merged line */
            cmd = merge(save, line);
            free((void*) save);
            save = NULL;

            if (cmd == NULL) {
                /* error while merging strings */
                int saverr = errno;
                fclose(f);
                errno = saverr;
                return -1;
            }
        } else {
            /* no overlong lines */
            cmd = line;
        }

        if ((len=strlen(cmd)) > 0) {
            int result = append_arg(cmd, arg, index, capacity);

            if (result == -1) {
                int saverr = errno;
                fclose(f);
                if (cmd != line) free((void*) cmd);
                errno = saverr;
                return -1;
            }
        }

        /* done with this argument */
        if (cmd != line) free((void*) cmd);
    }

    fclose(f);
    return 0;
}

