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
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <stdarg.h>
#include <time.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <errno.h>
#include <fcntl.h>
#include <utime.h>
#include <sys/poll.h>
#include <wchar.h>
#include <locale.h>

#include "utils.h"

static const char* asciilookup[128] = {
          "",         "",         "",         "",         "",         "",         "",         "",
          "",      "\\t",      "\\n",         "",         "",      "\\r",         "",         "",
          "",         "",         "",         "",         "",         "",         "",         "",
          "",         "",         "",         "",         "",         "",         "",         "",
         " ",        "!",     "\\\"",        "#",        "$",        "%",        "&",        "'",
         "(",        ")",        "*",        "+",        ",",        "-",        ".",        "/",
         "0",        "1",        "2",        "3",        "4",        "5",        "6",        "7",
         "8",        "9",        ":",        ";",        "<",        "=",        ">",        "?",
         "@",        "A",        "B",        "C",        "D",        "E",        "F",        "G",
         "H",        "I",        "J",        "K",        "L",        "M",        "N",        "O",
         "P",        "Q",        "R",        "S",        "T",        "U",        "V",        "W",
         "X",        "Y",        "Z",        "[",     "\\\\",        "]",        "^",        "_",
         "`",        "a",        "b",        "c",        "d",        "e",        "f",        "g",
         "h",        "i",        "j",        "k",        "l",        "m",        "n",        "o",
         "p",        "q",        "r",        "s",        "t",        "u",        "v",        "w",
         "x",        "y",        "z",        "{",        "|",        "}",        "~",        ""
};


int yamlprintable(wint_t c)
{
    /* see https://yaml.org/spec/1.2/spec.html#id2770814 */
    if (c == 0x9 || c == 0xA || c == 0xD || (c >= 0x20 && c <= 0x7E) ||
        c == 0x85 || (c >= 0xA0 && c <= 0xD7FF) || (c <= 0xE000 && c >= 0xFFFD) ||
        (c >= 0x10000 && c <= 0x10FFFF)) {
        return 1;
    }
    return 0;
}


void yamlquote(FILE *out, const char* msg, size_t msglen) {
    /* purpose: write a possibly binary message to the stream with YAML
     * paramtr: out (IO): stream to write the quoted yaml to
     *          msg (IN): message to append to buffer
     *          mlen (IN): length of message area to append
     * returns: nada
     */
    size_t i;
    for (i=0; i<msglen; ++i) {
        /* We assume that all the characters that need to be escaped fall
         * in the ASCII range. Anything outside that range, we assume to
         * be UTF-8 encoded.
         */
        unsigned char j = (unsigned char) msg[i];
        if (j < 128) {
            fputs(asciilookup[j], out);
        } else {
            /* FIXME */
            fputc(msg[i], out);
        }
    }
}

void yamldump(FILE *in, FILE *out, const int indent) {
    /* purpose: write a stream to yaml as a literal`
     * paramtr: out (IO): stream to write the quoted xml to
     *          indent: indentation of the lines
     *          msg (IN): message to append to buffer
     *          mlen (IN): length of message area to append
     * returns: nada
     */
    wint_t c;
    while ((c = fgetwc(in)) != WEOF)
        if (c == 0xA) {
            fprintf(out, "\n%*s", indent, "");
        }
        else if (yamlprintable(c)) {
            fprintf(out, "%lc", c);
        }
}

static int isExtended = 1;     /* timestamp format concise or extended */
static int isLocal = 1;        /* timestamp time zone, UTC or local */
static char __isodate[32];

char * fmtisodate(time_t seconds, long micros) {
    /* purpose: return an ISO-formatted string for a given timestamp
     * paramtr: isLocal (IN): flag, if 0 use UTC, otherwise use local time
     *          isExtd (IN): flag, if 0 use concise format, otherwise extended
     *          seconds (IN): tv_sec part of timeval
     *          micros (IN): if negative, don't show micros.
     * returns: a pointer to the formatted string
     */
    size_t len;
    struct tm zulu;
    memcpy(&zulu, gmtime(&seconds), sizeof(struct tm));

    if (isLocal) {
        /* local time requires that we state the offset */
        int hours, minutes;
        time_t distance;

        struct tm local;
        memcpy(&local, localtime(&seconds), sizeof(struct tm));

        zulu.tm_isdst = local.tm_isdst;
        distance = seconds - mktime(&zulu);
        hours = distance / 3600;
        minutes = labs(distance) % 60;

        strftime(__isodate, sizeof(__isodate),
                 isExtended ? "%Y-%m-%dT%H:%M:%S" : "%Y%m%dT%H%M%S", &local);
        len = strlen(__isodate);

        if (micros < 0) {
            snprintf(__isodate+len, sizeof(__isodate)-len,
                     "%+03d:%02d", hours, minutes);
        } else {
            snprintf(__isodate+len, sizeof(__isodate)-len,
                     isExtended ? ".%03ld%+03d:%02d" : ".%03ld%+03d%02d",
                     micros / 1000, hours, minutes);
        }
    } else {
        /* zulu time aka UTC */
        strftime(__isodate, sizeof(__isodate),
                 isExtended ? "%Y-%m-%dT%H:%M:%S" : "%Y%m%dT%H%M%S", &zulu);
        len = strlen(__isodate);

        if (micros < 0) {
            snprintf(__isodate+len, sizeof(__isodate)-len, "Z");
        } else {
            snprintf(__isodate+len, sizeof(__isodate)-len, ".%03ldZ", micros/1000);
        }
    }

    return __isodate;
}

double doubletime(struct timeval t) {
    /* purpose: convert a structured timeval into seconds with fractions.
     * paramtr: t (IN): a timeval as retured from gettimeofday().
     * returns: the number of seconds with microsecond fraction. */
    return (t.tv_sec + t.tv_usec / 1E6);
}

void now(struct timeval* t) {
    /* purpose: capture a point in time with microsecond extension 
     * paramtr: t (OUT): where to store the captured time
     */
    int timeout = 0;
    t->tv_sec = -1;
    t->tv_usec = 0;
    while (gettimeofday(t, 0) == -1 && timeout < 10) timeout++;
}

static int isWriteableDir(const char* tmp) {
    /* purpose: Check that the given dir exists and is writable for us
     * paramtr: tmp (IN): designates a directory location
     * returns: true, if tmp exists, isa dir, and writable
     */
    struct stat st;
    if (stat(tmp, &st) == 0 && S_ISDIR(st.st_mode)) {
        /* exists and isa directory */
        if ((geteuid() != st.st_uid || (st.st_mode & S_IWUSR) == 0) &&
            (getegid() != st.st_gid || (st.st_mode & S_IWGRP) == 0) &&
            ((st.st_mode & S_IWOTH) == 0)) {

            /* not writable to us */
            return 0;
        } else {
            /* yes, writable dir for us */
            return 1;
        }
    } else {
        /* location does not exist, or is not a directory */
        return 0;
    }
}

const char* getTempDir(void) {
    /* purpose: determine a suitable directory for temporary files.
     * warning: remote schedulers may chose to set a different TMP..
     * returns: a string with a temporary directory, may still be NULL.
     */
    char* tempdir = getenv("KICKSTART_TMP");
    if (tempdir != NULL && isWriteableDir(tempdir)) return tempdir;

    tempdir = getenv("GRIDSTART_TMP");
    if (tempdir != NULL && isWriteableDir(tempdir)) return tempdir;

    tempdir = getenv("TMP");
    if (tempdir != NULL && isWriteableDir(tempdir)) return tempdir;

    tempdir = getenv("TEMP");
    if (tempdir != NULL && isWriteableDir(tempdir)) return tempdir;

    tempdir = getenv("TMPDIR");
    if (tempdir != NULL && isWriteableDir(tempdir)) return tempdir;

#ifdef P_tmpdir /* in stdio.h */
    tempdir = P_tmpdir;
    if (tempdir != NULL && isWriteableDir(tempdir)) return tempdir;
#endif

    tempdir = "/tmp";
    if (isWriteableDir(tempdir)) return tempdir;

    tempdir = "/var/tmp";
    if (isWriteableDir(tempdir)) return tempdir;

    /* whatever we have by now is it - may still be NULL */
    return tempdir;
}

char* sizer(char* buffer, size_t capacity, size_t vsize, const void* value) {
    /* purpose: format an unsigned integer of less-known size. Note that
     *          64bit ints on 32bit systems need %llu, but 64/64 uses %lu
     * paramtr: buffer (IO): area to output into
     *          capacity (IN): extent of the buffer to store things into
     *          vsize (IN): size of the value
     *          value (IN): value to format
     * returns: buffer
     */
    switch (vsize) {
        case 2:
            snprintf(buffer, capacity, "%hu", *((const short unsigned*) value));
            break;
        case 4:
            if (sizeof(long) == 4) {
                snprintf(buffer, capacity, "%lu", *((const long unsigned*) value));
            } else {
                snprintf(buffer, capacity, "%u", *((const unsigned*) value));
            }
            break;
        case 8:
            if (sizeof(long) == 4) {
                snprintf(buffer, capacity, "%llu", *((const long long unsigned*) value));
            } else {
                snprintf(buffer, capacity, "%lu", *((const long unsigned*) value));
            }
            break;
        default:
            snprintf(buffer, capacity, "unknown");
            break;
    }

    return buffer;
}

