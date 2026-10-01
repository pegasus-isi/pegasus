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
        c == 0x85 || (c >= 0xA0 && c <= 0xD7FF) || (c >= 0xE000 && c <= 0xFFFD) ||
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

int yamlgetutf8(FILE *in, wint_t *out) {
    /* purpose: read the next code point from a byte stream, decoding UTF-8
     *          by hand instead of relying on the locale-aware fgetwc(). Under
     *          a C/POSIX locale, glibc's fgetwc() treats any byte >= 0x80 as
     *          an invalid multibyte sequence and returns WEOF -- indistin-
     *          guishable from real end-of-file -- which silently truncates
     *          callers that loop on it (see GH-2250).
     * paramtr: in (IO): stream to read from
     *          out (OUT): decoded code point; the raw leading byte if this
     *                     call returns 2
     * returns: 0 at end of stream, 1 if a code point was decoded, 2 if the
     *          leading byte started an invalid/incomplete UTF-8 sequence
     *          (the caller should skip it and keep reading)
     */
    int b0 = fgetc(in);
    if (b0 == EOF) return 0;

    if ((b0 & 0x80) == 0x00) {
        *out = (wint_t) b0;
        return 1;
    }

    int extra, i, b;
    wint_t c, min;
    if ((b0 & 0xE0) == 0xC0)      { extra = 1; min = 0x80;    c = b0 & 0x1F; }
    else if ((b0 & 0xF0) == 0xE0) { extra = 2; min = 0x800;   c = b0 & 0x0F; }
    else if ((b0 & 0xF8) == 0xF0) { extra = 3; min = 0x10000; c = b0 & 0x07; }
    else {
        *out = (wint_t) b0;
        return 2;
    }

    for (i = 0; i < extra; ++i) {
        b = fgetc(in);
        if (b == EOF || (b & 0xC0) != 0x80) {
            if (b != EOF) ungetc(b, in);
            *out = (wint_t) b0;
            return 2;
        }
        c = (c << 6) | (b & 0x3F);
    }

    if (c < min || c > 0x10FFFF || (c >= 0xD800 && c <= 0xDFFF)) {
        /* overlong encoding, out of Unicode range, or a surrogate half
         * (surrogates are never valid in UTF-8 -- only CESU-8/WTF-8 encode
         * them, and yamlprintable() would silently drop them anyway) */
        *out = (wint_t) b0;
        return 2;
    }

    *out = c;
    return 1;
}

static void yamlpututf8(FILE *out, wint_t c) {
    /* purpose: write a code point to the stream as UTF-8, by hand, instead
     *          of relying on the locale-aware fprintf("%lc", ...), which
     *          under a C/POSIX locale cannot encode anything outside ASCII
     *          and silently drops it.
     * paramtr: out (IO): stream to write to
     *          c (IN): code point to encode
     * returns: nada
     */
    if (c < 0x80) {
        fputc((int) c, out);
    } else if (c < 0x800) {
        fputc((int) (0xC0 | (c >> 6)), out);
        fputc((int) (0x80 | (c & 0x3F)), out);
    } else if (c < 0x10000) {
        fputc((int) (0xE0 | (c >> 12)), out);
        fputc((int) (0x80 | ((c >> 6) & 0x3F)), out);
        fputc((int) (0x80 | (c & 0x3F)), out);
    } else {
        fputc((int) (0xF0 | (c >> 18)), out);
        fputc((int) (0x80 | ((c >> 12) & 0x3F)), out);
        fputc((int) (0x80 | ((c >> 6) & 0x3F)), out);
        fputc((int) (0x80 | (c & 0x3F)), out);
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
    int first_line = 1;
    int status;
    while ((status = yamlgetutf8(in, &c)) != 0) {
        /* undecodable byte: skip it, but keep dumping the rest of the
         * stream instead of stopping there (see GH-2250) */
        if (status == 2) continue;

        /* first line can not have leading white spaces */
        if (first_line && (
              c == 0x20 ||
              c == 0x9  ||
              c == 0xD  )) {
            continue;
        }

        /* newline or cr maps to a new line. YAML 1.1 (unlike 1.2) also
         * treats NEL (U+0085), LS (U+2028) and PS (U+2029) as line breaks;
         * passing them through raw would let a job emit one of these and
         * de-indent whatever follows right out of this literal block, so
         * they get the same re-indented newline treatment as LF/CR. */
        if (c == 0xA || c == 0xD || c == 0x85 || c == 0x2028 || c == 0x2029) {
            fprintf(out, "\n%*s", indent, "");
        }
        else if (yamlprintable(c)) {
            yamlpututf8(out, c);
            first_line = 0;
        }
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
     * paramtr: t (IN): a timeval as returned from gettimeofday().
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
