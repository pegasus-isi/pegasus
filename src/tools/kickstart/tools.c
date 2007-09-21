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
#include "tools.h"
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <stdarg.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

static const char* RCS_ID =
"$Id$";

void
full_append( char* buffer, const size_t size, size_t* len, 
	     const char* msg, size_t msglen )
/* purpose: append a binary message to the buffer while maintaining length information.
 * paramtr: buffer (IO): buffer area to put strings into
 *          size (IN): capacity of buffer
 *          len (IO): current end of buffer, updated on return
 *          msg (IN): message to append to buffer
 *          mlen (IN): length of message area to append
 * returns: nada 
 */          
{
  if ( *len + msglen + 1 < size ) {
    /* JSV 20070921: msglen may be smaller than strlen(msg) ! */
    strncat( buffer + *len, msg, msglen );
    *len += msglen;
  } else {
    strncat( buffer + *len, msg, size - *len - 1 );
    *len += strlen( buffer + *len );
  }
}

#if 0
void
append( char* buffer, const size_t size, size_t* len, 
	const char* msg )
/* purpose: append a string to the buffer while maintaining length information.
 * paramtr: buffer (IO): buffer area to put strings into
 *          size (IN): capacity of buffer
 *          len (IO): current end of buffer, updated on return
 *          msg (IN): message to append to buffer
 * returns: nada 
 */          
{
  full_append( buffer, size, len, msg, strlen(msg) );
}
#endif

static const char* iso88591lookup[256] = 
{
  "&#xe000;", "&#xe001;", "&#xe002;", "&#xe003;", "&#xe004;", "&#xe005;", "&#xe006;", "&#xe007;", 
  "&#xe008;",       "\t",       "\n", "&#xe00b;", "&#xe00c;",       "\r", "&#xe00e;", "&#xe00f;", 
  "&#xe010;", "&#xe011;", "&#xe012;", "&#xe013;", "&#xe014;", "&#xe015;", "&#xe016;", "&#xe017;", 
  "&#xe018;", "&#xe019;", "&#xe01a;", "&#xe01b;", "&#xe01c;", "&#xe01d;", "&#xe01e;", "&#xe01f;", 

         " ",        "!",   "&quot;",        "#",        "$",        "%",    "&amp;",   "&apos;", 
         "(",        ")",        "*",        "+",        ",",        "-",        ".",        "/", 
	 "0",        "1",        "2",        "3",        "4",        "5",        "6",        "7", 
	 "8",        "9",        ":",        ";",     "&lt;",        "=",     "&gt;",        "?", 

	 "@",        "A",        "B",        "C",        "D",        "E",        "F",        "G", 
	 "H",        "I",        "J",        "K",        "L",        "M",        "N",        "O", 
	 "P",        "Q",        "R",        "S",        "T",        "U",        "V",        "W", 
	 "X",        "Y",        "Z",        "[",       "\\",        "]",        "^",        "_", 

	 "`",        "a",        "b",        "c",        "d",        "e",        "f",        "g", 
	 "h",        "i",        "j",        "k",        "l",        "m",        "n",        "o", 
	 "p",        "q",        "r",        "s",        "t",        "u",        "v",        "w", 
	 "x",        "y",        "z",        "{",        "|",        "}",        "~", "&#xe07f;", 

  "&#xe080;", "&#xe081;", "&#xe082;", "&#xe083;", "&#xe084;", "&#xe085;", "&#xe086;", "&#xe087;", 
  "&#xe088;", "&#xe089;", "&#xe08a;", "&#xe08b;", "&#xe08c;", "&#xe08d;", "&#xe08e;", "&#xe08f;", 
  "&#xe090;", "&#xe091;", "&#xe092;", "&#xe093;", "&#xe094;", "&#xe095;", "&#xe096;", "&#xe097;", 
  "&#xe098;", "&#xe099;", "&#xe09a;", "&#xe09b;", "&#xe09c;", "&#xe09d;", "&#xe09e;", "&#xe09f;", 

	 " ",        "¡",        "¢",        "£",        "¤",        "¥",        "¦",        "§", 
	 "¨",        "©",        "ª",        "«",        "¬",        "­",        "®",        "¯", 
	 "°",        "±",        "²",        "³",        "´",        "µ",        "¶",        "·", 
	 "¸",        "¹",        "º",        "»",        "¼",        "½",        "¾",        "¿", 

	 "À",        "Á",        "Â",        "Ã",        "Ä",        "Å",        "Æ",        "Ç", 
	 "È",        "É",        "Ê",        "Ë",        "Ì",        "Í",        "Î",        "Ï", 
	 "Ð",        "Ñ",        "Ò",        "Ó",        "Ô",        "Õ",        "Ö",        "×", 
	 "Ø",        "Ù",        "Ú",        "Û",        "Ü",        "Ý",        "Þ",        "ß", 

	 "à",        "á",        "â",        "ã",        "ä",        "å",        "æ",        "ç", 
	 "è",        "é",        "ê",        "ë",        "ì",        "í",        "î",        "ï", 
	 "ð",        "ñ",        "ò",        "ó",        "ô",        "õ",        "ö",        "÷", 
	 "ø",        "ù",        "ú",        "û",        "ü",        "ý",        "þ", "&#xe0ff;"
};

void
xmlquote( char* buffer, const size_t size, size_t* len,
	  const char* msg, size_t msglen )
/* purpose: append a possibly binary message to the buffer while XML
 *          quoting and maintaining buffer length information.
 * paramtr: buffer (IO): buffer area to put strings into
 *          size (IN): capacity of buffer
 *          len (IO): current end of buffer, updated on return
 *          msg (IN): message to append to buffer
 *          mlen (IN): length of message area to append
 * returns: nada 
 */          
{
  size_t i, tsize = size-2;
  for ( i=0; i < msglen; ++i ) {
    append( buffer, tsize, len, iso88591lookup[ (unsigned char) msg[i] ] );
  }
}

void
myprint( char* buffer, const size_t size, size_t* len, 
	 const char* fmt, ... )
/* purpose: format a string at the end of a buffer while maintaining length information.
 * paramtr: buffer (IO): buffer area to put strings into
 *          size (IN): capacity of buffer
 *          len (IO): current end of buffer, updated on return
 *          fmt (IN): printf compatible format
 *          ... (IN): parameters to format
 * returns: nada 
 */          
{
  va_list ap;
  va_start( ap, fmt );

  vsnprintf( buffer + *len, size - *len, fmt, ap );
  *len += strlen(buffer + *len);

  va_end(ap);
}

size_t
mydatetime( char* buffer, const size_t size, size_t* offset,
	    int isLocal, int isExtended, time_t seconds, long micros )
/* purpose: append an ISO timestamp to a buffer
 * paramtr: buffer (IO): buffer area to store things into
 *          size (IN): capacity of buffer
 *          offset (IO): current position of end of meaningful buffer
 *          isLocal (IN): flag, if 0 use UTC, otherwise use local time
 *          isExtd (IN): flag, if 0 use concise format, otherwise extended
 *          seconds (IN): tv_sec part of timeval
 *          micros (IN): if negative, don't show micros.
 * returns: number of characters added
 */
{
  char line[32];
  size_t len;
  struct tm zulu;
  memcpy( &zulu, gmtime(&seconds), sizeof(struct tm) );

  if ( isLocal ) {
    /* local time requires that we state the offset */
    int hours, minutes;
    time_t distance;

    struct tm local;
    memcpy( &local, localtime(&seconds), sizeof(struct tm) );

    zulu.tm_isdst = local.tm_isdst;
    distance = seconds - mktime(&zulu);
    hours = distance / 3600;
    minutes = abs(distance) % 60;

    strftime( line, sizeof(line), 
	      isExtended ? "%Y-%m-%dT%H:%M:%S" : "%Y%m%dT%H%M%S", &local );
    len = strlen(line);

    if ( micros < 0 )
      myprint( line, sizeof(line), &len, "%+03d:%02d", hours, minutes );
    else
      myprint( line, sizeof(line), &len, 
	       isExtended ? ".%03ld%+03d:%02d" : ".%03ld%+03d%02d",
	       micros / 1000, hours, minutes );
  } else {
    /* zulu time aka UTC */
    strftime( line, sizeof(line), 
	      isExtended ? "%Y-%m-%dT%H:%M:%S" : "%Y%m%dT%H%M%S", &zulu );
    len = strlen(line);

    if ( micros < 0 ) 
      append( line, sizeof(line), &len, "Z" );
    else
      myprint( line, sizeof(line), &len, ".%03ldZ", micros / 1000 );
  }

  append( buffer, size, offset, line );
  return len;
}

double
mymaketime( struct timeval t )
/* purpose: convert a structured timeval into seconds with fractions.
 * paramtr: t (IN): a timeval as retured from gettimeofday().
 * returns: the number of seconds with microsecond fraction. */
{
  return ( t.tv_sec + t.tv_usec / 1E6 );
}

void
now( struct timeval* t )
/* purpose: capture a point in time with microsecond extension 
 * paramtr: t (OUT): where to store the captured time
 */
{
  int timeout = 0;
  t->tv_sec = -1;
  t->tv_usec = 0;
  while ( gettimeofday( t, 0 ) == -1 && timeout < 10 ) timeout++;
}

static
int
isDir( const char* tmp )
/* purpose: Check that the given dir exists and is writable for us
 * paramtr: tmp (IN): designates a directory location
 * returns: true, if tmp exists, isa dir, and writable
 */
{
  struct stat st;
  if ( stat( tmp, &st ) == 0 && S_ISDIR(st.st_mode) ) {
    /* exists and isa directory */
    if ( (geteuid() != st.st_uid || (st.st_mode & S_IWUSR) == 0) &&
	 (getegid() != st.st_gid || (st.st_mode & S_IWGRP) == 0) &&
	 ((st.st_mode & S_IWOTH) == 0) ) {
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

const char*
getTempDir( void )
/* purpose: determine a suitable directory for temporary files.
 * warning: remote schedulers may chose to set a different TMP..
 * returns: a string with a temporary directory, may still be NULL.
 */
{
  char* tempdir = getenv("GRIDSTART_TMP");
  if ( tempdir != NULL && isDir(tempdir) ) return tempdir;

  tempdir = getenv("TMP");
  if ( tempdir != NULL && isDir(tempdir) ) return tempdir;

  tempdir = getenv("TEMP");
  if ( tempdir != NULL && isDir(tempdir) ) return tempdir;

  tempdir = getenv("TMPDIR");
  if ( tempdir != NULL && isDir(tempdir) ) return tempdir;

#ifdef P_tmpdir /* in stdio.h */
  tempdir = P_tmpdir;
  if ( tempdir != NULL && isDir(tempdir) ) return tempdir;
#endif

  tempdir = "/tmp";
  if ( isDir(tempdir) ) return tempdir;

  tempdir = "/var/tmp";
  if ( isDir(tempdir) ) return tempdir;

  /* whatever we have by now is it - may still be NULL */
  return tempdir;
}

char*
sizer( char* buffer, size_t capacity, size_t vsize, const void* value )
/* purpose: format an unsigned integer of less-known size. Note that
 *          64bit ints on 32bit systems need %llu, but 64/64 uses %lu
 * paramtr: buffer (IO): area to output into
 *          capacity (IN): extent of the buffer to store things into
 *          vsize (IN): size of the value
 *          value (IN): value to format
 * returns: buffer
 */
{
  switch ( vsize ) {
  case 2:
    snprintf( buffer, capacity, "%hu", 
              *((const short unsigned*) value) );
    break;
  case 4:
    if ( sizeof(long) == 4 ) 
      snprintf( buffer, capacity, "%lu", 
                *((const long unsigned*) value) );
    else 
      snprintf( buffer, capacity, "%u", 
                *((const unsigned*) value) );
    break;
  case 8:
    if ( sizeof(long) == 4 ) {
      snprintf( buffer, capacity, "%llu", 
                *((const long long unsigned*) value) );
    } else {
      snprintf( buffer, capacity, "%lu", 
                *((const long unsigned*) value) );
    }
    break;
  default:
    snprintf( buffer, capacity, "unknown" );
    break;
  }

  return buffer;
}

