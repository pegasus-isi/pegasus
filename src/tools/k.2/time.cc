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
#include "time.hh"

static const char* RCS_ID =
"$Id: time.cc,v 1.4 2005/04/08 19:40:51 griphyn Exp $";

bool Time::c_local = true;
bool Time::c_extended = true;

const Time
Time::c_epoch( 0, 0, Time::c_local, Time::c_extended );

std::string 
Time::date( time_t seconds, long micros, bool isLocal, bool isExtended )
  // purpose: create an ISO timestamp
  // paramtr: seconds (IN): tv_sec part of timeval
  //          micros (IN): if negative, don't show micros.
  //          isLocal (IN): flag, if 0 use UTC, otherwise use local time
  //          isExtd (IN): flag, if 0 use concise format, otherwise extended
  // returns: a string with the formatted ISO 8601 timestamp
{
  std::string result;
  char line[32];
  struct tm zulu = *gmtime(&seconds);

  result.reserve(32);
  if ( isLocal ) {
    // requirement that we attach our time zone offset 
    struct tm local = *localtime(&seconds);
    zulu.tm_isdst = local.tm_isdst;
    time_t distance = (seconds - mktime(&zulu)) / 60;
    int hours = distance / 60;
    // Solaris does not like std::abs(int) vs std::abs(double)
    int minutes = distance < 0 ? -distance % 60 : distance % 60;
    
    // timestamp
    strftime( line, sizeof(line),
	      isExtended ? "%Y-%m-%dT%H:%M:%S" : "%Y%m%dT%H%M%S", &local );
    result += line;

    // show microseconds
    if ( micros >= 0 ) {
      snprintf( line, sizeof(line), ".%03ld", micros / 1000 );
      result += line;
    }

    // show timezone offset
    snprintf( line, sizeof(line), 
	      isExtended ? "%+03d:%02d" : "%+03d%02d",
	      hours, minutes );
    result += line;
  } else {
    // zulu time aka UTC
    strftime( line, sizeof(line), 
	      isExtended ? "%Y-%m-%dT%H:%M:%S" : "%Y%m%dT%H%M%S", &zulu );
    result += line;

    // show microseconds
    if ( micros >= 0 ) {
      snprintf( line, sizeof(line), ".%03ld", micros / 1000 );
      result += line;
    }

    // show timezone zulu
    result += 'Z';
  }

  return result;
}

struct timeval
Time::now( void )
  // purpose: capture a point in time with microsecond extension 
  // returns: a time record
{
  struct timeval t = { -1, 0 };
  int timeout = 0;
  while ( gettimeofday( &t, 0 ) == -1 && timeout < 10 ) timeout++;
  return t;
}
