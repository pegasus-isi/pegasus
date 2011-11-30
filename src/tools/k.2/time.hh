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
#ifndef _CHIMERA_TIME_HH
#define _CHIMERA_TIME_HH

#include <math.h>
#include <time.h>
#include <sys/time.h>

#include <string>
#include <iostream>

class Time {
  // encapsulate time-specific functionality. While instances of this
  // class allow to capture moments in time, the static members provide
  // various time-related conversion functions.
private:
  struct timeval m_tv;
  bool           m_local;
  bool           m_extended;

public: // static members
  static bool	 c_extended;	// timestamp format concise or extended
  static bool	 c_local;	// timestamp time zone, UTC or local
  static const Time c_epoch;	// Unix epoch as Time constant

public:
  inline Time()
    :m_tv( Time::now() ), 
     m_local(Time::c_local),
     m_extended(Time::c_extended) { }

  inline Time( const struct timeval* tv,
	       bool isLocal = Time::c_local,
	       bool isExtended = Time::c_extended )
    :m_tv( tv ? *tv : Time::now() ), 
     m_local(isLocal),
     m_extended(isExtended) { }

  inline Time( const struct timeval& tv, 
	       bool isLocal = Time::c_local,
	       bool isExtended = Time::c_extended )
    :m_tv(tv), m_local(isLocal), m_extended(isExtended) { }

  inline Time( time_t tv, 
	       long micros,
	       bool isLocal = Time::c_local,
	       bool isExtended = Time::c_extended )
    :m_local(isLocal), m_extended(isExtended) 
  { m_tv.tv_sec = tv; m_tv.tv_usec = micros; }

  //
  // Accessors
  //
  inline bool getLocal() const		{ return m_local; }
  inline void setLocal( bool isLocal )	{ m_local = isLocal; }
  inline bool getExtended() const	{ return m_extended; }
  inline void setExtended( bool isExt ) { m_extended = isExt; }

  // naughty
  inline operator std::string() const { return this->date(); }

  //
  // Member functions
  //
  inline double seconds() const
    // purpose: convert a timeval into a duration as seconds since epoch.
    // returns: a double containing information since epoch.
  { return Time::seconds(m_tv); }

  inline double elapsed( const struct timeval* tv = 0 ) const
    // purpose: Determines the elapsed time since the time was taken
    // paramtr: tv (IN): timestamp structure, may be NULL for current moment
    // returns: the difference := tv - this
  { return fabs(Time::seconds(tv?*tv:Time::now()) - Time::seconds(m_tv)); }

  inline double elapsed( const struct timeval& tv ) const
    // purpose: Determines the elapsed time since the time was taken
    // paramtr: tv (IN): timestamp structure
    // returns: the difference := tv - this
  { return fabs(Time::seconds(tv) - Time::seconds(m_tv)); }

  inline double elapsed( const Time& t ) const
    // purpose: Determines the elapsed time since the time was taken
    // paramtr: tv (IN): other time instance
    // returns: the difference := tv - this
  { return fabs(Time::seconds(t.m_tv) - Time::seconds(m_tv)); }

  inline std::string date( bool micros = true ) const
    // purpose: create an ISO timestamp
    // returns: a string with the formatted ISO 8601 timestamp
  { return Time::date( m_tv.tv_sec, micros ? m_tv.tv_usec : -1, 
		       m_local, m_extended ); }
  
public: // static functions
  inline static double seconds( const struct timeval* tv )
    // purpose: convert a timeval into a duration as seconds since epoch.
    // paramtr: tv (IN): timeval structure with a duration or timestamp
    // returns: a double containing information since epoch.
  { return ( tv->tv_sec + tv->tv_usec / 1E6 ); }

  inline static double seconds( const struct timeval& tv )
    // purpose: convert a timeval into a duration as seconds since epoch.
    // paramtr: tv (IN): timeval structure with a duration or timestamp
    // returns: a double containing information since epoch.
  { return ( tv.tv_sec + tv.tv_usec / 1E6 ); }

  static struct timeval now();
    // purpose: capture a point in time with microsecond extension 
    // returns: a time record

  static std::string date( time_t seconds, long micros = -1,
			   bool isLocal = Time::c_local, 
			   bool isExtended = Time::c_extended );
    // purpose: create an ISO timestamp
    // paramtr: seconds (IN): tv_sec part of timeval
    //          micros (IN): if negative, don't show micros.
    //          isLocal (IN): flag, if 0 use UTC, otherwise use local time
    //          isExtd (IN): flag, if 0 use concise format, otherwise extended
    // returns: a string with the formatted ISO 8601 timestamp

  friend std::ostream& operator<<( std::ostream& s, const Time& t );
};

inline std::ostream& operator<<( std::ostream& s, const Time& t )
{ return s << t.date(); }

#endif // _CHIMERA_TIME_HH
