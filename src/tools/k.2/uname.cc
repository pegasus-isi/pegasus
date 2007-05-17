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
#include "uname.hh"

static const char* RCS_ID =
"$Id$";

#if 0
static
size_t
mystrlen( const char* s )
{
  size_t i=0;
  while ( i < SYS_NMLN && s[i] ) ++i;
  return i;
}
#endif

static
char*
mytolower( char* s )
{
  // array version
  for ( size_t i=0; i < SYS_NMLN && s[i]; ++i ) s[i] = tolower(s[i]);
  return s;
}

static
std::string
convert( const char* s )
{
  size_t i=0;
  while ( i < SYS_NMLN && s[i] ) ++i;
  return std::string( s, i );
}

Uname::Uname()
{
  // find out where we run at (might stall for some time on DNS probs?)
  if ( uname( &m_uname ) == -1 ) {
    memset( &m_uname, 0, sizeof(m_uname) );
  } else {
    // downcase most things
    mytolower( m_uname.sysname );
    mytolower( m_uname.nodename );
    mytolower( m_uname.machine );
#ifdef _GNU_SOURCE
    mytolower( m_uname.domainname );
#endif
  }

  // now for the messy part, which needs adjustments for each and every
  // operating system architecture we run this on
#if defined(AIX)
  strncpy( m_archmode, "IBM", SYS_NMLN );
#elif defined(SUNOS)
#if defined(_LP64)
  strncpy( m_archmode, "LP64", SYS_NMLN );
#elif defined(_ILP32)
  strncpy( m_archmode, "ILP32", SYS_NMLN );
#else
  strncpy( m_archmode, "unknown SUN", SYS_NMLN );
#endif // SunOS architecture
#elif defined(LINUX) && #machine(i386)
  switch ( sizeof(int) ) {
  case 4:
    strncpy( m_archmode, "IA32", SYS_NMLN );
    break;
  case 8:
    strncpy( m_archmode, "IA64", SYS_NMLN );
    break;
  default:
    strncpy( m_archmode, "unknown LINUX", SYS_NMLN );
  }
#else // LINUX architecture
  strncpy( m_archmode, "unknown", SYS_NMLN );
#endif // SUNOS
}

Uname::~Uname()
{
  // empty
}

std::ostream& 
Uname::toXML( std::ostream& s, int indent, const char* nspace ) const
  // purpose: XML format a rusage info record onto a given stream
  // paramtr: s (IO): stream to put information into
  //          indent (IN): indentation level of tag
  //          nspace (IN): If defined, namespace prefix before element
  // returns: s
{
  // start <uname>
  s << XML::startElement( s, "uname", indent, nspace );

  // attributes
  s << " system=\"" << convert( m_uname.sysname ) << '"';
  if ( *m_archmode )
    s << " archmode=\"" << convert( m_archmode ) << '"';
  s << " nodename=\"" << convert( m_uname.nodename ) << '"';
  s << " release=\"" << convert( m_uname.release ) << '"';
  s << " machine=\"" << convert( m_uname.machine ) << '"';
  
#ifdef _GNU_SOURCE
  if ( *m_uname.domainname )
    s << " domainname=\"" << convert( m_uname.domainname ) << '"';
#endif

  s << '>' << convert( m_uname.version );
  s << XML::finalElement( "uname", 0, nspace );
  return s;
}

std::string
Uname::toXML( int indent, const char* nspace ) const
  // purpose: XML format a uname record. 
  // paramtr: indent (IN): indentation level of tag
  //          nspace (IN): If defined, namespace prefix before element
  // returns: string containing the element data
{
  // start <uname>
  std::string result( XML::startElement( "uname", indent, nspace ) );
  
  // attributes
  result += " system=\"" + convert( m_uname.sysname );
  if ( *m_archmode )
    result += "\" archmode=\"" + convert( m_archmode );
  result += "\" nodename=\"" + convert( m_uname.nodename );
  result += "\" release=\"" + convert( m_uname.release );
  result += "\" machine=\"" + convert( m_uname.machine );

#ifdef _GNU_SOURCE
  if ( *m_uname.domainname )
    result += "\" domainname=\"" + convert( m_uname.domainname );
#endif

  result += "\">" + convert( m_uname.version );

  result += XML::finalElement( "uname", 0, nspace );
  return result;
}
