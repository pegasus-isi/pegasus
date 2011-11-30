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
#include "useinfo.hh"
#include "time.hh"

#include <string.h>

static const char* RCS_ID =
"$Id$";

UseInfo::UseInfo()
  :m_tag()
  // purpose: empty ctor
{
  memset( &m_use, 0, sizeof(struct rusage) );
}

UseInfo::UseInfo( const char* tag, __rusage_who who )
  // purpose: construction from getrusage call
  // paramtr: tag (IN): name of the element
  //          who (IN): RUSAGE_SELF or RUSAGE_CHILDREN
{
  // recoded for SunCC
  if ( tag ) m_tag = tag;
  else throw null_pointer();

  memset( &m_use, 0, sizeof(struct rusage) );
  if ( who == RUSAGE_SELF || who == RUSAGE_CHILDREN ) {
    // may still fail
    getrusage( who, &m_use );
  }
}

UseInfo::UseInfo( const char* tag, const struct rusage* use )
  // purpose: ctor
  // paramtr: tag (IN): name of the element
  //          use (IN): Usage record to initialize to 
{
  // recoded for SunCC
  if ( tag ) m_tag = tag;
  else throw null_pointer();

  if ( use != &m_use ) memcpy( &m_use, use, sizeof(struct rusage) );
}

std::string
UseInfo::toXML( int indent, const char* nspace ) const
  // purpose: XML format a rusage info record into a given buffer
  // paramtr: buffer (IO): area to store the output in
  //          indent (IN): indentation level of tag
  // returns: buffer
{
  /* <usage> */
  std::string result( XML::startElement( m_tag, indent, nspace ) );

  result += XML::printf( " utime=\"%.3f\" stime=\"%.3f\""
			 " minflt=\"%lu\" majflt=\"%lu\""
			 " nswap=\"%lu\" nsignals=\"%lu\""
			 " nvcsw=\"%lu\" nivcsw=\"%lu\"/>\r\n",
			 Time::seconds(m_use.ru_utime), 
			 Time::seconds(m_use.ru_stime),
			 m_use.ru_minflt, m_use.ru_majflt, 
			 m_use.ru_nswap, m_use.ru_nsignals,
			 m_use.ru_nvcsw, m_use.ru_nivcsw );
  return result;
}

std::ostream& 
UseInfo::toXML( std::ostream& s, int indent, const char* nspace ) const
  // purpose: format content as XML onto stream
  // paramtr: s (IO): stream to put things on
  //          indent (IN): indentation depth, negative for none
  //          nspace (IN): tag namespace, if not null
  // returns: s
{
  // start element
  XML::startElement( s, m_tag, indent, nspace );

  // print attributes
  s << " utime=\"" << std::setprecision(3) << Time::seconds(m_use.ru_utime) << '"';
  s << " stime=\"" << std::setprecision(3) << Time::seconds(m_use.ru_stime) << '"';

  // no need to quote integers
  s << " minftl=\""	<< m_use.ru_minflt << '"';
  s << " majftl=\""	<< m_use.ru_majflt << '"';
  s << " nswap=\""	<< m_use.ru_nswap << '"';
  s << " nsignals=\""	<< m_use.ru_nsignals << '"';
  s << " nvcsw=\""	<< m_use.ru_nvcsw << '"';
  s << " nivcsw=\""	<< m_use.ru_nivcsw << '"';

  // finalize element
  return s << "/>\r\n";
}

UseInfo&
UseInfo::operator+=( const struct rusage* summand )
  // purpose: add another resource usage to the current record
  // paramtr: summand (IN): usage record to add
  // returns: current object, modified to be sum += summand
{
  // Total amount of user time used. 
  m_use.ru_utime += summand->ru_utime;

  // Total amount of system time used. 
  m_use.ru_stime += summand->ru_utime;

  // Maximum resident set size (in kilobytes).  
  m_use.ru_maxrss += summand->ru_maxrss;
  
  // Amount of sharing of text segment memory
  // with other processes (kilobyte-seconds). 
  m_use.ru_ixrss += summand->ru_ixrss;

  // Amount of data segment memory used (kilobyte-seconds). 
  m_use.ru_idrss += summand->ru_idrss;

  // Amount of stack memory used (kilobyte-seconds).  
  m_use.ru_isrss += summand->ru_isrss;

  // Number of soft page faults (i.e. those serviced by reclaiming
  // a page from the list of pages awaiting reallocation. 
  m_use.ru_minflt += summand->ru_minflt;

  // Number of hard page faults (i.e. those that required I/O). 
  m_use.ru_majflt += summand->ru_majflt;

  // Number of times a process was swapped out of physical memory.  
  m_use.ru_nswap += summand->ru_nswap;

  // Number of input operations via the file system.  Note: This
  // and `ru_oublock' do not include operations with the cache.
  m_use.ru_inblock += summand->ru_inblock;

  // Number of output operations via the file system. 
  m_use.ru_oublock += summand->ru_oublock;

  // Number of IPC messages sent. 
  m_use.ru_msgsnd += summand->ru_msgsnd;

  // Number of IPC messages received. 
  m_use.ru_msgrcv += summand->ru_msgrcv;
  
  // Number of signals delivered.
  m_use.ru_nsignals += summand->ru_nsignals;

  // Number of voluntary context switches, i.e. because the process
  // gave up the process before it had to (usually to wait for some
  // resource to be available). 
  m_use.ru_nvcsw += summand->ru_nvcsw;

  // Number of involuntary context switches, i.e. a higher priority process
  // became runnable or the current process used up its time slice. 
  m_use.ru_nivcsw += summand->ru_nivcsw;

  return *this;
}

UseInfo&
UseInfo::operator+=( const UseInfo& summand )
  // purpose: add another resource usage to the current record
  // paramtr: summand (IN): usage record to add
  // returns: current object, modified to be sum += summand
{
  return operator+=( &summand.m_use );
}

struct timeval&
operator+=( struct timeval& sum, const struct timeval& summand )
  // purpose: add one timeval structure to another w/ normalization
  // paramtr: sum (IO): first operand and result
  //          summand (IN): second operand
  // returns: sum
{
  sum.tv_usec += summand.tv_usec;
  sum.tv_sec  += summand.tv_sec;
  while ( sum.tv_usec >= 1000000 ) {
    sum.tv_sec++;
    sum.tv_usec -= 1000000;
  }

  return sum;
}

struct timeval
operator+( const struct timeval& a, const struct timeval& b )
  // purpose: adds two timeval structures
  // paramtr: a (IN): first operand (summand)
  //          b (IN): second operand (summand)
  // returns: normalized sum = a + b
{
  struct timeval result = a;
  return result += b;
}

UseInfo
operator+( const UseInfo& a, const UseInfo& b )
  // purpose: Add two useinfo records
  // paramtr: a (IN): first operand (summand)
  //          b (IN): second operand (summand)
  // returns: sum = a + b
{
  UseInfo result = a;
  return result += b;
}
