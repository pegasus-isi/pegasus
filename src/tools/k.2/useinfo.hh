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
#ifndef _USEINFO_HH
#define _USEINFO_HH

#include <sys/types.h>
#include <sys/resource.h>

#include <exception>
#include <string>
#include "xml.hh"

#ifndef HAS_RUSAGE_WHO
#define __rusage_who int
#endif

#include "null.hh"

class UseInfo : public XML {
  // This class encapsulated a rusage record. The default copy ctor and
  // assignment operator are safe to employ.
public:
  UseInfo( const char* tag, __rusage_who who );
    // purpose: construction from getrusage call
    // paramtr: tag (IN): name of the element
    //          who (IN): RUSAGE_SELF or RUSAGE_CHILDREN

  UseInfo( const char* tag, const struct rusage* use );
    // purpose: ctor
    // paramtr: tag (IN): name of the element
    //          use (IN): Usage record to initialize to 

  virtual std::string toXML( int indent = 0, 
			     const char* nspace = 0 ) const;
    // purpose: XML format a rusage record. 
    // paramtr: indent (IN): indentation level of tag
    //          nspace (IN): If defined, namespace prefix before element
    // returns: string containing the element data

  virtual std::ostream& toXML( std::ostream& s, 
			       int indent = 0,
			       const char* nspace = 0 ) const;
    // purpose: XML format a rusage info record onto a given stream
    // paramtr: s (IO): stream to put information into
    //          indent (IN): indentation level of tag
    //          nspace (IN): If defined, namespace prefix before element
    // returns: s

  UseInfo& operator+=( const UseInfo& summand );
    // purpose: add another resource usage to the current record
    // paramtr: summand (IN): usage record to add
    // returns: current object, modified to be sum += summand

  UseInfo& operator+=( const struct rusage* summand );
    // purpose: add another resource usage to the current record
    // paramtr: summand (IN): usage record to add
    // returns: current object, modified to be sum += summand

  virtual ~UseInfo() { };
    // purpose: dtor

private:
  std::string    m_tag;
  struct rusage  m_use;

  // render inaccessible
  UseInfo();
};

struct timeval&
operator+=( struct timeval& sum, const struct timeval& summand );
  // purpose: add one timeval structure to another w/ normalization
  // paramtr: sum (IO): first operand and result
  //          summand (IN): second operand
  // returns: sum

struct timeval
operator+( const struct timeval& a, const struct timeval& b );
  // purpose: adds two timeval structures
  // paramtr: a (IN): first operand (summand)
  //          b (IN): second operand (summand)
  // returns: normalized sum = a + b


UseInfo
operator+( const UseInfo& a, const UseInfo& b );
  // purpose: Add two useinfo records
  // paramtr: a (IN): first operand (summand)
  //          b (IN): second operand (summand)
  // returns: sum = a + b

#endif // _USEINFO_HH
