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
#ifndef _CHIMERA_UNAME_HH
#define _CHIMERA_UNAME_HH

#include <sys/types.h>
#include <sys/resource.h>
#include <sys/utsname.h>

/* MacOS Darwin fix */
#if defined(_SYS_NAMELEN) && ! defined(SYS_NMLN)
#define SYS_NMLN _SYS_NAMELEN
#endif

#include "xml.hh"

class Uname : public XML {
  // class to encapsulate the uname(2) information. This information
  // will only be printed. The code is somewhat system dependent.
public:
  Uname();
  virtual ~Uname();

  virtual std::string toXML( int indent = 0, 
			     const char* nspace = 0 ) const;
    // purpose: XML format a uname record. 
    // paramtr: indent (IN): indentation level of tag
    //          nspace (IN): If defined, namespace prefix before element
    // returns: string containing the element data

  virtual std::ostream& toXML( std::ostream& s, 
			       int indent = 0,
			       const char* nspace = 0 ) const;
    // purpose: XML format a uname record onto a given stream
    // paramtr: s (IO): stream to put information into
    //          indent (IN): indentation level of tag
    //          nspace (IN): If defined, namespace prefix before element
    // returns: s

private:
  struct utsname m_uname;       // system environment (uname -a)
  char           m_archmode[SYS_NMLN]; // IA32, IA64, ILP32, LP64, ...
};

#endif // _CHIMERA_UNAME_HH
