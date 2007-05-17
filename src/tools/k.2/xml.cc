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
#include <stdarg.h>
#include <stdio.h>
#include <sys/types.h>

#include "xml.hh"
#include <iomanip>

static const char* RCS_ID =
"$Id: xml.cc,v 1.2 2004/02/23 20:21:54 griphyn Exp $";

std::string
XML::indent( int width )
  // purpose: create a string with indentation 
  // paramtr: width (IN): if >0, generate that many spaces
  // returns: a string either empty, or with the wanted number of spaces.
{
  return ( width > 0 ? std::string( width, ' ' ) : std::string() );
}

std::string
XML::quote( const std::string& original, 
	    bool isAttribute )
  // purpose: Escapes certain characters inappropriate for XML content output.
  // paramtr: original (IN): is a string that needs to be quoted
  //          isAttribute (IN): denotes an attributes value, if set to true.
  //          If false, it denotes regular XML content outside of attributes.
  // returns: a string that is "safe" to print as XML.
{
  std::string buffer;

  for ( size_t i=0; i < original.size(); ++i ) {
    switch ( original[i] ) {
    case '\'':
      buffer += "&apos;";
      break;
    case '"':
      buffer += "&quot;";
      break;
    case '<':
      buffer += "&lt;";
      break;
    case '&':
      buffer += "&amp;";
      break;
    case '>':
      buffer += "&gt;";
      break;
    default:
      buffer += original[i]; 
      break;
    }
  }

  return buffer;
}

std::string 
XML::printf( const char* fmt, ... )
  // purpose: format arbitrary information into a C++ string.
  // paramtr: fmt (IN): printf compatible format
  //          ... (IN): parameters to format
  // returns: a string with the formatted information
{
  char temp[4096];
  va_list ap;

  *temp = '\0';
  va_start( ap, fmt );
  vsnprintf( temp, sizeof(temp), fmt, ap );
  va_end(ap);
  
  return std::string(temp);
}
