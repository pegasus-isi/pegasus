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
#ifndef _CHIMERA_XML_HH
#define _CHIMERA_XML_HH

#include <string>
#include <iostream>
#include <iomanip>	// for convenience

class XML {
  // interface to enable recursive printing of XML data
public:
  static std::string quote( const std::string& original, 
			    bool isAttribute = false );
    // purpose: Escapes certain characters inappropriate for XML output.
    // paramtr: original (IN): is a string that needs to be quoted
    //          isAttribute (IN): denotes an attributes value, if set to true.
    //          If false, it denotes regular XML content outside of attributes.
    // returns: a string that is "safe" to print as XML.

  static std::string indent( int width );
    // purpose: create a string with indentation 
    // paramtr: width (IN): if >0, generate that many spaces
    // returns: a string either empty, or with the wanted number of spaces.
  
  static std::string printf( const char* fmt, ... );
    // purpose: format a string into a buffer.
    // paramtr: fmt (IN): printf compatible format
    //          ... (IN): parameters to format
    // returns: a string with the formatted information

  //
  // --- string based --------------------------------------------------
  //
  static inline std::string startElement( const std::string& tag, 
					  int indent = 0, 
					  const char* nspace = 0 )
    // purpose: starts an XML element without closing the angular bracket
    // paramtr: tag (IN): name of the tag to use
    //          indent (IN): indentation depth, negative for none
    //          nspace (IN): tag namespace, if not null
    // returns: a string containing the opening tag
  {
    std::string result( XML::indent(indent) );

    // result.reserve( tag.size() + 3 + ( nspace ? strlen(nspace) : 0) );
    result += '<';
    if ( nspace ) result.append(nspace).append(":");
    return result.append(tag);
  }

  static inline std::string finalElement( const std::string& tag, 
					  int indent = 0, 
					  const char* nspace = 0,
					  bool crlf = true )
    // purpose: creates an XML closing element with the angular bracket CRLF
    // paramtr: tag (IN): name of the tag to use
    //          indent (IN): indentation depth, negative for none
    //          nspace (IN): tag namespace, if not null
    // returns: a string containing the opening tag
  {
    std::string result( XML::indent(indent) );

    result += "</";
    if ( nspace ) result.append(nspace).append(":");
    result += tag + ">";
    if ( crlf ) result += "\r\n";
    return result;
  }

  virtual std::string toXML( int indent = 0, 
			     const char* nspace = 0 ) const = 0;
    // purpose: XML format a record. 
    // paramtr: indent (IN): indentation level of tag
    //          nspace (IN): If defined, namespace prefix before element
    // returns: string containing the element data

  //
  // --- stream based --------------------------------------------------
  //
  inline static std::ostream& startElement( std::ostream& s, 
					    const std::string& tag, 
					    int indent = 0, 
					    const char* nspace = 0 )
    // purpose: starts an XML element without closing the angular bracket
    // paramtr: s (IO): stream to put the element on
    //          tag (IN): name of the tag to use
    //          indent (IN): indentation depth, negative for none
    //          nspace (IN): tag namespace, if not null
    // returns: s
  {
    s << XML::indent(indent) << '<';
    if ( nspace ) s << nspace << ':';
    return s << tag;
  }

  static inline std::ostream& finalElement( std::ostream& s, 
					    const std::string& tag, 
					    int indent = 0, 
					    const char* nspace = 0,
					    bool crlf = true )
    // purpose: creates an XML closing element with the angular bracket CRLF
    // paramtr: s (IO): stream to put the element on
    //          tag (IN): name of the tag to use
    //          indent (IN): indentation depth, negative for none
    //          nspace (IN): tag namespace, if not null
    // returns: s
  {
    s << XML::indent(indent) << "</";
    if ( nspace ) s << nspace << ':';
    s << tag << '>';
    if ( crlf ) s << "\r\n";
    return s;
  }

  virtual std::ostream& toXML( std::ostream& s, 
			       int indent = 0,
			       const char* nspace = 0 ) const = 0;
    // purpose: XML format a record onto a given stream
    // paramtr: s (IO): stream to put information into
    //          indent (IN): indentation level of tag
    //          nspace (IN): If defined, namespace prefix before element
    // returns: s
};

#endif // _CHIMERA_XML_HH
