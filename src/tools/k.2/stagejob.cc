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
#include <ctype.h>
#include <errno.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <stdarg.h>
#include <stdlib.h>

#include <sys/poll.h>
#include <sys/wait.h>
#include <unistd.h>
#include <signal.h>
#include <fcntl.h>

#include <stdexcept>

#include "statinfo.hh"
#include "stagejob.hh"
#include "jobinfo.hh"
#include "appinfo.hh"

static const char* RCS_ID =
"$Id: stagejob.cc,v 1.5 2004/02/23 20:21:53 griphyn Exp $";

StageJob::StageJob( const char* tag, 
		    const std::string& format, 
		    const char* commandline )
  // purpose: initialize the data structure by parsing a command string.
  // paramtr: tag (IN): stage-in or stage-out
  //          format (IN): format string for output
  //          commandline (IN): commandline concatenated string to separate
  :JobInfo(tag,commandline),m_tempfile(0),m_format(format)
{
  // FIXME: We could do checking of the format string, but what do we care?
  if ( m_format.find("%l") == std::string::npos &&
       m_format.find("%s") == std::string::npos &&
       m_format.find("%t") == std::string::npos ) 
    throw std::invalid_argument( "format string lacks %l,%s,%t" );
}

StageJob::StageJob( const char* tag, 
		    const std::string& format, 
		    const StringList& argv )
  // purpose: initialize the data structure by parsing a command string.
  // paramtr: tag (IN): stage-in or stage-out
  //          format (IN): format string for output
  //          argv (IN): commandline already split into arg vector
  :JobInfo(tag,argv),m_tempfile(0),m_format(format)
{
  // FIXME: We could do checking of the format string, but what do we care?
  if ( m_format.find("%l") == std::string::npos &&
       m_format.find("%s") == std::string::npos &&
       m_format.find("%t") == std::string::npos ) 
    throw std::invalid_argument( "format string lacks %l,%s,%t" );
}

StageJob::~StageJob()
  // purpose: dtor
{
  if ( m_tempfile ) delete m_tempfile;
  m_tempfile = 0;
}

int
StageJob::createTempfile( const char* id, 
			  const FilenameMap& l2s,
			  const FilenameMultiMap& s2t )
  // purpose: create the tempfile from the external filemaps
  // paratmr: id (IN): stage-in or stage-out 
  //          l2s (IN): map with LFN to SFN mapping
  //          s2t (IN): multimap with SFN to TFN mapping
  // warning: filehandle for tempfile is open forthwith
  // returns: -1 in case of error
  //           0 for nothing to do
  //          >0 for number of files
{
  int result(-1);

  // check, if there is anything to do
  if ( s2t.size() == 0 || l2s.size() == 0 ) return 0;

  // Create tmpfile with filelist
  std::string tempfn( AppInfo::getTempDir() );
  tempfn += "/gs.";
  tempfn += id;
  tempfn += ".XXXXXX";

  StatTemporary* st = new StatTemporary(tempfn,false);
  if ( st && st->isValid() ) {
    if ( m_tempfile ) delete m_tempfile;
    m_tempfile = st;

    // if we created the temporary file
    result = 0;

    // fill in file information
    m_tempfile->setId(id);
    
    // enumerate LFN -> SFN => TFN 
    for ( FilenameMap::const_iterator i(l2s.begin()); i!=l2s.end(); ++i ) {
      // std::string lfn( i->first );
      // std::string sfn( i->second );
      FilenameMMRange p( s2t.equal_range((*i).second.first) );

      // check, if there are TFNs available
      // only do the next intensive steps, if necessary
      std::string::size_type n;
      if ( p.first != p.second ) {
	// start with a format template
	std::string msg( m_format );
	
	// replace all %l with LFN
	for ( n=msg.find("%l"); n != std::string::npos; n=msg.find("%l") ) {
	  msg.replace( n, 2, i->first );
	}

	// replace all %s with SFN
	for ( n=msg.find("%s"); n != std::string::npos; n=msg.find("%s") ) {
	  msg.replace( n, 2, (*i).second.first );
	}

	// dynamically determine the separator string
	// it may have contained %s or %l itself
	for ( n=msg.find("%t"); n != std::string::npos; n=msg.find("%t") ) {
	  std::string separator;
	  std::string::size_type m(n+2);
	  if ( n+2 >= msg.size() || msg.at(n+2) != '{' ) {
	    // no "{separator}" string, default to " "
	    separator = " ";
	  } else {
	    try {
	      for ( m=n+3; msg.at(m) != '}'; ++m ) ;
	      // postcondition: [n+2,m] == "{separator}"
	      separator = msg.substr(n+3,m-1);
	    } catch ( std::out_of_range ) {
	      // unterminated separator, make a guess
	      separator = msg.substr(n+3,m-1);
	    }
	  }

	  // assemble replacement string
	  std::string replacement;
	  for ( FilenameMultiMap::const_iterator j = p.first; 
		j != p.second; 
		++j ) {
	    if ( j != p.first ) replacement += separator;
	    replacement += j->second;
	  }

	  // replace the %t
	  msg.replace( n, m, replacement );
	}

	// terminate message string
	msg += "\r\n";

	// FIXME: very inefficient continuous writes, use buffering
	ssize_t wsize = 
	  write( m_tempfile->getDescriptor(), msg.c_str(), msg.size() );
	if ( wsize == static_cast<ssize_t>(msg.size()) ) result++;
	else {
	  // throw IOException
	  result = -1;
	  break;
	}
      }
    }

    // update stat info record
    if ( result >= 0 ) m_tempfile->update();
  }

  return result;
}

void 
StageJob::rewrite()
  // purpose: rewrite the argv vector before calling the job
  // warning: called from system()
{
  if ( m_tempfile && m_tempfile->isValid() ) {
    this->addArgument( m_tempfile->getFilename() );
  } else {
    fprintf( stderr, "%s tempfile\n", m_tempfile ? "invalid" : "null" );
  }
  JobInfo::rewrite();
}
