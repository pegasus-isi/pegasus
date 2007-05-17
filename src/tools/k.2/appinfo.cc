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

#include <sys/wait.h>
#include <unistd.h>
#include <fcntl.h>

#include "getif.hh"
#include "useinfo.hh"
#include "jobinfo.hh"
#include "statinfo.hh"
#include "appinfo.hh"

static const char* RCS_ID =
"$Id: appinfo.cc,v 1.10 2006/07/12 15:22:06 benc Exp $";

#define XML_SCHEMA_URI "http://www.griphyn.org/chimera/Invocation"
#define XML_SCHEMA_LOC "http://www.griphyn.org/chimera/iv-1.3.xsd"
#define XML_SCHEMA_VERSION "1.3"

inline
int
lock_reg( int fd, int cmd, int type, off_t offset, int whence, off_t len )
  //  purpose: wrap the lock functionality of fcntl
  //  paramtr: fd (IN): opened file descriptor, appropriate for lock mode
  //           cmd (IN): F_SETLK or F_SETLKW
  //           type (IN): F_RDLCK (shared), F_WRLCK (exclusive) or F_UNLCK
  //           offset (IN): offset into file
  //           whence (IN): SEEK_SET, SEEK_CUR or SEEK_END
  //           len (IN): region size, or 0 for complete file
  //  returns: result of fcntl() call
{
  struct flock lock;
  
  lock.l_type   = type;
  lock.l_whence = whence;
  lock.l_start  = offset;
  lock.l_len    = len;

  return ( fcntl( fd, cmd, &lock ) );
}

int
AppInfo::nfs_sync( int fd, long micros )
  // purpose: tries to force NFS to update the given file descriptor
  // paramtr: fd (IN): descriptor of an open file
  // returns: 0 is ok, -1 for failure
{
  // lock file
  if ( lock_reg( fd, F_SETLK, F_WRLCK, 0, SEEK_SET, 0 ) == -1 )
    return -1;

  // wait a while
  struct timeval p = { micros / 1000000, micros % 1000000 };
  select( 0, 0, 0, 0, &p );

  // unlock file
  return lock_reg( fd, F_SETLK, F_UNLCK, 0, SEEK_SET, 0 );
}

static
std::string
split_fqdi( const std::string& fqdi )
{
  std::string result;
  std::string::size_type first = fqdi.find( "::" );
  std::string::size_type second = fqdi.rfind(":");
  std::string ns, id, vs;

  if ( first != std::string::npos ) {
    // has a namespace
    ns = fqdi.substr( 0, first );
    if ( second != std::string::npos && second > first+1 ) {
      // has valid version
      id = fqdi.substr( first+2, second-(first+2) );
      vs = fqdi.substr( second+1 );
    } else {
      // no version
      id = fqdi.substr( first+2 );
    }
  } else {
    // no namespace
    if ( second != std::string::npos ) {
      // has version
      id = fqdi.substr( 0, second );
      vs = fqdi.substr( second+1 );
    } else {
      // no version
      id = fqdi;
    }
  }

  // combine result
  if ( ns.size() )
    result.append(" namespace=\"").append(ns).append("\"");
  result.append(" name=\"").append(id).append("\"");
  if ( vs.size() )
    result.append(" version=\"").append(vs).append("\"");

  return result;
}

std::ostream& 
AppInfo::toXML( std::ostream& s, int indent, const char* nspace ) const
  // purpose: XML format a rusage info record onto a given stream
  // paramtr: s (IO): stream to put information into
  //          indent (IN): indentation level of tag
  //          nspace (IN): If defined, namespace prefix before element
  // warning: does not include the preamble <?xml ... ?>
  // returns: s
{
  // start root element
  XML::startElement( s, "invocation", indent, nspace ) << " xmlns";

  // assign our own namespace, if that is wanted
  if ( nspace ) s << ':' << nspace;

  // XML generic attributes
  s << "=\"" XML_SCHEMA_URI "\" xmlns:gvds_xsi=\""
    "http://www.w3.org/2001/XMLSchema-instance\" "
    "gvds_xsi:schemaLocation=\"" XML_SCHEMA_URI " " XML_SCHEMA_LOC
    "\" version=\"" XML_SCHEMA_VERSION "\"";

  // non-generic attributes
  s << " start=\"" << m_start
    << XML::printf( "\" duration=\"%.3f\"", m_start.elapsed() );

  // optional attributes for root element: application process id
  if ( m_child != 0 )
    s << " pid=\"" << m_child << '"';

  // user and group info about who ran this thing
  s << " uid=\"" << getuid() << '"';
  s << " gid=\"" << getgid() << '"';

  // finalize open tag of root element
  s << ">\r\n";

  // <provenance> pseudo-element
  XML::startElement( s, "provenance", indent+2, nspace );

  // optional attribute: host address dotted quad
  if ( isdigit( m_ipv4[0] ) )
    s << " host=\"" << m_ipv4 << '"';

  // optional site handle
  if ( m_pool_handle.size() ) 
    s << " pool=\"" << m_pool_handle << '"';
  s << ">\r\n";

  // <uname>
  m_uname.toXML( s, indent+4, nspace );

  // optional element for provenance: derivation fqdn
  if ( m_derivation.size() ) {
    s << XML::startElement( s, "dv", indent+4, nspace );
    s << split_fqdi(m_derivation) << "/>\r\n";
  }

  // optional element for provenance: transformation fqdn 
  if ( m_xformation.size() ) {
    for ( StringList::const_iterator i = m_xformation.begin(); 
	  i != m_xformation.end(); ++i ) {
      s << XML::startElement( s, "tr", indent+4, nspace );
      s << split_fqdi(*i) << "/>\r\n";
    }
  }

  // done with provenance
  XML::finalElement( s, "provenance", indent+2, nspace );

  // <cwd>
  XML::startElement( s, "cwd", indent+2, nspace );
  if ( m_workdir.size() ) {
    s << '>' << m_workdir;
    XML::finalElement( s, "cwd", 0, nspace );
  } else {
#if 0
    s << " xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\""
      " xsi:nil=\"true\"/>\r\n";
#else
    s << "/>\r\n";
#endif
  }

  // <usage> own resources
  if ( m_usage ) m_usage->toXML( s, indent+2, nspace );

  // job record for stage-in job
  if ( m_stagein  )  m_stagein->toXML( s, indent+2, nspace );

  // job lists: <setup>, <prejob>, <application>, <postjob>, <cleanup>
  for ( JobInfoList::const_iterator i( m_setup.begin() ); 
	i != m_setup.end(); i++ )
    (*i)->toXML( s, indent+2, nspace );
  for ( JobInfoList::const_iterator i( m_prejob.begin() ); 
	i != m_prejob.end(); i++ )
    (*i)->toXML( s, indent+2, nspace );
  m_application->toXML( s, indent+2, nspace );
  for ( JobInfoList::const_iterator i( m_postjob.begin() ); 
	i != m_postjob.end(); i++ )
    (*i)->toXML( s, indent+2, nspace );
  for ( JobInfoList::const_iterator i( m_cleanup.begin() ); 
	i != m_cleanup.end(); i++ )
    (*i)->toXML( s, indent+2, nspace );

  // job record for stage-out job
  if ( m_stageout ) m_stageout->toXML( s, indent+2, nspace );

  // <statcall> records
  if ( m_gridstart ) m_gridstart->toXML( s, indent+2, nspace );
  if ( m_stdin ) m_stdin->toXML( s, indent+2, nspace );
  if ( m_stdout ) m_stdout->toXML( s, indent+2, nspace );
  if ( m_stderr ) m_stderr->toXML( s, indent+2, nspace );
  if ( m_logfile ) m_logfile->toXML( s, indent+2, nspace );
  if ( m_channel ) m_channel->toXML( s, indent+2, nspace );

  // <statcall> records for input and output
  for ( StatFileMap::const_iterator i( m_input_info.begin() );
	i != m_input_info.end(); ++i ) {
    i->second->toXML( s, indent+2, nspace );
  }

  for ( StatFileMap::const_iterator i( m_output_info.begin() );
	i != m_output_info.end(); ++i ) {
    i->second->toXML( s, indent+2, nspace );
  }

  // finish root element
  return XML::finalElement( s, "invocation", indent, nspace );
}

std::string
AppInfo::toXML( int indent, const char* nspace ) const
  // purpose: generate provenance tracking information
  // paramtr: indent (IN): indentation level of tag
  //          nspace (IN): If defined, namespace prefix before element
  // warning: does not include the preamble <?xml ... ?>
  // returns: string containing the element data
{
  std::string result;
  result.reserve( getpagesize() << 2 );

  // start root element
  result += XML::startElement( "invocation", indent, nspace ) + " xmlns";
  if ( nspace ) result.append(":").append(nspace);

  // XML generic attributes
  result += "=\"" XML_SCHEMA_URI "\" xmlns:gvds_xsi=\""
    "http://www.w3.org/2001/XMLSchema-instance\" "
    "gvds_xsi:schemaLocation=\"" XML_SCHEMA_URI " " XML_SCHEMA_LOC
    "\" version=\"" XML_SCHEMA_VERSION "\"";

  // non-generic attributes
  result += " start=\"" + m_start.date();
  result += XML::printf( "\" duration=\"%.3f\"", m_start.elapsed() );

  // optional attributes for root element: application process id
  if ( m_child != 0 )
    result += XML::printf( " pid=\"%d\"", m_child );

  // user and group info about who ran this thing
  result += XML::printf( " uid=\"%d\" gid=\"%d\">\r\n", getuid(), getgid() );

  // <provenance> pseudo-element
  result += XML::startElement( "provenance", indent+2, nspace );

  // optional attribute: host address dotted quad
  if ( isdigit( m_ipv4[0] ) ) {
    result += " host=\"";
    result += m_ipv4;
    result += "\"";
  }

  // optional site handle
  if ( m_pool_handle.size() ) 
    result += " pool=\"" + m_pool_handle + "\"";
      
  result += ">\r\n";

  // <uname>
  result += m_uname.toXML( indent+4, nspace );

  // optional element for provenance: derivation fqdn
  if ( m_derivation.size() ) {
    result += XML::startElement( "dv", indent+4, nspace );
    result += split_fqdi(m_derivation) + "/>\r\n";
  }
  
  // optional element for provenance: transformation fqdn 
  if ( m_xformation.size() ) {
    for ( StringList::const_iterator i = m_xformation.begin(); 
	  i != m_xformation.end(); ++i ) {
      result += XML::startElement( "tr", indent+4, nspace );
      result += split_fqdi(*i) + "/>\r\n";
    }
  }

  // done with provenance
  result += XML::finalElement( "provenance", indent+2, nspace );

  // <cwd>
  result += XML::startElement( "cwd", indent+2, nspace );
  if ( m_workdir.size() ) {
    result += ">" + m_workdir;
    result += XML::finalElement( "cwd", 0, nspace );
  } else {
#if 0
    result += " xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\""
      " xsi:nil=\"true\"/>\r\n";
#else
    result += "/>\r\n";
#endif
  }

  // <usage> own resources
  if ( m_usage ) result += m_usage->toXML( indent+2, nspace );

  // job record for stage-in job
  if ( m_stagein  ) result += m_stagein->toXML( indent+2, nspace );

  // job lists: <setup>, <prejob>, <application>, <postjob>, <cleanup>
  for ( JobInfoList::const_iterator i=m_setup.begin(); 
	i != m_setup.end(); i++ )
    result += (*i)->toXML( indent+2, nspace );

  for ( JobInfoList::const_iterator i=m_prejob.begin(); 
	i != m_prejob.end(); i++ )
    result += (*i)->toXML( indent+2, nspace );

  result += m_application->toXML( indent+2, nspace );

  for ( JobInfoList::const_iterator i=m_postjob.begin(); 
	i != m_postjob.end(); i++ )
    result += (*i)->toXML( indent+2, nspace );

  for ( JobInfoList::const_iterator i=m_cleanup.begin(); 
	i != m_cleanup.end(); i++ )
    result += (*i)->toXML( indent+2, nspace );

  // job record for stage-out job
  if ( m_stageout ) result += m_stageout->toXML( indent+2, nspace );

  // <statcall> records
  if ( m_gridstart ) result += m_gridstart->toXML( indent+2, nspace );
  if ( m_stdin ) result += m_stdin->toXML( indent+2, nspace );
  if ( m_stdout ) result += m_stdout->toXML( indent+2, nspace );
  if ( m_stderr ) result += m_stderr->toXML( indent+2, nspace );
  if ( m_logfile ) result += m_logfile->toXML( indent+2, nspace );
  if ( m_channel ) result += m_channel->toXML( indent+2, nspace );

  // <statcall> records for input and output
  for ( StatFileMap::const_iterator i( m_input_info.begin() );
	i != m_input_info.end(); ++i ) {
    result += i->second->toXML( indent+2, nspace );
  }

  for ( StatFileMap::const_iterator i( m_output_info.begin() );
	i != m_output_info.end(); ++i ) {
    result += i->second->toXML( indent+2, nspace );
  }

  // finish root element
  result += XML::finalElement( "invocation", indent, nspace );
  return result;
}

char*
AppInfo::pattern( char* buffer, size_t size,
		  const char* dir, const char* sep, const char* file ) 
  // purpose: concatenate directory, separator and filename into one string
  // paramtr: buffer (OUT): where to put the string
  //          size (IN): capacity of buffer
  //          dir (IN): directory pointer
  //          sep (IN): pathname separator as string
  //          file (IN): file to add to path
  // returns: buffer
{
  --size;
  buffer[size] = 0; // reliably terminate string
  strncpy( buffer, dir, size );
  strncat( buffer, sep, size );
  strncat( buffer, file, size );
  return buffer;
}

AppInfo::AppInfo( const char* self )
  // purpose: initialize the data structure with defaults.
  // paramtr: self (IN): the argv[0] from main()
  :m_self(self), 
//   m_start( now() ),
   m_child( getpid() ), m_isPrinted(false),
   m_usage(0), 
   m_stdin(0), m_stdout(0), m_stderr(0), 
   m_logfile(0), m_gridstart(0), m_channel(0), 
   m_uname(), m_application(0),
   m_stagein(0), m_stageout(0)
{
  size_t tempsize = getpagesize();
  char* tempname = new char[tempsize];

  // find a suitable directory for temporary files - not malloc'ed
  const char* tempdir = getTempDir();

  // initialize some data for myself
  m_gridstart = new StatFile( self, O_RDONLY, 0 );
  m_gridstart->setId("gridstart");

  // default for stdin
#if 1
  // original
  m_stdin = new StatFile( "/dev/null", O_RDONLY, 0 );
#else
  m_stdin = new StatHandle( STDIN_FILENO );
#endif
  m_stdin->setId("stdin");

  // default for stdout
#if 1
  pattern( tempname, tempsize, tempdir, "/", "gs.out.XXXXXX" );
  m_stdout = new StatTemporary( tempname );
#else
  m_stdout = new StatFile( "/dev/null", O_WRONLY | O_CREAT, 1 );
#endif
  m_stdout->setId("stdout");

  // default for stderr
#if 1
  pattern( tempname, tempsize, tempdir, "/", "gs.err.XXXXXX" );
  m_stderr = new StatTemporary( tempname );
#else
  m_stderr = new StatHandle( STDERR_FILENO );
#endif
  m_stderr->setId("stderr");

  // default for stdlog
  m_logfile = new StatHandle( STDOUT_FILENO );
  m_logfile->setId("logfile");

#if 0 // FIXME: Make this config-driven !!!
  // default for application-level feedback-channel
  pattern( tempname, tempsize, tempdir, "/", "gs.app.XXXXXX" );
  m_channel = new StatFifo( tempname, "GRIDSTART_CHANNEL" );
  m_channel->setId("channel");
#endif

  // which workdir
  if ( getcwd( tempname, tempsize ) != 0 ) m_workdir = tempname;

  // clean-up
  delete[] tempname;
  tempname = 0;

  // where do I run -- guess the primary interface IPv4 dotted quad
  PrimaryInterface::instance().whoami( m_ipv4, sizeof(m_ipv4) );
}

void
AppInfo::free_job( JobInfo* job )
{
  if ( job != 0 ) delete job;
  job = 0;
}

void
AppInfo::free_statinfo( StatFileMap::value_type& sim  )
{
  
  if ( sim.second != 0 ) delete sim.second;
  sim.second = 0;
}

AppInfo::~AppInfo()
  // purpose: destructor
{
#if 0
  // can we do this?!
  if ( ! m_isPrinted ) print();
#endif

  if ( m_usage ) delete m_usage;
  m_usage = 0;

  // standard stat info records
  if ( m_stdin ) delete m_stdin;
  m_stdin = 0;
  if ( m_stdout ) delete m_stdout;
  m_stdout = 0;
  if ( m_stderr ) delete m_stderr;
  m_stderr = 0;
  if ( m_logfile ) delete m_logfile;
  m_logfile = 0;
  if ( m_gridstart ) delete m_gridstart;
  m_gridstart = 0;
  if ( m_channel ) delete m_channel;
  m_channel = 0;

  // jobs
  for_each( m_setup.begin(), m_setup.end(), AppInfo::free_job );
  m_setup.clear();

  for_each( m_prejob.begin(), m_prejob.end(), AppInfo::free_job );
  m_prejob.clear();

  for_each( m_postjob.begin(), m_postjob.end(), AppInfo::free_job );
  m_postjob.clear();

  for_each( m_cleanup.begin(), m_cleanup.end(), AppInfo::free_job );
  m_cleanup.clear();

  if ( m_application ) delete m_application;
  m_application = 0;

  if ( m_stagein ) delete m_stagein;
  m_stagein = 0;
  if ( m_stageout ) delete m_stageout;
  m_stageout = 0;

  // non-standard statinfo records
  for_each( m_input_info.begin(), m_input_info.end(), AppInfo::free_statinfo );
  m_input_info.clear();
  m_input_sfn_tfn.clear();
  m_input_lfn_sfn.clear();

  for_each( m_output_info.begin(), m_output_info.end(), AppInfo::free_statinfo );
  m_output_info.clear();
  m_output_sfn_tfn.clear();
  m_output_lfn_sfn.clear();
}

void
AppInfo::setChannel( StatFifo* handle )
{
  // sanity check
  if ( handle == 0 ) throw null_pointer();

  // renew
  if ( m_channel ) delete m_channel;
  m_channel = handle;
  m_channel->setId("channel");
}

void
AppInfo::setStdin( StatInfo* handle )
{
  // sanity check
  if ( handle == 0 ) throw null_pointer();

  // delete old and reset new
  if ( m_stdin ) delete m_stdin;
  m_stdin = handle;
  m_stdin->setId("stdin");
}

void
AppInfo::setStdout( StatInfo* handle )
{
  // sanity check
  if ( handle == 0 ) throw null_pointer();

  // delete old and reset new
  if ( m_stdout ) delete m_stdout;
  m_stdout = handle;
  m_stdout->setId("stdout");
}

void
AppInfo::setStderr( StatInfo* handle )
{
  // sanity check
  if ( handle == 0 ) throw null_pointer();

  // delete old and reset new
  if ( m_stderr ) delete m_stderr;
  m_stderr = handle;
  m_stderr->setId("stderr");
}

size_t
AppInfo::createInputInfo()
  // purpose: update the internal input file statinfo map
  // returns: number of updates processed successfully.
{
  static std::string c_input("input");
  size_t result(0);

  // delete old info
  if ( m_input_info.size() > 0 ) {
    for_each( m_input_info.begin(), m_input_info.end(), 
	      AppInfo::free_statinfo );
    m_input_info.clear();
  }
   
  // create new mappings
  for ( FilenameMap::iterator i(m_input_lfn_sfn.begin()); 
	i != m_input_lfn_sfn.end(); ++i ) {
    StatFile* file = new StatFile( (*i).second.first, O_RDONLY, false );
    if ( file ) { 
      file->setId(c_input);
      file->setLFN(i->first);
      if ( (*i).second.second ) file->md5sum();
      m_input_info[i->first] = file;
      ++result;
    }
  }

  return result;
}

size_t
AppInfo::createOutputInfo()
  // purpose: update the internal output file statinfo map
  // returns: number of updates processed successfully.
{
  static std::string c_output("output");
  size_t result(0);

  // delete old info
  if ( m_output_info.size() > 0 ) {
    for_each( m_output_info.begin(), m_output_info.end(), 
	      AppInfo::free_statinfo );
    m_output_info.clear();
  }
   
  // create new mappings
  for ( FilenameMap::iterator i(m_output_lfn_sfn.begin()); 
	i != m_output_lfn_sfn.end(); ++i ) {
    StatFile* file = new StatFile( (*i).second.first, O_RDONLY, false );
    if ( file ) {
      file->setId(c_output);
      file->setLFN(i->first);
      if ( (*i).second.second ) file->md5sum();
      m_output_info[i->first] = file;
      ++result;
    }
  }

  return result;
}


ssize_t
AppInfo::print()
    // purpose: output the given app info onto the given (stdout) fd
    // paramtr: appinfo (IN): is the collective information about the run
    // returns: the number of characters actually written (as of write() call).
    // mutable: will update the self resource usage record before print.
    // mutable: will update the isPrinted predicate after print.
{
  int logfd = -1;

  if ( m_logfile ) {
    if ( StatHandle* sh = dynamic_cast<StatHandle*>(m_logfile) ) {
      // upcast to a handle is ok, we can use the handle
      logfd = dup( sh->getDescriptor() );
    } else if ( StatFile* sf = dynamic_cast<StatFile*>(m_logfile) ) {
      // upcast to a file is ok, we can use the name
      logfd = open( sf->getFilename().c_str(), 
		    O_WRONLY | O_APPEND | O_CREAT, 0644 );
    } else {
      // This is a "this should not happen" case
      // no cast possible, run into error
      throw null_pointer();
    }
  }

  ssize_t result = -1;
  if ( logfd != -1 ) {
    // what about myself? Update stat info on log file
    m_logfile->update();

    // obtain resoure usage for xxxx
#if 0
    m_usage = new UseInfo( "usage", RUSAGE_SELF );
    *m_usage += UseInfo( "usage", RUSAGE_CHILDREN );
#else
    // getrusage( RUSAGE_SELF, (struct rusage*) &m_usage );
    m_usage = new UseInfo( "usage", RUSAGE_SELF );
#endif
    
    std::string record( "<?xml version=\"1.0\" charset=\"ISO-8859-1\"?>\r\n" );
    if ( m_xmlns.size() ) record += this->toXML( 0, m_xmlns.c_str() );
    else record += this->toXML( 0, 0 );
    result = writen( logfd, record.c_str(), record.size(), 3 );
    // FIXME: what about wsize != result

    // synchronize trick for Globus and gatekeepers...
    nfs_sync( logfd );

    // done
    m_isPrinted = 1;
    close(logfd);
  }

  return result;
}

inline
bool
isDir( const char* tmp )
  // purpose: Check that the given dir exists and is writable for us
  // paramtr: tmp (IN): designates a directory location
  // returns: true, if tmp exists, isa dir, and writable
{
  struct stat st;
  if ( stat( tmp, &st ) == 0 && S_ISDIR(st.st_mode) ) {
    // exists and isa directory
    if ( (geteuid() != st.st_uid || (st.st_mode & S_IWUSR) == 0) &&
	 (getegid() != st.st_gid || (st.st_mode & S_IWGRP) == 0) &&
	 ((st.st_mode & S_IWOTH) == 0) ) {
      // not writable to us
      return false;
    } else {
      // yes, writable dir for us
      return true;
    }
  } else {
    // location does not exist, or is not a directory 
    return false;
  }
}

const char*
AppInfo::getTempDir( void )
  // purpose: determine a suitable directory for temporary files.
  // warning: remote schedulers may chose to set a different TMP..
  // returns: a string with a temporary directory, may still be NULL.
  // warning: result is _not_ allocated, so don't free the string
{
  const char* tempdir = getenv("GRIDSTART_TMP");
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

ssize_t
AppInfo::writen( int fd, const char* buffer, ssize_t n, unsigned restart )
  // purpose: write all n bytes in buffer, if possible at all
  // paramtr: fd (IN): filedescriptor open for writing
  //          buffer (IN): bytes to write (must be at least n byte long)
  //          n (IN): number of bytes to write 
  //          restart (IN): if true, try to restart write at max that often
  // returns: n, if everything was written, or
  //          [0..n-1], if some bytes were written, but then failed,
  //         < 0, if some error occurred.
{
  ssize_t start = 0;
  while ( start < n ) {
    ssize_t size = write( fd, buffer+start, n-start );
    if ( size < 0 ) {
      if ( restart && errno == EINTR ) { restart--; continue; }
      return size;
    } else {
      start += size;
    }
  }
  return n;
}

size_t
AppInfo::run( int& status )
  // purpose: run all runnable jobs. This constitutes a logical chaining of
  //          pre && main && post ; cleanup
  // paramtr: status (IO): first "failed" return code. Must be 0 to come in
  // returns: the number of jobs run
{
  size_t result(0);
  
  // run setup - this loop may be by-passed in the absence of setup jobs.
  // In the presence of setup jobs, the loop will always be executed
  for ( JobInfoList::iterator i=m_setup.begin(); i!=m_setup.end(); ++i ) {
    (*i)->system(this);
    result++;
  }
  
  // run prejobs - this loop may be by-passed in the absence of prejobs
  if ( status == 0 ) {
    for ( JobInfoList::iterator i=m_prejob.begin(); i!=m_prejob.end(); ++i ) {
      if ( (status = (*i)->system(this)) != 0 ) break;
      result++;
    }
  }

  // run main job - unless there were previous errors
  if ( status == 0 ) { 
    status = m_application->system(this);
    result++;
  }

  // run postjobs - this loop may be by-passed in the presence of errors,
  // or absence of postjobs
  if ( status == 0 ) {
    for ( JobInfoList::iterator i=m_postjob.begin(); i!=m_postjob.end(); ++i ) {
      if ( (status = (*i)->system(this)) != 0 ) break;
      result++;
    }
  }
  
  // run cleanup - this loop may be by-passed in the absence of cleanups
  // In the presence of cleanup jobs, the loop will always be executed
  for ( JobInfoList::iterator i=m_cleanup.begin(); i!=m_cleanup.end(); ++i ) {
    (*i)->system(this);
    result++;
  }

  return result;
}

int
AppInfo::runStageIn()
  // purpose: sets or replaces a stage-in job
  // paramtr: job (IN): newly allocated job information record
  // warning: ownership of job will pass here
{
  int result(0);

  if ( m_stagein ) {
    m_stagein->createTempfile( "stage-in", m_input_lfn_sfn, m_input_sfn_tfn );
    result = m_stagein->system(this);
    // note: filehandle will be kept open... 
  }

  return result;
}

int
AppInfo::runStageOut()
  // purpose: run the stage-out job, if one exists.
  // returns: Return code from running the stage-out job. 
  // warning: No existing job will also result in status of 0.
{
  int result(0);

  if ( m_stageout ) {
    m_stageout->createTempfile( "stage-out", m_output_lfn_sfn, m_output_sfn_tfn );
    result = m_stageout->system(this);
    // note: filehandle will be kept open... 
  }

  return result;
}
