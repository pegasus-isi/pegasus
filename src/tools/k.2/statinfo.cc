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
#include <errno.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>

#include "statinfo.hh"
#include "time.hh"

#ifndef HAS_SETENV
// implementation found in scan.y
extern int setenv( const char* name, const char* value, int overwrite );
#endif

static const char* RCS_ID =
"$Id: statinfo.cc,v 1.8 2004/07/22 21:05:04 griphyn Exp $";

//
// --- StatInfo -------------------------------------------------
//

const unsigned int 
StatInfo::c_valid = 0xCAFEBABE;

StatInfo::StatInfo()
  // purpose: ctor
  :m_valid(StatInfo::c_valid)
{
  memset( &m_stat, 0, sizeof(struct stat) );
  m_error = -1;
}

StatInfo::~StatInfo()
  // purpose: dtor
{
  invalidate();
}

bool 
StatInfo::isValid( void ) const
  // purpose: check, if object was correctly constructed
  // returns: true for a valid object, false for an invalid one
{
  return ( StatInfo::c_valid == m_valid );
}

void 
StatInfo::invalidate( void )
  // purpose: invalide that current member
{
  m_valid = -1ul;
}

StatInfo::StatSource
StatInfo::whoami( void ) const
  // purpose: if you don't like RTTI
  // returns: type
{
  return StatInfo::IS_INVALID;
}

std::ostream& 
StatInfo::toXML( std::ostream& s, int indent, const char* nspace ) const
  // purpose: format content as XML onto stream
  // paramtr: s (IO): stream to put things on
  //          indent (IN): indentation depth, negative for none
  //          nspace (IN): tag namespace, if not null
  // returns: s
{
  // sanity checks
  if ( this->whoami() == StatInfo::IS_INVALID || ! isValid() ) return s;

  // start tag
  s << XML::startElement( s, "statcall", indent, nspace );

  // print known attributes
  s << " error=\"" << m_error << '"';
  if ( m_id.size() ) s << " id=\"" << m_id << '"';
  if ( m_lfn.size() ) s << " lfn=\"" << m_lfn << '"';
  s << ">\r\n";

  // child printing
  show( s, indent+2, nspace );

  // <stat> record
  if ( m_error == 0 ) {
    s << XML::startElement(s, "statinfo", indent+2, nspace );
    
    // stat attributes
    s << XML::printf( " mode=\"0%o\" size=\"%lu\" inode=\"%lu\""
		      " nlink=\"%lu\" blksize=\"%lu\"",
		      m_stat.st_mode, m_stat.st_size, m_stat.st_ino,
		      m_stat.st_nlink, m_stat.st_blksize );

    Time mtime( m_stat.st_mtime, -1 );
    s << "\" mtime=\"" << mtime;
    Time ctime( m_stat.st_ctime, -1 );
    s << "\" ctime=\"" << ctime;
    Time atime( m_stat.st_atime, -1 );

    s << "\" atime=\"" << atime;
    s << "\" uid=\"" << m_stat.st_uid;
    s << "\" gid=\"" << m_stat.st_gid << "\">\r\n";
  }

  // data section from stdout and stderr of application
  data( s, indent+2, nspace );

  // finalize </element>
  s << XML::finalElement( s, "statcall", indent, nspace );
  return s;
}


std::string
StatInfo::toXML( int indent, const char* nspace ) const
  // purpose: XML format a stat info record into a given buffer
  // paramtr: indent (IN): indentation level of tag
  //          nspace (IN): tag namespace, if not null
  // returns: buffer
{
  std::string result;

  // sanity check
  if ( this->whoami() == StatInfo::IS_INVALID ) return result;
  if ( ! isValid() ) return result;

  result += XML::startElement( "statcall", indent, nspace );
  if ( m_id.size() ) result.append(" id=\"").append(m_id).append("\"");
  if ( m_lfn.size() ) result.append( " lfn=\"").append(m_lfn).append("\"");
  result += ">\r\n";

  // child class printing
  result += show( indent+2, nspace );

  // <stat> record
  if ( m_error == 0 ) {
    result += XML::startElement( "statinfo", indent+2, nspace );
    result += XML::printf( " mode=\"0%o\" size=\"%lu\" inode=\"%lu\""
			   " nlink=\"%lu\" blksize=\"%lu\"",
			   m_stat.st_mode, m_stat.st_size, m_stat.st_ino,
			   m_stat.st_nlink, m_stat.st_blksize );
    Time mtime( m_stat.st_mtime, -1 );
    result += " mtime=\"" + mtime.date();

    Time atime( m_stat.st_atime, -1 );
    result += " atime=\"" + atime.date();

    Time ctime( m_stat.st_ctime, -1 );
    result += " ctime=\"" + ctime.date();

    result += XML::printf( "\" uid=\"%lu\" gid=\"%lu\"/>\r\n",
			   m_stat.st_uid, m_stat.st_gid );
  }

  // data section from stdout and stderr of application
  result += data( indent+2, nspace );

  // finalize </element>
  result += XML::finalElement( "statcall", indent, nspace );
  return result;
}

//
// --- StatFile -------------------------------------------------
//

StatFile::StatFile()
  :StatInfo(), m_filename(""), m_openmode(O_RDONLY), m_hsize(0), 
   m_done_md5(false)
{
  memset( m_header, 0, sizeof(m_header) );
  memset( m_digest, 0, sizeof(m_digest) );
}

StatFile::StatFile( const std::string& filename, int openmode, bool truncate )
  // purpose: Initialize a stat info buffer with a filename to point to
  // paramtr: filename (IN): the filename to memorize (deep copy)
  //          openmode (IN): are the fcntl O_* flags to later open calls
  //          truncate (IN): flag to truncate stdout or stderr
  :StatInfo(), m_filename(filename), m_openmode(openmode), 
   m_hsize(0), m_done_md5(false)
{
  memset( m_header, 0, sizeof(m_header) );
  memset( m_digest, 0, sizeof(m_digest) );
  if ( truncate ) {
    // FIXME: As long as we use shared stdio for stdout and stderr, we need
    // to explicitely truncate (and create) file to zero, if not appending.
    int fd = open( filename.c_str(), 
		   (openmode & O_ACCMODE) | O_CREAT | O_TRUNC, 0666 );
    if ( fd != -1 ) close(fd);
  }

  errno = 0;
  if ( StatFile::update() == -1 ) invalidate();
}

#if 0
StatFile::StatFile( const char* filename, int openmode, int truncate )
  // purpose: Initialize a stat info buffer with a filename to point to
  // paramtr: filename (IN): the filename to memorize (deep copy)
  //          openmode (IN): are the fcntl O_* flags to later open calls
  //          truncate (IN): flag to truncate stdout or stderr
  :StatInfo(), m_filename(filename), m_openmode(openmode), 
   m_hsize(0), m_done_md5(false)
{
  memset( m_header, 0, sizeof(m_header) );
  memset( m_digest, 0, sizeof(m_digest) );
  if ( truncate ) {
    // FIXME: As long as we use shared stdio for stdout and stderr, we need
    // to explicitely truncate (and create) file to zero, if not appending.
    int fd = open( filename, (openmode & O_ACCMODE) | O_CREAT | O_TRUNC, 0666 );
    if ( fd != -1 ) close(fd);
  }

  errno = 0;
  if ( StatFile::update() == -1 ) invalidate();
}
#endif

StatFile::~StatFile()
  // purpose: dtor
{
  // clean
  m_filename.clear();
  invalidate();
}

StatInfo::StatSource 
StatFile::whoami( void ) const
  // purpose: if you don't like RTTI
  // returns: type
{ 
  return StatInfo::IS_FILE; 
}

int
StatFile::update()
  // purpose: update existing and initialized statinfo with latest info 
  // returns: the result of the stat() or fstat() system call.
{
  int result = stat( m_filename.c_str(), &m_stat );
  m_error = errno;

  if ( isValid() && result != -1 && 
       S_ISREG(m_stat.st_mode) && 
       m_stat.st_size > 0 ) {
    int fd = open( m_filename.c_str(), O_RDONLY );
    if ( fd != -1 ) {
      m_hsize = read( fd, (char*) m_header, sizeof(m_header) );
      close(fd);
    }
  }

  return result;
}

std::ostream& 
StatFile::show( std::ostream& s, int indent, const char* nspace ) const
  // purpose: format content as XML onto stream
  // paramtr: s (IO): stream to put things on
  //          indent (IN): indentation depth, negative for none
  //          nspace (IN): tag namespace, if not null
  // returns: s
{
  // sanity check
  if ( ! isValid() ) return s;

  // <file> element
  s << XML::startElement( s, "file", indent, nspace );
  s << " name=\"" << m_filename << '"';

  // optional MD5 sum
  Digest d;
  if ( getMD5sum(d) ) {
    s << " md5sum=\"";
    for ( size_t i=0; i<sizeof(d); ++i )
      s << std::hex << std::setw(2) << std::setfill('0') << d[i];
    s << '"';
  }

  // optional content
  if ( m_error == 0 && S_ISREG(m_stat.st_mode) && m_stat.st_size > 0 ) {
    // hex information
    ssize_t end = sizeof(m_header);
    if ( m_stat.st_size < end ) end = m_stat.st_size;

    s << '>';
    for ( ssize_t i=0; i<end; ++i )
      s << std::hex << std::setw(2) << std::setfill('0') << m_header[i];
    s << "</file>\r\n";
  } else {
    s << "/>\r\n";
  }

  return s;
}

std::string
StatFile::show( int indent, const char* nspace ) const
  // purpose: Generate the element-specific information. Called from toXML()
  // paramtr: buffer (IO): area to store the output in
  //          indent (IN): indentation level of tag
  // returns: string with the information.
{
  std::string buffer;

  // sanity check
  if ( ! isValid() ) return buffer;

  // <file> element
  buffer += XML::startElement( "file", indent, nspace );
  buffer += " name=\"" + m_filename + "\"";

  // optional MD5 sum
  Digest d;
  if ( getMD5sum(d) ) {
    buffer += " md5sum=\"";
    for ( size_t i=0; i<sizeof(d); ++i )
      buffer += XML::printf( "%02x", d[i] );
    buffer += "\"";
  }

  // optional content
  if ( m_error == 0 && S_ISREG(m_stat.st_mode) && m_stat.st_size > 0 ) {
    // hex information
    ssize_t i, end = sizeof(m_header);
    if ( m_stat.st_size < end ) end = m_stat.st_size;

    buffer += '>';
    for ( i=0; i<end; ++i ) buffer += XML::printf( "%02x", m_header[i] );
    buffer += "</file>\r\n";
  } else {
    buffer += "/>\r\n";
  }

  return buffer;
}

int
StatFile::forcefd( int fd ) const
  // purpose: force open a file on a certain fd
  // paramtr: fd (IN): is the file descriptor to plug onto. If this fd is 
  //          the same as the descriptor in info, nothing will be done.
  // returns: 0 if all is well, or fn was NULL or empty.
  //          1 if opening a filename failed,
  //          2 if dup2 call failed
{
  int newfd = ( (m_openmode & O_ACCMODE) == O_RDONLY ) ? 
    open( m_filename.c_str(), m_openmode ) :
    open( m_filename.c_str(), m_openmode | O_APPEND, 0666 );

  // check for open success
  if ( newfd == -1 ) {
#if 0
    char msg[4096];
    snprintf( msg, sizeof(msg), "unable to open %s: %d: %s\n", 
	      m_filename.c_str(), errno, strerror(errno) );
    write( STDERR_FILENO, msg, strlen(msg) );
#endif
    return 1;
#if 0
  } else {
    char msg[4096];
    snprintf( msg, sizeof(msg), "# open( %s, %d ) = %d\n", 
	      m_filename.c_str(), m_openmode, newfd );
    write( STDERR_FILENO, msg, strlen(msg) );
#endif
  }

  // create a duplicate of the new fd onto the given (stdio) fd. This operation
  // is guaranteed to close the given (stdio) fd first, if open.
  if ( newfd != fd ) {
    // FIXME: Does dup2 guarantee noop for newfd==fd on all platforms ? 
    if ( dup2( newfd, fd ) == -1 ) {
#if 0
      char msg[4096];
      snprintf( msg, sizeof(msg), "unable to dup2(%s=%d,%d): %d: %s\n", 
		m_filename.c_str(), newfd, fd, errno, strerror(errno) );
      write( STDERR_FILENO, msg, strlen(msg) );
#endif
      return 2;
    }

    // we opened it, we must close it, too.
    // BUT only if old and new are not equal!
    close(newfd);
  }
  
  return 0;
}

extern "C" {
  /* use OpenSSL for the MD5 implementation */
#include <openssl/md5.h>
}

void
StatFile::md5sum()
  // purpose: calculate the MD5 checksum over the complete file
  // throws : sum_error on IO error, bad_alloc on out of memory
{
  // FIXME: use mmap instead of read
  int fd = open( m_filename.c_str(), O_RDONLY );
  if ( fd == -1 ) throw sum_error( "open " + m_filename, errno );

  MD5_CTX context;
  MD5_Init( &context );

  size_t size( getpagesize() << 4 );
  std::auto_ptr<char> buffer( new char[size] );

  ssize_t rsize;
  while ( (rsize = read( fd, &(*buffer), size )) > 0 ) {
    MD5_Update( &context, &(*buffer), rsize );
  }
  if ( rsize == -1 ) throw sum_error( "read " + m_filename, errno );
  if ( close(fd) == -1 ) throw sum_error( "close " + m_filename, errno );
  
  MD5_Final( m_digest, &context );
  m_done_md5 = true;
}

bool 
StatFile::getMD5sum( StatFile::Digest digest ) const
  // purpose: obtains the stored MD5 sum
  // paramtr: digest (OUT): a digest area to store the 128 bits into
  // returns: true, if a string was stored in the digest area,
  //          false, if no sum was obtained, and the digest is untouched
{
  // FIXME: can store to NULL :-(
  if ( m_done_md5 ) memcpy( digest, m_digest, sizeof(m_digest) );
  return m_done_md5;
}

//
// --- StatHandle -------------------------------------------------
//

StatHandle::StatHandle( int descriptor )
  // purpose: Initialize a stat info buffer with a filename to point to
  // paramtr: descriptor (IN): the handle to attach to
  :StatInfo(), m_descriptor(descriptor)
{
  StatHandle::update();
}

StatHandle::~StatHandle()
  // purpose: dtor
{
  // DO NOT close foreign handle
  invalidate();
}

StatInfo::StatSource 
StatHandle::whoami( void ) const
  // purpose: if you don't like RTTI
  // returns: type
{ 
  return StatInfo::IS_FILE; 
}

int
StatHandle::update()
  // purpose: update existing and initialized statinfo with latest info 
  // returns: the result of the stat() or fstat() system call.
{
  int result = -1;
  if ( m_descriptor != -1 ) {
    errno = 0;
    fstat( m_descriptor, &m_stat );
    m_error = errno;
  } else {
    memset( &m_stat, 0, sizeof(struct stat) );
    m_error = errno = EBADF;
  }

  return result;
}

std::ostream& 
StatHandle::show( std::ostream& s, int indent, const char* nspace ) const
  // purpose: format content as XML onto stream
  // paramtr: s (IO): stream to put things on
  //          indent (IN): indentation depth, negative for none
  //          nspace (IN): tag namespace, if not null
  // returns: s
{
  // sanity check
  if ( ! isValid() ) return s;

  // <descriptor> element
  s << XML::startElement( s, "descriptor", indent, nspace );
  s << " number=\"" << m_descriptor << "/>\r\n";
  return s;
}

std::string
StatHandle::show( int indent, const char* nspace ) const
  // purpose: Generate the element-specific information. Called from toXML()
  // paramtr: buffer (IO): area to store the output in
  //          indent (IN): indentation level of tag
  // returns: string with the information.
{
  std::string buffer;

  // sanity check
  if ( ! isValid() ) return buffer;

  // <descriptor> element
  buffer += XML::startElement( "descriptor", indent, nspace );
  buffer += XML::printf( " number=\"%d\"/>\r\n", m_descriptor );
  return buffer;
}

int
StatHandle::forcefd( int fd ) const
  // purpose: force open a file on a certain fd
  // paramtr: fd (IN): is the file descriptor to plug onto. If this fd is 
  //          the same as the descriptor in info, nothing will be done.
  // returns: 0 if all is well, or fn was NULL or empty.
  //          1 if opening a filename failed,
  //          2 if dup2 call failed
{
  // check for validity
  if ( m_descriptor == -1 ) return 1;

  // create a duplicate of the new fd onto the given (stdio) fd. This operation
  // is guaranteed to close the given (stdio) fd first, if open.
  if ( m_descriptor != fd ) {
    // FIXME: Does dup2 guarantee noop for newfd==fd on all platforms ? 
    if ( dup2( m_descriptor, fd ) == -1 ) return 2;
  }

  return 0;
}

//
// --- StatTemporary -------------------------------------------------
//

StatTemporary::StatTemporary( int fd, const char* fn )
  // purpose: Initialize for an externally generated temporary file
  // paramtr: fd (IN): is the connected file descriptor
  //          fn (IN): is the concretized temporary filename
  :StatFile(),StatHandle(-1)
{
  m_descriptor = fd;
  m_filename = fn;
  this->StatHandle::update();
}

StatTemporary::StatTemporary( char* pattern )
  // purpose: Initialize a stat info buffer with a temporary file
  // paramtr: pattern (IO): is the input pattern to mkstemp(), will be modified!
  :StatFile(),StatHandle(-1)
{
  int result = mkstemp(pattern); 

  if ( result == -1 ) {
    // mkstemp has failed, au weia!
    m_error = errno;
    invalidate();
  } else {
    // try to ensure append mode for the file, because it is shared
    // between jobs. If the SETFL operation fails, well there is nothing
    // we can do about that. 
    int flags = fcntl( result, F_GETFL );
    if ( flags != -1 ) 
      fcntl( result, F_SETFL, (m_openmode = (flags | O_APPEND)) );

    // this file descriptor is NOT to be passed to the jobs? So far, the
    // answer is true. We close this fd on exec of sub jobs, so it will
    // be invisible to them.
    flags = fcntl( result, F_GETFD );
    if ( flags != -1 ) fcntl( result, F_SETFD, flags | FD_CLOEXEC );

    // the return is the chosen filename as well as the opened descriptor.
    // we *could* unlink the filename right now, and be truly private, but
    // users may want to look into the log files of long persisting operations. 
    m_descriptor = result;
    m_filename = pattern;
    // m_openmode = O_RDWR | O_EXCL;

    this->StatHandle::update();
  }
}

StatTemporary::StatTemporary( const std::string& p, bool c_o_e )
  // purpose: Initialize a stat info buffer with a temporary file
  // paramtr: p (IO): is the input pattern to mkstemp(), will be modified!
  //          c_o_e (IN): if true, set FD_CLOEXEC fd flag, unset if false
  // warning: pattern will be copied for modification
  :StatFile(),StatHandle(-1)
{
  std::auto_ptr<char> pattern( new char[p.size()+1] );
  memcpy( &(*pattern), p.c_str(), p.size()+1 );

  int result = mkstemp( &(*pattern) ); 
  if ( result == -1 ) {
    // mkstemp has failed, au weia!
    m_error = errno;
    invalidate();
  } else {
    // try to ensure append mode for the file, because it is shared
    // between jobs. If the SETFL operation fails, well there is nothing
    // we can do about that. 
    int flags = fcntl( result, F_GETFL );
    if ( flags != -1 ) 
      fcntl( result, F_SETFL, (m_openmode = (flags | O_APPEND)) );

    // this file descriptor is NOT to be passed to the jobs? So far, the
    // answer is true. We close this fd on exec of sub jobs, so it will
    // be invisible to them.
    flags = fcntl( result, F_GETFD );
    if ( flags != -1 ) {
      if ( c_o_e ) flags |= FD_CLOEXEC;
      else flags &= ~FD_CLOEXEC;
      fcntl( result, F_SETFD, flags | FD_CLOEXEC );
    }

    // the return is the chosen filename as well as the opened descriptor.
    // we *could* unlink the filename right now, and be truly private, but
    // users may want to look into the log files of long persisting operations.
    m_descriptor = result;
    m_filename = &(*pattern);
    // m_openmode = O_RDWR | O_EXCL;

    this->StatHandle::update();
  }
}

StatTemporary::~StatTemporary()
  // purpose: dtor
{
  // close descriptor
  close( m_descriptor );
  m_descriptor = -1;

  // remove filename
  unlink( m_filename.c_str() );
  m_filename.clear();

  invalidate();
}

StatInfo::StatSource 
StatTemporary::whoami( void ) const
  // purpose: if you don't like RTTI
  // returns: type
{ 
  return StatInfo::IS_TEMP; 
}

int
StatTemporary::update()
  // purpose: update existing and initialized statinfo with latest info 
  // returns: the result of the stat() or fstat() system call.
{
#if 1
  return this->StatFile::update();
#else
  return this->StatHandle::update();
#endif
}

std::ostream& 
StatTemporary::show( std::ostream& s, int indent, const char* nspace ) const
  // purpose: format content as XML onto stream
  // paramtr: s (IO): stream to put things on
  //          indent (IN): indentation depth, negative for none
  //          nspace (IN): tag namespace, if not null
  // returns: s
{
  // sanity check
  if ( ! isValid() ) return s;

  // late update for temp files
  errno = 0;
  const_cast<StatTemporary*>(this)->StatFile::update();

  // <temporary> element
  s << XML::startElement( s, "temporary", indent, nspace );
  s << " name=\"" << m_filename << '"';
  s << " descriptor=\"" << m_descriptor << "\"/>\r\n";

  return s;
}

std::string
StatTemporary::show( int indent, const char* nspace ) const 
  // purpose: Generate the element-specific information. Called from toXML()
  // paramtr: indent (IN): indentation level of tag
  //          nspace (IN): tag namespace, if not null
  // returns: string with the information.
{
  std::string buffer;

  // preparation for <temporary> element
  if ( ! isValid() ) return buffer;

  // late update for temp files
  errno = 0;
  const_cast<StatTemporary*>(this)->StatFile::update();

  // <temporary> element
  buffer += XML::startElement( "temporary", indent, nspace ) +
    " name=\"" + m_filename + "\" descriptor=\"";
  buffer += XML::printf( "%d\"/>\r\n", m_descriptor );

  return buffer;
}

int
StatTemporary::forcefd( int fd ) const
  // purpose: force open a file on a certain fd
  // paramtr: fd (IN): is the file descriptor to plug onto. If this fd is 
  //          the same as the descriptor in info, nothing will be done.
  // returns: 0 if all is well, or fn was NULL or empty.
  //          1 if opening a filename failed,
  //          2 if dup2 call failed
{
  return StatHandle::forcefd( fd );
}

std::ostream&
StatTemporary::data( std::ostream& s, int indent, const char* nspace ) const
    // purpose: format content as XML onto stream
    // paramtr: s (IO): stream to put things on
    //          indent (IN): indentation depth, negative for none
    //          nspace (IN): tag namespace, if not null
    // returns: s
{
  if ( ! isValid() ) return s;

  // data section from stdout and stderr of application 
  if ( m_error == 0 && m_stat.st_size ) {
    size_t dsize = getpagesize()-1;
    size_t fsize = m_stat.st_size;

    // <data> element
    s << XML::startElement( s, "data", indent, nspace );
    if ( fsize > dsize ) s << " truncated=\"true\"";

    // content for <data> element
    if ( fsize > 0 ) {
      char* data = new char[dsize+1];
      int fd = dup(m_descriptor);

      s << '>';
      if ( fd != -1 ) {
	if ( lseek( fd, SEEK_SET, 0 ) != -1 ) {
	  ssize_t rsize = read( fd, data, dsize );
	  s << XML::quote( std::string( data, rsize) );
	}
	close(fd);
      }

      delete[] data;
      s << "</data>\r\n";
    } else {
      s << "/>\r\n";
    }
  }

  return s;
}

std::string 
StatTemporary::data( int indent, const char* nspace ) const
  // purpose: Generate special post-element code, e.g. stderr and stdout data
  // paramtr: buffer (IO): area to store the output in
  //          indent (IN): indentation level for tag
  // returns: buffer
{
  std::string buffer;
  if ( ! isValid() ) return buffer;

  // data section from stdout and stderr of application 
  if ( m_error == 0 && m_stat.st_size ) {
    size_t dsize = getpagesize()-1;
    size_t fsize = m_stat.st_size;

    // <data> element
    buffer += XML::startElement( "data", indent, nspace );
    if ( fsize > dsize ) buffer += " truncated=\"true\"";

    // content for <data> element
    if ( fsize > 0 ) {
      char* data = new char[dsize+1];
      int fd = dup(m_descriptor);

      buffer += '>';
      if ( fd != -1 ) {
	if ( lseek( fd, SEEK_SET, 0 ) != -1 ) {
	  ssize_t rsize = read( fd, data, dsize );
	  buffer += XML::quote( std::string( data, rsize ) );
	}
	close(fd);
      }

      buffer += "</data>\r\n";
      delete[] data;
    } else {
      buffer += "/>\r\n";
    }
  }

  return buffer;
}

//
// --- StatFifo -------------------------------------------------
//

StatFifo::StatFifo( const std::string& pattern, const std::string& key )
  // purpose: Initialize a stat info buffer associated with a named pipe
  // paramtr: pattern (IN): input pattern to mkstemp(), will be modified!
  //                        otherwise append hyphen-XXXXXX
  //          key (IN): is the environment key at which to store the filename
  :StatFile(),StatHandle(-1)
{
  std::string temp(pattern);
  if ( temp.rfind("XXXXXX") != temp.size()-6 )
    temp += "-XXXXXX";

  // make a copy that is modifiable (for mkstemp)
  std::auto_ptr<char> p( new char[ temp.size()+1 ] );
  memcpy( &(*p), temp.c_str(), temp.size()+1 );

  // create a temporary filename
  int result = mkstemp( &(*p) );

  if ( result == -1 ) {
    // mkstemp has failed, au weia!
    m_error = errno;
    invalidate();
  } else {
    // create a FIFO instead of a regular tmp file. 
    // we could have used mktemp() right away, mkstemp() is NOT safer here! 
    close( result );
    unlink( &(*p) );

    // FIXME: race condition possible
    if ( (result = mkfifo( &(*p), 0660 )) == -1 ) {
      m_error = errno;
      invalidate();
    } else {
      // open in non-blocking mode for reading. 
      // WARNING: DO NOT open in O_RDONLY or suffer the consequences. 
      // You must open in O_RDWR to avoid having to deal with EOF
      // whenever the clients drop to zero.
      m_openmode = O_RDWR | O_NONBLOCK;
      if ( (result = open( &(*p), m_openmode )) == -1 ) {
	m_error = errno;
	invalidate();
      } else {
	// this file descriptor is NOT to be passed to the jobs? So far,
	// the answer is true. We close this fd on exec of sub jobs, so
	// it will be invisible to them.
	int flags = fcntl( result, F_GETFD );
	if ( flags != -1 ) fcntl( result, F_SETFD, flags | FD_CLOEXEC );

	// the return is the chosen filename as well as the opened
	// descriptor. We cannot unlink the filename right now. */
	m_descriptor = result;
	// obtain a copy inside the string
	m_filename = std::string( &(*p) );

	// fix environment -- use setenv from our/system implementation
	if ( key.size() && (isalnum(key[1]) || key[1]=='_') ) {
	  size_t size = strlen(&(*p))+1;
	  char* value = static_cast<char*>( malloc(size) );
	  memcpy( value, &(*p), size );
	  if ( setenv( key.c_str(), value, 1 ) == -1 ) 
	    fprintf( stderr, "# Warning: Unable to set %s: %d: %s\n",
		     key.c_str(), errno, strerror(errno) );
	  delete[] value;
	}
	  
	StatHandle::update();
      }
    }
  }
}

#if 0
StatFifo::StatFifo( char* pattern, const char* key )
  // purpose: Initialize a stat info buffer associated with a named pipe
  // paramtr: pattern (IO): input pattern to mkstemp(), will be modified!
  //          key (IN): is the environment key at which to store the filename
  :StatFile(),StatHandle(-1)
{
  int result = mkstemp(pattern);

  if ( result == -1 ) {
    // mkstemp has failed, au weia!
    m_error = errno;
    invalidate();
  } else {
    // create a FIFO instead of a regular tmp file. 
    // we could have used mktemp() right away, mkstemp() is NOT safer here! 
    close( result );
    unlink( pattern );

    if ( (result = mkfifo( pattern, 0660 )) == -1 ) {
      m_error = errno;
      invalidate();
    } else {
      // open in non-blocking mode for reading. 
      // WARNING: DO NOT open in O_RDONLY or suffer the consequences. 
      // You must open in O_RDWR to avoid having to deal with EOF
      // whenever the clients drop to zero.
      m_openmode = O_RDWR | O_NONBLOCK;
      if ( (result = open( pattern, m_openmode )) == -1 ) {
	m_error = errno;
	invalidate();
      } else {
	// this file descriptor is NOT to be passed to the jobs? So far,
	// the answer is true. We close this fd on exec of sub jobs, so
	// it will be invisible to them.
	int flags = fcntl( result, F_GETFD );
	if ( flags != -1 ) fcntl( result, F_SETFD, flags | FD_CLOEXEC );

	// the return is the chosen filename as well as the opened
	// descriptor. We cannot unlink the filename right now. */
	m_descriptor = result;
	m_filename = pattern;

	// fix environment
	if ( key != NULL ) {
	  size_t size = strlen(key) + strlen(pattern) + 2;
	  char* temp = static_cast<char*>( malloc(size) );
	  memset( temp, 0, size-- );
	  strncpy( temp, key, size );
	  strncat( temp, "=", size );
	  strncat( temp, pattern, size );
	  putenv( temp );
	  // DO NOT free this string here nor now 
	}
	  
	StatHandle::update();
      }
    }
  }
}
#endif

StatFifo::~StatFifo()
  // purpose: dtor
{
  // close descriptor
  close( m_descriptor );
  m_descriptor = -1;

  // remove filename
  unlink( m_filename.c_str() );
  m_filename.clear();
}

StatInfo::StatSource 
StatFifo::whoami( void ) const
  // purpose: if you don't like RTTI
  // returns: type
{ 
  return StatInfo::IS_FIFO; 
}

int
StatFifo::update()
  // purpose: update existing and initialized statinfo with latest info 
  // returns: the result of the stat() or fstat() system call.
{
  return StatHandle::update();
}

std::ostream& 
StatFifo::show( std::ostream& s, int indent, const char* nspace ) const
  // purpose: format content as XML onto stream
  // paramtr: s (IO): stream to put things on
  //          indent (IN): indentation depth, negative for none
  //          nspace (IN): tag namespace, if not null
  // returns: s
{
  // sanity check
  if ( ! isValid() ) return s;

  // <fifo> element
  s << XML::startElement( s, "fifo", indent, nspace );
  s << " name=\"" << m_filename << '"';
  s << " descriptor=\"" << m_descriptor << '"';
  s << " count=\"" << m_count << '"';
  s << " rsize=\"" << m_rsize << '"';
  s << " wsize=\"" << m_wsize << '"';

  return s << "/>\r\n";
}

std::string 
StatFifo::show( int indent, const char* nspace ) const
  // purpose: Generate the element-specific information. Called from toXML()
  // paramtr: indent (IN): indentation level of tag
  //          nspace (IN): tag namespace, if not null
  // returns: string with the information.
{
  std::string buffer;
  if ( ! isValid() ) return buffer;

  // <fifo> element 
  buffer += XML::startElement( "fifo", indent, nspace ) +
    " name=\"" + m_filename + "\" descriptor=\"";
  buffer += XML::printf( "%d\" count=\"%u\" rsize=\"%u\" wsize=\"%u\"/>\r\n",
			 m_descriptor, m_count, m_rsize, m_wsize );

  return buffer;
}

int
StatFifo::forcefd( int fd ) const
  // purpose: force open a file on a certain fd
  // paramtr: fd (IN): is the file descriptor to plug onto. If this fd is 
  //          the same as the descriptor in info, nothing will be done.
  // returns: 0 if all is well, or fn was NULL or empty.
  //          1 if opening a filename failed,
  //          2 if dup2 call failed
{
  return StatHandle::forcefd( fd );

}
