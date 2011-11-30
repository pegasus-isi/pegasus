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
#ifndef _CHIMERA_STATINFO_HH
#define _CHIMERA_STATINFO_HH

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

#include "null.hh"
#include "xml.hh"

#if 0
#ifndef HAS_MUTABLE
#define mutable
#endif
#endif

class StatInfo : public XML {
  // Class to encapsulate and maintain stat call information about various
  // files or descriptors or temporary thingies.
public:
  enum StatSource { 
    IS_INVALID,		// not initialized
    IS_FILE,		// regular file
    IS_HANDLE,		// file descriptor
    IS_TEMP,		// temporary file (name + handle)
    IS_FIFO		// POSIX FIFO (name + handle)
  };

  StatInfo();
    // purpose: ctor

  virtual ~StatInfo();
    // purpose: dtor

  virtual int update() = 0;
    // purpose: update existing and initialized statinfo with latest info 
    // returns: the result of the stat() or fstat() system call.

  virtual std::string toXML( int indent = 0,
			      const char* id = 0 ) const;
    // purpose: XML format a stat info record into a given buffer
    // paramtr: buffer (IO): area to store the output in
    //          indent (IN): indentation level of tag
    //          id (IN): id attribute, use NULL to not generate
    // returns: string containing the element data
    // warning: dependent elements are formatted using show() and data()

  virtual std::ostream& toXML( std::ostream& s, 
			       int indent = 0, 
			       const char* nspace = 0 ) const;
    // purpose: format content as XML onto stream
    // paramtr: s (IO): stream to put things on
    //          indent (IN): indentation depth, negative for none
    //          nspace (IN): tag namespace, if not null
    // returns: s
    // warning: dependent elements are formatted using show() and data()

  virtual StatSource whoami( void ) const;
    // purpose: if you don't like RTTI
    // returns: type

  virtual int forcefd( int fd ) const = 0;
    // purpose: force open a file on a certain fd
    // paramtr: fd (IN): is the file descriptor to plug onto. If this fd is 
    //          the same as the descriptor in info, nothing will be done.
    // returns: 0 if all is well, or fn was NULL or empty.
    //          1 if opening a filename failed,
    //          2 if dup2 call failed

  bool isValid( void ) const;
    // purpose: check, if object was correctly constructed
    // returns: true for a valid object, false for an invalid one

  void invalidate( void );
    // purpose: invalide that current member
  
protected: 
  std::string	m_id;	// for certain entities, there is an ID
  std::string   m_lfn;	// for certain entities, there is a LFN

public:
  inline void setId( const std::string& id )	{ m_id = id; }
  inline std::string getId(void) const		{ return m_id; }

  inline void setLFN( const std::string& lfn )	{ m_lfn = lfn; }
  inline std::string getLFN() const		{ return m_lfn; }

private:
  static const unsigned int c_valid;
  unsigned      m_valid;	// set to a sensible cookie if valid
  
protected:
  virtual std::string data( int indent = 0, 
			    const char* nspace = 0 ) const
    // purpose: Generate special post-element code, e.g. stderr and stdout data
    // paramtr: indent (IN): indentation level for tag
    //          nspace (IN): tag namespace, if not null
    // returns: string with <data> section
  { return std::string(); }


  virtual std::ostream& data( std::ostream& s, 
			      int indent = 0, 
			      const char* nspace = 0 ) const
    // purpose: format content as XML onto stream
    // paramtr: s (IO): stream to put things on
    //          indent (IN): indentation depth, negative for none
    //          nspace (IN): tag namespace, if not null
    // returns: s
  { return s; }

  virtual std::string show( int indent = 0, 
			    const char* nspace = 0 ) const = 0;
    // purpose: Generate the element-specific information. Called from toXML()
    // paramtr: buffer (IO): area to store the output in
    //          indent (IN): indentation level of tag
    // returns: string with the information.

  virtual std::ostream& show( std::ostream& s, 
			      int indent = 0, 
			      const char* nspace = 0 ) const = 0;
    // purpose: format content as XML onto stream, called from toXML()
    // paramtr: s (IO): stream to put things on
    //          indent (IN): indentation depth, negative for none
    //          nspace (IN): tag namespace, if not null
    // returns: s

protected:
  mutable struct stat	m_stat;  // result from stat() or fstat() 
  mutable int           m_error; // reason for failed stat call
};



class StatFile : public virtual StatInfo {
  // This class handles all regular files.
public:
  typedef unsigned char Digest[16];

  StatFile( const std::string& filename, int openmode, bool truncate );
    // purpose: Initialize a stat info buffer with a filename to point to
    // paramtr: filename (IN): the filename to memorize (deep copy)
    //          openmode (IN): are the fcntl O_* flags to later open calls
    //          truncate (IN): flag to truncate stdout or stderr

#if 0
  StatFile( const char* filename, int openmode, int truncate );
    // purpose: Initialize a stat info buffer with a filename to point to
    // paramtr: filename (IN): the filename to memorize (deep copy)
    //          openmode (IN): are the fcntl O_* flags to later open calls
    //          truncate (IN): flag to truncate stdout or stderr
#endif

  virtual ~StatFile();
    // dtor

  virtual StatInfo::StatSource whoami( void ) const;
    // purpose: if you don't like RTTI
    // returns: type

  virtual int update();
    // purpose: update existing and initialized statinfo with latest info 
    // returns: the result of the stat() or fstat() system call.

  virtual int forcefd( int fd ) const;
    // purpose: force open a file on a certain fd
    // paramtr: fd (IN): is the file descriptor to plug onto. If this fd is 
    //          the same as the descriptor in info, nothing will be done.
    // returns: 0 if all is well, or fn was NULL or empty.
    //          1 if opening a filename failed,
    //          2 if dup2 call failed

  // 
  // Accessors
  //
  const std::string& getFilename() const
  { return m_filename; }

  int getOpenmode() const
  { return m_openmode; }

  ssize_t getHeaderSize() const
  { return m_hsize; }

  const unsigned char* getHeader() const
  { return m_header; }

  class sum_error {
    // class to encapsulate IO exceptions from the checksum functions.
  public:
    inline sum_error( const std::string& msg, int error = 0 )
      :m_msg(msg)
    { 
      if ( error != 0 ) {
	m_msg += ": ";
	m_msg += strerror(error);
      }
    }

    inline std::string getMessage() const
    { return m_msg; }

  private:
    std::string m_msg;
  };

  virtual void md5sum();
    // purpose: calculate the MD5 checksum over the complete file
    // throws : sum_error on IO error, bad_alloc on out of memory

  bool getMD5sum( Digest digest ) const;
    // purpose: obtains the stored MD5 sum
    // paramtr: digest (OUT): a digest area to store the 128 bits into
    // returns: true, if a string was stored in the digest area,
    //          false, if no sum was obtained, and the digest is untouched

protected:
  virtual std::string show( int indent = 0, 
			    const char* nspace = 0 ) const;
    // purpose: Generate the element-specific information. Called from toXML()
    // paramtr: buffer (IO): area to store the output in
    //          indent (IN): indentation level of tag
    // returns: string with the information.

  virtual std::ostream& show( std::ostream& s, 
			      int indent = 0, 
			      const char* nspace = 0 ) const;
    // purpose: format content as XML onto stream
    // paramtr: s (IO): stream to put things on
    //          indent (IN): indentation depth, negative for none
    //          nspace (IN): tag namespace, if not null
    // returns: s

  StatFile::StatFile();
    // purpose: ctor

protected:
  std::string   m_filename;	// name of the file to access
  int           m_openmode;	// open mode for open call
  ssize_t       m_hsize;	// valid bytes in header (result of read())
  bool          m_done_md5;	// valid info in md5sum member

  Digest        m_header;	// first bytes from file
  std::string   m_logical;	// LFN
  Digest        m_digest;	// md5sum, if applicable
};



class StatHandle : public virtual StatInfo {
  // This class handles already open files, of which only the descriptor
  // is known.
public:
  StatHandle( int descriptor );
    // purpose: Initialize a stat info buffer with a filename to point to
    // paramtr: descriptor (IN): the handle to attach to

  virtual ~StatHandle();
    // purpose: dtor

  virtual StatSource whoami( void ) const;
    // purpose: if you don't like RTTI
    // returns: type

  virtual int update();
    // purpose: update existing and initialized statinfo with latest info 
    // returns: the result of the stat() or fstat() system call.

  virtual int forcefd( int fd ) const;
    // purpose: force open a file on a certain fd
    // paramtr: fd (IN): is the file descriptor to plug onto. If this fd is 
    //          the same as the descriptor in info, nothing will be done.
    // returns: 0 if all is well, or fn was NULL or empty.
    //          1 if opening a filename failed,
    //          2 if dup2 call faile

  // 
  // Accessors
  //
  const int getDescriptor() const
  { return m_descriptor; }

protected:
  virtual std::string show( int indent = 0, 
			    const char* nspace = 0 ) const;
    // purpose: Generate the element-specific information. Called from toXML()
    // paramtr: buffer (IO): area to store the output in
    //          indent (IN): indentation level of tag
    // returns: string with the information.

  virtual std::ostream& show( std::ostream& s, 
			      int indent = 0, 
			      const char* nspace = 0 ) const;
    // purpose: format content as XML onto stream
    // paramtr: s (IO): stream to put things on
    //          indent (IN): indentation depth, negative for none
    //          nspace (IN): tag namespace, if not null
    // returns: s

protected:
  int		m_descriptor; // file descriptor 
};



class StatTemporary : public StatFile, public StatHandle {
  // This class handles temporary files.
public:
  StatTemporary( int fd, const char* fn );
    // purpose: Initialize for an externally generated temporary file
    // paramtr: fd (IN): is the connected file descriptor
    //          fn (IN): is the concretized temporary filename

  StatTemporary( char* pattern );
    // purpose: Initialize a stat info buffer with a temporary file
    // paramtr: pattern (IO): is the input pattern to mkstemp()
    // warning: pattern will be modified!

  StatTemporary( const std::string& pattern, bool c_o_e = true );
    // purpose: Initialize a stat info buffer with a temporary file
    // paramtr: pattern (IN): is the input pattern to mkstemp()
    //          c_o_e (IN): if true, set FD_CLOEXEC fd flag, unset if false
    // warning: pattern will be copied for modification

  virtual ~StatTemporary();
    // purpose: dtor

  virtual StatSource whoami( void ) const;
    // purpose: if you don't like RTTI
    // returns: type

  virtual int update();
    // purpose: update existing and initialized statinfo with latest info 
    // returns: the result of the stat() or fstat() system call.

  virtual int forcefd( int fd ) const;
    // purpose: force open a file on a certain fd
    // paramtr: fd (IN): is the file descriptor to plug onto. If this fd is 
    //          the same as the descriptor in info, nothing will be done.
    // returns: 0 if all is well, or fn was NULL or empty.
    //          1 if opening a filename failed,
    //          2 if dup2 call failed

protected:
  virtual std::string show( int indent = 0, 
			    const char* nspace = 0 ) const;
    // purpose: Generate the element-specific information. Called from toXML()
    // paramtr: indent (IN): indentation level of tag
    //          nspace (IN): tag namespace, if not null
    // returns: string with the information.

  virtual std::ostream& show( std::ostream& s, 
			      int indent = 0, 
			      const char* nspace = 0 ) const;
    // purpose: format content as XML onto stream
    // paramtr: s (IO): stream to put things on
    //          indent (IN): indentation depth, negative for none
    //          nspace (IN): tag namespace, if not null
    // returns: s

  virtual std::string data( int indent = 0, 
			    const char* nspace = 0 ) const;
    // purpose: Generate special post-element code, e.g. stderr and stdout data
    // paramtr: indent (IN): indentation level for tag
    //          nspace (IN): tag namespace, if not null
    // returns: string with the information.

  virtual std::ostream& data( std::ostream& s, 
			      int indent = 0, 
			      const char* nspace = 0 ) const;
    // purpose: format content as XML onto stream
    // paramtr: s (IO): stream to put things on
    //          indent (IN): indentation depth, negative for none
    //          nspace (IN): tag namespace, if not null
    // returns: s

protected:
  // no member data
};



class StatFifo : public StatFile, public StatHandle {
public:
  StatFifo( const std::string& pattern, const std::string& key );
    // purpose: Initialize a stat info buffer associated with a named pipe
    // paramtr: pattern (IN): input pattern to mkstemp(), iff XXXXXX suffix
    //                        otherwise append hyphen-XXXXXX
    //          key (IN): is the environment key at which to store the filename
    //                    unused if empty or not starting like an identifier

#if 0
  StatFifo( char* pattern, const char* key );
    // purpose: Initialize a stat info buffer associated with a named pipe
    // paramtr: pattern (IO): input pattern to mkstemp(), will be modified!
    //          key (IN): is the environment key at which to store the filename
#endif

  virtual ~StatFifo();
    // purpose: dtor

  virtual StatSource whoami( void ) const;
    // purpose: if you don't like RTTI
    // returns: type

  virtual int update();
    // purpose: update existing and initialized statinfo with latest info 
    // returns: the result of the stat() or fstat() system call.

  virtual int forcefd( int fd ) const;
    // purpose: force open a file on a certain fd
    // paramtr: fd (IN): is the file descriptor to plug onto. If this fd is 
    //          the same as the descriptor in info, nothing will be done.
    // returns: 0 if all is well, or fn was NULL or empty.
    //          1 if opening a filename failed,
    //          2 if dup2 call failed

  virtual void add( ssize_t read, ssize_t write, ssize_t count )
    // purpose: update the size information passed through the FIFO
  { 
    m_rsize += read; 
    m_wsize += write;
    m_count += count;
  }

protected:
  virtual std::string show( int indent = 0, 
			    const char* nspace = 0 ) const;
    // purpose: Generate the element-specific information. Called from toXML()
    // paramtr: indent (IN): indentation level of tag
    //          nspace (IN): tag namespace, if not null
    // returns: string with the information.

  virtual std::ostream& show( std::ostream& s, 
			      int indent = 0, 
			      const char* nspace = 0 ) const;
    // purpose: format content as XML onto stream
    // paramtr: s (IO): stream to put things on
    //          indent (IN): indentation depth, negative for none
    //          nspace (IN): tag namespace, if not null
    // returns: s

protected:
  size_t	m_count;	// number of total bytes
  size_t	m_rsize;	// number of bytes read from fifo
  size_t	m_wsize;	// number of bytes written to socket
};


#endif // _CHIMERA_STATINFO_HH
