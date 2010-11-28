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
#ifndef _JOBINFO_HH
#define _JOBINFO_HH

#include <sys/types.h>
#include <sys/time.h>
#include <sys/resource.h>
#include "shared.hh"

#include "null.hh"
#include "time.hh"
#include "xml.hh"
#include "statinfo.hh"
#include "useinfo.hh"
#include <string>

#ifndef HAS_MUTABLE
#define mutable
#endif

class AppInfo; // forward declaration

class JobInfo : public XML {
  // This class assembles information about each job that could be run.
  // A job can be a prejob, the main job, a post job, or a cleanup jobs.
  // Except for the main job, all other jobs may occur 0..N times. 
private:
  std::string   m_tag;	       // element tag, e.g. mainjob etc.

public:
  enum Validity { INVALID, VALID, NOTFOUND };

private:
  Validity      m_isValid;
  char*         m_copy;        // buffer for argument separation 

protected:
  char* const*  m_argv;	       // application executable and arguments 
  int           m_argc;        // application CLI number of arguments 

private:
  Time          m_start;      // point of time that app was started 
  Time          m_finish;     // point of time that app was reaped 

  pid_t         m_child;       // pid of process that ran application 
  int           m_status;      // raw exit status of application 
  int           m_saverr;      // errno for status < 0 

  UseInfo*      m_use;         // rusage record from reaping application status
  StatInfo*     m_executable;  // stat() info for executable, if available 

  // render inaccessible
  JobInfo();
  JobInfo( const JobInfo& );
  JobInfo& operator=( const JobInfo& );

public:
  JobInfo( const char* tag, const char* commandline );
    // purpose: initialize the data structure by parsing a command string.
    // paramtr: tag (IN): kind of job, used for XML element tag name
    //          commandline (IN): commandline concatenated string to separate

  JobInfo( const char* tag, const StringList& args );
    // purpose: initialize the data structure by parsing a command string.
    // paramtr: tag (IN): kind of job, used for XML element tag name
    //          args (IN): commandline already split into arg vector


  virtual ~JobInfo();
    // purpose: dtor

  virtual std::string toXML( int indent = 0, 
			     const char* nspace = 0 ) const;
    // purpose: XML format a job info record. 
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

  inline Validity getValidity() const	{ return m_isValid; }
  inline void setValidity( Validity v ){ m_isValid = v; }

  inline int getStatus() const		{ return m_status; }
  inline void setStatus( int status )	{ m_status = status; }

  inline int getErrno() const	{ return m_saverr; }
  inline void setErrno()	{ m_saverr = errno; }

  inline const char* getArg0() const 
  { if ( m_argv ) return m_argv[0]; else throw null_pointer(); }

  void setUse( const struct rusage* use );
    // purpose: sets the rusage information from an external source
    // paramtr: use (IN): pointer to a valid rusage record

  int wait4( int flags = 0 );
    // purpose: wrapper around system wait4() call
    // returns: result from the wait4 call. 
    // sidekck: m_child (IN): pid to check for, 1st argument of wait4
    //          m_status (OUT): set by the wait4 call
    //          m_use (OUT): rusage record will be updated

protected:
  void addArgument( const std::string& arg );
    // purpose: Adds an additional argument to the end of the CLI
    // paramtr: arg (IN): Argument string to add. Will _not_ be interpreted!
    // warning: You cannot set the application to run with this
    // warning: This (ugly) hack is for internal use for stage-in jobs.

  virtual void rewrite();
    // purpose: rewrite the argv vector before calling the job
    // warning: called from system()

  virtual bool forcefd( const StatInfo* si, int fd ) const
    // purpose: force a stdio filehandle from a statinfo record
    // paramtr: si (IN): StatInfo placeholder for a filehandle
    //          fd (IN): stdio filehandle to connect with new
    // returns: true, if all went well, or false otherwise
    // warning: called from system()
  { return ( si == 0 ? false : si->forcefd(fd)==0 ); }

public:
  int system( AppInfo* appinfo );
    // purpose: runs the current job with the given stdio connections
    // paramtr: appinfo (IO): shared record of information
    //                        isPrinted (IO): only to reset isPrinted in child
    //                        stdin (IN): connect to stdin or share
    //                        stdout (IN): connect to stdout or share
    //                        stderr (IN): connect to stderr or share
    // returns:   -1: failure in mysystem processing, check errno
    //           126: connecting child to its new stdout failed
    //           127: execve() call failed
    //          else: status of child

public:
  static int exitCode( int raw );
    // purpose: convert the raw result from wait() into a status code
    // paramtr: raw (IN): the raw exit code
    // returns: a cooked exit code
    //          < 0 --> error while invoking job
    //          [0..127] --> regular exitcode
    //          [128..] --> terminal signal + 128

  static char* findApplication( const char* fn );
    // purpose: check the executable filename and correct it if necessary
    //          absolute names will not be matched against a PATH
    // paramtr: fn (IN): current knowledge of filename
    // returns: newly allocated fqpn of path to exectuble, or NULL if not found
    // warning: use free() to free the allocation

  static bool hasAccess( const char* fn );
    // purpose: check a given file for being accessible and executable
    //          under the currently effective user and group id. 
    // paramtr: path (IN): fully qualified path to check
    // returns: true if the file is accessible, false for not

};

#endif // _JOBINFO_HH
