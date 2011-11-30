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
#ifndef _APPINFO_HH
#define _APPINFO_HH

#include <sys/types.h>
#include <sys/resource.h>

#include "maptypes.hh"
#include "time.hh"
#include "xml.hh"
#include "statinfo.hh"
#include "jobinfo.hh"
#include "stagejob.hh"
#include "shared.hh"
#include "uname.hh"

#include <list>
#include <map>

#ifndef HAS_MUTABLE
#define mutable
#endif

class AppInfo : public XML {
  // Class to collect everything around grid shell. There is usually 
  // only one instance of this class. Nevertheless, the singleton
  // pattern is not enforced.
private:
  std::string    m_self;	// gridstart's argv[0] argument
  Time           m_start;       // point of time that app was started 
  pid_t          m_child;       // pid of gridstart itself
  mutable bool   m_isPrinted;   // flag to set after successful print op

  UseInfo*       m_usage;       // rusage record for myself
  StatInfo*      m_stdin;       // stat() info for "input", if available
  StatInfo*      m_stdout;      // stat() info for "output", if available
  StatInfo*      m_stderr;      // stat() info for "error", if available
  StatInfo*      m_logfile;     // stat() info for "logfile", if available
  StatInfo*      m_gridstart;   // stat() info for myself, if available
  StatInfo*      m_channel;     // stat() on application channel FIFO

  char           m_ipv4[16];    // host address of primary interface
  Uname	         m_uname;	// uname -a and architecture

  StringList     m_xformation;  // chosen VDC TR fqdn trail (incl. compound)
  std::string    m_derivation;  // chosen VDC DV fqdn for this invocation 
  std::string    m_workdir;     // CWD at point of execution
  std::string    m_pool_handle;	// optional pool handle
  std::string    m_xmlns;	// XML namespace for output, usually empty

  typedef std::list<JobInfo*> JobInfoList;
  static void free_job( JobInfo* );

  JobInfoList    m_setup;       // optional setup application to run
  JobInfoList    m_prejob;      // optional pre-job application to run
  JobInfo*       m_application; // the application itself that was run
  JobInfoList    m_postjob;     // optional post-job application to run
  JobInfoList    m_cleanup;     // optional clean-up application to run

  StageJob*	 m_stagein;	// optional script for 2nd-level stage-in
  StageJob*	 m_stageout;	// optional script for 2nd-level stage-out

  FilenameMap      m_input_lfn_sfn;	// iLFN -> iSFN
  FilenameMultiMap m_input_sfn_tfn;	// iSFN -> iTFN, iTFN, ...
  FilenameMap	   m_output_lfn_sfn;	// oLFN -> oSFN
  FilenameMultiMap m_output_sfn_tfn;	// oSFN -> oTFN, oTFN, ...

  typedef std::map< std::string, StatFile* > StatFileMap;
  static void free_statinfo( StatFileMap::value_type& sim );

  StatFileMap	 m_input_info;
  StatFileMap    m_output_info;

  // render inaccessible
  AppInfo();
  AppInfo( const AppInfo& );
  AppInfo& operator=( const AppInfo& );

public:
  AppInfo( const char* self );
    // purpose: initialize the data structure with defaults.
    // paramtr: self (IN): the argv[0] from main()

  virtual ~AppInfo();
    // purpose: dtor

  inline std::string getSelf() const	{ return m_self; }
  
  inline const StatInfo* getStdin() const	{ return m_stdin; }
  inline const StatInfo* getStdout() const	{ return m_stdout; }
  inline const StatInfo* getStderr() const	{ return m_stderr; }

  void setStdin( StatInfo* handle );
  void setStdout( StatInfo* handle );
  void setStderr( StatInfo* handle );

  inline StatInfo* getChannel() const	{ return m_channel; }
  void setChannel( StatFifo* handle );

  inline void setPrinted( bool isPrinted )	{ m_isPrinted = isPrinted; }
  inline bool getPrinted() const		{ return m_isPrinted; }

  inline void addTransformation( const std::string& tr ) 
  { m_xformation.push_back(tr); }
  inline std::string getDerivation() const	
  { return m_derivation; }
  inline void setDerivation( const std::string& dv )	
  { m_derivation = dv; }

  inline std::string getPoolHandle() const	{ return m_pool_handle; }
  inline void setPoolHandle( const std::string& ph )	{ m_pool_handle = ph; }

  inline std::string getXMLNamespace() const	{ return m_xmlns; }
  inline void setXMLNamespace( const std::string& ns )	{ m_xmlns = ns; }

  inline void addInputSFN( const std::string& lfn, 
			   const std::string& sfn,
			   bool do_md5 = false )
  { m_input_lfn_sfn[lfn] = FilenameBool(sfn,do_md5); }

  inline void addInputTFN( const std::string& sfn, 
			   const std::string& tfn )
  { m_input_sfn_tfn.insert( FilenameMultiMap::value_type(sfn,tfn) ); }

  inline void addOutputSFN( const std::string& lfn, 
			    const std::string& sfn,
			    bool do_md5 = false )
  { m_output_lfn_sfn[lfn] = FilenameBool(sfn,do_md5); }

  inline void addOutputTFN( const std::string& sfn, 
			    const std::string& tfn )
  { m_output_sfn_tfn.insert( FilenameMultiMap::value_type(sfn,tfn) ); }

  virtual size_t createInputInfo();
    // purpose: update the internal input file statinfo map
    // returns: number of updates processed successfully.

  virtual size_t createOutputInfo();
    // purpose: update the internal output file statinfo map
    // returns: number of updates processed successfully.

  virtual ssize_t print();
    // purpose: output the given app info onto the given (stdout) fd
    // paramtr: appinfo (IN): is the collective information about the run
    // returns: the number of characters actually written (as of write() call).
    // mutable: will update the self resource usage record before print.
    // mutable: will update the isPrinted predicate after print.

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

  inline void addSetupJob( JobInfo* job )
    // purpose: add a job to the list of pre jobs, first come, first served
    // paramtr: job (IN): newly allocated job information record
    // warning: ownership of job record will pass here
  { if ( job != 0 ) m_setup.push_back(job); }

  inline void addPreJob( JobInfo* job )
    // purpose: add a job to the list of pre jobs, first come, first served
    // paramtr: job (IN): newly allocated job information record
    // warning: ownership of job record will pass here
  { if ( job != 0 ) m_prejob.push_back(job); }

  inline void addPostJob( JobInfo* job )
    // purpose: add a job to the list of post jobs, first come, first served
    // paramtr: job (IN): newly allocated job information record
    // warning: ownership of job record will pass here
  { if ( job != 0 ) m_postjob.push_back(job); }

  inline void addCleanJob( JobInfo* job )
    // purpose: add a job to the list of cleanup jobs, first come, first served
    // paramtr: job (IN): newly allocated job information record
    // warning: ownership of job record will pass here
  { if ( job != 0 ) m_cleanup.push_back(job); }

  inline void setMainJob( JobInfo* job )
    // purpose: set or replace the compute job
    // paramtr: job (IN): newly allocated job information record
    // warning: ownership of job record will pass here
  { 
    if ( job != 0 ) {
      if ( m_application != 0 ) delete m_application;
      m_application = job;
    }
  }

  inline bool hasMainJob() const
    // purpose: Predicate to determine, if a main job exists
    // returns: true, if a job exists, false otherwise
  { return m_application != 0; }

  inline void setStageIn( StageJob* job )
    // purpose: sets or replaces a stage-in job
    // paramtr: job (IN): newly allocated job information record
    // warning: ownership of job will pass here
  {
    if ( job ) {
      if ( m_stagein != 0 ) delete m_stagein;
      m_stagein = job;
    }
  }

  inline void setStageOut( StageJob* job )
    // purpose: sets or replaces a stage-out job
    // paramtr: job (IN): newly allocated job information record
    // warning: ownership of job will pass here
  {
    if ( job ) {
      if ( m_stageout != 0 ) delete m_stageout;
      m_stageout = job;
    }
  }
  
  inline std::string getWorkdir( void ) const		{ return m_workdir; }
  inline void setWorkdir( const std::string& dir )	{ m_workdir = dir; }

  size_t run( int& status );
    // purpose: run all runnable jobs. This constitutes a logical chaining of
    //          pre && main && post ; cleanup
    // paramtr: status (IO): first "failed" return code. Must be 0 to come in
    // returns: the number of jobs run

  virtual int runStageIn();
    // purpose: run the stage-in job, if one exists.
    // returns: Return code from running the stage-in job. 
    // warning: No existing job will also result in status of 0.
    // require: m_stagein valid, m_input_sfn_tfn defined

  virtual int runStageOut();
    // purpose: run the stage-out job, if one exists.
    // returns: Return code from running the stage-out job. 
    // warning: No existing job will also result in status of 0.
    // require: m_stageout valid, m_output_sfn_tfn defined

public: // static section
  static const char* getTempDir( void );
    // purpose: determine a suitable directory for temporary files.
    // warning: remote schedulers may chose to set a different TMP..
    // returns: a string with a temporary directory, may still be NULL.
    // warning: result is _not_ allocated, so don't free the string

  static char* pattern( char* buffer, size_t size,
			const char* dir, const char* sep, const char* file );
    // purpose: concatenate directory, separator and filename into one string
    // paramtr: buffer (OUT): where to put the string
    //          size (IN): capacity of buffer
    //          dir (IN): directory pointer, see getTempDir()
    //          sep (IN): pathname separator as string
    //          file (IN): file to add to path
    // returns: buffer

  static int nfs_sync( int fd, long micros = 100000 );
    // purpose: tries to force NFS to update the given file descriptor
    // paramtr: fd (IN): descriptor of an open file
    // returns: 0 is ok, -1 for failure

  static ssize_t writen( int fd, const char* buffer, ssize_t n, 
			 unsigned restart );
    // purpose: write all n bytes in buffer, if possible at all
    // paramtr: fd (IN): filedescriptor open for writing
    //          buffer (IN): bytes to write (must be at least n byte long)
    //          n (IN): number of bytes to write 
    //          restart (IN): if true, try to restart write at max that often
    // returns: n, if everything was written, or
    //          [0..n-1], if some bytes were written, but then failed,
    //         < 0, if some error occurred.
};

#endif // _APPINFO_HH
