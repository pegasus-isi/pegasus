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
#ifndef _STAGEJOB_HH
#define _STAGEJOB_HH

#include "maptypes.hh"
#include "jobinfo.hh"

class StageJob : public JobInfo {
  // This class assembles information about each job that could be run.
  // The difference to a regular job is that certain simplifications are
  // possible, and that it allows for certain run-time substitutions in
  // its arguments.
private:
  StatTemporary*   m_tempfile;	// information about the tempfile
  std::string      m_format;	// stores the format string

  // render inaccessible
  StageJob();
  StageJob( const StageJob& );
  StageJob& operator=( const StageJob& );

public:
  StageJob( const char* tag, const std::string& format, const char* commandline );
    // purpose: initialize the data structure by parsing a command string.
    // paramtr: tag (IN): stage-in or stage-out
    //          format (IN): format string for output
    //          commandline (IN): commandline concatenated string to separate

  StageJob( const char* tag, const std::string& format, const StringList& argv );
    // purpose: initialize the data structure by parsing a command string.
    // paramtr: tag (IN): stage-in or stage-out
    //          format (IN): format string for output
    //          argv (IN): commandline already split into arg vector

  virtual ~StageJob();
    // purpose: dtor
  
  virtual int createTempfile( const char* id, 
			      const FilenameMap& l2s,
			      const FilenameMultiMap& s2t ); 
    // purpose: create the tempfile from the external filemaps
    // paratmr: id (IN): stage-in or stage-out 
    //          l2s (IN): map with LFN to SFN mapping
    //          s2t (IN): multimap with SFN to TFN mapping
    // returns: -1 in case of error
    //           0 for nothing to do
    //          >0 for number of files

protected:
  virtual void rewrite();
    // purpose: rewrite the argv vector before calling the job
    // warning: called from system()
};

#endif // _STAGEJOB_HH
