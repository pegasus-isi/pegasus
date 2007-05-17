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
#include <sys/types.h>
#include <ctype.h>
#include <errno.h>
#include <stdio.h>
#include <time.h>
#include <sys/time.h>
#include <sys/resource.h>
#include <sys/wait.h>
#include <signal.h>
#include <unistd.h>
#include <fcntl.h>
#include <string.h>
#include <stdlib.h>

#include <iostream>
#include "null.hh"
#include "appinfo.hh"

// truely shared globals
bool isExtended = true;     // timestamp format concise or extended 
bool isLocal = true;        // timestamp time zone, UTC or local
AppInfo* app;               // make globally visible for bison

// from bison/flex
extern FILE* yyin;
extern int yyparse();

static const char* RCS_ID =
"$Id$";

static
void
helpMe()
  // purpose: print invocation quick help and exit with error condition.
  // paramtr: run (IN): constitutes the set of currently set parameters.
  // returns: does not return - exits with an error to the OS level.
{
  std::cerr << RCS_ID << std::endl
	    << "Usage: " << app->getSelf() << " [cfg]" << std::endl
	    << " cfg\tOptional configuration file to use instead of stdin" 
	    << std::endl;

  // avoid printing of results in exit handler
  app->setPrinted( true );

  // exit with error condition
  exit(127);
}

int
main( int argc, char* argv[] )
{
  int result = -1;

  // avoid libc tz malloc errors during app->print()
  tzset();

  try {
    // create me
    if ( (app = new AppInfo(argv[0])) == 0 ) throw null_pointer();

    // take ownership for auto-delete on exit from try block
    std::auto_ptr<AppInfo> owner( app );

#if 0
    fprintf( stderr, "# appinfo=%d, jobinfo=%d, statinfo=%d, useinfo=%d\n",
	     sizeof(AppInfo), sizeof(JobInfo), sizeof(StatInfo),
	     sizeof(UseInfo) );
#endif

    // if there are no arguments, and stdin is a TTY, print help and exit
    if ( argc == 1 && isatty(STDIN_FILENO) || argc > 2 ) helpMe();

    // open parsing file
    if ( argc == 1 ) yyin = fdopen( STDIN_FILENO, "r" ); /// stdin;
    else yyin = fopen( argv[1], "r" );

    // sanity check
    if ( yyin == 0 ) {
      int saverr = errno;
      std::cerr << "open " << argv[1] << ": " << strerror(saverr) << std::endl;
      app->setPrinted( true );
      return 127;
    }

    // read until the bitter end
    /// while ( ! feof(yyin) ) yyparse();
    if ( yyparse() ) {
      // unresolved parse error
      std::cerr << "Unrecoverable error while parsing configuration" 
		<< std::endl;
      app->setPrinted( true );
      return 127;
    }

    // done with config file input
    fclose(yyin);

    // 
    // >>> start with the main work of running applications and stat calls
    //

    // act on file
    if ( ! app->hasMainJob() ) {
      std::cerr << "There is no main job" << std::endl;
      app->setPrinted( true );
      return 127;
    } 

    // Maybe here the 2nd-level stage-in???
    errno = 0;
    if ( app->runStageIn() ) {
      // what now?
      int saverr( errno );
      std::cerr << "Failure ";
      if ( saverr != 0 ) std::cerr << '"' << strerror(saverr) << "\" ";
      std::cerr << "during stage-in, ignoring" << std::endl;
    }

    // Do the stat on the input files
    app->createInputInfo();

    // execute all jobs
    result = 0;
    app->run(result);

    // Do the stat on the output files
    app->createOutputInfo();

    // Maybe here the 2nd-level stage-out???
    errno = 0;
    if ( app->runStageOut() ) {
      // what now?
      int saverr( errno );
      std::cerr << "Failure ";
      if ( saverr != 0 ) std::cerr << '"' << strerror(saverr) << "\" ";
      std::cerr << "during stage-out, ignoring" << std::endl;
    }

    // 
    // <<< done with the main work of running applications and stat calls
    //
    
    // append results atomically to logfile
    app->print();

    // done
  } catch ( null_pointer e ) {
    std::cerr << "caught kickstart NULL pointer exception" << std::endl;
  } catch ( std::exception e ) {
    std::cerr << "caught STL or derived exception" << std::endl;
  } catch (...) {
    std::cerr << "caught *unknown* exception in main()" << std::endl;
  }

  // done
  return JobInfo::exitCode(result);
}
