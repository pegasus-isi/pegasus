%{
/* This may look like -*- C++ -*- code, but it is really bison
 *
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
 *
 * $Id: scan.y,v 1.11 2004/07/22 21:17:21 griphyn Exp $
 * 
 * Author:   Jens-S. Vöckler <voeckler@cs.uchicago.edu>
 * File:     scan.y
 *           2001-01-15
 *
 * $Log: scan.y,v $
 * Revision 1.11  2004/07/22 21:17:21  griphyn
 * Make the setenv() function available to statinfo.o.
 *
 * Revision 1.10  2004/06/15 20:42:36  griphyn
 * Added support for AIX.
 *
 * Revision 1.9  2004/06/08 23:51:06  griphyn
 * Fixed bug in stdin handling for statinfo StatFile, where the dup2
 * command closed the FD even if it was identical to the one we dup2'ed
 * to. This led to the wrong behavior (bad filehandle) later.
 *
 * Revision 1.8  2004/06/07 22:17:19  griphyn
 * Added "setup" feature to maintain symmetrie to "cleanup" feature.
 * Added "here" option for stdin configuration, which permits a string
 * to be dumped into temporary file from the configuration, to be used
 * as stdin for jobs.
 *
 * Revision 1.7  2004/02/23 20:21:53  griphyn
 * Added new GTPL license schema -- much shorter :-)
 *
 * Revision 1.6  2004/02/16 23:06:58  griphyn
 * Updated TR argument from string to list of strings. This will enable the TR
 * to capture the compound TR hierarchy.
 *
 * Revision 1.5  2004/02/11 22:36:28  griphyn
 * new parser.
 *
 * Revision 1.3  2004/02/04 22:16:44  griphyn
 * Made TFN argument a string list.
 *
 * Revision 1.2  2004/02/04 21:29:53  griphyn
 * Made the mapping from SFN to TFN a multimap.
 * Introduced a format string for the file format of the staging file.
 *
 * Revision 1.1  2004/02/03 23:13:17  griphyn
 * Kickstart version 2.
 *
 */
#include <sys/types.h>
#include <sys/stat.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <stdio.h>
#include <fcntl.h>

#include <ctype.h>
#include <errno.h>
#include "shared.hh"
#include "quote.hh"

#include "xml.hh"
#include "useinfo.hh"
#include "statinfo.hh"
#include "jobinfo.hh"
#include "appinfo.hh"

extern AppInfo* app;	// make globally visible for bison

unsigned long lineno = 1; // make available for flex

extern void  yyerror( const char* );
extern void  warning( const char*, const char* );
extern int   yylex();
extern int   yy_push_file( const char* );

static int   debug( const char* fmt, ... );

#ifndef HAS_SETENV
// Please do export for statinfo.o 
int setenv(const char *name, const char *value, int overwrite);
#endif // HAS_SETENV
#ifndef HAS_UNSETENV
static int unsetenv(const char *name);
#endif // HAS_UNSETENV

class setup_exception {
  // exception throw by invalid jobs in setupJob or setupStage
public:
  inline setup_exception( const std::string& a )
    :m_msg(a) { }
  inline setup_exception( const std::string& a, const std::string& b )
    :m_msg(a + " " + b) { }
  inline ~setup_exception() { }
  inline std::string getMessage() const
  { return m_msg; }

private:
  std::string m_msg;
};

typedef void (AppInfo::* AIJobMember)( JobInfo* );
static void setupJob( AIJobMember add, const char* jobtype, 
		      StringList* args );
  // purpose: Add a pre, post, cleanup or main job
  // paramtr: add (IN): pointer to AppInfo::set(Pre|Post|Main|Clean)Job member
  //          jobtype (IN): symbolic name for the job class
  //          args (IO): commandline argument string
  // except.: setup_exception for invalid jobs
  // warning: cleans up args after use

typedef void (AppInfo::* AIStageMember)( StageJob* );
static void setupStage( AIStageMember add, const char* jobtype, 
			std::string* fmt, StringList* args );
  // purpose: Add a stage-in or stage-out job
  // paramtr: add (IN): pointer to AppInfo::setStage(In|Out) member
  //          jobtype (IN): symbolic name for the job class
  //          fmt (IO): format for the temporary file
  //          args (IO): commandline argument string
  // except.: setup_exception for invalid jobs
  // warning: cleans up fmt and args after use

typedef void (AppInfo::* AIStdioMember)( StatInfo* );
static void setupStdin( AIStdioMember add, int fd, const char* name, 
			StringSet* options, std::string* fn );
  // purpose: set stdin handle for sub-processes
  // paramtr: add (IN): pointer to AppInfo::setStdin member
  //          fd (IN): STDIN_FILENO
  //          name (IN): stdin
  //          options (IO): option string list for stdin
  //          fn (IO): file description to connect stdio to
  // except.: null_pointer exception for certain file operations
  // warning: cleans up options and fn after use

static void setupStdio( AIStdioMember add, int fd, const char* name, 
			StringSet* options, std::string* fn );
  // purpose: set stdio handle for sub-processes
  // paramtr: add (IN): pointer to AppInfo::setStd(in|out|err) member
  //          fd (IN): value of STD(IN|OUT|ERR)_FILENO
  //          name (IN): one of stdin, stdout, or stderr
  //          options (IO): value of STD(IN|OUT|ERR)_FILENO
  //          fn (IO): file description to connect stdio to
  // except.: null_pointer exception for certain file operations
  // warning: cleans up options and fn after use

typedef void (AppInfo::* AIIOMember)( const std::string&, const std::string&, bool );
typedef void (AppInfo::* AIMIOMember)( const std::string&, const std::string& );
static void setupFile( AIIOMember add1, AIMIOMember add2, StringSet* options, 
		       std::string* lfn, std::string* sfn, StringList* tfn );
  // purpose: register input and output files
  // paramtr: add1 (IN): pointer to AppInfo::add(In|Out)putSFN member
  //          add2 (IN): pointer to AppInfo::add(In|Out)putTFN member
  //          options (IO): value of STD(IN|OUT|ERR)_FILENO
  //          lfn (IO): logical filename pointer
  //          sfn (IO): storage filename pointer
  //          tfn (IO): transfer filename list pointer
  // warning: cleans up after use

%}

/* reentrant parser */
/* %pure-parser */

/* token value union */
%union {
  char* 	string;
  std::string*  cxxstr;
  StringSet*	set;
  StringList*   list;
}

/* type of terminal symbols */
%token <string> TK_IDENT TK_QSTR1 TK_QSTR2

/* typeless terminal symbols: just token class, no token value */
%token TK_EOC
%token TK_INCLUDE TK_SET TK_XMLNS
%token TK_SETUP TK_PRE TK_MAIN TK_POST TK_CLEANUP
%token TK_TR1 TK_TR2 TK_DV1 TK_DV2
%token TK_INPUT TK_OUTPUT
%token TK_STAGEIN TK_STAGEOUT
%token TK_CHDIR TK_SITE TK_FEEDBACK
%token TK_STDIN TK_STDOUT TK_STDERR

/* type of non-terminals */
%type <string> identifier reserved_word
%type <cxxstr> string
%type <set> options
%type <list> stringlist argvstr
/* %type <int> command */

%%
configuration
	: /* empty rule */
	  {
	    // say hi
	    char t[64];
	    time_t now = time(0);
	    strftime( t, sizeof(t), "%Y-%m-%dT%H:%M:%S kickstart is running\n",
		      localtime(&now) );
	    debug( t );
	  }
	| configuration error TK_EOC
	  { // resynchronize after error
	    yyerrok;
	    fprintf( stderr, "reset to line %lu after error.\n", lineno );
	  }
	| configuration command
	;

string	: TK_QSTR1
	  { // dquote
	    std::string result;
	    if ( Quote::parse( $1, result, Quote::DQ_MAIN ) != Quote::FINAL ) {
	      // parse error in string
	      yyerror( "string does not parse" );
	      YYERROR;
	    } else {
	      $$ = new std::string(result);
	    }
	    if ( $1 ) free( static_cast<void*>($1) );
	  }
	| TK_QSTR2
	  { // squote
	    std::string result;
	    if ( Quote::parse( $1, result, Quote::SQ_MAIN ) != Quote::FINAL ) {
	      yyerror( "string does not parse" );
	      YYERROR;
	    } else {
	      $$ = new std::string(result);
	    }
	    if ( $1 ) free( static_cast<void*>($1) );
	  }
	;

argvstr : TK_QSTR1
	  { // dquote
	    StringList result;
	    if ( Quote::parse( $1, result, Quote::NQ_LWS ) != Quote::FINAL ) {
	      yyerror( "string does not parse" );
	      YYERROR;
	    } else {
	      $$ = new StringList(result);
	    }
	    if ( $1 ) free( static_cast<void*>($1) );
	  }
	| TK_QSTR2
	  { // squote
	    StringList result;
	    if ( Quote::parse( $1, result, Quote::NQ_LWS ) != Quote::FINAL ) {
	      yyerror( "string does not parse" );
	      YYERROR;
	    } else {
	      $$ = new StringList(result);
	    }
	    if ( $1 ) free( static_cast<void*>($1) );
	  }
	;

stringlist : /* empty list */
	  {
	    // start -- create new, empty list
	    $$ = new StringList();
	  }
	| stringlist string
	  {
	    // cont'd -- add element to list
	    if ( $2 ) {
	      $1->push_back( *$2 );
	      delete $2;
	    }
	    $$ = $1;
	  }
	;

options	: /* empty list */
	  { 
	    // start -- create new, empty list
	    $$ = new StringSet(); 
	  }
	| options TK_IDENT /* identifier */
	  {
	    if ( $2 && *$2 ) {
	      // downcase
	      for ( const char* s=$2; *s; s++ ) 
		*( const_cast<char*>(s) ) = tolower(*s);

	      // insert
	      $1->insert( std::string($2) );
	      free((void*) $2);
	    }
	    $$ = $1;
	  }
	;

reserved_word: TK_INCLUDE
	  { $$=strdup("include"); }
	| TK_SET
	  { $$=strdup("set"); }
	| TK_SETUP
	  { $$=strdup("setup"); }
	| TK_PRE
	  { $$=strdup("pre"); }
	| TK_MAIN
	  { $$=strdup("main"); }
	| TK_POST
	  { $$=strdup("post"); }
	| TK_CLEANUP
	  { $$=strdup("cleanup"); }
	| TK_TR1
	  { $$=strdup("transformation"); }
	| TK_DV1
	  { $$=strdup("derivation"); }
	| TK_TR2
	  { $$=strdup("tr"); }
	| TK_DV2
	  { $$=strdup("dv"); }
	| TK_CHDIR
	  { $$=strdup("chdir"); }
	| TK_SITE 
	  { $$=strdup("site"); }
	| TK_STDIN
	  { $$=strdup("stdin"); }
	| TK_STDOUT
	  { $$=strdup("stdout"); }
	| TK_STDERR
	  { $$=strdup("stderr"); }
	| TK_INPUT
	  { $$=strdup("input"); }
	| TK_OUTPUT
	  { $$=strdup("output"); }
	| TK_FEEDBACK
	  { $$=strdup("feedback"); }
	| TK_STAGEIN
	  { $$=strdup("stagein"); }
	| TK_STAGEOUT
	  { $$=strdup("stageout"); }
	| TK_XMLNS
	  { $$=strdup("xmlns"); }
	;

identifier: TK_IDENT
	| reserved_word
	;

command: TK_EOC
	  /* empty rule */

	/*
	 * 4.1 Other commands section 
	 */
	| TK_INCLUDE string
	  { 
	    if ( $2 != 0 ) {
	      int fd = open( $2->c_str(), O_RDONLY );
	      if ( fd != -1 ) {
		close(fd);
		debug( "including file '%s'\n", $2->c_str() );
		if ( yy_push_file($2->c_str()) == 0 ) yyparse();
	      } else {
		// file won't be accessible
		debug( "open '%s': %s\n", $2->c_str(), strerror(errno) );
	      }
	      delete $2;
	    } else {
	      debug( "illegal filename for include, ignoring\n" );
	    }
	  }
	| TK_XMLNS identifier
	  {
	    if ( $2 ) {
	      app->setXMLNamespace($2);
	      debug( "setting XML namespace to %s", $2 );
	      free((void*) $2);
	    }
	  }

	/* FIXME: add debug HERE */

	/* 
	 * 4.2 Descriptions section
	 */

	/* if we need to access RLS, set the site attribute */
	| TK_SITE string
	  { 
	    if ( $2 && $2->size() ) {
	      app->setPoolHandle( *$2 );
	      debug( "set site handle to '%s'\n", $2->c_str() );
	    } else {
	      debug( "site handle identifier string is empty, ignoring\n" );
	    }
	    if ( $2 ) delete $2;
	  }
	| TK_TR1 stringlist
	  { 
	    if ( $2 && $2->size() ) {
	      for ( StringList::const_iterator i = $2->begin(); 
		    i != $2->end(); ++i ) {
		app->addTransformation( (*i) );
		debug( "adding transformation name '%s'\n", i->c_str() ); 
	      }
	    } else {
	      debug( "transformation name is empty, ignoring\n" );
	    }
	    if ( $2 ) delete $2;
	  }
	| TK_TR2 stringlist
	  { 
	    if ( $2 && $2->size() ) {
	      for ( StringList::const_iterator i = $2->begin(); 
		    i != $2->end(); ++i ) {
		app->addTransformation( (*i) );
		debug( "adding transformation name '%s'\n", i->c_str() ); 
	      }
	    } else {
	      debug( "transformation name is empty, ignoring\n" );
	    }
	    if ( $2 ) delete $2;
	  }
	| TK_DV1 string
	  { 
	    if ( $2 && $2->size() ) {
	      app->setDerivation( *$2 );
	      debug( "set derivation name to '%s'\n", $2->c_str() ); 
	    } else {
	      debug( "derivation name is empty, ignoring\n" );
	    }
	    if ( $2 ) delete $2;
	  }
	| TK_DV2 string
	  { 
	    if ( $2 && $2->size() ) {
	      app->setDerivation( *$2 );
	      debug( "set derivation name to '%s'\n", $2->c_str() ); 
	    } else {
	      debug( "derivation name is empty, ignoring\n" );
	    }
	    if ( $2 ) delete $2;
	  }

	/* describe files so we can do intelligent things with them */
	| TK_INPUT options string string stringlist
	  { try {
	    setupFile( &AppInfo::addInputSFN, &AppInfo::addInputTFN, $2, $3, $4, $5 );
	  } catch (...) {
	    yyerror( "an exception while adding an input" );
	    YYERROR;
	  } }

	| TK_OUTPUT options string string stringlist
	  { try {
	    setupFile( &AppInfo::addOutputSFN, &AppInfo::addOutputTFN, $2, $3, $4, $5 );
	  } catch (...) {
	    yyerror( "an exception while adding an output" );
	    YYERROR;
	  } }

	/* 
	 * 4.3 Processing environment section 
	 */

	/* set an environment variable */
	| TK_SET identifier string
	  {
	    if ( $2 && *$2 && $3 ) { // *$3 can be empty to unset
	      // set the environment variable as appropriate
	      if ( $3->size() == 0 ) {
		// remove variable from environment
		unsetenv( $2 );
		debug( "removed variable '%s'\n", $2 );
	      } else {
		// add the variable to the environment
		if ( setenv( $2, $3->c_str(), 1 ) != 0 ) {
		  // unable to modify environment
		  yyerror( "unable to set environment variable" );
		  YYERROR;
		} else {
		  // new environment
		  debug( "setenv '%s' '%s'\n", $2, $3->c_str() );
		}
	      }
	    } else {
	      debug( "illegal identifier '%s'\n", ($2 ? $2 : "null") );
	    }

	    // cleanup
	    if ( $2 != 0 ) free((void*) $2);
	    if ( $3 ) delete $3;
	  }

	/* runtime environment manipulations */
	| TK_CHDIR options string
	  { 
	    if ( $3 && $3->size() ) {
	      // check the option string
	      if ( $2->find( std::string("create") ) != $2->end() ) {
		// create the directory if it does not exist
		// implemented via mkdir, which may fail -> silently
		if ( mkdir( $3->c_str(), 0777 ) == -1 ) {
		  // mkdir failed, ignore for existing dir
		  if ( errno == EEXIST ) {
		    debug( "directory '%s' already exists\n", $3->c_str() );
		  } else {
		    debug( "mkdir '%s': %s\n", $3->c_str(), strerror(errno) );
		    // make it a hard error
		    yyerror( "unable to create directory" );
		    YYERROR;
		  }
		} else {
		  debug( "created directory '%s'\n", $3->c_str() );
		}
	      }
	      // post-condition: directory may still *not* exist

	      // attempt to change into $3
	      if ( chdir($3->c_str()) == -1 ) {
		// error while chdir
		debug( "chdir '%s': %s\n", $3->c_str(), strerror(errno) );
	      } else {
		// change ok, update app
		app->setWorkdir(*$3);
		debug( "changed into directory '%s'\n", $3->c_str() );
	      }

	      // done
	      delete $3;
	    } else {
	      debug( "empty working directory string, ignoring\n" );
	    }
	    delete $2;
	  }

	/* Feedback channel for grid-aware applications */
	| TK_FEEDBACK identifier string
	  { 
	    if ( $2 == 0 || *$2 == 0 ) {
	      debug( "empty identifier for feedback, ignoring\n" );
	    } else if ( $3 == 0 || $3->size() == 0 ) {
	      debug( "empty file appointment for feedback, ignoring\n" );
	    } else {
	      // ok, $2 and $3 are existing and valid
	      std::string fn;

	      // do we need to prepend a tempdir?
	      if ( $3->at(0) != '/' ) {
		// prepend tempdir 
		const char* temp = AppInfo::getTempDir();
		if ( temp == 0 ) temp = "/tmp"; // last resort

		fn += temp;
		fn += "/";
	      }
	      fn += *$3;

	      StatFifo* fifo = new StatFifo( fn, $2 );
	      if ( fifo ) app->setChannel(fifo);
	      else {
		yyerror( "error while allocating feedback channel" );
		YYERROR;
	      }
	    }

	    if ( $2 != 0 ) free((void*) $2 );
	    if ( $3 ) delete $3;
	  }

	| TK_FEEDBACK string
	  { 
	    if ( $2 == 0 || $2->size() == 0 ) {
	      debug( "empty file appointment for feedback, ignoring\n" );
	    } else {
	      // ok, $2 is existing and valid
	      std::string fn;

	      // do we need to prepend a tempdir?
	      if ( $2->at(0) != '/' ) {
		// prepend tempdir 
		const char* temp = AppInfo::getTempDir();
		if ( temp == 0 ) temp = "/tmp"; // last resort

		fn += temp;
		fn += "/";
	      }
	      fn += *$2;

	      StatFifo* fifo = new StatFifo( fn, "GRIDSTART_CHANNEL" );
	      if ( fifo ) app->setChannel(fifo);
	      else {
		yyerror( "error while allocating feedback channel" );
		YYERROR;
	      }
	    }

	    if ( $2 ) delete $2;
	  }

	/* connect stdio filehandle of subprocesses with something */
	| TK_STDIN options string
	  { try {
	    setupStdin( &AppInfo::setStdin, STDIN_FILENO, "stdin", $2, $3 );
	  } catch ( null_pointer np ) {
	    yyerror( "unable to renew stdin" );
	    YYERROR;
	  } }

	| TK_STDOUT options string
	  { try {
	    setupStdio( &AppInfo::setStdout, STDOUT_FILENO, "stdout", $2, $3 );
	  } catch ( null_pointer np ) {
	    yyerror( "unable to renew stdout" );
	    YYERROR;
	  } }

	| TK_STDERR options string
	  { try {
	    setupStdio( &AppInfo::setStderr, STDERR_FILENO, "stderr", $2, $3 );
	  } catch ( null_pointer np ) {
	    yyerror( "unable to renew stderr" );
	    YYERROR;
	  } }

	/* 
	 * 4.4 Job commands section 
	 */
	| TK_SETUP argvstr
	  { try {
	    setupJob( &AppInfo::addSetupJob, "setup", $2 );
	  } catch ( setup_exception je ) {
	    yyerror( je.getMessage().c_str() );
	    YYERROR;
	  } }

	| TK_PRE argvstr
	  { try {
	    setupJob( &AppInfo::addPreJob, "prejob", $2 );
	  } catch ( setup_exception je ) {
	    yyerror( je.getMessage().c_str() );
	    YYERROR;
	  } }

	| TK_POST argvstr
	  { try {
	    setupJob( &AppInfo::addPostJob, "postjob", $2 );
	  } catch ( setup_exception je ) {
	    yyerror( je.getMessage().c_str() );
	    YYERROR;
	  } }

	| TK_CLEANUP argvstr
	  { try {
	    setupJob( &AppInfo::addCleanJob, "cleanup", $2 );
	  } catch ( setup_exception je ) {
	    yyerror( je.getMessage().c_str() );
	    YYERROR;
	  } }

	| TK_MAIN argvstr
	  { try {
	    setupJob( &AppInfo::setMainJob, "main", $2 );
	  } catch ( setup_exception je ) {
	    yyerror( je.getMessage().c_str() );
	    YYERROR;
	  } }

	/*
	 * 4.5 Optional 2nd-level staging section
	 */
	| TK_STAGEIN string argvstr
	  { try {
	    setupStage( &AppInfo::setStageIn, "stage-in", $2, $3 );
	  } catch ( setup_exception je ) {
	    yyerror( je.getMessage().c_str() );
	    YYERROR;
	  } }

	| TK_STAGEOUT string argvstr
	  { try {
	    setupStage( &AppInfo::setStageOut, "stage-out", $2, $3 );
	  } catch ( setup_exception je ) {
	    yyerror( je.getMessage().c_str() );
	    YYERROR;
	  } }
	;

%%

static const char* RCS_ID = 
  "$Id: scan.y,v 1.11 2004/07/22 21:17:21 griphyn Exp $";

#include <stdio.h>
#include <stdarg.h>
#include <memory>

static
int
debug( const char* fmt, ... )
{
  size_t size = getpagesize();
  std::auto_ptr<char> buffer( new char[size] );

  snprintf( &(*buffer), size, "# line %lu: ", lineno );
  strncat( &(*buffer), fmt, size-strlen(&(*buffer))-1 );

  va_list ap;
  va_start( ap, fmt );
  int result = vfprintf( stderr, &(*buffer), ap );
  va_end(ap);

  return result;
}

#ifndef HAS_SETENV
// Sorry, but I have to insist on setenv and unsetenv
extern char** environ;

int 
setenv( const char *name, const char *value, int overwrite )
{
  // cheat, ignore the overwrite flag
  size_t s1 = strlen(name);
  size_t s2 = strlen(value);

  // check for presence
  if ( ! overwrite ) {
    for ( char** s = environ; s && *s; ++s ) {
      if ( strncmp( *s, name, s1 ) != 0 && (*s)[s1] == '=' ) return 0;
    }
  }

  char* keep = static_cast<char*>( malloc(s1+s2+2) );
  if ( keep == 0 ) {
    errno = ENOMEM;
    return -1;
  }

  strcpy( keep, name );
  strcat( keep, "=" );
  strcat( keep, value );

  int result = putenv(keep);
  if ( result == -1 ) free((void*) keep);
  return result;
}
#endif // HAS_SETENV


#ifndef HAS_UNSETENV
#ifdef HAS_SETENV
extern char** environ;
#endif

static 
int
unsetenv( const char *name )
{
  // sanity checks
  if ( name == 0 || *name == 0 || strchr(name,'=') != 0 ) {
    errno = EINVAL;
    return -1;
  }

  // let's start
  size_t size = strlen(name);
  for ( char** s = environ; *s; ) {
    if ( strncmp( *s, name, size ) != 0 && (*s)[size] == '=' ) {
      // found it -- move the rest
      for ( char** t = s; *t; ++t ) t[0] = t[1];
    } else {
      // continue loop for more of the same key
      ++s;
    }
  }
  return 0;
}
#endif // HAS_UNSETENV



static
void
setupJob( AIJobMember add, const char* jobtype, StringList* args )
{
  if ( args && args->size() ) {
    // context exists
    JobInfo* job = new JobInfo( jobtype, *args );

    if ( job != 0 && job->getValidity() == JobInfo::VALID ) {
      (app->*add)( job ); // indirect member invocation
      debug( "added valid %s application %s\n", jobtype, job->getArg0() );
    } else {
      throw setup_exception( jobtype, "contains an invalid spec" );
    }
  } else {
    debug( "%s specification is empty, ignoring\n", jobtype );
  }

  if ( args ) delete args;
}

static
void
setupStage( AIStageMember add, const char* jobtype, 
	    std::string* fmt, StringList* args )
{
  if ( fmt && args && args->size() ) {
    // context exists
    StageJob* job = new StageJob( jobtype, *fmt, *args );

    if ( job != 0 && job->getValidity() == JobInfo::VALID ) {
      (app->*add)( job ); // indirect member invocation
      debug( "added valid %s application %s\n", jobtype, job->getArg0() );
    } else {
      throw setup_exception( jobtype, "job contains an invalid spec" );
    }
  } else {
    debug( "%s specification is empty, ignoring\n", jobtype );
  }

  if ( fmt ) delete fmt;
  if ( args ) delete args;
}

static
void
setupStdin( AIStdioMember add, int fd, const char* name,
	    StringSet* options, std::string* fn )
  // purpose: set stdin handle for sub-processes
  // paramtr: add (IN): pointer to AppInfo::setStdin member
  //          fd (IN): STDIN_FILENO
  //          name (IN): stdin
  //          options (IO): option list for stdin
  //          fn (IO): file description to connect stdin to
  // except.: null_pointer exception for certain file operations
  // warning: cleans up options and fn after use
{
  StatInfo* si = 0;
  for ( StringSet::iterator i=options->begin(); i != options->end(); ++i ) {
    if ( *i == "here" ) {
      // create here script, contents in fn
      char  realfn[4096];
      const char* tempdir = AppInfo::getTempDir();
      if ( tempdir == 0 ) tempdir = "/tmp";
      AppInfo::pattern( realfn, sizeof(realfn), tempdir, "/", "gs.in.XXXXXX" );
      int tempfd = mkstemp( realfn );
      if ( tempfd == -1 ) {
	debug( "unable to create tmp file %s: %d: %s\n",
	       realfn, errno, strerror(errno) );
	throw null_pointer(); 
      }
      // write contents into tmp file
      AppInfo::writen( tempfd, fn->c_str(), fn->size(), 3 );
      // rewind to start of file
      lseek( tempfd, 0, SEEK_SET );
      // add temporary file from external temp constructor
      // added benefit: The data section will repeat the first page
      delete fn;
      fn = new std::string(realfn);
      si = new StatTemporary( tempfd, realfn );
      // not nice, but there can be only one. 
      goto DONE;
    } else {
      debug( "ignoring invalid option '%s' for %s handle\n", 
	     i->c_str(), name );
    }
  }

  if  ( fn->at(0) == '-' && fn->size() == 1 ) {
    si = new StatHandle(fd);
  } else {
    si = new StatFile( *fn, O_RDONLY, false );
  }

  // may throw null_pointer exception!
 DONE:
  (app->*add)( si );
  debug( "set %s handle to use '%s'\n", name, fn->c_str() );

  if ( fn ) delete fn;
  if ( options ) delete options;
}

static
void
setupStdio( AIStdioMember add, int fd, const char* name,
	    StringSet* options, std::string* fn )
  // purpose: set stdio handle for sub-processes
  // paramtr: add (IN): pointer to AppInfo::setStd(in|out|err) member
  //          fd (IN): value of STD(IN|OUT|ERR)_FILENO
  //          name (IN): one of stdin, stdout, or stderr
  //          options (IO): value of STD(IN|OUT|ERR)_FILENO
  //          fn (IO): file description to connect stdio to
  // except.: null_pointer exception for certain file operations
  // warning: cleans up options and fn after use
{
  if ( fn && fn->size() ) {
    // check options 
    int mode = O_WRONLY | O_CREAT;
    for ( StringSet::iterator i=options->begin(); i != options->end(); ++i ) {
      if ( *i == "append" ) {
	mode |= O_APPEND;
      } else if ( *i == "truncate" ) {
	mode &= ~O_APPEND;
      } else {
	debug( "ignoring invalid option '%s' for %s handle\n", 
	       i->c_str(), name );
      }
    }

    StatInfo* si = 0;
    if  ( fn->at(0) == '-' && fn->size() == 1 ) {
      si = new StatHandle(fd);
    } else {
      si = ( fd == STDIN_FILENO ) ?
	new StatFile( *fn, O_RDONLY, 0 ) : 
	new StatFile( *fn, mode, (mode & O_APPEND) != 0 );
    }

    // may throw null_pointer exception!
    (app->*add)( si );

    debug( "set %s handle to use '%s'\n", name, fn->c_str() );
  } else {
    debug( "empty filename for %s handle, ignoring\n", name );
  }

  if ( fn ) delete fn;
  if ( options ) delete options;
}

static
void
setupFile( AIIOMember add1, AIMIOMember add2, StringSet* options, 
	   std::string* lfn, std::string* sfn, StringList* tfn )
  // purpose: register input and output files
  // paramtr: add1 (IN): pointer to AppInfo::add(In|Out)putSFN member
  //          add2 (IN): pointer to AppInfo::add(In|Out)putTFN member
  //          options (IO): value of STD(IN|OUT|ERR)_FILENO
  //          lfn (IO): logical filename pointer
  //          sfn (IO): storage filename pointer
  //          tfn (IO): transfer filename list pointer
  // warning: cleans up after use
{
  bool do_md5(false);
	    
  if ( options ) {
    // check the option string
    static std::string c_md5("md5");
    
    if ( options->find(c_md5) != options->end() ) 
      do_md5 = true;

    delete options;
  }

  if ( lfn && lfn->size() && sfn && sfn->size() && tfn ) {
    // essential information is valid and existing
    (app->*add1)( *lfn, *sfn, do_md5 );
    debug( "added SFN mapping '%s' => '%s'\n", lfn->c_str(), sfn->c_str() );

    for ( StringList::iterator i=tfn->begin(); i != tfn->end(); ++i ) {
      (app->*add2)( *sfn, *i );
      debug( "added TFN mapping '%s' => '%s'\n", sfn->c_str(), i->c_str() );
    }
  } else {
    debug( "invalid output filename specification, ignoring\n" );
  }

  if ( lfn ) delete lfn;
  if ( sfn ) delete sfn;
  if ( tfn ) delete tfn;
}
