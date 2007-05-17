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

extern "C" {
  extern char** environ;
}

#include "getif.hh"
#include "jobinfo.hh"
#include "appinfo.hh"
#include "mysignal.hh"

static const char* RCS_ID =
"$Id: jobinfo.cc,v 1.10 2005/05/20 15:00:08 griphyn Exp $";

#ifdef sun
#define sys_siglist _sys_siglist
#endif

#if defined(AIX)
extern const char* const sys_siglist[64];
#endif

static int debug = 0;	// debug parser state machinery


bool 
JobInfo::hasAccess( const char* fn )
  // purpose: check a given file for being accessible and executable
  //          under the currently effective user and group id. 
  // paramtr: path (IN): fully qualified path to check
  //          mode (IN): permissions to check against, default executable
  // returns: true if the file is accessible, false for not
{
  // sanity check 
  if ( fn && *fn ) {
    struct stat st;
    if ( stat(fn,&st) == 0 && S_ISREG(st.st_mode) &&
#if 1
	 ( ( st.st_uid == geteuid() && (S_IXUSR & st.st_mode) == S_IXUSR ) ||
	   ( st.st_gid == getegid() && (S_IXGRP & st.st_mode) == S_IXGRP ) ||
	   ( (S_IXOTH & st.st_mode) == S_IXOTH ) )
#else
	 ( ( st.st_uid == geteuid() && 
	     (st.st_mode & mode & S_IRWXU) == (mode & S_IRWXU) ) ||
	   ( st.st_gid == getegid() && 
	     (st.st_mode & mode & S_IRWXG) == (mode & S_IRWXG) ) ||
	   ( (st.st_mode & mode & S_IRWXO) == (mode & S_IRWXO) ) )
#endif
	 )
      return true;
    else
      return false;
  } else {
    return false;
  }
}

char* 
JobInfo::findApplication( const char* fn )
  // purpose: check the executable filename and correct it if necessary
  //          absolute names will not be matched against a PATH
  // paramtr: fn (IN): current knowledge of filename
  // returns: newly allocated fqpn of path to exectuble, or NULL if not found
{
  // sanity check
  if ( ! (fn && *fn) ) return 0;

  // only check, but don't touch absolute paths
  if ( *fn == '/' ) {
    if ( JobInfo::hasAccess(fn) ) return strdup(fn);
    else return 0;
  }

  // try from CWD first (suprise!)
  if ( JobInfo::hasAccess(fn) ) return strdup(fn);

  // continue only if there is a PATH to check
  char* path = 0;
  if ( char* s = getenv("PATH") ) path = strdup(s);
  else return 0;

  // tokenize to compare
  char* t = 0;
  for ( char* s = strtok(path,":"); s; s = strtok(0,":") ) {
    size_t len = strlen(fn) + strlen(s) + 2;
    t = (char*) malloc(len);
    strncpy( t, s, len );
    strncat( t, "/", len );
    strncat( t, fn, len );
    if ( JobInfo::hasAccess(t) ) break;
    else {
      free((void*) t);
      t = 0;
    }
  }

  /* some or no matches found */
  free((void*) path);
  return t;
}




#if 0

  Parsing pre- and postjob argument line splits whitespaces in shell fashion.
  state transition table maps from start state and input character to
  new state and action. The actions are abbreviated as follows:

  abb | # | meaning
  ----+---+--------------------------------------------------------
   Sb | 0 | store input char into argument buffer
   Fb | 1 | flush regular buffer and reset argument buffer pointers
   Sv | 2 | store input char into variable name buffer
   Fv | 3 | flush varname via lookup into argument buffer and reset vpointers
  Fvb | 4 | Do Fv followed by Fb
   -  | 5 | skip (ignore) input char (do nothing)
   *  | 6 | translate abfnrtv to controls, other store verbatim
   FS | 7 | Do Fv followed by Sb
      | 8 | print error and exit

  special final states:

  state | meaning
  ------+-----------------
  F  32 | final, leave machine
  E1 33 | error 1: missing closing apostrophe
  E2 34 | error 2: missing closing quote
  E3 35 | error 3: illegal variable name
  E4 36 | error 4: missing closing brace
  E5 37 | error 5: premature end of string


  STATE |  eos |  ""  |  ''  |   {  |   }  |   $  |   \  | alnum| wspc | else |
  ------+------+------+------+------+------+------+------+------+------+------+
      0 | F,-  | 4,-  | 2,-  | 1,Sb | 1,Sb | 11,- | 14,- | 1,Sb | 0,-  | 1,Sb |
      1 | F,Fb | 4,-  | 2,-  | 1,Sb | 1,Sb | 11,- | 14,- | 1,Sb | 0,Fb | 1,Sb |
      2 | E1   | 2,Sb | 1,-  | 2,Sb | 2,Sb | 2,Sb | 3,-  | 2,Sb | 2,Sb | 2,Sb |
      3 | E1   | 2,Sb | 2,Sb | 2,Sb | 2,Sb | 2,Sb | 2,Sb | 2,Sb | 2,Sb | 2,Sb |
      4 | E2   | 1,-  | 4,Sb | 4,Sb | 4,Sb | 8,-  | 7,-  | 4,Sb | 4,Sb | 4,Sb |
      7 | E2   | 4,Sb | 4,Sb | 4,Sb | 4,Sb | 4,Sb | 4,Sb | 4,*  | 4,Sb | 4,Sb |
      8 | E2   | E2   | E2   | 9,-  | E3   | E3   | E3   |10,Sv | E3   | E3   |
      9 | E4   | E4   | E4   | E4   | 4,Fv | E3   | 9,Sv | 9,Sv | 9,Sv | 9,Sv |
     10 | E2   | 1,Fv | 4,Fv | 4,Fv | 4,Fv | 8,Fv | 7,Fv |10,Sv | 4,Fv |10,Sv |
     11 | E3   | E3   | E3   |12,-  | E3   | E3   | E3   |13,Sv | E3   | E3   |
     12 | E4   | E4   | E4   | E4   | 1,Fv | E3   |12,Sv |12,Sv |12,Sv |12,Sv |
     13 | F,Fvb| 4,Fv | 2,Fv | 1,Fv | 1,Fv | E3   |14,Fv |13,Sv | 1,Fv | 1,FS |
     14 | E5   | 1,Sb | 1,Sb | 1,Sb | 1,Sb | 1,Sb | 1,Sb | 1,Sb | 1,Sb | 1,Sb |

  REMOVED:
      5 | E1   | 5,Sb | 4,-  | 5,Sb | 5,Sb | 5,Sb | 6,-  | 5,Sb | 5,Sb | 5,Sb |
      6 | E1   | 5,Sb | 5,Sb | 5,Sb | 5,Sb | 5,Sb | 5,Sb | 5,Sb | 5,Sb | 5,Sb |

#endif

typedef const char Row[10];
typedef const Row Map[15];

static const Map actionmap = {
  { 5, 5, 5, 0, 0, 5, 5, 0, 5, 0 }, //  0 
  { 1, 5, 5, 0, 0, 5, 5, 0, 1, 0 }, //  1 
  { 8, 0, 5, 0, 0, 0, 5, 0, 0, 0 }, //  2 
  { 8, 0, 0, 0, 0, 0, 0, 0, 0, 0 }, //  3 
  { 8, 5, 4, 0, 0, 5, 5, 0, 0, 0 }, //  4 
  { 8, 0, 5, 0, 0, 0, 5, 0, 0, 0 }, //  5 (unused) 
  { 8, 0, 0, 0, 0, 0, 0, 0, 0, 0 }, //  6 (unused) 
  { 8, 0, 0, 0, 0, 0, 0, 6, 0, 0 }, //  7 
  { 8, 8, 8, 5, 8, 8, 8, 2, 8, 8 }, //  8 
  { 8, 8, 8, 8, 3, 8, 2, 2, 2, 2 }, //  9 
  { 8, 3, 3, 3, 3, 3, 3, 2, 3, 2 }, // 10 
  { 8, 8, 8, 5, 8, 8, 8, 2, 8, 8 }, // 11 
  { 8, 8, 8, 8, 3, 8, 2, 2, 2, 2 }, // 12 
  { 4, 3, 3, 3, 3, 8, 3, 2, 3, 7 }, // 13 
  { 8, 0, 0, 0, 0, 0, 0, 0, 0, 0 }  // 14 
};

static const Map statemap = {
  { 32,  4,  2,  1,  1, 11, 14,  1,  0,  1 }, //  0 
  { 32,  4,  2,  1,  1, 11, 14,  1,  0,  1 }, //  1 
  { 33,  2,  1,  2,  2,  2,  3,  2,  2,  2 }, //  2 
  { 33,  2,  2,  2,  2,  2,  2,  2,  2,  2 }, //  3 
  { 34,  1,  0,  4,  4,  8,  7,  4,  4,  4 }, //  4 
  { 33,  5,  4,  5,  5,  5,  6,  5,  5,  5 }, //  5 (unused) 
  { 33,  5,  5,  5,  5,  5,  5,  5,  5,  5 }, //  6 (unused) 
  { 34,  4,  4,  4,  4,  4,  4,  4,  4,  5 }, //  7 
  { 34, 34, 34,  9, 34, 34, 34, 10, 34, 34 }, //  8 
  { 36, 36, 36, 36,  4, 36,  9,  9,  9,  9 }, //  9 
  { 34,  1,  4,  4,  4,  8,  7, 10,  4, 10 }, // 10 
  { 35, 35, 35, 12, 35, 35, 35, 13, 35, 35 }, // 11 
  { 36, 36, 36, 36,  1, 35, 12, 12, 12, 12 }, // 12 
  { 32,  4,  2,  1,  1, 35, 14, 13,  1,  1 }, // 13 
  { 37,  1,  1,  1,  1,  1,  1,  1,  1,  1 }  // 14 
};

static 
const char* errormessage[5] = {
  "Error 1: missing closing apostrophe\n",
  "Error 2: missing closing quote\n",
  "Error 3: illegal variable name\n",
  "Error 4: missing closing brace\n",
  "Error 5: premature end of string\n"
};

static const char* translation = "abnrtv";
static const char translationmap[] = "\a\b\n\r\t\v";

static
int
xlate( const char input ) 
  // purpose: translate an input character into the character class.
  // paramtr: input (IN): input character
  // returns: numerical character class for input character.
{
  switch ( input ) {
  case 0:
    return 0;
  case '"':
    return 1;
  case '\'':
    return 2;
  case '{':
    return 3;
  case '}':
    return 4;
  case '$':
    return 5;
  case '\\':
    return 6;
  default: 
    return ( (isalnum(input) || input=='_') ? 7 :
	     ( isspace(input) ? 8 : 9 ) );
  }
}

static
void
resolve( const std::string& varname, std::string& buffer )
  // purpose: resolves a given environment variable into its value
  // paramtr: varname (IN): buffer with the variable name
  //          buffer (OUT): where to append the translation to
  // returns: -
{
  char* x = getenv( varname.c_str() );

  if ( x == 0 ) {
    // variable not found
    if ( debug ) 
      fprintf( stderr, "# %s does not exist\n", varname.c_str() );
  } else {
    // replace with value
    if ( debug ) 
      fprintf( stderr, "# %s=%s\n", varname.c_str(), x );
    buffer.append( x );
  }
}

static
int
internalParse( const char*& s, StringList& result, int state = 0 )
  // purpose: parse a commandline string into arguments
  // paramtr: s (IO): pointer into the commandline string buffer
  //          result (OUT): list of strings, partially filled on errors
  //          state (IN): start state of parse, use state 0
  // returns: final state; a stop state of 32 is fine, >32 implies an error
{
  const char* x = 0;
  std::string buffer;
  std::string varname;

  // house keeping
  if ( result.size() ) result.clear();

  // parse
  while ( state < 32 ) {
    int charclass = xlate( *s );
    int newstate = statemap[state][charclass];
    int action = actionmap[state][charclass];

    if ( debug )
      fprintf( stderr, "# state=%02d, class=%d, action=%d, newstate=%02d, "
	       "char=%02X (%c)\n",
	       state, charclass, action, newstate, 
	       *s, ((*s & 127) >= 32) ? *s : '.' );

    switch ( action ) {
    case 0: // store into buffer
      buffer += *s;
      break;
    case 1: // conditionally finalize buffer
      result.push_back(buffer);
      buffer.clear();
      break;
    case 2: // store variable part
      varname += *s;
      break;
    case 3: // finalize variable name
      resolve( varname, buffer );
      varname.clear();
      break;
    case 4: // case 3 followed by case 1
      resolve( varname, buffer );
      varname.clear();

      result.push_back( buffer );
      buffer.clear();
      break;
    case 5: // noop
      break;
    case 6: // translate control escapes
      x = strchr( translation, *s );
      buffer += ( x == 0 ? *s : translationmap[x-translation] );
      if ( debug ) fprintf( stderr, "# escape %c -> %d\n", 
			    *s, buffer[buffer.size()-1] );
      break;
    case 7: // case 3 followed by case 0
      resolve( varname, buffer );
      varname.clear();

      buffer += *s;
      break;
    case 8: // print error message
      fputs( errormessage[newstate-33], stderr );
      break;
    }
    ++s;
    state = newstate;
  }
  
  return state;
}




#if 0
JobInfo::JobInfo()
  :m_isValid(INVALID), m_copy(0),
   m_argv(0), m_argc(0),
//   m_start( c_notime ), m_finish( c_notime ),
   m_child(0), m_status(0), m_saverr(0),
   m_use(0), m_executable(0)
{
  // empty
}
#endif

static
void
init( char*& m_copy, char* const*& m_argv, int& m_argc, 
      const StringList& args )
{
  // only continue if there is anything to do
  if ( args.size() ) {
    size_t total = 0;
    for ( StringList::const_iterator i=args.begin(); i != args.end(); i++ ) {
      m_argc++;
      total += (i->size() + 1); 
    }

    // prepare copy area and argument vector
    char* t = m_copy = new char[ total + m_argc ];
    m_argv = static_cast<char* const*>( calloc( m_argc+1, sizeof(char*) ) );

    // copy list while updating argument vector 
    StringList::const_iterator temp = args.begin();
    for ( int i=0; i < m_argc && temp != args.end(); ++i ) {
      // append string to copy area
      size_t len = temp->size()+1;
      memcpy( t, temp->c_str(), len );

      // put string into argument vector - assign to readonly!
      memcpy( (void*) &m_argv[i], &t, sizeof(char*) );

      // advance
      t += len;
      temp++;
    }
  }
}

JobInfo::JobInfo( const char* tag, const char* commandline )
  // purpose: initialize the data structure by parsing a command string.
  // paramtr: commandline (IN): commandline concatenated string to separate
  :m_isValid(JobInfo::INVALID), m_copy(0),
   m_argv(0), m_argc(0),
//   m_start(), m_finish(),
   m_child(0), m_status(0), m_saverr(0),
   m_use(0), m_executable(0)
{
  // recoded for SunCC
  if ( tag ) m_tag = tag;
  else throw null_pointer();

  StringList args;
  int state = internalParse( commandline, args );

  // only continue in ok state AND if there is anything to do
  if ( state == 32 )
    init( this->m_copy, this->m_argv, this->m_argc, args );

  // free list
  args.clear();

  // this is a valid and initialized entry
  if ( m_argc > 0 ) {
    // check job path 
    const char* realpath = JobInfo::findApplication( m_argv[0] );

    if ( realpath ) {
      memcpy( (void*) &m_argv[0], &realpath, sizeof(char*) );
      m_isValid = VALID;

      // initialize data for myself
      m_executable = new StatFile( m_argv[0], O_RDONLY, 0 );
    } else {
      m_status = -127;
      m_saverr = errno;
      m_isValid = NOTFOUND;
    }
  }
}


JobInfo::JobInfo( const char* tag, const StringList& args )
  // purpose: initialize the data structure by parsing a command string.
  // paramtr: tag (IN): kind of job, used for XML element tag name
  //          argv (IN): commandline already split into arg vector
  :m_isValid(JobInfo::INVALID), m_copy(0),
   m_argv(0), m_argc(0),
//   m_start(), m_finish(),
   m_child(0), m_status(0), m_saverr(0),
   m_use(0), m_executable(0)
{
  // recoded for SunCC
  if ( tag ) m_tag = tag;
  else throw null_pointer();

  init( this->m_copy, this->m_argv, this->m_argc, args );

  // this is a valid and initialized entry
  if ( m_argc > 0 ) {
    // check job path 
    const char* realpath = JobInfo::findApplication( m_argv[0] );

    if ( realpath ) {
      memcpy( (void*) &m_argv[0], &realpath, sizeof(char*) );
      m_isValid = VALID;

      // initialize data for myself
      m_executable = new StatFile( m_argv[0], O_RDONLY, 0 );
    } else {
      m_status = -127;
      m_saverr = errno;
      m_isValid = NOTFOUND;
    }
  }
}


#if 0
static
std::string
show_argv( int argc, char* const* argv )
{
  std::string result( XML::printf( "%p:[", static_cast<const void*>(argv) ) );
  for ( int i=0; i<=argc; ++i ) {
    if ( i > 0 ) result += ',';
    result += XML::printf( "%p", static_cast<const void*>(argv[i]) );
    if ( argv[i] ) result.append("=").append(argv[i]);
  }
  result += "]";
  return result;
}
#endif

void 
JobInfo::addArgument( const std::string& arg )
  // purpose: Adds an additional argument to the end of the CLI
  // paramtr: arg (IN): Argument string to add. Will _not_ be interpreted!
  // warning: You cannot set the application to run with this
  // warning: This (ugly) hack is for internal use for stage-in jobs.
{
  // this is a valid and initialized entry
  if ( m_isValid == VALID && m_argc > 0 ) {
#if 0
    fprintf( stderr, "# old_argc=%d, old_copy=%p=%s\n",
	     m_argc, static_cast<const void*>(m_copy), m_copy );
    fprintf( stderr, "# old_argv=%s\n",
	     show_argv( m_argc, m_argv ).c_str() );
#endif
    
    size_t new_argc( m_argc+1 );
    char* const* new_argv =
      static_cast<char* const*>( calloc( new_argc+1, sizeof(char*) ) );

    // determine new copy area size
    size_t old_size = strlen(m_copy)+1; // original argv[0]
    for ( int i=1; i<m_argc && m_argv[i]; ++i )
      old_size += strlen(m_argv[i])+1;
    size_t new_size( old_size + arg.size() + 1 );

    // copy over to new area
    char* new_copy = new char[new_size];
    memset( new_copy, 0, new_size );

    char* t = new_copy;
    for ( int i=0; i<m_argc && m_argv[i]; ++i ) {
      // append to copy area
      size_t len;
      if ( i == 0 ) {
	// copy original argv[0] from m_copy
	len = strlen( m_copy )+1;
	memcpy( t, m_copy, len );
      } else {
	// copy from m_argv[] for convenience
	len = strlen( m_argv[i] )+1;
	memcpy( t, m_argv[i], len );
      }

      // put into argument vector - assign to readonly!
      memcpy( (void*) &new_argv[i], &t, sizeof(char*) );

      // advance
      t += len;
    }

    // add FQPN-modified original argv[0]
    memcpy( (void*) &new_argv[0], &m_argv[0], sizeof(char*) );

    // add new CLI argument
    memcpy( t, arg.c_str(), arg.size()+1 );
    memcpy( (void*) &new_argv[m_argc], &t, sizeof(char*) );

    // finalize vector
    t = 0;
    memcpy( (void*) &new_argv[new_argc], &t, sizeof(char*) );

#if 0
    fprintf( stderr, "# new_argc=%d, new_copy=%p=%s\n",
	     new_argc, static_cast<const void*>(new_copy), new_copy );
    fprintf( stderr, "# new_argv=%s\n",
	     show_argv( new_argc, new_argv ).c_str() );
#endif

    // switch new to old
    m_argc = new_argc;
    delete[] m_copy;
    m_copy = new_copy;
    free((void*) m_argv);
    m_argv = new_argv;
  }
}

JobInfo::~JobInfo()
  // purpose: dtor
{
  if ( m_isValid == VALID ) {
    // from findApp() allocation
    if ( m_argv[0] ) free((void*) m_argv[0]);

    // done with stat information
    delete m_executable;
    m_executable = 0;
  }

  if ( m_use ) {
    delete m_use;
    m_use = 0;
  }

  if ( m_copy ) {
    free((void*) m_argv );
    m_argv = 0;

    delete[] m_copy;
    m_copy = 0;
  }

  /* final invalidation */
  m_isValid = INVALID;
}

void
JobInfo::rewrite()
{
  // empty
}

std::ostream& 
JobInfo::toXML( std::ostream& s, int indent, const char* nspace ) const
  // purpose: XML format a rusage info record onto a given stream
  // paramtr: s (IO): stream to put information into
  //          indent (IN): indentation level of tag
  //          nspace (IN): If defined, namespace prefix before element
  // returns: s
{
  // sanity check
  if ( m_isValid != VALID ) return s;

  // start element
  s << XML::startElement( s, m_tag, indent, nspace );

  // attributes
  s << " start=\"" << m_start
    << "\" duration=\""
    << std::setfill('0') << std::setprecision(3) 
    << m_start.elapsed(m_finish) << '"';

  // optional attribute: application process id
  if ( m_child != 0 ) s << " pid=\"" << m_child << '"';
  s << ">\r\n";

  // <usage>
  if ( m_use ) m_use->toXML( s, indent+2, nspace );

  // <status>: open tag
  s << XML::startElement( s, "status", indent+2, nspace );
  s << " raw=\"" << m_status << "\">";

  // <status>: cases of completion
  if ( m_status < 0 ) {
    // <failure>
    s << XML::startElement( s, "failure", 0, nspace );
    s << " error=\"" << m_saverr << "\">" << strerror(m_saverr);
    s << XML::finalElement( s, "failure", 0, nspace, false );
  } else if ( WIFEXITED(m_status) ) {
    // <regular>
    s << XML::startElement( s, "regular", 0, nspace );
    s << " exitcode=\"" << WEXITSTATUS(m_status) << "\"/>";
  } else if ( WIFSIGNALED(m_status) ) {
    // <signalled>
    // result = 128 + WTERMSIG(m_status);
    s << XML::startElement( s, "signalled", 0, nspace );
    s << " signal=\"" << WTERMSIG(m_status) << '"';
#ifdef WCOREDUMP
    s << " corefile=\"" << (WCOREDUMP(m_status) ? "true" : "false" ) << '"';
#endif
    s << '>' << sys_siglist[WTERMSIG(m_status)];
    s << XML::finalElement( s, "signalled", 0, nspace, false );
  } else if ( WIFSTOPPED(m_status) ) {
    // <suspended>
    s << XML::startElement( s, "suspended", 0, nspace );
    s << " signal=\"" << WSTOPSIG(m_status) << "\">";
    s << sys_siglist[WSTOPSIG(m_status)];
    s << XML::finalElement( s, "suspended", 0, nspace, false );
  } // FIXME: else?

  // </status>
  s << XML::finalElement( s, "status", 0, nspace );

  // <executable>
  m_executable->toXML( s, indent+2, nspace );

  // <arguments>
  s << XML::startElement( s, "arguments", indent+2, nspace );
  s << " argc=\"" << m_argc << '"';

  if ( m_argc == 1 ) {
    // empty element
    s << "/>\r\n";
  } else {
    // content are the CLI args
    s << ">\r\n";

    for ( int i=1; i<m_argc; ++i ) {
      s << XML::startElement( s, "argv", indent+4, nspace ) << '>';
      s << XML::quote(m_argv[i]) << XML::finalElement( s, "argv", 0, nspace );
    }

    // </arguments>
    s << XML::finalElement( s, "arguments", indent+2, nspace );
  }
  
  // finalize close tag of outmost element </"m_tag">
  s << XML::finalElement( s, m_tag, indent, nspace );
  return s;
}

std::string
JobInfo::toXML( int indent, const char* nspace ) const
  // purpose: format the job information into the given buffer as XML.
  // paramtr: buffer (IO): area to store the output in (append)
  //          indent (IN): indentation level
  // returns: the buffer
{
  std::string buffer;

  // sanity check
  if ( m_isValid != VALID ) return buffer;

  // start tag with indentation
  buffer += XML::startElement( m_tag, indent, nspace );

  // start time and duration
  buffer += " start=\"" + m_start.date();
  buffer += XML::printf( "\" duration=\"%.3f\"", m_start.elapsed(m_finish) );

  // optional attribute: application process id
  if ( m_child != 0 ) buffer += XML::printf( " pid=\"%d\"", m_child );
  buffer += ">\r\n";

  // <usage>
  if ( m_use ) buffer += m_use->toXML( indent+2, nspace );

  // <status>: open tag
  buffer += XML::startElement( "status", indent+2, nspace );
  buffer += XML::printf( " raw=\"%d\">", m_status );

  // <status>: cases of completion
  if ( m_status < 0 ) {
    // <failure>
    buffer += XML::startElement( "failure", 0, nspace );
    buffer += XML::printf( " error=\"%d\">%s",
			   m_saverr, strerror(m_saverr) );
    buffer += XML::finalElement( "failure", 0, nspace, false );
  } else if ( WIFEXITED(m_status) ) {
    buffer += XML::startElement( "regular", 0, nspace );
    buffer += XML::printf( " exitcode=\"%d\"/>", WEXITSTATUS(m_status) );
  } else if ( WIFSIGNALED(m_status) ) {
    // result = 128 + WTERMSIG(m_status);
    buffer += XML::startElement( "signalled", 0, nspace );
    buffer += XML::printf( " signal=\"%u\"", WTERMSIG(m_status) );
#ifdef WCOREDUMP
    buffer += XML::printf( " corefile=\"%s\"", 
			   WCOREDUMP(m_status) ? "true" : "false" );
#endif
    buffer += ">";
    buffer += sys_siglist[WTERMSIG(m_status)];
    buffer += XML::finalElement( "signalled", 0, nspace, false );
  } else if ( WIFSTOPPED(m_status) ) {
    buffer += XML::startElement( "suspended", 0, nspace );
    buffer += XML::printf( " signal=\"%u\">", WSTOPSIG(m_status) );
    buffer += sys_siglist[WSTOPSIG(m_status)];
    buffer += XML::finalElement( "suspended", 0, nspace, false );
  } // FIXME: else?
  buffer += XML::finalElement( "status", 0, nspace );

  // <executable>
  buffer += m_executable->toXML( indent+2, nspace );

  // <arguments>
  buffer += XML::startElement( "arguments", indent+2, nspace );
  buffer += XML::printf( " argc=\"%d\"", m_argc );

  if ( m_argc == 1 ) {
    // empty element
    buffer += "/>\r\n";
  } else {
    // content are the CLI args
    buffer += ">\r\n";
    for ( int i=1; i<m_argc; ++i ) {
      buffer += XML::startElement( "argv", indent+4, nspace ) + ">";
      buffer += XML::quote(m_argv[i]);
      buffer += XML::finalElement( "argv", 0, nspace );
    }

    // end tag
    buffer += XML::finalElement( "arguments", indent+2, nspace );
  }
  
  // finalize close tag of outmost element
  buffer += XML::finalElement( m_tag, indent, nspace );
  return buffer;
}

void 
JobInfo::setUse( const struct rusage* use )
  // purpose: sets the rusage information from an external source
  // paramtr: use (IN): pointer to a valid rusage record
{
  if ( m_use ) delete m_use;
  // the element name inside a job element is "usage"
  m_use = new UseInfo( "usage", use );
}

int
JobInfo::exitCode( int raw ) 
  // purpose: convert the raw result from wait() into a status code
  // paramtr: raw (IN): the raw exit code
  // returns: a cooked exit code
  //          < 0 --> error while invoking job
  //          [0..127] --> regular exitcode
  //          [128..] --> terminal signal + 128
{
  int result = 127;

  if ( raw < 0 ) {
    // nothing to do to result
  } else if ( WIFEXITED(raw) ) {
    result = WEXITSTATUS(raw);
  } else if ( WIFSIGNALED(raw) ) {
    result = 128 + WTERMSIG(raw); 
  } else if ( WIFSTOPPED(raw) ) {
    // nothing to do to result
  }

  return result;
}

int
JobInfo::wait4( int flags )
{
  struct rusage ru;
  int result = ::wait4( m_child, &m_status, flags, &ru );
  setUse( &ru );
  return result;
}

static
int
debug_msg( const char* fmt, ... )
  // try to be reentrant... 
{
  char buffer[1024];
  size_t size = sizeof(buffer);
  struct timeval me( Time::now() );
  struct tm temp;

  strftime( buffer, size, "# %Y-%m-%dT%H:%M:%S",
	    localtime_r(&me.tv_sec,&temp) );
  snprintf( buffer+strlen(buffer), size-strlen(buffer)-1,
	    ".%03ld: ", me.tv_usec / 1000 );

  va_list ap;
  va_start( ap, fmt );
  int result = vsnprintf( buffer + strlen(buffer),
			  size-strlen(buffer)-1, fmt, ap );
  write( STDERR_FILENO, buffer, strlen(buffer) );
  va_end(ap);

  return result;
}

//
// --- class set_signal ------------------------------------------------
//

class set_signal : public null_pointer {
public:
  set_signal() throw() { }
  virtual ~set_signal() throw();
};

set_signal::~set_signal() throw()
{ 
  // empty
}

//
// --- class SignalHandlerCommunication --------------------------------
//

#ifdef SUNOS
#ifdef SIG_ERR
#undef SIG_ERR
#define SIG_ERR ((SigFunc*)-1)
#endif
#endif

class SignalHandlerCommunication {
  // Encapsulates communications with the signal handler into a Singleton.
  // It also installs a SIGCHLD handler with the constructor, and removes
  // it with the destructor. Since both, ctor and dtor are protected, the
  // only access is granted through a single instance via the Singleton
  // pattern.
  //
  // This class is not thread-safe, nor truly reentrant.
protected:
  SignalHandlerCommunication( JobInfo* job );
    // purpose: c'tor installs SIGCHLD handler
    // paramtr: job (IN): job information to update in SIGCHLD handler

  ~SignalHandlerCommunication();
    // purpose: d'tor

  static SIGRETTYPE sig_child( SIGPARAM signo );
    // purpose: signal handler for SIGCHLD, updating job information
    // paramtr: signo (IN): signal number from OS, os-dependent
    // returns: usually a void function

public:
  // Singleton accessors
  static SignalHandlerCommunication* init( JobInfo* job );
    // purpose: first time initialization instead of instance()
    // paramtr: job (IO): pointer to the job which installs the handler
    // returns: a pointer to the single instance

  static SignalHandlerCommunication* instance();
    // purpose: next time instance() accessor
    // returns: a pointer to the single instance
    // warning: need to initialize with init() first

  static void done( void );
    // purpose: last time tear down of the singleton object

  //
  // Accessors
  //
  typedef volatile sig_atomic_t AtomicType;

  inline AtomicType isDone() const	{ return m_done; }
  inline AtomicType& isDone()		{ return m_done; }

  inline const JobInfo* job() const	{ return m_job; }
  inline JobInfo* job()			{ return m_job; }

private:
  // protected from ever being used
  SignalHandlerCommunication();
  SignalHandlerCommunication( const SignalHandlerCommunication& );
  SignalHandlerCommunication& operator=( const SignalHandlerCommunication& );

  // singleton instance 
  static SignalHandlerCommunication*	m_instance;

  // regular members
  AtomicType	m_done;
  JobInfo*	m_job;
  SigFunc*	m_old_sigchild;
};

SignalHandlerCommunication* 
SignalHandlerCommunication::m_instance = 0; 

SignalHandlerCommunication::SignalHandlerCommunication( JobInfo* job )
  // purpose: c'tor installs SIGCHLD handler
  // paramtr: job (IN): job information to update in SIGCHLD handler
  :m_done(0), m_job(job) 
{ 
  if ( job == 0 ) throw null_pointer();
  m_old_sigchild = mysignal( SIGCHLD, sig_child, true );
  if ( m_old_sigchild == SIG_ERR ) throw set_signal();
}

SignalHandlerCommunication::~SignalHandlerCommunication()
  // purpose: dtor
{
  // reset signal -- all we can attempt
  if ( mysignal( SIGCHLD, m_old_sigchild, true ) == SIG_ERR )
    throw set_signal();
}

SIGRETTYPE
SignalHandlerCommunication::sig_child( SIGPARAM signo )
  // purpose: signal handler for SIGCHLD, updating job information
  // paramtr: signo (IN): signal number from OS, os-dependent
  // returns: usually a void function
{
  if ( debug ) debug_msg( "seen signal %d\n", signo );
#if 0
  if ( instance()->job()->wait4(0) == -1 ) instance()->job()->setStatus(-1);
  instance()->job()->setErrno();
#else
  if ( ! instance()->isDone() ) {
    while ( instance()->job()->wait4(0) < 0 ) {
      if ( errno != EINTR ) {
	instance()->job()->setStatus(-1);
	break;
      }
    }
    instance()->job()->setErrno();
  }
#endif
  instance()->isDone() = 1;
}

SignalHandlerCommunication* 
SignalHandlerCommunication::init( JobInfo* job )
  // purpose: first time initialization instead of instance()
  // paramtr: job (IO): pointer to the job which installs the handler
  // returns: a pointer to the single instance
{
  if ( m_instance ) delete m_instance;
  m_instance = new SignalHandlerCommunication(job);
  return m_instance;
}

SignalHandlerCommunication* 
SignalHandlerCommunication::instance()
  // purpose: next time instance() accessor
  // returns: a pointer to the single instance
  // warning: need to initialize with init() first
{
  if ( m_instance == 0 ) throw null_pointer();
  return m_instance;
}

void 
SignalHandlerCommunication::done( void )
  // purpose: last time tear down of the singleton object
{
  if ( m_instance ) delete m_instance;
  m_instance = 0;
}

//
// --- class EventLoop -------------------------------------------------
//

class EventLoop {
  // Encapsulates the event loop for optional reading from a FIFO, and
  // propagating the information onto the stderr of the gridshell. The
  // stderr will be forwarded by Globus-IO to the remote submit host.
  //
  // The EventLoop handler will _always_ be called, even in the absence
  // of a feedback channel. The handler does provide an exponential backed
  // off heartbeat of the child.
  //
  // This class is not thread-safe, nor truly reentrant.
public:
  EventLoop( pid_t child, StatInfo* fifo, int outfd = STDERR_FILENO );
    // purpose: Set up the connection between a FIFO (in) and stderr (out)
    // paramtr: child (IN): child process to check status of
    //          fifo (IN): Stat handle for the FIFO -- may be 0
    //          outfd (IN): handle to format messages onto
    // warning: If fifo cannot be casted to StatFifo*, it will throw
    //          a null_pointer exception. However, NULL is a legal value. 

  ssize_t send( const char* msg, ssize_t msize, int channel = 0 ) const;
    // purpose: writes the message to the remote submit host
    // paramtr: msg (IN): message buffer
    //          msize (IN): size of message to actually use
    //          channel (IN): channel number - negative are system channels!
    // returns: size of actual message written. Since the message will be
    //          XML wrapped, it is larger than the input message.
    // warning: If the FIFO (src) or channel (dst) is not defined, it will
    //          simulate success without writing anything. 

  bool handle( struct pollfd& pfds, int& result,
	       SignalHandlerCommunication::AtomicType& terminate,
	       char* rbuffer, size_t bufsize );
    // purpose: if poll returned 1, handle the waiting data
    // paramtr: pfds (IO): poll structure
    //          result (IO): return value for outer loop
    //          terminate (IN): volatile flag from SIGCHLD
    //          rbuffer (IO): i/o buffer area
    //          bufsize (IN): size of buffer area
    // returns: true, if outer loops needs to be exited, 
    //          false to continue with outer loop.

  int loop( SignalHandlerCommunication::AtomicType& terminate,
	    double heartbeat = 30.0 );
    // purpose: Periodically wake up and send back FIFO stuff and heart beats.
    // paramtr: termiante (IO): reference to an external "done" flag. This
    //                          is typically from the SIGCHLD handler.
    //          heartbeat (IN): initial heartbeat interval.
    // returns: -1 in case of error, 0 for o.k.

private:
  // render inaccessible
  EventLoop();
  EventLoop( const EventLoop& );
  EventLoop& operator=( const EventLoop& );

  static SignalHandlerCommunication::AtomicType m_seen_sigpipe;

  static SIGRETTYPE sig_pipe( SIGPARAM signo )
  {
    if ( debug ) debug_msg( "seen signal %d\n", signo );
    EventLoop::m_seen_sigpipe = 1; 
  }

  pid_t         m_child;
  int		m_outfd;
  StatFifo*	m_fifo;
  SigFunc*	m_old_sigpipe;
};

SignalHandlerCommunication::AtomicType 
EventLoop::m_seen_sigpipe = 0;

EventLoop::EventLoop( pid_t child, StatInfo* fifo, int outfd )
  // purpose: Set up the connection between a FIFO (in) and stderr (out)
  // paramtr: child (IN): child process to check status of
  //          fifo (IN): Stat handle for the FIFO
  //          outfd (IN): handle to format messages onto
  // warning: If fifo cannot be casted to StatFifo*, it will throw
  //          a null_pointer exception. However, NULL is a legal value. 
  :m_child(child), m_outfd(outfd), m_fifo(0)
{ 
  if ( fifo && (m_fifo = dynamic_cast<StatFifo*>(fifo)) == 0 ) 
    throw null_pointer();
  // NULL m_fifo is legal
}

ssize_t
EventLoop::send( const char* msg, ssize_t msize, int channel ) const
  // purpose: writes the message to the remote submit host
  // paramtr: msg (IN): message buffer
  //          msize (IN): size of message to actually use
  //          channel (IN): channel number - negative are system channels!
  // returns: size of actual message written. Since the message will be
  //          XML wrapped, it is larger than the input message.
  // warning: If the channel (dst) is not defined, it will
  //          simulate success without writing anything. 
{
  // sanity -- simulate success
  if ( m_outfd == -1 ) return msize;
  
  // compose output message
  std::string buffer;
  buffer.reserve( msize+128 );
  buffer += XML::printf( "<chunk channel=\"%u\" size=\"%ld\" when=\"",
			 channel, msize );
  
  Time t;
  buffer += t.date();
  buffer += "\"><![CDATA[" + std::string( msg, msize );
  buffer += "]]></chunk>\r\n";
  
  // almost-atomic write to outfd
  ssize_t result = 
    AppInfo::writen( m_outfd, buffer.c_str(), buffer.size(), 3 );
  
  // NFS sync for gatekeeper troubles
  AppInfo::nfs_sync( m_outfd );
  
  // done
  return result;
}

bool
EventLoop::handle( struct pollfd& pfds, int& result,
		   SignalHandlerCommunication::AtomicType& terminate,
		   char* rbuffer, size_t bufsize )
  // purpose: if poll returned 1, handle the waiting data
  // paramtr: pfds (IO): poll structure
  //          result (IO): return value for outer loop
  //          terminate (IN): volatile flag from SIGCHLD
  //          rbuffer (IO): i/o buffer area
  //          bufsize (IN): size of buffer area
  // returns: true, if outer loops needs to be exited, 
  //          false to continue with outer loop.
{
  static const int mask = POLLIN | POLLERR | POLLHUP | POLLNVAL;

  // poll OK, data is waiting
  if ( (pfds.revents & mask) > 0 ) {
    ssize_t rsize = read( pfds.fd, rbuffer, bufsize-1 );
    if ( rsize == -1 ) {
      // error while reading
      if ( errno != EINTR ) {
	result = -1;
	return true; // do exit
      }

      // check our signal interruptions
      if ( terminate || EventLoop::m_seen_sigpipe ) {
	result = 0;
	return true; // do exit
      }
    } else if ( rsize == 0 ) {
      // EOF -- close file and be done? This is a FIFO!
      if ( debug ) debug_msg( "seen an EOF on FIFO\n" );
      // FIXME: Faulty logic! If the child spawned multiple subprocesses
      // we may have multiple writers. Only exit, if the child is gone. 
      // result = 0;
      // return true;

      // FIXED: Open a server FIFO with O_RDWR to *not* having to deal
      // with EOF conditions whenever the clients drop to zero!
      result = 0;
      return true;
    } else {
      // data available
      ssize_t wsize = this->send( rbuffer, rsize, 1 );
      if ( wsize == -1 ) {
	// unable to send anything further due to error condition
	result = -1;
	return true; // do exit
      } else {
	// update statistics
	if ( m_fifo ) m_fifo->add( rsize, wsize, 1 );
      }
    } // IF rsize > 0
  } // IF revents & mask

  return false;
}

int
EventLoop::loop( SignalHandlerCommunication::AtomicType& terminate,
		 double heartbeat )
  // purpose: Periodically wake up and send back FIFO stuff and heart beats.
  // paramtr: termiante (IO): reference to an external "done" flag. This
  //                          is typically from the SIGCHLD handler.
  //          heartbeat (IN): initial heartbeat interval.
  // returns: -1 in case of error, 0 for o.k.
{
  int result = 0;

  // sanity checks
  if ( m_outfd == -1 ) return 0;
  
  // become aware of SIGPIPE for write failures
  SigFunc* old_pipe = mysignal( SIGPIPE, EventLoop::sig_pipe, true );
  if ( old_pipe == SIG_ERR ) {
    if ( debug ) debug_msg( "unable to set SIGPIPE handler\n" );
    return -1;
  } else {
    if ( debug ) debug_msg( "in poll loop\n" );
  }
  
  // prepare poll FDs
  struct pollfd pfds = {
    m_fifo ? m_fifo->getDescriptor() : -1,  // .fd
    POLLIN,	// .events
    0 };	// .revents
  
  // allocate buffer
  size_t bufsize = getpagesize();
  char*  rbuffer = new char[bufsize];
  
  // heart-beat variables
  Time   hb_start;
  size_t hb_count = 0;
  
  // poll -- may be interrupted by SIGCHLD
  bool isNext = false;
  while ( terminate == 0 && EventLoop::m_seen_sigpipe == 0 ) {
    // race condition possible, thus we MUST time out -- default 30s
    // due to introduction of heartbeat, we must wake for it, too.
    if ( debug ) debug_msg( "invoking poll()\n" );
    long timeo = ( terminate==0 && EventLoop::m_seen_sigpipe==0 && isNext ) ?
      30000 : 0;
    int status = m_fifo ? poll( &pfds, 1, timeo ) : poll( 0, 0, timeo );
    int saverr = errno;
    if ( debug ) debug_msg( "poll returns %d, errno %d\n", status, saverr );
    isNext = true;
    
    // heart-beat
    Time t;
    if ( hb_start.seconds() + heartbeat <= t.seconds() ) {
      // test for presence of child process by using the kill(2) checks
      // FIXME: /proc filesystems allow for more magic :-)
      errno = 0;

      int result = kill( m_child, 0 );
      // FIXME: if ( result == -1 && errno == ESRCH ) CHILD_IS_GONE;

      std::string msg( XML::printf( "heartbeat %u: %.3f %d/%d", 
				    ++hb_count, hb_start.elapsed(t), 
				    result, errno ) );
      heartbeat *= 2.0;	// exponential backoff
      ssize_t wsize = this->send( msg.c_str(), msg.size(), 0 );
      if ( wsize > 0 )
	if ( m_fifo ) m_fifo->add( msg.size(), wsize, 1 );
    }

    // ensure invariance... 
    errno = saverr;
    
    // handle the status
    if ( status == -1 ) {
      // poll error
      if ( terminate || EventLoop::m_seen_sigpipe ) {
	// we were interrupted by our own signal handlers
	result = 0;
	break;
      }
      if ( errno != EINTR ) {
	// not a regular interruption
	result = -1;
	break;
      }
    } else if ( status > 0 ) {
      // poll OK, data is waiting
      if ( handle( pfds, result, terminate, rbuffer, bufsize ) ) break;
    } // IF status > 0
  } // WHILE

  // some final message?
  while ( m_fifo && poll( &pfds, 1, 0 ) == 1 ) {
    // handle waiting message(s)
    if ( handle( pfds, result, terminate, rbuffer, bufsize ) ) break;
  }
  
  // restore defaults
  mysignal( SIGPIPE, old_pipe, 1 );
  if ( debug ) debug_msg( "leaving poll loop\n" );
  
  // done
  delete[] rbuffer;
  return result;
}

//
// --- class JobInfo cont'd --------------------------------------------
//
#include <limits.h>

inline
long
getMaxFD( void )
{
  long result = sysconf( _SC_OPEN_MAX );
#ifdef OPEN_MAX
  if ( result == -1 ) result = OPEN_MAX;
#endif
#ifdef _POSIX_OPEN_MAX
  if ( result == -1 ) result = _POSIX_OPEN_MAX;
#endif
  return result;
}

int 
JobInfo::system( AppInfo* appinfo )
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
{
  static const char* msg = "unable to restore SIGCHLD handler\r\n";
  struct sigaction ignore, saveintr, savequit;
  
  // sanity checks first
  if ( ! m_isValid ) {
    errno = ENOEXEC; // no executable
    return -1;
  }

  ignore.sa_handler = SIG_IGN;
  sigemptyset( &ignore.sa_mask );
  ignore.sa_flags = 0;
  if ( sigaction( SIGINT, &ignore, &saveintr ) < 0 )
    return -1;
  if ( sigaction( SIGQUIT, &ignore, &savequit ) < 0 )
    return -1;

  // install our own SIGCHLD handler
  try {
    SignalHandlerCommunication::init( this );
  } catch ( set_signal ss ) {
    return -1;
  }

  // start wall-clock
  m_start = Time();

  // do this early, before fork()
  this->rewrite();

  // what are we doing
  long maxfd = getMaxFD();
  debug_msg( "about to invoke %s\n", m_argv[0] );

  if ( (m_child=fork()) < 0 ) {
    // no more process table space
    m_status = -1;
  } else if ( m_child == 0 ) {
    //
    // child
    //
    appinfo->setPrinted( true );

    // connect jobs stdio
    if ( ! this->forcefd( appinfo->getStdin(),  STDIN_FILENO  ) ) _exit(126);
    if ( ! this->forcefd( appinfo->getStdout(), STDOUT_FILENO ) ) _exit(126);
    if ( ! this->forcefd( appinfo->getStderr(), STDERR_FILENO ) ) _exit(126);
    
    // close all other FDs
    for ( int fd=STDERR_FILENO+1; fd < maxfd; ++fd ) close(fd);

    // undo signal handlers
    sigaction( SIGINT, &saveintr, 0 );
    sigaction( SIGQUIT, &savequit, 0 );

    // restore old SIGCHLD handler in child process
    try {
      SignalHandlerCommunication::done();
    } catch ( set_signal ss ) {
      write( STDERR_FILENO, msg, strlen(msg) );
    }

    execve( m_argv[0], (char* const*) m_argv, environ );
    _exit(127); // executed in child process on error
  } else {
    //
    // parent
    //

    // channel checkups in parallel to child waiting
    EventLoop e( m_child, appinfo->getChannel(), STDERR_FILENO );
//    while ( ! SignalHandlerCommunication::instance()->isDone() )
      e.loop( SignalHandlerCommunication::instance()->isDone() );
  }

#if 0 // done in signal handler now
  // save any errors before anybody overwrites this
  this->setErrno(); // m_saverr = errno;
#endif

  // stop wall-clock
  m_finish = Time();

  // restore old SIGCHLD handler in child process
  try {
    SignalHandlerCommunication::done();
  } catch ( set_signal ss ) {
    write( STDERR_FILENO, msg, strlen(msg) );
  }

  // ignore errors on these, too.
  sigaction( SIGINT, &saveintr, 0 );
  sigaction( SIGQUIT, &savequit, 0 );

  // finalize
  return m_status;
}
