%{
/* This may look like -*-C++-*- code, but it is really flex
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
 * $Id: scan.l,v 1.5 2004/06/07 22:17:19 griphyn Exp $
 * 
 * Author:   Jens-S. Vöckler <voeckler@cs.uchicago.edu>
 * File:     scan.l
 *           2004-01-16
 *
 * $Log: scan.l,v $
 * Revision 1.5  2004/06/07 22:17:19  griphyn
 * Added "setup" feature to maintain symmetrie to "cleanup" feature.
 * Added "here" option for stdin configuration, which permits a string
 * to be dumped into temporary file from the configuration, to be used
 * as stdin for jobs.
 *
 * Revision 1.4  2004/02/23 20:21:53  griphyn
 * Added new GTPL license schema -- much shorter :-)
 *
 * Revision 1.3  2004/02/11 22:36:28  griphyn
 * new parser.
 *
 * Revision 1.1  2004/02/03 23:13:17  griphyn
 * Kickstart version 2.
 *
 *
 */ 
#include <ctype.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>

#define YY_SKIP_YYWRAP false

extern unsigned long lineno;
extern char parserErrorSeen;

/* forward declaration */
void  yyerror( const char* ); 
void  warning( const char*, const char* ); 
int   yywrap();
int   yy_push_file( const char* path );

#include <deque>
#include "shared.hh"
#include "scan.tab.h"

// local forward declaration
static std::string find_eos( char quote );

struct FileInfo {
  // Maintain information about the recursion of include files
  // Recursing into includes requires unput buffer switching. 
public:
  FileInfo( const char* fn, unsigned lno, YY_BUFFER_STATE& yy )
    :m_filename(fn), m_yylineno(lno)
  { m_yybuffer = yy; }

  std::string     m_filename;
  unsigned long   m_yylineno;
  YY_BUFFER_STATE m_yybuffer;
};

typedef std::deque<FileInfo*> FileStack;
static FileStack filestack;
%}

id      [A-Za-z_][A-Za-z0-9_]*
lws	[ \t\r\v\f]

%%

#[^\n]*		; /* do nothing - a comment */

include		return TK_INCLUDE;
setup		return TK_SETUP;
set		return TK_SET;
pre		return TK_PRE;
main		return TK_MAIN;
post		return TK_POST;
cleanup		return TK_CLEANUP;
transformation	return TK_TR1;
tr		return TK_TR2;
derivation	return TK_DV1;
dv		return TK_DV2;
chdir		return TK_CHDIR;
site		return TK_SITE;
stdin		return TK_STDIN;
stdout		return TK_STDOUT;
stderr		return TK_STDERR;
input		return TK_INPUT;
output		return TK_OUTPUT;
feedback	return TK_FEEDBACK;
stagein		return TK_STAGEIN;
stageout	return TK_STAGEOUT;
xmlns		return TK_XMLNS;

{id}		{ /* identifier */
		  yylval.string = strdup(yytext);
		  return TK_IDENT;
		}

\"		{ /* " found start of double-quoted string */
		  yylval.string = strdup( find_eos(yytext[0]).c_str() );
		  return TK_QSTR1;
		}

\'		{ /* found start of single-quoted string */
		  yylval.string = strdup( find_eos(yytext[0]).c_str() );
		  return TK_QSTR2;
		}

\n		{ /* count line number and terminate command */
  		  lineno++;
		  return TK_EOC;
		}
\;		return TK_EOC;
{lws}+		; /* ignore linear whitespace */
.		return yytext[0]; /* error or one-char-token */

%%

static const char *RCS_ID = 
  "$Id: scan.l,v 1.5 2004/06/07 22:17:19 griphyn Exp $";

static
std::string
find_eos( char quote )
  // purpose: find closing quote to opening quote, counting backslashing
  // paramtr: quote (IN): The quote we look for
  // returns: a string containing the string with outer quotes removed
  //          Using a string allows for multi-line large tokens w/o overflow
{
  std::string result;
  int state = 0;

  for (;;) {
    char ch = yyinput();
    if ( ch==EOF || ch==0 ) break;

    if ( state == 0 ) {
      // regular state
      if ( ch==quote /* || ch=='\n' */ ) break;
      result.push_back(ch);
      if ( ch=='\\' ) state=1; // switch to backslash state
    } else {
      // seen-backslash state
      state=0;
      if ( ch=='\n' ) result.erase(result.size()-1); // continuation line
      else result.push_back(ch);
    }
  }

  return result;
}

#include <stdio.h>
#include <algorithm>

class FileInfo_eq : public std::unary_function<FileInfo*,bool> {
  // Predicate to find existing filenames in the stack of include
  // file recursion. This is used to detect circular references. 
public:
  FileInfo_eq( const std::string& predicate )
    :m_predicate(predicate) 
  { }

  bool operator()( const FileInfo* fi ) const
  { return fi->m_filename == m_predicate; }

private:
  std::string m_predicate;
};

int
yy_push_file( const char* filename )
{
  FILE* temp;
  FileInfo* fi = new FileInfo( filename, lineno, YY_CURRENT_BUFFER );

  // check for out of memory
  if ( fi == NULL ) {
    yyerror( "Out of memory: Includes nested too deeply" );
    return -1;
  }

  // check for circular references
  FileStack::iterator circular = 
    find_if( filestack.begin(), filestack.end(), FileInfo_eq(filename) );
  if ( circular != filestack.end() ) {
    char msg[1024];
    snprintf( msg, sizeof(msg),
	      "Detected circular reference for file \"%s\"", filename );
    yyerror( msg );
    return -1;
  }

  if ( (temp = fopen( filename, "r" )) == NULL ) {
    char msg[1024];
    snprintf( msg, sizeof(msg),
	      "Unable to open include file \"%s\"", filename );
    delete fi;
    yyerror( msg );
    return -1;
  } else {
    filestack.push_back( fi );
  }

  yy_switch_to_buffer( yy_create_buffer( temp, YY_BUF_SIZE ) );
  BEGIN(INITIAL);
  lineno = 1;
  return 0;
}

int
yywrap()
{
  if ( filestack.size() == 0 ) {
    return 1;
  } else {
    FileInfo* fi = filestack.back();
    filestack.pop_back();

    yy_delete_buffer( YY_CURRENT_BUFFER );
    yy_switch_to_buffer( fi->m_yybuffer );
    lineno = fi->m_yylineno;
    delete fi;
    return 0;
  }
}

void
yyerror( const char* msg )
{
  /* parserErrorSeen = 1; */
  fprintf( stderr, "ERROR: %lu: %s at input \"%s\"\n",
	   lineno, msg, yytext );
}

void
warning( const char* msg, const char* addon )
{
  fprintf( stderr, "Warning: %lu: %s at input \"%s\"\n", 
	   lineno, msg, addon );
}
