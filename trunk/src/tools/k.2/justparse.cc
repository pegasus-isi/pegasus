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
#include <stdio.h>
#include <stdlib.h>

extern FILE* yyin;
extern int yylex();

unsigned long lineno = 1;
union {
  char*	string;
} yylval;

int
main( int argc, char* argv[] )
{
  int tokenclass;

  if ( argc != 2 ) return 1;
  yyin = fopen( argv[1], "r" );
  if ( yyin == NULL ) return 2;

  do {
    yylval.string = 0;
    tokenclass = yylex();
    printf( "%3d %s\n", tokenclass, yylval.string ? yylval.string : "" );
    if ( yylval.string ) free((void*) yylval.string);
  } while ( tokenclass != 0 );

  return 0;
}
