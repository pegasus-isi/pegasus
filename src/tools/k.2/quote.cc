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
#include "quote.hh"
#include <cctype>

//
// STATE	EOS	""	''	$	{	}	BS	ALNUM	LWS	ELSE
// ------------+-------+-------+-------+-------+-------+-------+-------+-------+-------+-----
// 0 NQ_LWS	F	5	3	10	1	1	2	1	0	1
// 		-	-	-	-	Sb	Sb	-	Sb	-	Sb
// 1 NQ_MAIN	F	5	3	10	1	1	2	1	0	1
// 		Fb	-	-	-	Sb	Sb	-	Sb	Fb	Sb
// 2 NQ_BS	E1	1	1	1	1	1	1	1	1	1
// 		-	Sb		Sb	Sb	Sb	Sb	Sb	Sb	Sb
// ------------+-------+-------+-------+-------+-------+-------+-------+-------+-------+-----
// 3 SQ_MAIN	E2 F	3	1 3	3	3	3	4	3	3	3
// 		-  Fb	Sb	- Sb	Sb	Sb	Sb	-	Sb	Sb	Sb
// 4 SQ_BS	E1	3	3	3	3	3	3	3	3	3
// 		-	Sb	Sb	Sb	Sb	Sb	Sb	Sb	Sb	Sb
// ------------+-------+-------+-------+-------+-------+-------+-------+-------+-------+-----
// 5 DQ_MAIN	E3 F	1  5	5	7	5	5	6	5	5	5
// 		-  Fb	-  Sb	Sb	-	Sb	Sb	-	Sb	Sb	Sb
// 6 DQ_BS	E1	5	5	5	5	5	5	5	5	5
// 		-	Sb	Sb	Sb	Sb	Sb	Sb	Xb	Sb	Sb
// 7 DQ_DLLR	E3	1  E4	E4	E4	9	E4	6	8	5	E4
// 		-	-  -	-	-	-	-	S($)	Sv	S($)	-
// 8 DQ_VAR1	E3	1  5	5	7	5	5	6	8	5	5
// 		-	Fv Fvpb	Fvpb	Fv	Fvpb	Fvpb	Fv	Sv	Fvpb	Fvpb
// 9 DQ_VAR2	E1	9	9	E4	E4	5	9	9	9	9
// 		-	Sv	Sv	-	-	Fv	Sv	Sv	Sv	Sv
// ------------+-------+-------+-------+-------+-------+-------+-------+-------+-------+-----
// 10 NQ_DLLR	1	5	3	10	12	E4	2	11	1	1
// 		S($)pb	S($)	S($)	Sb	-	-	S($)	Sv	S($)pb	S($)pb
// 11 NQ_VAR1	1	5	3	10	1	1	2	11	1	1
// 		Fvpb	Fv	Fv	Fv	Fvpb	Fvpb	Fv	Sv	Fvpb	Fvpb
// 12 NQ_VAR2	E4	12	12	E4	E4	1	12	12	12	12
// 		-	Sv	Sv	-	-	Fv	Sv	Sv	Sv	Sv
// ------------+-------+-------+-------+-------+-------+-------+-------+-------+-------+-----
// 13 FINAL	final state: ok
// 14 ERR1		error state 1: premature end of string
// 15 ERR2		error state 2: missing apostrophe
// 16 ERR3		error state 3: missing quote
// 17 ERR4		error state 4: illegal character in varname
// 

namespace Quote {
  // and more
  struct Mealy {
    State s;
    Action a;
    
    inline Mealy( State _s, Action _a ):s(_s),a(_a) { }
    inline Mealy():s(NQ_LWS),a(Noop) { }
  };

  typedef Mealy MealyRow[C_MAX];
  typedef MealyRow MealyMap[FINAL];

  static MealyMap statemap = // sm[FINAL][C_MAX] = 
    { { // state NQ_LWS
      Mealy( FINAL,    Noop ),	// state NQ_LWS, input EOS
      Mealy( DQ_MAIN,  Noop ),	// state NQ_LWS, input QUOTE
      Mealy( SQ_MAIN,  Noop ),	// state NQ_LWS, input APOS
      Mealy( NQ_DLLR,  Noop ),	// state NQ_LWS, input DOLLAR
      Mealy( NQ_MAIN,  Sb ),	// state NQ_LWS, input LBRACE
      Mealy( NQ_MAIN,  Sb ),	// state NQ_LWS, input RBRACE
      Mealy( NQ_BS,    Noop ),	// state NQ_LWS, input BSLASH
      Mealy( NQ_MAIN,  Sb ),	// state NQ_LWS, input ALNUM
      Mealy( NQ_LWS,   Noop ),	// state NQ_LWS, input LWS
      Mealy( NQ_MAIN,  Sb )	// state NQ_LWS, input *
    },{ // state NQ_MAIN
      Mealy( FINAL,    Fb ),	// state NQ_LWS, input EOS
      Mealy( DQ_MAIN,  Noop ),	// state NQ_LWS, input QUOTE
      Mealy( SQ_MAIN,  Noop ),	// state NQ_LWS, input APOS
      Mealy( NQ_DLLR,  Noop ),	// state NQ_LWS, input DOLLAR
      Mealy( NQ_MAIN,  Sb ),	// state NQ_LWS, input LBRACE
      Mealy( NQ_MAIN,  Sb ),	// state NQ_LWS, input RBRACE
      Mealy( NQ_BS,    Noop ),	// state NQ_LWS, input BSLASH
      Mealy( NQ_MAIN,  Sb ),	// state NQ_LWS, input ALNUM
      Mealy( NQ_LWS,   Fb ),	// state NQ_LWS, input LWS
      Mealy( NQ_MAIN,  Sb )	// state NQ_LWS, input *
    },{ // state NQ_BS
      Mealy( ERR1,     Noop ),	// state NQ_LWS, input EOS
      Mealy( NQ_MAIN,  Sb ),	// state NQ_LWS, input QUOTE
      Mealy( NQ_MAIN,  Sb ),	// state NQ_LWS, input APOS
      Mealy( NQ_MAIN,  Sb ),	// state NQ_LWS, input DOLLAR
      Mealy( NQ_MAIN,  Sb ),	// state NQ_LWS, input LBRACE
      Mealy( NQ_MAIN,  Sb ),	// state NQ_LWS, input RBRACE
      Mealy( NQ_MAIN,  Sb ),	// state NQ_LWS, input BSLASH
      Mealy( NQ_MAIN,  Sb ),	// state NQ_LWS, input ALNUM
      Mealy( NQ_MAIN,  Fb ),	// state NQ_LWS, input LWS
      Mealy( NQ_MAIN,  Sb )	// state NQ_LWS, input *

    },{ // state NQ_DLLR
      Mealy( NQ_MAIN,  SDpb ),	// state NQ_DLLR, input EOS
      Mealy( DQ_MAIN,  SD ),	// state NQ_DLLR, input QUOTE
      Mealy( SQ_MAIN,  SD ),	// state NQ_DLLR, input APOS
      Mealy( NQ_DLLR,  Sb ),	// state NQ_DLLR, input DOLLAR
      Mealy( NQ_VAR2,  Noop ),	// state NQ_DLLR, input LBRACE
      Mealy( ERR4,     Noop ),	// state NQ_DLLR, input RBRACE
      Mealy( NQ_BS,    SD ),	// state NQ_DLLR, input BSLASH
      Mealy( NQ_VAR1,  Sv ),	// state NQ_DLLR, input ALNUM
      Mealy( NQ_MAIN,  SDpb ),	// state NQ_DLLR, input LWS
      Mealy( NQ_MAIN,  SDpb )	// state NQ_DLLR, input *
    },{ // state NQ_VAR1
      Mealy( NQ_MAIN,  Fvpb ),	// state NQ_VAR1, input EOS
      Mealy( DQ_MAIN,  Fv ),	// state NQ_VAR1, input QUOTE
      Mealy( SQ_MAIN,  Fv ),	// state NQ_VAR1, input APOS
      Mealy( NQ_DLLR,  Fv ),	// state NQ_VAR1, input DOLLAR
      Mealy( NQ_MAIN,  Fvpb ),	// state NQ_VAR1, input LBRACE
      Mealy( NQ_MAIN,  Fvpb ),	// state NQ_VAR1, input RBRACE
      Mealy( NQ_BS,    Fv ),	// state NQ_VAR1, input BSLASH
      Mealy( NQ_VAR1,  Sv ),	// state NQ_VAR1, input ALNUM
      Mealy( NQ_MAIN,  Fvpb ),	// state NQ_VAR1, input LWS
      Mealy( NQ_MAIN,  Fvpb )	// state NQ_VAR1, input *
    },{ // state NQ_VAR2
      Mealy( ERR4,     Noop ),	// state NQ_VAR2, input EOS
      Mealy( NQ_VAR2,  Sv ),	// state NQ_VAR2, input QUOTE
      Mealy( NQ_VAR2,  Sv ),	// state NQ_VAR2, input APOS
      Mealy( ERR4,     Noop ),	// state NQ_VAR2, input DOLLAR
      Mealy( ERR4,     Noop ),	// state NQ_VAR2, input LBRACE
      Mealy( NQ_MAIN,  Fv ),	// state NQ_VAR2, input RBRACE
      Mealy( NQ_VAR2,  Sv ),	// state NQ_VAR2, input BSLASH
      Mealy( NQ_VAR2,  Sv ),	// state NQ_VAR2, input ALNUM
      Mealy( NQ_VAR2,  Sv ),	// state NQ_VAR2, input LWS
      Mealy( NQ_VAR2,  Sv )	// state NQ_VAR2, input *

    },{ // state DQ_MAIN 
      Mealy( FINAL,    Fb ),	// state DQ_MAIN, input EOS
      Mealy( DQ_MAIN,  Sb ),	// state DQ_MAIN, input QUOTE
      Mealy( DQ_MAIN,  Sb ),	// state DQ_MAIN, input APOS
      Mealy( DQ_DLLR,  Noop ),	// state DQ_MAIN, input DOLLAR
      Mealy( DQ_MAIN,  Sb ),	// state DQ_MAIN, input LBRACE
      Mealy( DQ_MAIN,  Sb ),	// state DQ_MAIN, input RBRACE
      Mealy( DQ_BS,    Noop ),	// state DQ_MAIN, input BSLASH
      Mealy( DQ_MAIN,  Sb ),	// state DQ_MAIN, input ALNUM
      Mealy( DQ_MAIN,  Sb ),	// state DQ_MAIN, input LWS
      Mealy( DQ_MAIN,  Sb )	// state DQ_MAIN, input *
    },{ // state DQ_BS
      Mealy( ERR1,     Noop ),	// state DQ_BS, input EOS
      Mealy( DQ_MAIN,  Sb ),	// state DQ_BS, input QUOTE
      Mealy( DQ_MAIN,  Sb ),	// state DQ_BS, input APOS
      Mealy( DQ_MAIN,  Sb ),	// state DQ_BS, input DOLLAR
      Mealy( DQ_MAIN,  Sb ),	// state DQ_BS, input LBRACE
      Mealy( DQ_MAIN,  Sb ),	// state DQ_BS, input RBRACE
      Mealy( DQ_MAIN,  Sb ),	// state DQ_BS, input BSLASH
      Mealy( DQ_MAIN,  Xb ),	// state DQ_BS, input ALNUM
      Mealy( DQ_MAIN,  Sb ),	// state DQ_BS, input LWS
      Mealy( DQ_MAIN,  Sb )	// state DQ_BS, input *

    },{ // state DQ_DLLR
      Mealy( ERR3,     Noop ),	// state DQ_DLLR, input EOS
      Mealy( ERR4,     Noop ),	// state DQ_DLLR, input QUOTE
      Mealy( ERR4,     Noop ),	// state DQ_DLLR, input APOS
      Mealy( ERR4,     Noop ),	// state DQ_DLLR, input DOLLAR
      Mealy( DQ_VAR2,  Noop ),	// state DQ_DLLR, input LBRACE
      Mealy( ERR4,     Noop ),	// state DQ_DLLR, input RBRACE
      Mealy( DQ_BS,    SD ),	// state DQ_DLLR, input BSLASH
      Mealy( DQ_VAR1,  Sv ),	// state DQ_DLLR, input ALNUM
      Mealy( DQ_MAIN,  SD ),	// state DQ_DLLR, input LWS
      Mealy( ERR4,     Noop )	// state DQ_DLLR, input *
    },{ // state DQ_VAR1
      Mealy( ERR3,     Fvpb ),	// state DQ_VAR1, input EOS
      Mealy( DQ_MAIN,  Fvpb ),	// state DQ_VAR1, input QUOTE
      Mealy( DQ_MAIN,  Fvpb ),	// state DQ_VAR1, input APOS
      Mealy( DQ_DLLR,  Fv ),	// state DQ_VAR1, input DOLLAR
      Mealy( DQ_MAIN,  Fvpb ),	// state DQ_VAR1, input LBRACE
      Mealy( DQ_MAIN,  Fvpb ),	// state DQ_VAR1, input RBRACE
      Mealy( DQ_BS,    Fv ),	// state DQ_VAR1, input BSLASH
      Mealy( DQ_VAR1,  Sv ),	// state DQ_VAR1, input ALNUM
      Mealy( DQ_MAIN,  Fvpb ),	// state DQ_VAR1, input LWS
      Mealy( DQ_MAIN,  Fvpb )	// state DQ_VAR1, input *
    },{ // state DQ_VAR2
      Mealy( ERR1,     Noop ),	// state DQ_VAR2, input EOS
      Mealy( DQ_VAR2,  Sv ),	// state DQ_VAR2, input QUOTE
      Mealy( DQ_VAR2,  Sv ),	// state DQ_VAR2, input APOS
      Mealy( ERR4,     Noop ),	// state DQ_VAR2, input DOLLAR
      Mealy( ERR4,     Noop ),	// state DQ_VAR2, input LBRACE
      Mealy( DQ_MAIN,  Fv ),	// state DQ_VAR2, input RBRACE
      Mealy( DQ_VAR2,  Sv ),	// state DQ_VAR2, input BSLASH
      Mealy( DQ_VAR2,  Sv ),	// state DQ_VAR2, input ALNUM
      Mealy( DQ_VAR2,  Sv ),	// state DQ_VAR2, input LWS
      Mealy( DQ_VAR2,  Sv )	// state DQ_VAR2, input *

    },{ // state SQ_MAIN
      Mealy( FINAL,    Fb ),	// state SQ_MAIN, input EOS
      Mealy( SQ_MAIN,  Sb ),	// state SQ_MAIN, input QUOTE
      Mealy( SQ_MAIN,  Sb ),	// state SQ_MAIN, input APOS
      Mealy( SQ_MAIN,  Sb ),	// state SQ_MAIN, input DOLLAR
      Mealy( SQ_MAIN,  Sb ),	// state SQ_MAIN, input LBRACE
      Mealy( SQ_MAIN,  Sb ),	// state SQ_MAIN, input RBRACE
      Mealy( SQ_BS,    Noop ),	// state SQ_MAIN, input BSLASH
      Mealy( SQ_MAIN,  Sb ),	// state SQ_MAIN, input ALNUM
      Mealy( SQ_MAIN,  Sb ),	// state SQ_MAIN, input LWS
      Mealy( SQ_MAIN,  Sb )	// state SQ_MAIN, input *
    },{ // state SQ_BS
      Mealy( ERR1,     Noop ),	// state SQ_BS, input EOS
      Mealy( SQ_MAIN,  Sb ),	// state SQ_BS, input QUOTE
      Mealy( SQ_MAIN,  Sb ),	// state SQ_BS, input APOS
      Mealy( SQ_MAIN,  Sb ),	// state SQ_BS, input DOLLAR
      Mealy( SQ_MAIN,  Sb ),	// state SQ_BS, input LBRACE
      Mealy( SQ_MAIN,  Sb ),	// state SQ_BS, input RBRACE
      Mealy( SQ_MAIN,  Sb ),	// state SQ_BS, input BSLASH
      Mealy( SQ_MAIN,  Sb ),	// state SQ_BS, input ALNUM
      Mealy( SQ_MAIN,  Sb ),	// state SQ_BS, input LWS
      Mealy( SQ_MAIN,  Sb )	// state SQ_BS, input *
    }
  };

  static const char* translation = "abenrtv";
  static const char translationmap[] = "\a\b\033\n\r\t\v";

  CharClass 
  xlate( int input )
    // purpose: translates an input character into its character class
    // paramtr: input (IN): input character
    // returns: the character class
  {
    switch ( input ) {
    case EOF:
    case 0:
      return EOS;
    case '"':
      return QUOTE;
    case '\'':
      return APOS;
    case '$':
      return DOLLAR;
    case '{':
      return LBRACE;
    case '}':
      return RBRACE;
    case '\\':
      return BSLASH;
    case '_':
      return ALNUM;
    default: 
      return ( isalnum(input) ? ALNUM : ( isspace(input) ? LWS : ELSE ) );
    }
  }

  State 
  parse( const std::string& input, std::string& output, State state )
    // purpose: parse a single or doubly-quoted string into a single string
    // paramtr: input (IN): The raw string without outer quotes
    //          output (OUT): The cooked string, one level removed
    //          state (IN): start start: 0 -> squotes, 2 -> dquotes
    // returns: the final state after being done
  {
    std::string buffer;
    std::string varname;

    const char* x = 0;
    const char* s = input.c_str();
    while ( state < FINAL ) {
      Mealy s_a( statemap[state][xlate(*s)] ); // (So,A) := F(Si,I)
      switch ( s_a.a ) {
      case Noop:
	// do nothing
	break;
      case Sb:
	// expand regular buffer
	buffer += *s;
	break;
      case Xb:
	// expand regular buffer, translate \n and friends
	x = strchr( translation, *s );
	buffer += ( x == 0 ? *s : translationmap[x-translation] );
	break;
      case Fb:
	// finalize buffer
	output = buffer;
	buffer.clear();
	break;
      case Sv:
	// store varname
	varname += *s;
	break;
      case Fvpb:
	// put back and do Fv
	--s;
	// FALL THROUGH
      case Fv:
	// resolve varname
	if ( (x = getenv( varname.c_str() )) == 0 ) {
	  // variable not found: keep original string
	  buffer += '$';
	  if ( state == DQ_VAR2 ) buffer += '{';
	  buffer.append( varname );
	  if ( state == DQ_VAR2 ) buffer += '}';
	} else {
	  // resolve variable
	  buffer.append( x );
	}

	varname.clear();
	break;
      case SDpb:
	// put back and store dollar
	--s;
	// FALL THROUGH
      case SD:
	// put back a dollar sign
	buffer += "$";
	break;
      case A_MAX:
	// illegal action
	break;
      }

      // advance to next state
      s++;
      state = s_a.s; // new state
    }

    return state;
  }

  State 
  parse( const std::string& input, StringList& output, State state )
    // purpose: parse a single or doubly-quoted string into an argv[] list
    // paramtr: input (IN): The raw string without outer quotes
    //          output (OUT): The cooked ws-split argv, one level removed
    //          state (IN): start start: 0 -> squotes, 2 -> dquotes
    // returns: the final state after being done
  {
    std::string buffer;
    std::string varname;

    // update local copy of Mealy map for argv parsing
    MealyMap m;
    memcpy( m, statemap, sizeof(MealyMap) );
    m[SQ_MAIN][EOS] =   Mealy( ERR2,    Noop );
    m[SQ_MAIN][APOS] =  Mealy( NQ_MAIN, Noop );
    m[DQ_MAIN][EOS] =   Mealy( ERR3,    Noop );
    m[DQ_MAIN][QUOTE] = Mealy( NQ_MAIN, Noop );
    m[DQ_DLLR][QUOTE] = Mealy( NQ_MAIN, Noop );
    m[DQ_VAR1][QUOTE] = Mealy( NQ_MAIN, Fv );

    const char* x = 0;
    const char* s = input.c_str();
    while ( state < FINAL ) {
      Mealy s_a( m[state][xlate(*s)] ); // (So,A) := F(Si,I)
      switch ( s_a.a ) {
      case Noop:
	// do nothing
	break;
      case Sb:
	// expand regular buffer
	buffer += *s;
	break;
      case Xb:
	// expand regular buffer, translate \n and friends
	x = strchr( translation, *s );
	buffer += ( x == 0 ? *s : translationmap[x-translation] );
	break;
      case Fb:
	// finalize buffer
	output.push_back(buffer);
	buffer.clear();
	break;
      case Sv:
	// store varname
	varname += *s;
	break;
      case Fvpb:
	// put back and do Fv
	--s;
	// FALL THROUGH
      case Fv:
	// resolve varname
	if ( (x = getenv( varname.c_str() )) == 0 ) {
	  // variable not found: keep original string
	  buffer += '$';
	  if ( state == DQ_VAR2 ) buffer += '{';
	  buffer.append( varname );
	  if ( state == DQ_VAR2 ) buffer += '}';
	} else {
	  // resolve variable
	  buffer.append( x );
	}

	varname.clear();
	break;
      case SDpb:
	// put back and store dollar
	--s;
	// FALL THROUGH
      case SD:
	// put back a dollar sign
	buffer += "$";
	break;
      case A_MAX:
	// illegal action
	break;
      }

      // advance to next state
      s++;
      state = s_a.s; // new state
    }

    return state;
  }

// done with namespace Quote
} // namespace Quote
