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
#ifndef _CHIMERA_QUOTE_HH
#define _CHIMERA_QUOTE_HH

#include "shared.hh"

namespace Quote {
  // deal with unquoting one level of quotes, variable interpolation
  // and backslash escapes. 

  enum Action {
    Noop,	// nothing to do
    Sb, 	// store char into buffer
    Xb,		// like Sb, but interprete \n and friends
    Fb,		// finalize buffer (call context sensitive)
    Sv,		// store char into varname buffer
    Fvpb,	// put back char, and resolve varname buffer
    Fv,		// resolve content of varname buffer
    SDpb,	// store dollar and push back input
    SD,		// store dollar
    A_MAX
  };

  enum State { 
    NQ_LWS,	// start state for CLI-argv splitting parser, skips lws
    NQ_MAIN,	// main state for filling one argv 
    NQ_BS,	// backslash processing state for "unquoted" strings

    NQ_DLLR,	// dollar recognized in "unquoted" strings
    NQ_VAR1,    // collecting $var varname
    NQ_VAR2,	// collecting ${var} varname

    DQ_MAIN,	// main state for double quoted strings, fills buffer
    DQ_BS,	// backslash processing state
    DQ_DLLR,	// dollar recognized state
    DQ_VAR1,	// collecting $var varname
    DQ_VAR2,	// collecting ${var} varname

    SQ_MAIN,	// main state for single quoted strings, fills buffer
    SQ_BS,	// backslash processing state

    FINAL,	// good final state: done parsing
    ERR1,	// bad final state: premature end of string
    ERR2,	// bad final state: missing apostrophe
    ERR3,	// bad final state: missing quote
    ERR4,	// bad final state: illegal character in varname

    S_MAX
  };

  enum CharClass {
    EOS,	// end of string, NUL character
    QUOTE,	// double quote character
    APOS,	// single quote character
    DOLLAR,	// dollar sign
    LBRACE,	// left brace, opening
    RBRACE,	// right brace, opening
    BSLASH,	// backslash character
    ALNUM,	// legal identifier character
    LWS,	// any whitespace
    ELSE,	// any other character

    C_MAX
  };

  extern CharClass xlate( int ch );
    // purpose: translates an input character into its character class
    // paramtr: ch (IN): input character
    // returns: the character class

  extern State parse( const std::string& input, 
		      std::string& output,
		      State state );
    // purpose: parse a single or doubly-quoted string into a single string
    // paramtr: input (IN): The raw string without outer quotes
    //          output (OUT): The cooked string, one level removed
    //          state (IN): start start: 0 -> squotes, 2 -> dquotes
    // returns: the final state after being done

  extern State parse( const std::string& input, 
		      StringList& output,
		      State state );
    // purpose: parse a single or doubly-quoted string into an argv[] list
    // paramtr: input (IN): The raw string without outer quotes
    //          output (OUT): The cooked ws-split argv, one level removed
    //          state (IN): start start: 0 -> squotes, 2 -> dquotes
    // returns: the final state after being done
}

#endif // _CHIMERA_QUOTE_HH
