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
package org.griphyn.cPlanner.classes;

import java.io.IOException;
import java.io.LineNumberReader;
import java.io.Reader;

/**
 * Implements the scanner for reserved words and other tokens that are
 * generated from the input stream. This class is module-local on
 * purpose.
 *
 * @author Jens Vöckler
 */
class PoolConfigScanner
{
  /**
   * Stores the stream from which we are currently scanning.
   */
  private LineNumberReader m_in;

  /**
   * Captures the look-ahead character.
   */
  private int m_lookAhead;

  /**
   * Starts to scan the given stream.
   *
   * @param reader  the reader stream from which we are reading the site catalog.
   */
  public PoolConfigScanner( Reader reader )
    throws IOException
  {
    this.m_in = new LineNumberReader(reader);
    this.m_lookAhead = m_in.read();
    // skipWhitespace();
  }

  /**
   * Obtains the current line number in the input stream from the outside.
   *
   * @return the current line number.
   */
  public int getLineNumber()
  {
    return m_in.getLineNumber();
  }

  /**
   * Skips any white space and comments in the input. This method stops either
   * at the end of file, or at any non-whitespace input character.
   */
  private void skipWhitespace()
    throws IOException
  {
    // end of file?
    if ( m_lookAhead == -1 ) return;

    // skip over whitespace
    while ( m_lookAhead != -1 && Character.isWhitespace((char) m_lookAhead) )
      m_lookAhead = m_in.read();

    // skip over comments until eoln
    if ( m_lookAhead == '#' ) {
      m_in.readLine();
      m_lookAhead = m_in.read();
      skipWhitespace(); // FIXME: reformulate end-recursion into loop
    }
  }

  /**
   * Checks for the availability of more input.
   *
   * @return true, if there is more to read, false for EOF.
   */
  public boolean hasMoreTokens()
    throws IOException
  {
    skipWhitespace();
    return ( this.m_lookAhead != -1 );
  }

  /**
   * Obtains the next token from the input stream.
   *
   * @return an instance conforming to the token interface, or null for eof.
   * @throws IOException if something went wrong while reading
   * @throws PoolConfigException if a lexical error was encountered.
   */
  public PoolConfigToken nextToken()
    throws IOException, PoolConfigException
  {
    // sanity check
    skipWhitespace();
    if ( m_lookAhead == -1 ) return null;

    // are we parsing a reserved word or identifier
    if ( Character.isJavaIdentifierStart((char) m_lookAhead) ) {
      StringBuffer identifier = new StringBuffer(8);
      identifier.append( (char) m_lookAhead );
      m_lookAhead = m_in.read();
      while ( m_lookAhead != -1 &&
	      Character.isJavaIdentifierPart((char) m_lookAhead) ) {
	identifier.append( (char) m_lookAhead );
	m_lookAhead = m_in.read();
      }

      // done parsing identifier or reserved word
      skipWhitespace();
      String s = identifier.toString().toLowerCase();
      if ( PoolConfigReservedWord.symbolTable().containsKey(s) ) {
	// isa reserved word
	return (PoolConfigReservedWord) PoolConfigReservedWord.symbolTable().get(s);
      } else {
	// non-reserved identifier
	return new PoolConfigIdentifier(identifier.toString());
      }

    } else if ( m_lookAhead == '{' ) {
      m_lookAhead = m_in.read();
      skipWhitespace();
      return new PoolConfigOpenBrace();

    } else if ( m_lookAhead == '}' ) {
      m_lookAhead = m_in.read();
      skipWhitespace();
      return new PoolConfigCloseBrace();

    } else if ( m_lookAhead == '(' ) {
      m_lookAhead = m_in.read();
      skipWhitespace();
      return new PoolConfigOpenParanthesis();

    } else if ( m_lookAhead == ')' ) {
      m_lookAhead = m_in.read();
      skipWhitespace();
      return new PoolConfigCloseParanthesis();

    } else if ( m_lookAhead == '"' ) {
      // parser quoted string
      StringBuffer result = new StringBuffer(16);
      do {
	m_lookAhead = m_in.read();
	if ( m_lookAhead == -1 || m_lookAhead == '\r' || m_lookAhead == '\n' ) {
	  // eof is an unterminated string
	  throw new PoolConfigException( m_in, "unterminated quoted string" );
	} else if ( m_lookAhead == '\\' ) {
	  int temp = m_in.read();
	  if ( temp == -1 ) {
	    throw new PoolConfigException( m_in, "unterminated escape in quoted string" );
	  } else {
	    // always add whatever is after the backslash
	    // FIXME: We could to fancy C-string style \012 \n \r escapes here ;-P
	    result.append((char) temp);
	  }
	} else if ( m_lookAhead != '"' ) {
	  result.append((char) m_lookAhead);
	}
      } while ( m_lookAhead != '"' );

      // skip over final quote
      m_lookAhead = m_in.read();
      skipWhitespace();
      return new PoolConfigQuotedString( result.toString() );

    } else {
      // unknown material
      throw new PoolConfigException( m_in, "unknown character " + m_lookAhead );
    }
  }
}
