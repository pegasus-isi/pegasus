/**
 *  Copyright 2007-2008 University Of Southern California
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package edu.isi.pegasus.planner.parser;

import edu.isi.pegasus.planner.parser.tokens.OpenBrace;
import edu.isi.pegasus.planner.parser.tokens.ScannerException;
import edu.isi.pegasus.planner.parser.tokens.SiteCatalogReservedWord;
import edu.isi.pegasus.planner.parser.tokens.Token;
import edu.isi.pegasus.planner.parser.tokens.QuotedString;
import edu.isi.pegasus.planner.parser.tokens.OpenParanthesis;
import edu.isi.pegasus.planner.parser.tokens.Identifier;
import edu.isi.pegasus.planner.parser.tokens.CloseParanthesis;
import edu.isi.pegasus.planner.parser.tokens.CloseBrace;
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
class SiteCatalogTextScanner
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
  public SiteCatalogTextScanner( Reader reader )
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
   * @throws Exception if a lexical error was encountered.
   */
  public Token nextToken()
    throws IOException, ScannerException
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
      if ( SiteCatalogReservedWord.symbolTable().containsKey(s) ) {
	// isa reserved word
	return (SiteCatalogReservedWord) SiteCatalogReservedWord.symbolTable().get(s);
      } else {
	// non-reserved identifier
	return new Identifier(identifier.toString());
      }

    } else if ( m_lookAhead == '{' ) {
      m_lookAhead = m_in.read();
      skipWhitespace();
      return new OpenBrace();

    } else if ( m_lookAhead == '}' ) {
      m_lookAhead = m_in.read();
      skipWhitespace();
      return new CloseBrace();

    } else if ( m_lookAhead == '(' ) {
      m_lookAhead = m_in.read();
      skipWhitespace();
      return new OpenParanthesis();

    } else if ( m_lookAhead == ')' ) {
      m_lookAhead = m_in.read();
      skipWhitespace();
      return new CloseParanthesis();

    } else if ( m_lookAhead == '"' ) {
      // parser quoted string
      StringBuffer result = new StringBuffer(16);
      do {
	m_lookAhead = m_in.read();
	if ( m_lookAhead == -1 || m_lookAhead == '\r' || m_lookAhead == '\n' ) {
	  // eof is an unterminated string
	  throw new ScannerException( m_in, "unterminated quoted string" );
	} else if ( m_lookAhead == '\\' ) {
	  int temp = m_in.read();
	  if ( temp == -1 ) {
	    throw new ScannerException( m_in, "unterminated escape in quoted string" );
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
      return new QuotedString( result.toString() );

    } else {
      // unknown material
      throw new ScannerException( m_in, "unknown character " + m_lookAhead );
    }
  }
}
