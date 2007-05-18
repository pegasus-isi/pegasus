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

package org.griphyn.common.util;

import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.TreeMap;
import java.util.Iterator;
import org.griphyn.cPlanner.classes.Profile;

/**
 * Converts between the string version of a profile specification
 * and the parsed triples and back again.
 *
 * @author Gaurang Mehta
 * @author Jens-S. Vöckler
 */
public class ProfileParser
{
  /**
   * Table to contain the state transition diagram for the parser. The
   * rows are defined as current states 0 through 7. The columns is the
   * current input character. The cell contains first the action to be
   * taken, followed by the new state to transition to:
   *
   * <pre>
   *      | EOS | adu |  ,  |  ;  |  :  |  \  |  "  |  =  |other|
   *      |  0  |  1  |  2  |  3  |  4  |  5  |  6  |  7  |  8  |
   * -----+-----+-----+-----+-----+-----+-----+-----+-----+-----+
   *   0  | -,F |Cn,0 | -,E1| -,E1| -,1 | -,E1| -,E1| -,E1| -,E1|
   *   1  | -,E2| -,E1| -,E1| -,E1| -,2 | -,E1| -,E1| -,E1| -,E1|
   *   2  | -,F |Ck,2 | -,E1| -,E1| -,E1| -,E1| -,E1| -,3 | -,E1|
   *   3  | -,E2|Cv,6 | -E1 | -,E1| -,E1| -,E1| -,4 | -,E1|Cv,6 |
   *   4  | -,E2|Cv,4 |Cv,4 |Cv,4 |Cv,4 | -,5 | -,7 |Cv,4 |Cv,4 |
   *   5  | -,E2|Cv,4 |Cv,4 |Cv,4 |Cv,4 |Cv,4 |Cv,4 |Cv,4 |Cv,4 |
   *   6  |A1,F |Cv,6 |A2,2 |A1,0 | -,E1| -,E1| -,E1| -,E1|Cv,6 |
   *   7  |A1,F | -,E1|A2,2 |A1,0 | -,E1| -,E1| -,E1| -,E1| -,E1|
   * -----+-----+-----+-----+-----+-----+-----+-----+-----+-----+
   *   F  |  8  | final state
   *   E1 |  9  | error1: illegal character in input
   *   E2 | 10  | error2: premature end of input
   * </pre>
   *
   * The state variable collects the new state for a given
   * state (rows) and input character set (column) identifier.
   */
  private static final byte c_state[][] = {
    // E   A   ,   ;   :   \   "   =   O
    {  8,  0,  9,  9,  1,  9,  9,  9,  9 }, // 0: recognize ns
    { 10,  9,  9,  9,  2,  9,  9,  9,  9 }, // 1: found colon
    {  8,  2,  9,  9,  9,  9,  9,  3,  9 }, // 2: recognize key
    { 10,  6,  9,  9,  9,  9,  4,  9,  6 }, // 3: seen equals
    { 10,  4,  4,  4,  4,  5,  7,  4,  4 }, // 4: quoted value
    { 10,  4,  4,  4,  4,  4,  4,  4,  4 }, // 5: backslashed qv
    {  8,  6,  2,  0,  9,  9,  9,  9,  6 }, // 6: unquoted value
    {  8,  9,  2,  0,  9,  9,  9,  9,  9 }  // 7: closed quote
  };

  /**
   * There are six identified actions.
   *
   * <pre>
   *  -   | 0 | noop
   *  Cn  | 1 | append input character to namespace field
   *  Ck  | 2 | append input character to key field
   *  Cv  | 3 | append input character to value field
   *  A1  | 4 | create triple and flush all fields
   *  A2  | 5 | create triple and flush key and value only
   * </pre>
   *
   * The action variable collects the action to take for a
   * given state (rows) and input character set (column).
   */
  private static final byte c_action[][] = {
    // E   A   ,   ;   :   \   "   =   O
    {  0,  1,  0,  0,  0,  0,  0,  0,  0 }, // 0: recognize ns
    {  0,  0,  0,  0,  0,  0,  0,  0,  0 }, // 1: found colon
    {  0,  2,  0,  0,  0,  0,  0,  0,  0 }, // 2: recognize key
    {  0,  3,  0,  0,  0,  0,  0,  0,  3 }, // 3: seen equals
    {  0,  3,  3,  3,  3,  0,  0,  3,  3 }, // 4: quoted value
    {  0,  3,  3,  3,  3,  3,  3,  3,  3 }, // 5: backslashed qv
    {  4,  3,  5,  4,  0,  0,  0,  0,  3 }, // 6: unquoted value
    {  4,  0,  5,  4,  0,  0,  0,  0,  0 }  // 7: closed quote
  };

  /**
   * Parses a given user profile specification into a map of maps.
   *
   * @param s is the input string to parse
   * @return a map of namespaces mapping to maps of key value pairs.
   * @throws ProfileParserException if the input cannot be recognized
   * @see #combine( List m )
   */
  public static List parse( String s )
    throws ProfileParserException
  {
    char ch = '?';
    List result = new ArrayList();

    // sanity check
    if ( s == null ) return result;

    StringBuffer namespace = new StringBuffer();
    StringBuffer key = new StringBuffer();
    StringBuffer value = new StringBuffer();
    int index = 0;
    byte charset, state = 0;
    while ( state < 8 ) {
      //
      // determine character class
      //
      switch ( (ch = ( index < s.length() ? s.charAt(index++) : '\0' )) ) {
      case '\0': charset = 0; break;
      case '_':  charset = 1; break;
      case '.':  charset = 1; break;
      case '@':  charset = 1; break;
      case '-':  charset = 1; break;
      case '/':  charset = 1; break;
      case ',':  charset = 2; break;
      case ';':  charset = 3; break;
      case ':':  charset = 4; break;
      case '\\': charset = 5; break;
      case '"':  charset = 6; break;
      case '=':  charset = 7; break;
      default:
	if ( Character.isLetter(ch) || Character.isDigit(ch) ) charset = 1;
	else charset = 8;
	break;
      }

      //
      // perform action
      //
      switch ( c_action[state][charset] ) {
      case 1: // collect namespace
	namespace.append(ch);
	break;
      case 2: // collect key
	key.append(ch);
	break;
      case 3: // collect value
	value.append(ch);
	break;
      case 4: // flush
	result.add( new Profile( namespace.toString(),
				 key.toString(),
				 value.toString() ) );
	namespace.delete( 0, namespace.length() );
	key.delete( 0, key.length() );
	value.delete( 0, value.length() );
	break;
      case 5: // partial flush
	result.add( new Profile( namespace.toString(),
				 key.toString(),
				 value.toString() ) );
	key.delete( 0, key.length() );
	value.delete( 0, value.length() );
	break;
      }

      //
      // progress state
      //
      state = c_state[state][charset];
    }

    if ( state > 8 ) {
      switch ( state ) {
      case 9:
	throw new ProfileParserException( "Illegal character '" + ch +
					  "'", index );
      case 10:
	throw new ProfileParserException( "Premature end-of-string", index );
      default:
	throw new ProfileParserException( "Unknown error", index );
      }
    }

    return result;
  }

  /**
   * Creates a profile string from the internal representation.
   *
   * @param l is a list of profiles
   * @return a string containing the representation. The string can be
   * empty (FIXME: should it be "null" or null?) for an empty list.
   * @see #parse( String s )
   */
  public static String combine( List l )
  {
    StringBuffer result = new StringBuffer();

//     // phase 1: convert list into map of maps
//     Map m = new TreeMap();
//     for ( Iterator i=l.iterator(); i.hasNext(); ) {
//       Profile p = (Profile) i.next();
//       String ns = p.getProfileNamespace();
//       if ( ! m.containsKey(ns) ) m.put( ns, new TreeMap() );
//       Map kv = (Map) m.get(ns);
//       kv.put( p.getProfileKey(), p.getProfileValue() );
//     }
//
//     // phase 2: convert map of maps into string using minimal space
//     boolean flag1 = false;
//     for ( Iterator i=m.keySet().iterator(); i.hasNext(); ) {
//       String ns = (String) i.next();
//       Map kv = (Map) m.get(ns);
//       if ( flag1 ) result.append(';');
//       result.append(ns).append("::");
//
//       boolean flag2 = false;
//       for ( Iterator j=kv.keySet().iterator(); j.hasNext(); ) {
// 	String key = (String) j.next();
// 	String value = (String) kv.get(key);
// 	if ( flag2 ) result.append(',');
// 	result.append(key).append('=').append('"');
//
// 	// escape all dquote and backslash with backslash
// 	for ( int k=0; k<value.length(); ++k ) {
// 	  char ch = value.charAt(k);
// 	  if ( ch == '"' || ch == '\\' ) result.append('\\');
// 	  result.append(ch);
// 	}
//
// 	result.append('"');
// 	flag2 = true;
//       }
//       flag1 = true;
//     }

    // faster, shorter, less mem, retains ordering; alas, no minimal output
    boolean flag = false;
    String previous = "invalid namespace";
    for ( Iterator i=l.iterator(); i.hasNext(); ) {
      Profile p = (Profile) i.next();
      String ns = p.getProfileNamespace();
      if ( ns.equals(previous) ) result.append(',');
      else {
	if ( flag ) result.append(';');
	result.append(ns).append("::");
      }
      result.append( p.getProfileKey() ).append('=').append('"');

      // escape all dquote and backslash with backslash
      String value = p.getProfileValue();
      for ( int k=0; k<value.length(); ++k ) {
	char ch = value.charAt(k);
	if ( ch == '"' || ch == '\\' ) result.append('\\');
	result.append(ch);
      }
      result.append('"');
      previous = ns;
      flag = true;
    }

    return result.toString();
  }


  /**
   * Test program.
   *
   * @param args are command-line arguments
   */
  public static void main( String args[] )
  {
    ProfileParser me = new ProfileParser();

    for ( int i=0; i<args.length; ++i ) {
      System.out.println( "input string in next line\n" + args[i] );
      List l = null;
      try { l = me.parse(args[i]); }
      catch ( ProfileParserException ppe ) {
	for ( int x=1; x<ppe.getPosition(); ++x ) System.err.print(' ');
	System.err.println( "^" );
	System.err.println( "ERROR: " + ppe.getMessage() +
			    " at position " + ppe.getPosition() );
	continue;
      }
      System.out.println( "output mappings in next line\n" + l.toString() );
      String s = me.combine(l);
      System.out.println( "recombination in next line\n" + s );
      System.out.println();
    }
  }
}
