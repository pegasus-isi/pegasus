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

/**
 * This is the test program for the Separator class.
 *
 * @author Jens-S. Vöckler
 * @author Yong Zhao
 * @version $Revision: 1.2 $
 *
 * @see org.griphyn.vdl.classes.Definition
 */
public class SeparatorTest
{
  public static void show( String what )
  {
    System.out.print( what + " => [" );
    try {
      String[] x = Separator.split(what);
      for ( int i=0; i<x.length; ++i ) {
	System.out.print( Integer.toString(i) + ':' );
	System.out.print( x[i] == null ? "null" : "\"" + x[i] + "\"" );
	if ( i < x.length-1 ) System.out.print( ", " );
      }
    } catch ( IllegalArgumentException iae ) {
      System.out.print( iae.getMessage() );
    }
    System.out.println( ']' );
  }

  public static void main( String[] args )
  {
    if ( args.length > 0 ) {
      for ( int i=0; i<args.length; ++i )
	show( args[i] );
    } else {
      show( "test" );
      show( "test::me" );
      show( "test:me" );
      show( "test::me:too" );
      show( "illegal:::too" );

      show( "test::me:a,b" );
      show( "test::me:,b" );
      show( "test::me:a," );
      show( "il::legal:," ); // illegal spec

      show( "me:a,b" );
      show( "me:,b" );
      show( "me:a," );
      show( "illegal:," ); // illegal spec

      show( ":::," ); // illegal spec
    }
  }
}
