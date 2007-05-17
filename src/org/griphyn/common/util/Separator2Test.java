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
 * @version $Revision$
 *
 * @see org.griphyn.vdl.classes.Definition
 */
public class Separator2Test
{
  public static void x( String what, int len )
  {
    String s = ( what == null ? "(null)" : ("\"" + what + "\"") );
    System.out.print( s );
    for ( int i=s.length(); i<len; ++i ) System.out.print(' ');
  }

  public static void show( String what )
  {
    x( what, 16 );
    System.out.print( " => [" );
    try {
      String[] x = Separator.splitFQDI(what);
      for ( int i=0; i<x.length; ++i ) {
	System.out.print( Integer.toString(i) + ':' );
	x( x[i], 8 );
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
      show( "me" );
      show( "::me" );
      show( "::me:" );
      show( "me:" );
      show( "me:too" );
      show( "test::me" );
      show( "test::me:" );

      show( "::me:" );
      show( "::me:too" );
      show( "test::me:too" );
      show( "::me:too" );

      show( "::me::" );
      show( "::me:too:" );
      show( ":::" );
      show( "test:::" );
      show( ":::too" );
    }
  }
}
