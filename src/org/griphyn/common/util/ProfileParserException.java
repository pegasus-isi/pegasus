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
 * This class is used to signal errors while parsing profile strings
 * @see ProfileParser
 *
 * @author Gaurang Mehta
 * @author Jens-S. Vöckler
 * @version $Revision$
 */
public class ProfileParserException 
  extends Exception
{
  /**
   * Remembers the position that cause the exception to be thrown.
   */
  private int m_position;

  public ProfileParserException( String msg, int position )
  {
    super(msg);
    m_position = position;
  }

  public ProfileParserException( String msg, int position, Throwable cause )
  {
    super(msg,cause);
    m_position = position;
  }

  /**
   * Obtains the position at which point the exception was thrown.
   * @return a column position into the string
   */
  public int getPosition()
  {
    return this.m_position;
  }
}
