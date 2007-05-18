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

package org.griphyn.common.catalog.replica;
import org.griphyn.common.catalog.CatalogException;

/**
 * Class to notify of failures. Exceptions are chained like the
 * {@link java.sql.SQLException} interface.<p>
 *
 * @author Jens-S. Vöckler, Karan Vahi
 * @see org.griphyn.common.catalog.ReplicaCatalog
 */
public class ReplicaCatalogException 
  extends CatalogException
{
  /* 
   * Constructs a <code>ReplicaCatalogException</code> with no detail
   * message.
   */
  public ReplicaCatalogException()
  {
    super();
  }

  /**
   * Constructs a <code>ReplicaCatalogException</code> with the
   * specified detailed message.
   *
   * @param s is the detailled message.
   */
  public ReplicaCatalogException( String s )
  {
    super(s);
  }

  /**
   * Constructs a <code>ReplicaCatalogException</code> with the
   * specified detailed message and a cause. 
   *
   * @param s is the detailled message.
   * @param cause is the cause (which is saved for later retrieval by the 
   * {@link java.lang.Throwable#getCause()} method). A <code>null</code>
   * value is permitted, and indicates that the cause is nonexistent or
   * unknown.
   */
  public ReplicaCatalogException( String s, Throwable cause )
  {
    super( s, cause );
  }

  /**
   * Constructs a <code>ReplicaCatalogException</code> with the
   * specified just a cause. 
   *
   * @param cause is the cause (which is saved for later retrieval by the 
   * {@link java.lang.Throwable#getCause()} method). A <code>null</code>
   * value is permitted, and indicates that the cause is nonexistent or
   * unknown.
   */
  public ReplicaCatalogException( Throwable cause )
  {
    super(cause);
  }
}
