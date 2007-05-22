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

package org.griphyn.common.catalog;

import java.util.Properties;

/**
 * This interface create a common ancestor for all cataloging
 * interfaces.
 *
 * @author Jens-S. Vöckler
 * @author Yong Zhao
 * @version $Revision$
 */
public interface Catalog{


    /**
     * The default DB Driver properties prefix.
     */
    public static final String DBDRIVER_ALL_PREFIX = "pegasus.catalog.*.db.driver";


  /**
   * Establishes a link between the implementation and the thing the
   * implementation is build upon. <p>
   * FIXME: The cause for failure is lost without exceptions.
   *
   * @param props contains all necessary data to establish the link.
   * @return true if connected now, or false to indicate a failure.
   */
  public boolean connect( Properties props );

  /**
   * Explicitely free resources before the garbage collection hits.
   */
  public void close();

  /**
   * Predicate to check, if the connection with the catalog's
   * implementation is still active. This helps determining, if it makes
   * sense to call <code>close()</code>.
   *
   * @return true, if the implementation is disassociated, false otherwise.
   * @see #close()
   */
  public boolean isClosed();
}

