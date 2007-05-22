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

import org.griphyn.common.catalog.work.WorkCatalogException;

/**
 *
 * The catalog interface to the Work Catalog, the erstwhile Work DB, that is
 * populated by tailstatd and associates.
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public interface WorkCatalog extends Catalog {

    /**
     * Prefix for the property subset to use with this catalog.
     */
    public static final String c_prefix = "pegasus.catalog.work";

    /**
     * The  DB Driver properties prefix.
     */
    public static final String DB_PREFIX = "pegasus.catalog.work.db";



    /**
     * The version of the API
     */
    public static final String VERSION = "1.0";

    /**
     * Inserts a new mapping into the work catalog.
     *
     * @param basedir  the base directory
     * @param vogroup  the vo to which the user belongs to.
     * @param label    the label in the DAX
     * @param run      the run number.
     * @param creator  the user who is running.
     * @param cTime    the creation time of the DAX
     * @param mTime    the modification time.
     * @param state    the state of the workflow
     *
     *
     * @return number of insertions, should always be 1. On failure,
     * throw an exception, don't use zero.
     *
     *
     * @throws WorkCatalogException in case of unable to delete entry.
     */
    public int insert(String basedir,
                      String vogroup,
                      String label,
                      String run,
                      String creator,
                      java.util.Date cTime,
                      java.util.Date mTime,
                      int state) throws WorkCatalogException ;
    /**
     * Deletes a  mapping from the work catalog.
     *
     * @param basedir  the base directory
     * @param vogroup  the vo to which the user belongs to.
     * @param label    the label in the DAX
     * @param run      the run number.
     *
     * @return number of insertions, should always be 1. On failure,
     * throw an exception, don't use zero.
     *
     * @throws WorkCatalogException in case of unable to delete entry.
     */
    public int delete(String basedir,
                      String vogroup,
                      String label,
                      String run ) throws WorkCatalogException;


}
