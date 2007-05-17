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

package org.griphyn.cPlanner.visualize.spaceusage;

import java.io.IOException;

import java.util.List;

/**
 * A plot interface that allows us to plot the SpaceUsage in different
 * formats.
 *
 * @author Karan Vahi
 * @version $Revision$
 */

public interface Plot {

    /**
     * The version of this API
     */
    public static final String VERSION = "1.3";


    /**
     * Initializer method.
     *
     * @param directory  the directory where the plots need to be generated.
     * @param basename   the basename for the files that are generated.
     * @param useStatInfo  boolean indicating whether to use stat info or not.
     */
    public void initialize( String directory , String basename, boolean useStatInfo );

    /**
     * Plot out the space usage.
     *
     * @param su          the SpaceUsage.
     * @param sizeUnits   the size unit.
     * @param timeUnits   the time unit.
     *
     * @return List of file pathnames for the files that are written out.
     *
     * @exception IOException in case of unable to write to the file.
     */
    public List plot( SpaceUsage su, char u , String timeUnits) throws IOException;

}
