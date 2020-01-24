/*
 * This file or a portion of this file is licensed under the terms of
 * the Globus Toolkit Public License, found in file ../GTPL, or at
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

package org.griphyn.vdl.euryale;

import java.io.File;
import java.io.IOException;

/**
 * A Virtual Flat File Factory that does not do any existence checks while creating a directory. The
 * factory, is used to create remote paths without checking for correctness.
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public class VirtualFlatFileFactory extends FlatFileFactory {

    /**
     * Constructor: Creates the directory and employs sanity checks.
     *
     * @param directory is the place where files should be placed.
     * @throws IOException if the location is not a writable directory, or cannot be created as
     *     such.
     */
    public VirtualFlatFileFactory(File directory) throws IOException {
        super(directory);
    }

    /**
     * Constructor: Creates the directory and employs sanity checks.
     *
     * @param directory is the place where files should be placed.
     * @throws IOException if the location is not a writable directory, or cannot be created as
     *     such.
     */
    public VirtualFlatFileFactory(String directory) throws IOException {
        super(directory);
    }

    /**
     * Checks the destination location for existence, if it can be created, if it is writable etc.
     * Does no check as it is virtual.
     *
     * @param dir is the new base directory to optionally create
     * @throws IOException
     */
    protected void sanityCheck(File dir) throws IOException {}
}
