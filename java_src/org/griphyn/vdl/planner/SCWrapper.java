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

package org.griphyn.vdl.planner;

import java.util.*;

/**
 * This class wraps the shell planner's request into the new site catalog API. The site catalog is
 * only queried for the contents of its "local" special site.
 *
 * @author Jens-S. Vöckler
 * @author Yong Zhao
 * @version $Revision$
 */
public class SCWrapper implements Wrapper {

    /**
     * Connects the interface with the site catalog implementation. The choice of backend is
     * configured through properties.
     */
    public SCWrapper() {
        throw new RuntimeException("Unsupported wrapper ");
    }

    /**
     * Frees resources taken by the instance of the replica catalog. This method is safe to be
     * called on failed or already closed catalogs.
     */
    public void close() {
        throw new RuntimeException("Unsupported wrapper ");
    }

    /** garbage collection. */
    protected void finalize() {
        close();
    }

    /**
     * Determines the working directory for the site "local".
     *
     * @return the working directory, of <code>null</code>, if not available.
     */
    public String getWorkingDirectory() {

        throw new RuntimeException("Unsupported wrapper ");
    }

    /**
     * Determines the path to the local installation of a grid launcher for site "local".
     *
     * @return the path to the local kickstart, or <code>null</code>, if not available.
     */
    public String getGridLaunch() {
        throw new RuntimeException("Unsupported wrapper ");
    }

    /**
     * Gathers all profiles declared for pool local.
     *
     * @return a map of maps, the outer map indexed by the profile namespace, and the inner map
     *     indexed by the key in the profile. Returns <code>null</code> in case of error.
     */
    public Map getProfiles() {

        throw new RuntimeException("Unsupported wrapper ");
    }

    /**
     * Obtains the name of the class implementing the replica catalog.
     *
     * @return class name of the replica catalog implementor.
     */
    public String getName() {

        throw new RuntimeException("Unsupported wrapper ");
    }

    /**
     * Shows the contents of the catalog as one string. Warning, this may be very large, slow, and
     * memory expensive.
     *
     * @return the string with the complete catalog contents.
     * @throws RuntimeException because the method is not implemented.
     */
    public String toString() {
        throw new RuntimeException("method not implemented");
    }
}
