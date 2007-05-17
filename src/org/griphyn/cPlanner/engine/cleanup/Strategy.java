/**
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

package org.griphyn.cPlanner.engine.cleanup;

import org.griphyn.cPlanner.partitioner.graph.Graph;

/**
 * The interface that defines how the cleanup job is invoked and created.
 *
 * @author  Karan Vahi
 * @version $Revision$
 */
public interface Strategy {


    /**
     * The version number associated with this API Cleanup Strategy.
     */
    public static final String VERSION = "1.0";

    /**
     * Adds cleanup jobs to the workflow.
     *
     * @param workflow   the workflow to add cleanup jobs to.
     *
     * @return the workflow with cleanup jobs added to it.
     */
    public Graph addCleanupJobs( Graph workflow );

}
