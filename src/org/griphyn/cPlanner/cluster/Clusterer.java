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

package org.griphyn.cPlanner.cluster;

import org.griphyn.cPlanner.classes.ADag;
import org.griphyn.cPlanner.classes.PegasusBag;

import org.griphyn.cPlanner.partitioner.Partition;


import java.util.List;

/**
 * The clustering API, that constructs clusters of jobs out of a single
 * partition.
 *
 *
 * @author Karan Vahi
 * @version $Revision$
 */

public interface Clusterer {

    /**
     * The version number associated with this API of Code Generator.
     */
    public static final String VERSION = "1.1";


    /**
     *Initializes the Clusterer impelementation
     *
     * @param dag  the workflow that is being clustered.
     * @param bag   the bag of objects that is useful for initialization.
     *
     * @throws ClustererException in case of error.
     */
    public void initialize( ADag dag , PegasusBag bag  )
        throws ClustererException;

    /**
     * Determine the clusters for a partition.
     *
     * @param partition   the partition for which the clusters need to be
     *                    determined.
     *
     * @throws ClustererException in case of error.
     */
    public void determineClusters( Partition partition ) throws ClustererException;

    /**
     * Associates the relations between the partitions with the corresponding
     * relations between the clustered jobs that are created for each Partition.
     *
     * @param partitionID   the id of a partition.
     * @param parents       the list of <code>String</code> objects that contain
     *                      the id's of the parents of the partition.
     *
     * @throws ClustererException in case of error.
     */
    public void parents( String partitionID, List parents ) throws ClustererException;


    /**
     * Returns the clustered workflow.
     *
     * @return  the <code>ADag</code> object corresponding to the clustered workflow.
     *
     * @throws ClustererException in case of error.
     */
    public ADag getClusteredDAG() throws ClustererException;

    /**
     * Returns a textual description of the transfer implementation.
     *
     * @return a short textual description
     */
    public String description();


}
