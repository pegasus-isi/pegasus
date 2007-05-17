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
package org.griphyn.cPlanner.partitioner;

import org.griphyn.cPlanner.cluster.Clusterer;
import org.griphyn.cPlanner.cluster.ClustererException;

import org.griphyn.cPlanner.common.PegasusProperties;

import java.util.List;

/**
 * A Callback implementation that passes the partitions detected during the
 * partitioning of the worflow to a Clusterer for clustering. The clusterer
 * is passed off to the callback during the callback initialization.
 *
 * @author Karan Vahi
 * @version $Revision: 1.1 $
 */

public class ClustererCallback implements Callback {

    /**
     * The handle to the clusterer that does the clustering.
     */
    private Clusterer mClusterer;

    /**
     * The handle to the properties object.
     */
    private PegasusProperties mProps;

    /**
     * The default constructor.
     */
    public ClustererCallback(){

    }

    /**
     * Initializes the callback.
     *
     * @param properties the properties passed to the planner.
     * @param clusterer  the clusterer that has to be called out, in the callback
     *                   methods.
     */
    public void initialize (PegasusProperties properties, Clusterer clusterer){
        mProps     = properties;
        mClusterer = clusterer;
    }

    /**
     * Callback for when a partitioner determines that partition has been
     * constructed. The partition is passed off to the clusterer that the
     * callback has been initialized with.
     *
     * @param p the constructed partition.
     *
     * @throws RuntimeException in case of callback not being initialized, or
     *         a ClustererException being thrown during the Clusterer operation.
     */
    public void cbPartition( Partition p )  {

        //sanity check
        if( mClusterer == null ){
            throw new RuntimeException(
                "Callback needs to be initialized before being used");
        }

        //shallow wrap of exception for time being
        try{
            mClusterer.determineClusters(p);
        }
        catch ( ClustererException e ){
            throw new RuntimeException( "ClustererCallback cbPartition( Partition ) ",
                                        e );
        }
    }

    /**
     * Callback for when a partitioner determines the relations between partitions
     * that it has previously constructed.
     *
     * @param child    the id of a partition.
     * @param parents  the list of <code>String</code> objects that contain
     *                 the id's of the parents of the partition.
     *
     *
     * @throws RuntimeException in case of callback not being initialized, or
     *         a ClustererException being thrown during the Clusterer operation.
     */
    public void cbParents(String child, List parents) {
        //sanity check
        if( mClusterer == null ){
            throw new RuntimeException(
                "Callback needs to be initialized before being used");
        }


        //shallow wrap of exception for time being
        try{
            mClusterer.parents(child, parents);
        }
        catch ( ClustererException e ){
            throw new RuntimeException( "ClustererCallback cbParents( String, List ) ",
                                        e );
        }

    }

    /**
     * Callback for the partitioner to signal that it is done with the processing.
     *
     *
     */
    public void cbDone(){

    }

}
