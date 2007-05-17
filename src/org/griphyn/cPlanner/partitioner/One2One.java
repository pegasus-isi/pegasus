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
package org.griphyn.cPlanner.partitioner;

import org.griphyn.cPlanner.common.LogManager;
import org.griphyn.cPlanner.common.PegasusProperties;

import org.griphyn.cPlanner.partitioner.graph.GraphNode;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * This partitioning technique considers each of the job in the dax as a
 * separate partition. This is used for Euryale style mode of operation in
 * Pegasus.
 *
 * @author Karan Vahi
 * @version $Revision$
 */

public class One2One extends Partitioner {

    /**
     * A short description about the partitioner.
     */
    public static final String DESCRIPTION = "One to One Partitioning";


    /**
     * The overloaded constructor.
     *
     * @param root       the dummy root node of the graph.
     * @param graph      the map containing all the nodes of the graph keyed by
     *                   the logical id of the nodes.
     * @param properties the properties passed to the planner.
     */
    public One2One( GraphNode root, Map graph, PegasusProperties properties ) {
        super( root, graph, properties );
    }

    /**
     * This ends up writing out a partition for each job in the dax. It is a
     * one 2 one mapping from the jobs in the dax to the corresponding
     * partitions in the pdax. The ids of the partitions in pdax is same
     * as the ids of the corresponding jobs in the dax.
     *
     * @param c  the callback object to callout to while partitioning.
     */
    public void determinePartitions( Callback c ) {
        //we just traverse the graph via an iterator, as we do not
        //need to any particular graph traversal for this mode.

        String key     = null;
        GraphNode node = null;
        int currentIndex = 0;


        for( Iterator it = mGraph.keySet().iterator(); it.hasNext(); ){
            //the key is the logical id of the node specified in the dax
            key = (String)it.next();
            node = (GraphNode)mGraph.get(key);
            //we have to ignore the dummy root node.
            if( node.getID().equals( mRoot.getID() ) ){
                //we go to next node
                mLogger.log( "Ignoring node " + node.getID(),
                             LogManager.DEBUG_MESSAGE_LEVEL );
                continue;
            }
            currentIndex++;

            //construct the partition for this node
            //the partition has just one node with id same as the id
            //of the corresponding id of the job in the dax
            List levelList = new ArrayList(1);
            levelList.add( node );
            Partition p = new Partition( levelList, node.getID() );
            p.setIndex( currentIndex );
//            p.setName(mDAXWriter.getPartitionName());
            p.constructPartition();

            mLogger.log( "Partition is " + p.getNodeIDs(),
                         LogManager.DEBUG_MESSAGE_LEVEL );

            c.cbPartition( p );
        }

        //writing out the relations between the partitions in the file
        mLogger.log( "Building Relations between partitions ",
                     LogManager.DEBUG_MESSAGE_LEVEL );
        for(Iterator it = mGraph.keySet().iterator(); it.hasNext();){
            //the key is the logical id of the node specified in the dax
            key = (String)it.next();
            node = (GraphNode)mGraph.get(key);
            List parents = node.getParents();

            //we have to ignore the dummy root node.
            //and the node with no parents
            if( node.getID().equals(mRoot.getID()) || parents == null ){
                //we go to next node
                mLogger.log( "Ignoring node " + node.getID(),
                             LogManager.DEBUG_MESSAGE_LEVEL );
                continue;
            }

            //get the parents of the node and write out to the pdax file.
            List partitionIDs = new java.util.ArrayList( parents.size() );
            for( Iterator it1 = parents.iterator(); it1.hasNext(); ) {
                //the jobs in the dax have same id as corresponding paritions
                partitionIDs.add( ( (GraphNode) it1.next()).getID());
            }
            //write out to the pdax file
            c.cbParents( key, partitionIDs );
            partitionIDs = null;
        }
        mLogger.logCompletion("Building Relations between partitions ",
                              LogManager.DEBUG_MESSAGE_LEVEL);

        //we are done with the partitioning
        c.cbDone();
    }


    /**
     * Returns a textual description of the transfer implementation.
     *
     * @return a short textual description
     */
    public String description(){
        return this.DESCRIPTION;
    }

}
