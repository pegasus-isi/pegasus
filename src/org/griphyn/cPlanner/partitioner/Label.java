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

import org.griphyn.cPlanner.common.PegasusProperties;
import org.griphyn.cPlanner.common.LogManager;

import org.griphyn.cPlanner.partitioner.graph.GraphNode;
import org.griphyn.cPlanner.partitioner.graph.Bag;
import org.griphyn.cPlanner.partitioner.graph.LabelBag;

import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Iterator;
import java.util.Set;
import java.util.HashSet;

/**
 * This partitioner partitions the DAX into smaller partitions as specified by
 * the labels associated with the jobs. If no label is specified, then the
 * partitioner puts the job into a unique partition corresponding to the job
 * ID.
 *
 * @author Karan Vahi
 * @version $Revision$
 *
 *
 */
public class Label extends Partitioner {

    /**
     * The default label that is associated with the job in case of no label
     * being specified.
     */
//    public static final String DEFAULT_LABEL = "default";

    /**
     * A short description about the partitioner.
     */
    public static final String DESCRIPTION = "Label Based Partitioning";

    /**
     * A map indexed by the label. Each value is a partition object
     * consisting of jobs with that label.
     */
    private Map mPartitionMap;

    /**
     * The first in first out queue, that manages the set of gray vertices in a
     * breadth first search.
     */
    private LinkedList mQueue;

    /**
     * The handle to the Logging object.
     */
    private LogManager mLogger;

    /**
     * The overloaded constructor.
     *
     * @param root       the dummy root node of the graph.
     * @param graph      the map containing all the nodes of the graph keyed by
     *                   the logical id of the nodes.
     * @param properties the properties passed to the planner.
     */
    public Label(GraphNode root, Map graph, PegasusProperties properties) {
        super(root, graph, properties);
        mPartitionMap = new HashMap(10);
        mQueue = new LinkedList();
        mLogger = LogManager.getInstance();
    }

    /**
     * Partitions the graph passed in the constructor, on the basis of the labels
     * associated with the nodes in the graph. All the nodes, with the same label
     * are deemed to be in the same partition.
     *
     * @param c   the callback for the partitioner.
     */
    public void determinePartitions( Callback c ){
        int currentDepth = 0;
        GraphNode node;
        GraphNode parent;
        GraphNode child;
        int depth = 0;
        List levelList = new java.util.LinkedList();
        String currentLabel = null;
        int i = 0,partitionNum = 0;


        mLogger.log( "Starting Graph Traversal", LogManager.INFO_MESSAGE_LEVEL );
        //set the depth of the dummy root as 0
        mRoot.setDepth( currentDepth );

        mQueue.addLast( mRoot );

        while( !mQueue.isEmpty() ){
            node  = (GraphNode)mQueue.getFirst();
            depth = node.getDepth();
            currentLabel = getLabel( node );
            if(currentDepth < depth){
                //a new level starts
                currentDepth++;
                levelList.clear();
            }

            //get the partition for the label
            Partition p = null;
            if( mPartitionMap.containsKey( currentLabel ) ){
                p = (Partition)mPartitionMap.get( currentLabel );
            }
            else {
                p = new Partition();
                if( currentDepth > 0 ){
                    partitionNum++;
                    p.setIndex( partitionNum );
                    p.setID(getPartitionID( partitionNum ));
                    mPartitionMap.put( currentLabel, p );
                }
            }

            if( p.lastAddedNode()!= null && depth > p.lastAddedNode().getDepth() + 1 ){
                throw new RuntimeException( "Invalid labelled graph" );
                /*
                //partition with current label has been fully
                //constructed. write out the existing partition

                //create a new partition
                Partition newp = new Partition();
                newp.addNode(node);
                mPartitionMap.put(currentLabel,newp);
                */
            }
            else if(currentDepth > 0){
                //add to the existing partition for the current label
                p.addNode(node);
                //also associate the partition id with the node
                node.getBag().add( LabelBag.PARTITION_KEY,p.getID() );
            }


            mLogger.log("Adding to level " + currentDepth + " "
                        + node.getID(),LogManager.DEBUG_MESSAGE_LEVEL);
            levelList.add( node );


            node.setColor( GraphNode.BLACK_COLOR );
            for( Iterator it = node.getChildren().iterator(); it.hasNext(); ){
                child = (GraphNode)it.next();
                if(!child.isColor( GraphNode.GRAY_COLOR ) &&
                   child.parentsColored( GraphNode.BLACK_COLOR ) ){
                    mLogger.log( "Adding to queue " + child.getID(),
                                 LogManager.DEBUG_MESSAGE_LEVEL );
                    child.setDepth( depth + 1 );
                    child.setColor( GraphNode.GRAY_COLOR );
                    mQueue.addLast( child );
                }

            }
            node = (GraphNode)mQueue.removeFirst();
            mLogger.log( "Removed " + node.getID(),
                         LogManager.DEBUG_MESSAGE_LEVEL );
        }
        mLogger.logCompletion( "Starting Graph Traversal",
                               LogManager.INFO_MESSAGE_LEVEL );

        for( Iterator it = mPartitionMap.entrySet().iterator(); it.hasNext(); ){
            Map.Entry entry = (Map.Entry)it.next();
            Partition p = (Partition)entry.getValue();
            p.constructPartition();
            mLogger.log( "Partition is " + p.getNodeIDs() + " corresponding to label " +
                         entry.getKey(),
                         LogManager.DEBUG_MESSAGE_LEVEL );
            c.cbPartition( p );
        }

        mLogger.log( "Determining relations between partitions",
                     LogManager.INFO_MESSAGE_LEVEL );
        //construct the relations
        for( Iterator it = mPartitionMap.entrySet().iterator(); it.hasNext(); ){
            Map.Entry entry = (Map.Entry) it.next();
            Partition p = (Partition) entry.getValue();
            List roots = p.getRootNodes();
            Set parentPartitions = new HashSet( roots.size() );

            //get the Root nodes for each partition and
            //for each root, determine the partitions of it's parents
            for( Iterator rootIt = roots.iterator(); rootIt.hasNext(); ){
                node = (GraphNode)rootIt.next();
                for( Iterator parentsIt = node.getParents().iterator(); parentsIt.hasNext(); ){
                    parent = (GraphNode)parentsIt.next();
                    //the parents partition id is parent for the
                    //partition containing the root
                    parentPartitions.add( parent.getBag().get( LabelBag.PARTITION_KEY ) );
                }
            }
            //write out all the parents of the partition
            if(!parentPartitions.isEmpty()){
                c.cbParents( p.getID(), new ArrayList( parentPartitions ) );
            }
        }
        mLogger.logCompletion( "Determining relations between partitions",
                               LogManager.INFO_MESSAGE_LEVEL );

        //done with the partitioning
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


    /**
     * Returns the label for the node. If no label is associated with the node,
     * then the ID of the node is assumed as the label.
     *
     * @param node  the node for which the label is required.
     *
     * @return the label associated with the job, else the id of the node.
     */
    private String getLabel(GraphNode node){
        Bag b = (LabelBag)node.getBag();
        Object obj = b.get( LabelBag.LABEL_KEY );
        return (obj == null )? node.getID() /*this.DEFAULT_LABEL*/ : (String)obj;
    }

    /**
     * Constructs the id for the partition.
     *
     * @param id the integer id.
     *
     * @return the ID of the partition.
     */
    private String getPartitionID( int id ){
        StringBuffer sb = new StringBuffer(5);
        sb.append( "ID" ).append( id );
        return sb.toString();
    }



}
