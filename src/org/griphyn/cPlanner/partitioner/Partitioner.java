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

import java.util.Map;

/**
 * The abstract class that lays out the api to do the partitioning of the dax
 * into smaller daxes. It defines additional functions to get and set the name
 * of the partitions etc.
 *
 * @author Karan Vahi
 * @version $Revision: 1.11 $
 */

public abstract class Partitioner {

    /**
     * The package name where the implementing classes of this interface reside.
     */
    public static final String PACKAGE_NAME = "org.griphyn.cPlanner.partitioner";

    /**
     * The version number associated with this API of Code Generator.
     */
    public static final String VERSION = "1.2";


    /**
     * The root node of the graph from where to start the BFS.
     */
    protected GraphNode mRoot;

    /**
     * The map containing all the graph nodes. The key to the map are the logical
     * id's of the jobs as identified in the dax and the values are the
     * corresponding Graph Node objects.
     */
    protected Map mGraph;



    /**
     * The handle to the internal logging object.
     */
    protected LogManager mLogger;

    /**
     * The object holding all the properties pertaining to Pegasus.
     */
    protected PegasusProperties mProps;


    /**
     * The overloaded constructor.
     *
     * @param root       the dummy root node of the graph.
     * @param graph      the map containing all the nodes of the graph keyed by
     *                   the logical id of the nodes.
     * @param properties the properties passed out to the planner.
     */
    public Partitioner(GraphNode root, Map graph, PegasusProperties properties) {
        mRoot  = root;
        mGraph = graph;
        mLogger = LogManager.getInstance();
        mProps = properties;
        //set a default name to the partition dax
        //mPDAXWriter = null;

    }

    /**
     * The main function that ends up traversing the graph structure corrsponding
     * to the dax and creates the smaller dax files(one dax file per partition)
     * and the .pdax file that illustrates the partition graph. It is recommended
     * that the implementing classes use the already initialized handles to the
     * DAXWriter and PDAXWriter interfaces to write out the xml files. The
     * advantage of using these preinitialized handles is that they already
     * are correctly configured for the directories where Pegasus expects the
     * submit files and dax files to reside.
     *
     *
     * @param c  the callback object that the partitioner calls out to.
     */
    public abstract void determinePartitions( Callback c );



    /**
     * Returns a textual description of the transfer implementation.
     *
     * @return a short textual description
     */
    public abstract String description();



}
