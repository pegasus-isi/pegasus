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

package org.griphyn.cPlanner.partitioner.graph;

import org.griphyn.cPlanner.classes.ADag;
import org.griphyn.cPlanner.classes.SubInfo;
import org.griphyn.cPlanner.classes.PCRelation;
import org.griphyn.cPlanner.classes.PlannerOptions;

import org.griphyn.cPlanner.common.PegasusProperties;


import java.util.Iterator;
import java.util.Map;
import java.util.HashMap;
import java.util.Vector;
import java.util.List;

/**
 * A Adapter class that converts the <code>ADag</code> to <code>Graph</code> and
 * vice a versa.
 *
 *
 * @author Karan Vahi
 * @version $Revision: 1.1 $
 */

public class Adapter {


    /**
     * Converts the <code>ADag</code> to <code>Graph</code> instance.
     *
     * @param adag  the <code>ADag</code> object.
     *
     * @return  it's representation as a <code>Graph</code> instance.
     */
    public static Graph convert( ADag dag ){
        Graph graph = new MapGraph();

        //iterate through the list of jobs and populate the nodes in the graph
        for( Iterator it = dag.vJobSubInfos.iterator(); it.hasNext(); ){
            //pass the jobs to the callback
            //populate the job as a node in the graph
            SubInfo job = ( SubInfo )it.next();
            GraphNode node = new GraphNode( job.getID(), job );
            graph.addNode( node );
        }

        //add the edges between the nodes in the graph
        for( Iterator it = dag.dagInfo.relations.iterator(); it.hasNext(); ){
            PCRelation rel = (PCRelation) it.next();
            graph.addEdge( rel.getParent(), rel.getChild() );
        }

        return graph;

    }

}