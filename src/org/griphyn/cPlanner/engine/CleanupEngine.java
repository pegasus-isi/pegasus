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

package org.griphyn.cPlanner.engine;

import org.griphyn.cPlanner.classes.ADag;
import org.griphyn.cPlanner.classes.SubInfo;
import org.griphyn.cPlanner.classes.PlannerOptions;

import org.griphyn.cPlanner.common.LogManager;
import org.griphyn.cPlanner.common.PegasusProperties;

import org.griphyn.cPlanner.engine.cleanup.Strategy;
import org.griphyn.cPlanner.engine.cleanup.InPlace;

import org.griphyn.cPlanner.partitioner.graph.Graph;
import org.griphyn.cPlanner.partitioner.graph.GraphNode;
import org.griphyn.cPlanner.partitioner.graph.Adapter;

import java.util.Iterator;

/**
 * The refiner that results in the creation of cleanup jobs within the workflow.
 *
 * @author  Karan Vahi
 * @version $Revision: 1.2 $
 *
 */
public class CleanupEngine extends Engine {


    /**
     * The overloaded constructor.
     *
     * @param properties the handle to the properties object.
     * @param options    the options specified by the user to run the planner.
     *
     */
    public CleanupEngine( PegasusProperties properties, PlannerOptions options) {
        super( properties );
        mLogger = LogManager.getInstance();
        mPOptions = options;
    }

    /**
     * Adds the cleanup jobs in the workflow that removes the files staged to the
     * remote site.
     *
     * @param dag the scheduled dag that has to be clustered.
     *
     * @return ADag containing the cleanup jobs for the workflow.
     */
    public ADag addCleanupJobs( ADag dag ) {
        ADag result;

        //load the appropriate strategy that is to be used
        Strategy strategy = new InPlace( mProps, mPOptions );

        //we first need to convert internally into graph format
        Graph resultGraph =  strategy.addCleanupJobs( Adapter.convert(dag ) );

        //convert back to ADag and return
        result = dag;
        //we need to reset the jobs and the relations in it
        result.clearJobs();

        //traverse through the graph and jobs and edges
        for( Iterator it = resultGraph.nodeIterator(); it.hasNext(); ){
            GraphNode node = ( GraphNode )it.next();

            //get the job associated with node
            result.add( ( SubInfo )node.getContent() );

            //all the children of the node are the edges of the DAG
            for( Iterator childrenIt = node.getChildren().iterator(); childrenIt.hasNext(); ){
                GraphNode child = ( GraphNode ) childrenIt.next();
                result.addNewRelation( node.getID(), child.getID() );
            }
        }

        return result;
    }
}
