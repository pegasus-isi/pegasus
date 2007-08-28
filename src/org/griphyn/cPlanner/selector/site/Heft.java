/**
 * This file or a portion of this file is licensed under the terms of
 * the Globus Toolkit Public License, found at $PEGASUS_HOME/GTPL or
 * http://www.globus.org/toolkit/download/license.html.
 * This notice must appear in redistributions of this file
 * with or without modification.
 *
 * Redistributions of this Software, with or without modification, must reproduce
 * the GTPL in:
 * (1) the Software, or
 * (2) the Documentation or
 * some other similar material which is provided with the Software (if any).
 *
 * Copyright 1999-2004
 * University of Chicago and The University of Southern California.
 * All rights reserved.
 */

package org.griphyn.cPlanner.selector.site;


import org.griphyn.cPlanner.classes.SubInfo;
import org.griphyn.cPlanner.classes.PegasusBag;

import org.griphyn.cPlanner.common.LogManager;

import org.griphyn.cPlanner.selector.site.heft.HeftBag;
import org.griphyn.cPlanner.selector.site.heft.Algorithm;

import org.griphyn.cPlanner.partitioner.graph.Graph;
import org.griphyn.cPlanner.partitioner.graph.GraphNode;

import java.util.List;
import java.util.Iterator;

/**
 * The HEFT based site selector. The runtime for the job in seconds is picked
 * from the pegasus profile key runtime in the transformation catalog for a
 * transformation.
 *
 * The data communication costs between jobs if scheduled on different sites
 * is assumed to be fixed. Later on if required, the ability to specify this
 * value will be exposed via properties.
 *
 * The number of processors in a site is picked by the attribute idle-nodes
 * associated with the vanilla jobmanager for a site in the site catalog.
 *
 * @author Karan Vahi
 * @version $Revision$
 *
 * @see Algorithm#AVERAGE_BANDWIDTH
 * @see Algorithm#RUNTIME_PROFILE_KEY
 * @see Algorithm#DEFAULT_NUMBER_OF_FREE_NODES
 * @see Algorithm#AVERAGE_DATA_SIZE_BETWEEN_JOBS
 * @see org.griphyn.cPlanner.classes.JobManager.IDLE_NODES
 */
public class Heft extends Abstract {


    /**
     * An instance of the class that implements the HEFT algorithm.
     */
    private Algorithm mHeftImpl;

    /**
     * The default constructor.
     */
    public Heft() {
        super();
    }


    /**
     *  Initializes the site selector.
     *
     * @param bag   the bag of objects that is useful for initialization.
     */
    public void initialize( PegasusBag bag ){
        super.initialize( bag );
        mHeftImpl = new Algorithm( bag );
    }

    /**
     * Maps the jobs in the workflow to the various grid sites.
     * The jobs are mapped by setting the site handle for the jobs.
     *
     * @param workflow   the workflow in a Graph form.
     *
     * @param sites     the list of <code>String</code> objects representing the
     *                  execution sites that can be used.
     */
    public void mapWorkflow( Graph workflow, List sites ){

        //schedule the workflow, till i fix the interface
        mHeftImpl.schedule( workflow, sites );

        //get the makespan of the workflow
        mLogger.log( "Makespan of scheduled workflow is " + mHeftImpl.getMakespan() ,
                     LogManager.DEBUG_MESSAGE_LEVEL );

        //iterate through the jobs and just set the site handle
        //accordingly
        for( Iterator it = workflow.nodeIterator(); it.hasNext(); ){
            GraphNode node = ( GraphNode ) it.next();
            SubInfo job = ( SubInfo ) node.getContent();
            job.setSiteHandle( (String)node.getBag().get( HeftBag.SCHEDULED_SITE ) );
        }

    }

    /**
     * This method returns a String describing the site selection technique
     * that is being implemented by the implementing class.
     *
     * @return String
     */
    public String description() {
        return "Heft based Site Selector";
    }


}
