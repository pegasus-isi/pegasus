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


import org.griphyn.cPlanner.code.GridStart;

import org.griphyn.cPlanner.code.gridstart.GridStartFactory;
import org.griphyn.cPlanner.code.gridstart.GridStartFactoryException;

import org.griphyn.cPlanner.common.LogManager;
import org.griphyn.cPlanner.common.PegasusProperties;

import org.griphyn.cPlanner.classes.ADag;
import org.griphyn.cPlanner.classes.AggregatedJob;
import org.griphyn.cPlanner.classes.SubInfo;
import org.griphyn.cPlanner.classes.SiteInfo;

import org.griphyn.cPlanner.namespace.Condor;
import org.griphyn.cPlanner.namespace.VDS;

import org.griphyn.cPlanner.poolinfo.PoolInfoProvider;
import org.griphyn.cPlanner.poolinfo.PoolMode;


import org.griphyn.common.util.DynamicLoader;

import org.griphyn.common.catalog.TransformationCatalog;
import org.griphyn.common.catalog.TransformationCatalogEntry;

import org.griphyn.common.catalog.transformation.TCMode;

import org.griphyn.common.classes.TCType;


import java.io.File;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

import java.util.List;
import java.util.Set;
import java.util.Iterator;

/**
 * The interface that dictates how the jobs are clumped together into one single
 * larger job. The interface does not dictate how the graph structure is
 * to be modified as a result of the clumping. That is handled outside of the
 * implementing class in NodeCollapser.
 *
 * @author Karan Vahi vahi@isi.edu
 * @version $Revision: 1.2 $
 */
public interface JobAggregator {

    /**
     * The version number associated with this API of Job Aggregator.
     */
    public static final String VERSION = "1.2";


    /**
     * Constructs a new aggregated job that contains all the jobs passed to it.
     * The new aggregated job, appears as a single job in the workflow and
     * replaces the jobs it contains in the workflow.
     *
     * @param jobs the list of <code>SubInfo</code> objects that need to be
     *             collapsed. All the jobs being collapsed should be scheduled
     *             at the same pool, to maintain correct semantics.
     * @param name  the logical name of the jobs in the list passed to this
     *              function.
     * @param id   the id that is given to the new job.
     *
     * @return  the <code>SubInfo</code> object corresponding to the aggregated
     *          job containing the jobs passed as List in the input,
     *          null if the list of jobs is empty
     */
    public AggregatedJob construct(List jobs,String name,String id);

    /**
     * Setter method to indicate , failure on first consitutent job should
     * result in the abort of the whole aggregated job.
     *
     * @param fail  indicates whether to abort or not .
     */
    public void setAbortOnFirstJobFailure( boolean fail);

    /**
     * Returns a boolean indicating whether to fail the aggregated job on
     * detecting the first failure during execution of constituent jobs.
     *
     * @return boolean indicating whether to fail or not.
     */
    public boolean abortOnFristJobFailure();


    /**
     * Determines whether there is NOT an entry in the transformation catalog
     * for the job aggregator executable on a particular site.
     *
     * @param site       the site at which existence check is required.
     *
     * @return boolean  true if an entry does not exists, false otherwise.
     */
    public boolean entryNotInTC(String site);

    /**
     * Returns the logical name of the transformation that is used to
     * collapse the jobs.
     *
     * @return the the logical name of the collapser executable.
     */
    public String getCollapserLFN();


    }
