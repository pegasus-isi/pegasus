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
package org.griphyn.cPlanner.cluster.aggregator;

import org.griphyn.cPlanner.cluster.JobAggregator;

import org.griphyn.cPlanner.classes.ADag;
import org.griphyn.cPlanner.classes.SubInfo;
import org.griphyn.cPlanner.classes.AggregatedJob;

import org.griphyn.cPlanner.common.PegasusProperties;

import java.util.List;

/**
 * This class aggregates the smaller jobs in a manner such that
 * they are launched at remote end, by mpiexec on n nodes where n is the nodecount
 * associated with the aggregated job that is being lauched by mpiexec.
 * The executable mpiexec is a VDS tool distributed in the VDS worker package, and
 * can be usually found at $PEGASUS_HOME/bin/mpiexec.
 *
 * @author Karan Vahi vahi@isi.edu
 * @version $Revision: 1.2 $
 */

public class MPIExec extends Abstract {

    /**
     * The logical name of the transformation that is able to run multiple
     * jobs sequentially.
     */
    public static final String COLLAPSE_LOGICAL_NAME = "mpiexec";

    /**
     * The overloaded constructor, that is called by load method.
     *
     * @param properties the <code>PegasusProperties</code> object containing all
     *                   the properties required by Pegasus.
     * @param submitDir  the submit directory where the submit file for the job
     *                   has to be generated.
     * @param dag        the workflow that is being clustered.
     *
     * @see JobAggregatorFactory#loadInstance(String,PegasusProperties,String,ADag)
     *
     */
    public MPIExec(PegasusProperties properties, String submitDir,ADag dag){
        super(properties,submitDir,dag);
    }

    /**
     * Constructs a new aggregated job that contains all the jobs passed to it.
     * The new aggregated job, appears as a single job in the workflow and
     * replaces the jobs it contains in the workflow.
     * <p>
     * The aggregated job is executed at a site, using mpiexec that
     * executes each of the smaller jobs in the aggregated job on n number of
     * nodes where n is the nodecount associated with the job.
     * All the sub jobs are in turn launched via kickstart if kickstart is
     * installed at the site where the job resides.
     *
     * @param jobs the list of <code>SubInfo</code> objects that need to be
     *             collapsed. All the jobs being collapsed should be scheduled
     *             at the same pool, to maintain correct semantics.
     * @param name  the logical name of the jobs in the list passed to this
     *              function.
     * @param id   the id that is given to the new job.
     *
     *
     * @return  the <code>AggregatedJob</code> object corresponding to the aggregated
     *          job containing the jobs passed as List in the input,
     *          null if the list of jobs is empty
     */
    public AggregatedJob construct(List jobs,String name, String id) {
        AggregatedJob mergedJob = super.construct(jobs,name,id);
        //also put in jobType as mpi
        mergedJob.globusRSL.checkKeyInNS("jobtype","mpi");
        return mergedJob;
    }

    /**
     * Returns the logical name of the transformation that is used to
     * collapse the jobs.
     *
     * @return the the logical name of the collapser executable.
     * @see #COLLAPSE_LOGICAL_NAME
     */
    public String getCollapserLFN(){
        return COLLAPSE_LOGICAL_NAME;
    }

    /**
     * Determines whether there is NOT an entry in the transformation catalog
     * for the job aggregator executable on a particular site.
     *
     * @param site       the site at which existence check is required.
     *
     * @return boolean  true if an entry does not exists, false otherwise.
     */
    public boolean entryNotInTC(String site) {
        return this.entryNotInTC(null,COLLAPSE_LOGICAL_NAME,null,site);
    }


    /**
     * Returns the arguments with which the <code>AggregatedJob</code>
     * needs to be invoked with. At present any empty argument string is
     * returned.
     *
     * @param job  the <code>AggregatedJob</code> for which the arguments have
     *             to be constructed.
     *
     * @return argument string
     */
    public String aggregatedJobArguments( AggregatedJob job ){
        return "";
    }


    /**
     * Setter method to indicate , failure on first consitutent job should
     * result in the abort of the whole aggregated job. Ignores any value
     * passed, as MPIExec does not handle it for time being.
     *
     * @param fail  indicates whether to abort or not .
     */
    public void setAbortOnFirstJobFailure( boolean fail){

    }

    /**
     * Returns a boolean indicating whether to fail the aggregated job on
     * detecting the first failure during execution of constituent jobs.
     *
     * @return boolean indicating whether to fail or not.
     */
    public boolean abortOnFristJobFailure(){
        return false;
    }

}
