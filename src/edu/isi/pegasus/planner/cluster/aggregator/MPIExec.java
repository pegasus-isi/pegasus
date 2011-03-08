/**
 *  Copyright 2007-2008 University Of Southern California
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package edu.isi.pegasus.planner.cluster.aggregator;

import edu.isi.pegasus.planner.catalog.site.classes.SiteCatalogEntry;

import edu.isi.pegasus.planner.classes.ADag;
import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.classes.AggregatedJob;


import edu.isi.pegasus.planner.namespace.Pegasus;

import edu.isi.pegasus.planner.code.GridStartFactory;

import java.util.List;
import edu.isi.pegasus.planner.code.GridStart;
import edu.isi.pegasus.planner.classes.PegasusBag;

/**
 * This class aggregates the smaller jobs in a manner such that
 * they are launched at remote end, by mpiexec on n nodes where n is the nodecount
 * associated with the aggregated job that is being lauched by mpiexec.
 * The executable mpiexec is a Pegasus tool distributed in the Pegasus worker package, and
 * can be usually found at $PEGASUS_HOME/bin/mpiexec.
 *
 * @author Karan Vahi vahi@isi.edu
 * @version $Revision$
 */

public class MPIExec extends Abstract {

    /**
     * The logical name of the transformation that is able to run multiple
     * jobs sequentially.
     */
    public static final String COLLAPSE_LOGICAL_NAME = "mpiexec";


    /**
     * The default constructor.
     */
    public MPIExec(){
        super();
    }

    /**
     *Initializes the JobAggregator impelementation
     *
     * @param dag  the workflow that is being clustered.
     * @param bag   the bag of objects that is useful for initialization.
     *
     *
     */
    public void initialize( ADag dag , PegasusBag bag  ){
        super.initialize(dag, bag);
    }


    /**
     * Enables the abstract clustered job for execution and converts it to it's
     * executable form. Also associates the post script that should be invoked
     * for the AggregatedJob
     *
     * @param job          the abstract clustered job
     */
    public void makeAbstractAggregatedJobConcrete( AggregatedJob job ){
        super.makeAbstractAggregatedJobConcrete(job);

        //also put in jobType as mpi
        job.globusRSL.checkKeyInNS("jobtype","mpi");

        return;
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
     * @param jobs the list of <code>Job</code> objects that need to be
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
/*
    public AggregatedJob construct(List jobs,String name, String id) {
        AggregatedJob mergedJob = super.construct(jobs,name,id);
        //also put in jobType as mpi
        mergedJob.globusRSL.checkKeyInNS("jobtype","mpi");

        //ensure that AggregatedJob is invoked via NoGridStart
        mergedJob.vdsNS.construct( Pegasus.GRIDSTART_KEY,
                                   GridStartFactory.GRIDSTART_SHORT_NAMES[
                                                          GridStartFactory.NO_GRIDSTART_INDEX] );

        return mergedJob;
    }

*/



    /**
     * Enables the constitutent jobs that make up a aggregated job. Makes sure
     * that they all are enabled via no kickstart
     *
     * @param mergedJob   the clusteredJob
     * @param jobs         the constitutent jobs
     *
     * @return AggregatedJob
     */
/*
    protected AggregatedJob enable(  AggregatedJob mergedJob, List jobs  ){
        //we cannot invoke any of clustered jobs also via kickstart
        //as the output will be clobbered
        Job firstJob = (Job)jobs.get(0);

        SiteCatalogEntry site = mSiteStore.lookup( firstJob.getSiteHandle() );
        firstJob.vdsNS.construct( Pegasus.GRIDSTART_KEY,
                                   GridStartFactory.GRIDSTART_SHORT_NAMES[
                                                          GridStartFactory.NO_GRIDSTART_INDEX] );

        //NEEDS TO BE FIXED AS CURRENTLY NO PLACEHOLDER FOR Kickstart 
        //PATH IN THE NEW SITE CATALOG Karan July 10, 2008
        GridStart gridStart = mGridStartFactory.loadGridStart( firstJob,
                                                               site.getKickstartPath() );


        return gridStart.enable( mergedJob, jobs );
    }
*/

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
        return this.entryNotInTC( this.TRANSFORMATION_NAMESPACE,
                                  COLLAPSE_LOGICAL_NAME,
                                  this.TRANSFORMATION_VERSION,
                                  site);
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
