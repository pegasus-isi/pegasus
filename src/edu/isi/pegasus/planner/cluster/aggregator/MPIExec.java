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


import edu.isi.pegasus.planner.classes.ADag;
import edu.isi.pegasus.planner.classes.AggregatedJob;

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
     * jobs via mpi.
     */
    public static final String COLLAPSE_LOGICAL_NAME = "mpiexec";

    /**
     * The basename of the executable that is able to run multiple
     * jobs via mpi.
     */
    public static String EXECUTABLE_BASENAME = "pegasus-mpi-cluster";


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
     * Returns the logical name of the transformation that is used to
     * collapse the jobs.
     *
     * @return the the logical name of the collapser executable.
     * @see #COLLAPSE_LOGICAL_NAME
     */
    public String getClusterExecutableLFN(){
        return COLLAPSE_LOGICAL_NAME;
    }

    /**
     * Returns the executable basename of the clustering executable used.
     *
     * @return the executable basename.
     * @see #EXECUTABLE_BASENAME
     */
    public String getClusterExecutableBasename(){
        return MPIExec.EXECUTABLE_BASENAME;
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
        return this.entryNotInTC( MPIExec.TRANSFORMATION_NAMESPACE,
                                  MPIExec.COLLAPSE_LOGICAL_NAME,
                                  MPIExec.TRANSFORMATION_VERSION,
                                  this.getClusterExecutableBasename(),
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
