/**
 * Copyright 2007-2008 University Of Southern California
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.isi.pegasus.planner.cluster;

import edu.isi.pegasus.planner.classes.ADag;
import edu.isi.pegasus.planner.classes.AggregatedJob;
import edu.isi.pegasus.planner.classes.PegasusBag;
import java.util.List;

/**
 * The interface that dictates how the jobs are clumped together into one single larger job. The
 * interface does not dictate how the graph structure is to be modified as a result of the clumping.
 * That is handled outside of the implementing class in NodeCollapser.
 *
 * @author Karan Vahi vahi@isi.edu
 * @version $Revision$
 */
public interface JobAggregator {

    /** The version number associated with this API of Job Aggregator. */
    public static final String VERSION = "1.5";

    /**
     * Initializes the JobAggregator impelementation
     *
     * @param dag the workflow that is being clustered.
     * @param bag the bag of objects that is useful for initialization.
     */
    public void initialize(ADag dag, PegasusBag bag);

    /**
     * Constructs a new aggregated job that contains all the jobs passed to it. The new aggregated
     * job, appears as a single job in the workflow and replaces the jobs it contains in the
     * workflow.
     *
     * @param jobs the list of <code>SubInfo</code> objects that need to be collapsed. All the jobs
     *     being collapsed should be scheduled at the same pool, to maintain correct semantics.
     * @param name the logical name of the jobs in the list passed to this function.
     * @param id the id that is given to the new job.
     * @return the <code>SubInfo</code> object corresponding to the aggregated job containing the
     *     jobs passed as List in the input, null if the list of jobs is empty
     */
    //    public AggregatedJob construct(List jobs,String name,String id);

    /**
     * Constructs an abstract aggregated job that has a handle to the appropriate JobAggregator that
     * will be used to aggregate the jobs.
     *
     * @param jobs the list of <code>SubInfo</code> objects that need to be collapsed. All the jobs
     *     being collapsed should be scheduled at the same pool, to maintain correct semantics.
     * @param name the logical name of the jobs in the list passed to this function.
     * @param id the id that is given to the new job.
     * @return the <code>SubInfo</code> object corresponding to the aggregated job containing the
     *     jobs passed as List in the input, null if the list of jobs is empty
     */
    public AggregatedJob constructAbstractAggregatedJob(List jobs, String name, String id);

    /**
     * Enables the abstract clustered job for execution and converts it to it's executable form
     *
     * @param job the abstract clustered job
     */
    public void makeAbstractAggregatedJobConcrete(AggregatedJob job);

    /**
     * A boolean indicating whether ordering is important while traversing through the aggregated
     * job.
     *
     * @return a boolean
     */
    public boolean topologicalOrderingRequired();

    /**
     * Setter method to indicate , failure on first consitutent job should result in the abort of
     * the whole aggregated job.
     *
     * @param fail indicates whether to abort or not .
     */
    public void setAbortOnFirstJobFailure(boolean fail);

    /**
     * Returns a boolean indicating whether to fail the aggregated job on detecting the first
     * failure during execution of constituent jobs.
     *
     * @return boolean indicating whether to fail or not.
     */
    public boolean abortOnFristJobFailure();

    /**
     * Determines whether there is NOT an entry in the transformation catalog for the job aggregator
     * executable on a particular site.
     *
     * @param site the site at which existence check is required.
     * @return boolean true if an entry does not exists, false otherwise.
     */
    public boolean entryNotInTC(String site);

    /**
     * Returns the logical name of the transformation that is used to collapse the jobs.
     *
     * @return the the logical name of the collapser executable.
     */
    public String getClusterExecutableLFN();

    /**
     * Returns the executable basename of the clustering executable used.
     *
     * @return the executable basename.
     */
    public String getClusterExecutableBasename();
}
