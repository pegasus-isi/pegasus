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
package edu.isi.pegasus.planner.selector.site;

import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.planner.classes.ADag;
import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.partitioner.graph.GraphNode;
import edu.isi.pegasus.planner.selector.site.heft.Algorithm;
import edu.isi.pegasus.planner.selector.site.heft.HeftBag;
import java.util.Iterator;
import java.util.List;

/**
 * The HEFT based site selector. The runtime for the job in seconds is picked from the pegasus
 * profile key runtime in the transformation catalog for a transformation.
 *
 * <p>The data communication costs between jobs if scheduled on different sites is assumed to be
 * fixed. Later on if required, the ability to specify this value will be exposed via properties.
 *
 * <p>The number of processors in a site is picked by the attribute idle-nodes associated with the
 * vanilla jobmanager for a site in the site catalog.
 *
 * @author Karan Vahi
 * @version $Revision$
 * @see Algorithm#AVERAGE_BANDWIDTH
 * @see Algorithm#RUNTIME_PROFILE_KEY
 * @see Algorithm#DEFAULT_NUMBER_OF_FREE_NODES
 * @see Algorithm#AVERAGE_DATA_SIZE_BETWEEN_JOBS
 * @see org.griphyn.cPlanner.classes.JobManager#IDLE_NODES
 */
public class Heft extends Abstract {

    /** An instance of the class that implements the HEFT algorithm. */
    private Algorithm mHeftImpl;

    /** The default constructor. */
    public Heft() {
        super();
    }

    /**
     * Initializes the site selector.
     *
     * @param bag the bag of objects that is useful for initialization.
     */
    public void initialize(PegasusBag bag) {
        super.initialize(bag);
        mHeftImpl = new Algorithm(bag);
    }

    /**
     * Maps the jobs in the workflow to the various grid sites. The jobs are mapped by setting the
     * site handle for the jobs.
     *
     * @param workflow the workflow.
     * @param sites the list of <code>String</code> objects representing the execution sites that
     *     can be used.
     */
    public void mapWorkflow(ADag workflow, List sites) {
        // PM-747 no need for conversion as ADag now implements Graph interface
        this.mapWorkflow(workflow, sites, workflow.getLabel());
    }

    /**
     * Maps the jobs in the workflow to the various grid sites. The jobs are mapped by setting the
     * site handle for the jobs.
     *
     * @param workflow the workflow in a Graph form.
     * @param sites the list of <code>String</code> objects representing the execution sites that
     *     can be used.
     * @param label the label of the workflow
     */
    public void mapWorkflow(ADag workflow, List sites, String label) {

        // schedule the workflow, till i fix the interface
        mHeftImpl.schedule(workflow, sites, label);

        // get the makespan of the workflow
        mLogger.log(
                "Makespan of scheduled workflow is " + mHeftImpl.getMakespan(),
                LogManager.DEBUG_MESSAGE_LEVEL);

        // iterate through the jobs and just set the site handle
        // accordingly
        for (Iterator it = workflow.nodeIterator(); it.hasNext(); ) {
            GraphNode node = (GraphNode) it.next();
            Job job = (Job) node.getContent();
            job.setSiteHandle((String) node.getBag().get(HeftBag.SCHEDULED_SITE));
        }
    }

    /**
     * This method returns a String describing the site selection technique that is being
     * implemented by the implementing class.
     *
     * @return String
     */
    public String description() {
        return "Heft based Site Selector";
    }
}
