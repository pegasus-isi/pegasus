/**
 *  Copyright 2007-2015 University Of Southern California
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
package edu.isi.pegasus.planner.refiner.cleanup;

import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.planner.partitioner.graph.Graph;
import edu.isi.pegasus.planner.refiner.cleanup.constraint.Constrainer;
import java.io.FileNotFoundException;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 *
 * @author Sudarshan Srinivasan
 * @author Rafael Ferreira da Silva
 */
public class Constraint extends AbstractCleanupStrategy {

    /**
     * Adds cleanup jobs to the workflow.
     *
     * @param workflow the workflow to add cleanup jobs to.
     *
     * @return the workflow with cleanup jobs added to it.
     */
    @Override
    public Graph addCleanupJobs(Graph workflow) {
        // invoke addCleanupJobs from super class.
        workflow = super.addCleanupJobs(workflow);

        //Initialise constrainer
        try {
            Constrainer.initialize(workflow, mImpl, mDoNotClean, mProps);
            //for each site do the process of adding cleanup jobs
            for (Iterator it = mResMap.entrySet().iterator(); it.hasNext();) {
                Map.Entry entry = (Map.Entry) it.next();
                addCleanUpJobs((String) entry.getKey(), (Set) entry.getValue(), workflow);
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } finally {
            Constrainer.terminate();
        }

        return workflow;
    }

    /**
     * Adds cleanup jobs for the workflow scheduled to a particular site a
     * breadth first search strategy is implemented based on the depth of the
     * job in the workflow
     *
     * @param site the site ID
     * @param leaves the leaf jobs that are scheduled to site
     * @param workflow the Graph into which new cleanup jobs can be added
     */
    private void addCleanUpJobs(String site, Set leaves, Graph workflow) {

        mLogger.log(site + " " + leaves.size(), LogManager.DEBUG_MESSAGE_LEVEL);
        Constrainer.constrainify(site, leaves);
    }
}
