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
package edu.isi.pegasus.planner.refiner.cleanup;

import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.partitioner.graph.Graph;

/**
 * The interface that defines how the cleanup job is invoked and created.
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public interface CleanupStrategy {

    /** The version number associated with this API Cleanup CleanupStrategy. */
    public static final String VERSION = "1.1";

    /**
     * Intializes the class.
     *
     * @param bag bag of initialization objects
     * @param impl the implementation instance that creates file cleanup job
     */
    public void initialize(PegasusBag bag, CleanupImplementation impl);

    /**
     * Adds cleanup jobs to the workflow.
     *
     * @param workflow the workflow to add cleanup jobs to.
     * @return the workflow with cleanup jobs added to it.
     */
    public Graph addCleanupJobs(Graph workflow);
}
