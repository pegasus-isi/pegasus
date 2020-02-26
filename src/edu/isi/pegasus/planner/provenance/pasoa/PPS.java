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
package edu.isi.pegasus.planner.provenance.pasoa;

import edu.isi.pegasus.planner.refiner.Refiner;
import java.util.List;

/**
 * Pegasus P-assertion Support interface
 *
 * <p>Classes that implement this interface assist in the creation of p-assertions for the Pegasus
 * workflow refinement system. This interface follows a <a
 * href="http://en.wikipedia.org/wiki/Builder_pattern">builder pattern</a>.
 *
 * <p>Using this interface proceeds as follows: 1. At the beginning of a refinement step the
 * beginWorkflowRefinmentStep method should be called. 2. As nodes are transformed the particular
 * refinement operation method (siteSelectionFor, isParticiationOf...) should be called. 3. When the
 * refinement step is complete the endWorkflowStep method should be called. 4. At this point
 * identicalTo relationships are automatically created between the resulting workflow and the input
 * workflow
 *
 * <p>A note on PHeaders: For the first refinement step, the p-header can be passed in as null. For
 * each, subsequent refinement step the p-header provided by the endWorkflowRefinementStep method
 * should be passed into the beginWorkflowRefinementMethod
 */
public interface PPS {
    /**
     * A namespace we can use to identify relationships and concepts defined for Pegasus' provenance
     * data
     */
    public static final String NAMESPACE = "http://www.isi.edu/pasoa";

    // Actors: Every refinement step and Pegaus itself is given an identifying URI
    public static final String PEGASUS = NAMESPACE + "/actors#pegasus";
    public static final String REFINEMENT_CLUSTER = NAMESPACE + "/actors#cluster";
    public static final String REFINEMENT_REDUCE = NAMESPACE + "/actors#reduce";
    public static final String REFINEMENT_REGISTER = NAMESPACE + "/actors#register";
    public static final String REFINEMENT_SITE_SELECT = NAMESPACE + "/actors#siteSelect";
    public static final String REFINEMENT_STAGE = NAMESPACE + "/actors#stage";

    /** @return The ID used for the whole refinement process of this workflow */
    public String beginWorkflowRefinementStep(
            Refiner refiner, String refinementStepName, boolean firstStep) throws Exception;

    public void isIdenticalTo(String afterNode, String beforeNode) throws Exception;

    public void siteSelectionFor(String afterNode, String beforeNode) throws Exception;

    public void stagingIntroducedFor(List stagingNodes, String appNode) throws Exception;

    public void registrationIntroducedFor(String registrationNode, String dataStagingNode)
            throws Exception;

    public void clusteringOf(String clusteredJob, List jobs) throws Exception;

    public void isPartitionOf(String afterNode, List beforeNode) throws Exception;

    public void endWorkflowRefinementStep(Refiner refiner) throws Exception;
}
