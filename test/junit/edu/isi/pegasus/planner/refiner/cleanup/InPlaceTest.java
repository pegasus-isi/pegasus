/**
 * Copyright 2007-2013 University Of Southern California
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

import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

/** Structural tests for InPlace cleanup strategy. */
public class InPlaceTest {

    @Test
    public void testExtendsAbstractCleanupStrategy() {
        assertThat(AbstractCleanupStrategy.class.isAssignableFrom(InPlace.class), is(true));
    }

    @Test
    public void testImplementsCleanupStrategy() {
        assertThat(CleanupStrategy.class.isAssignableFrom(InPlace.class), is(true));
    }

    @Test
    public void testNumJobsPerLevelConstant() {
        assertThat(InPlace.NUM_JOBS_PER_LEVEL_PER_CLEANUP_JOB, is(5.0f));
    }

    @Test
    public void testDefaultConstructor() {
        InPlace ip = new InPlace();
        assertThat(ip, notNullValue());
    }

    @Test
    public void testAddCleanupJobsReturnsGraph() throws Exception {
        assertThat(
                (Object)
                        InPlace.class
                                .getMethod(
                                        "addCleanupJobs",
                                        edu.isi.pegasus.planner.partitioner.graph.Graph.class)
                                .getReturnType(),
                is((Object) edu.isi.pegasus.planner.partitioner.graph.Graph.class));
    }

    @Test
    public void testProtectedHelperMethodsExist() throws Exception {
        assertThat(
                InPlace.class.getDeclaredMethod(
                        "reduceDependency",
                        edu.isi.pegasus.planner.partitioner.graph.GraphNode.class),
                notNullValue());
        assertThat(
                InPlace.class.getDeclaredMethod(
                        "applyJobPriorities",
                        edu.isi.pegasus.planner.partitioner.graph.Graph.class),
                notNullValue());
        assertThat(
                InPlace.class.getDeclaredMethod(
                        "generateCleanupID", edu.isi.pegasus.planner.classes.Job.class),
                notNullValue());
    }

    @Test
    public void testTypeNeedsCleanupOverloadsExist() throws Exception {
        assertThat(
                InPlace.class.getDeclaredMethod(
                        "typeNeedsCleanUp",
                        edu.isi.pegasus.planner.partitioner.graph.GraphNode.class),
                notNullValue());
        assertThat(
                InPlace.class.getDeclaredMethod("typeNeedsCleanUp", Integer.TYPE), notNullValue());
    }

    @Test
    public void testPrivateClusteringHelpersExist() throws Exception {
        assertThat(
                InPlace.class.getDeclaredMethod(
                        "clusterCleanupGraphNodes",
                        java.util.List.class,
                        java.util.HashMap.class,
                        String.class,
                        Integer.TYPE),
                notNullValue());
        assertThat(
                InPlace.class.getDeclaredMethod(
                        "createClusteredCleanupGraphNode",
                        java.util.List.class,
                        java.util.HashMap.class,
                        String.class,
                        Integer.TYPE,
                        Integer.TYPE),
                notNullValue());
        assertThat(
                InPlace.class.getDeclaredMethod("getClusterSize", Integer.TYPE, Integer.TYPE),
                notNullValue());
    }
}
