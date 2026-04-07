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
import org.springframework.test.util.ReflectionTestUtils;

/** Structural tests for Constraint cleanup strategy. */
public class ConstraintTest {

    @Test
    public void testExtendsAbstractCleanupStrategy() {
        assertThat(AbstractCleanupStrategy.class.isAssignableFrom(Constraint.class), is(true));
    }

    @Test
    public void testImplementsCleanupStrategy() {
        assertThat(CleanupStrategy.class.isAssignableFrom(Constraint.class), is(true));
    }

    @Test
    public void testHasAddCleanupJobsMethod() throws Exception {
        assertThat(
                Constraint.class.getMethod(
                        "addCleanupJobs", edu.isi.pegasus.planner.partitioner.graph.Graph.class),
                notNullValue());
    }

    @Test
    public void testDefaultConstructor() {
        Constraint c = new Constraint();
        assertThat(c, notNullValue());
    }

    @Test
    public void testPrivateConstants() throws Exception {
        assertThat(
                ReflectionTestUtils.getField(Constraint.class, "PROPERTY_PREFIX"),
                is("pegasus.file.cleanup.constraint"));
        assertThat(
                ReflectionTestUtils.getField(Constraint.class, "PROPERTY_MAXSPACE_SUFFIX"),
                is("maxspace"));
        assertThat(
                ReflectionTestUtils.getField(Constraint.class, "DEFAULT_MAX_SPACE"),
                is("10737418240"));
    }

    @Test
    public void testHasExpectedPrivateHelperMethods() throws Exception {
        assertThat(
                Constraint.class.getDeclaredMethod(
                        "addCleanUpJobs",
                        String.class,
                        java.util.Set.class,
                        edu.isi.pegasus.planner.partitioner.graph.Graph.class),
                notNullValue());
        assertThat(Constraint.class.getDeclaredMethod("choose", Iterable.class), notNullValue());
        assertThat(
                Constraint.class.getDeclaredMethod("getPropertyName", String.class, String.class),
                notNullValue());
    }

    @Test
    public void testAddCleanupJobsReturnsGraph() throws Exception {
        assertThat(
                (Object)
                        Constraint.class
                                .getMethod(
                                        "addCleanupJobs",
                                        edu.isi.pegasus.planner.partitioner.graph.Graph.class)
                                .getReturnType(),
                is((Object) edu.isi.pegasus.planner.partitioner.graph.Graph.class));
    }
}
