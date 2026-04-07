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

import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.partitioner.graph.Graph;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import org.junit.jupiter.api.Test;

/** Structural tests for AbstractCleanupStrategy. */
public class AbstractCleanupStrategyTest {

    @Test
    public void testImplementsCleanupStrategy() {
        assertThat(CleanupStrategy.class.isAssignableFrom(AbstractCleanupStrategy.class), is(true));
    }

    @Test
    public void testCleanupJobPrefixConstant() {
        assertThat(AbstractCleanupStrategy.CLEANUP_JOB_PREFIX, is("clean_up_"));
    }

    @Test
    public void testDefaultMaxJobsConstant() {
        assertThat(AbstractCleanupStrategy.DEFAULT_MAX_JOBS_FOR_CLEANUP_CATEGORY, is("4"));
    }

    @Test
    public void testInPlaceExtendsAbstractCleanupStrategy() {
        assertThat(AbstractCleanupStrategy.class.isAssignableFrom(InPlace.class), is(true));
    }

    @Test
    public void testAbstractCleanupStrategyIsAbstract() {
        assertThat(Modifier.isAbstract(AbstractCleanupStrategy.class.getModifiers()), is(true));
    }

    @Test
    public void testAdditionalConstants() {
        assertThat(AbstractCleanupStrategy.NO_PROFILE_VALUE, is(-1));
        assertThat(AbstractCleanupStrategy.CLEANUP_SOURCE_SITE_KEY, is("cleanup_source_site"));
        assertThat(AbstractCleanupStrategy.DUMMY_LOCAL_CONTAINER_SITE, is("localC"));
    }

    @Test
    public void testInitializeAndAddCleanupJobsSignatures() throws Exception {
        Method initialize =
                AbstractCleanupStrategy.class.getMethod(
                        "initialize", PegasusBag.class, CleanupImplementation.class);
        Method addCleanupJobs =
                AbstractCleanupStrategy.class.getMethod("addCleanupJobs", Graph.class);
        assertThat((Object) initialize.getReturnType(), is((Object) void.class));
        assertThat((Object) addCleanupJobs.getReturnType(), is((Object) Graph.class));
    }

    @Test
    public void testProtectedHelperMethodsExist() throws Exception {
        assertThat(AbstractCleanupStrategy.class.getDeclaredMethod("reset"), notNullValue());
        assertThat(
                AbstractCleanupStrategy.class.getDeclaredMethod("typeStageOut", int.class),
                notNullValue());
    }
}
