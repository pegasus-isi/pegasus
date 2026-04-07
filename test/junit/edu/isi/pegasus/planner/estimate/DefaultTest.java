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
package edu.isi.pegasus.planner.estimate;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.*;

import edu.isi.pegasus.planner.classes.ADag;
import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.classes.PegasusBag;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Tests for the Default estimator. */
public class DefaultTest {

    private Default mEstimator;

    @BeforeEach
    public void setUp() {
        mEstimator = new Default();
    }

    @Test
    public void testInstantiation() {
        assertThat(mEstimator, notNullValue());
    }

    @Test
    public void testImplementsEstimator() {
        assertThat(mEstimator, instanceOf(Estimator.class));
    }

    @Test
    public void testGetRuntimeReturnsNull() {
        Job job = new Job();
        job.setTXName("test");
        assertThat(mEstimator.getRuntime(job), nullValue());
    }

    @Test
    public void testGetMemoryReturnsNull() {
        Job job = new Job();
        job.setTXName("test");
        assertThat(mEstimator.getMemory(job), nullValue());
    }

    @Test
    public void testGetAllEstimatesReturnsEmptyMap() {
        Job job = new Job();
        job.setTXName("test");
        Map<String, String> estimates = mEstimator.getAllEstimates(job);
        assertThat(estimates, notNullValue());
        assertThat(estimates.isEmpty(), is(true));
    }

    @Test
    public void testInitializeDoesNotThrow() {
        assertDoesNotThrow(
                () -> mEstimator.initialize(null, null),
                "Default initialize should not throw when called with nulls");
    }

    @Test
    public void testGetAllEstimatesMapNotNull() {
        Job job = new Job();
        Map<String, String> result = mEstimator.getAllEstimates(job);
        assertThat(result, notNullValue());
    }

    @Test
    public void testGetRuntimeWithNullJob() {
        // Default returns null regardless of job
        assertThat(mEstimator.getRuntime(null), nullValue());
    }

    @Test
    public void testGetMemoryWithNullJob() {
        assertThat(mEstimator.getMemory(null), nullValue());
    }

    @Test
    public void testGetAllEstimatesWithNullJobReturnsEmptyMap() {
        Map<String, String> estimates = mEstimator.getAllEstimates(null);

        assertThat(estimates, notNullValue());
        assertThat(estimates.isEmpty(), is(true));
    }

    @Test
    public void testGetAllEstimatesReturnsFreshMapEachTime() {
        Map<String, String> first = mEstimator.getAllEstimates(new Job());
        first.put("runtime", "12");

        Map<String, String> second = mEstimator.getAllEstimates(new Job());

        assertThat(first, not(sameInstance(second)));
        assertThat(second.isEmpty(), is(true));
    }

    @Test
    public void testInitializeAcceptsRealDagAndBag() {
        assertDoesNotThrow(
                () -> mEstimator.initialize(new ADag(), new PegasusBag()),
                "Default initialize should accept real workflow and bag objects");
    }
}
