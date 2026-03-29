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

import static org.junit.jupiter.api.Assertions.*;

import edu.isi.pegasus.planner.classes.Job;
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
        assertNotNull(mEstimator, "Default estimator should be instantiatable");
    }

    @Test
    public void testImplementsEstimator() {
        assertInstanceOf(Estimator.class, mEstimator, "Default should implement Estimator");
    }

    @Test
    public void testGetRuntimeReturnsNull() {
        Job job = new Job();
        job.setTXName("test");
        assertNull(mEstimator.getRuntime(job), "Default getRuntime should return null");
    }

    @Test
    public void testGetMemoryReturnsNull() {
        Job job = new Job();
        job.setTXName("test");
        assertNull(mEstimator.getMemory(job), "Default getMemory should return null");
    }

    @Test
    public void testGetAllEstimatesReturnsEmptyMap() {
        Job job = new Job();
        job.setTXName("test");
        Map<String, String> estimates = mEstimator.getAllEstimates(job);
        assertNotNull(estimates, "getAllEstimates should not return null");
        assertTrue(estimates.isEmpty(), "Default getAllEstimates should return empty map");
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
        assertNotNull(result, "getAllEstimates result should not be null");
    }

    @Test
    public void testGetRuntimeWithNullJob() {
        // Default returns null regardless of job
        assertNull(mEstimator.getRuntime(null), "getRuntime with null job should return null");
    }
}
