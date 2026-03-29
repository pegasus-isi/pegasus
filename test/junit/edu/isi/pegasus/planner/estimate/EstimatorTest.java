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
import org.junit.jupiter.api.Test;

/** Tests for the Estimator interface. Tests are exercised via the Default implementation. */
public class EstimatorTest {

    @Test
    public void testDefaultImplementsEstimator() {
        Default estimator = new Default();
        assertInstanceOf(Estimator.class, estimator, "Default should implement Estimator");
    }

    @Test
    public void testGetRuntimeInterfaceContract() {
        Estimator estimator = new Default();
        Job job = new Job();
        // Returning null is an acceptable contract for Default implementation
        String runtime = estimator.getRuntime(job);
        assertNull(runtime, "Default implementation should return null runtime");
    }

    @Test
    public void testGetMemoryInterfaceContract() {
        Estimator estimator = new Default();
        Job job = new Job();
        String memory = estimator.getMemory(job);
        assertNull(memory, "Default implementation should return null memory");
    }

    @Test
    public void testGetAllEstimatesInterfaceContract() {
        Estimator estimator = new Default();
        Job job = new Job();
        Map<String, String> estimates = estimator.getAllEstimates(job);
        assertNotNull(estimates, "getAllEstimates should not return null");
    }

    @Test
    public void testGetAllEstimatesIsMap() {
        Estimator estimator = new Default();
        Job job = new Job();
        Object result = estimator.getAllEstimates(job);
        assertInstanceOf(Map.class, result, "getAllEstimates should return a Map");
    }

    @Test
    public void testInitializeDoesNotThrowForDefault() {
        Estimator estimator = new Default();
        assertDoesNotThrow(
                () -> estimator.initialize(null, null), "Default.initialize should not throw");
    }

    @Test
    public void testMultipleCallsToGetRuntimeAreConsistent() {
        Estimator estimator = new Default();
        Job job = new Job();
        String r1 = estimator.getRuntime(job);
        String r2 = estimator.getRuntime(job);
        assertEquals(r1, r2, "Multiple calls to getRuntime should return consistent results");
    }
}
