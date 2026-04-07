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
import java.lang.reflect.Method;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** Tests for the Estimator interface. Tests are exercised via the Default implementation. */
public class EstimatorTest {

    @Test
    public void testDefaultImplementsEstimator() {
        Default estimator = new Default();
        assertThat(estimator, instanceOf(Estimator.class));
    }

    @Test
    public void testGetRuntimeInterfaceContract() {
        Estimator estimator = new Default();
        Job job = new Job();
        // Returning null is an acceptable contract for Default implementation
        String runtime = estimator.getRuntime(job);
        assertThat(runtime, nullValue());
    }

    @Test
    public void testGetMemoryInterfaceContract() {
        Estimator estimator = new Default();
        Job job = new Job();
        String memory = estimator.getMemory(job);
        assertThat(memory, nullValue());
    }

    @Test
    public void testGetAllEstimatesInterfaceContract() {
        Estimator estimator = new Default();
        Job job = new Job();
        Map<String, String> estimates = estimator.getAllEstimates(job);
        assertThat(estimates, notNullValue());
    }

    @Test
    public void testGetAllEstimatesIsMap() {
        Estimator estimator = new Default();
        Job job = new Job();
        Object result = estimator.getAllEstimates(job);
        assertThat(result, instanceOf(Map.class));
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
        assertThat(r2, is(r1));
    }

    @Test
    public void testEstimatorIsInterface() {
        assertThat(Estimator.class.isInterface(), is(true));
    }

    @Test
    public void testInitializeMethodSignature() throws Exception {
        Method method = Estimator.class.getMethod("initialize", ADag.class, PegasusBag.class);

        assertThat(method.getReturnType(), is(void.class));
    }

    @Test
    public void testGetAllEstimatesMethodSignature() throws Exception {
        Method method = Estimator.class.getMethod("getAllEstimates", Job.class);

        assertThat(method.getReturnType(), is(Map.class));
    }

    @Test
    public void testGetRuntimeAndGetMemoryMethodSignatures() throws Exception {
        Method runtime = Estimator.class.getMethod("getRuntime", Job.class);
        Method memory = Estimator.class.getMethod("getMemory", Job.class);

        assertThat(runtime.getReturnType(), is(String.class));
        assertThat(memory.getReturnType(), is(String.class));
    }
}
