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
package edu.isi.pegasus.planner.selector.site.heft;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.common.PegasusProperties;
import java.lang.reflect.Method;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

/** Tests for the HEFT Algorithm class constants. */
public class AlgorithmTest {

    @Test
    public void testAverageBandwidth() {
        assertThat((double) Algorithm.AVERAGE_BANDWIDTH, closeTo(5.0, 0.001));
    }

    @Test
    public void testAverageDataSizeBetweenJobs() {
        assertThat((double) Algorithm.AVERAGE_DATA_SIZE_BETWEEN_JOBS, closeTo(2.0, 0.001));
    }

    @Test
    public void testDefaultNumberOfFreeNodes() {
        assertThat(Algorithm.DEFAULT_NUMBER_OF_FREE_NODES, equalTo(10));
    }

    @Test
    public void testMaximumFinishTime() {
        assertThat(Algorithm.MAXIMUM_FINISH_TIME, equalTo(Long.MAX_VALUE));
    }

    @Test
    public void testRuntimeProfileKeyNotNull() {
        assertThat(Algorithm.RUNTIME_PROFILE_KEY, notNullValue());
    }

    @Test
    public void testDescriptionExactValue() {
        PegasusBag bag = new PegasusBag();
        bag.add(PegasusBag.PEGASUS_PROPERTIES, PegasusProperties.nonSingletonInstance());

        assertThat(new Algorithm(bag).description(), equalTo("Heft based Site Selector"));
    }

    @Test
    public void testConstructorInitializesAverageCommunicationCost() throws Exception {
        PegasusBag bag = new PegasusBag();
        PegasusProperties props = PegasusProperties.nonSingletonInstance();
        bag.add(PegasusBag.PEGASUS_PROPERTIES, props);

        Algorithm algorithm = new Algorithm(bag);

        assertThat(
                ((Number) ReflectionTestUtils.getField(algorithm, "mAverageCommunicationCost"))
                        .doubleValue(),
                closeTo(2.5, 0.001));
    }

    @Test
    public void testSelectedMethodSignatures() throws Exception {
        assertThat(
                Algorithm.class
                        .getMethod(
                                "schedule",
                                edu.isi.pegasus.planner.classes.ADag.class,
                                java.util.List.class)
                        .getReturnType(),
                equalTo(Void.TYPE));
        assertThat(Algorithm.class.getMethod("getMakespan").getReturnType(), equalTo(long.class));
        assertThat(
                Algorithm.class
                        .getMethod(
                                "mapJob2ExecPool",
                                edu.isi.pegasus.planner.classes.Job.class,
                                java.util.List.class)
                        .getReturnType(),
                equalTo(String.class));
    }

    @Test
    public void testPrivateGetFloatValueHelperExists() throws Exception {
        Method method = Algorithm.class.getDeclaredMethod("getFloatValue", Object.class);
        assertThat(method.getReturnType(), equalTo(float.class));
    }
}
