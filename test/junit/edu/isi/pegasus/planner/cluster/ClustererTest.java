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
package edu.isi.pegasus.planner.cluster;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.*;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Tests for the Clusterer interface: verifies constants and that known implementations conform to
 * the interface.
 */
public class ClustererTest {

    @Test
    public void testVersionConstantNotNull() {
        assertThat(Clusterer.VERSION, notNullValue());
    }

    @Test
    public void testVersionConstantNotEmpty() {
        assertThat(Clusterer.VERSION.isEmpty(), is(false));
    }

    @Test
    public void testHorizontalImplementsClusterer() {
        Horizontal h = new Horizontal();
        assertThat(h, instanceOf(Clusterer.class));
    }

    @Test
    public void testVerticalImplementsClusterer() {
        Vertical v = new Vertical();
        assertThat(v, instanceOf(Clusterer.class));
    }

    @Test
    public void testHorizontalCanBeInstantiated() {
        assertDoesNotThrow(Horizontal::new);
    }

    @Test
    public void testVerticalCanBeInstantiated() {
        assertDoesNotThrow(Vertical::new);
    }

    @Test
    public void testHorizontalDescriptionNotNull() {
        Horizontal h = new Horizontal();
        assertThat(h.description(), notNullValue());
    }

    @Test
    public void testVerticalDescriptionNotNull() {
        Vertical v = new Vertical();
        assertThat(v.description(), notNullValue());
    }

    @Test
    public void testClustererIsInterface() {
        assertThat(Clusterer.class.isInterface(), is(true));
    }

    @Test
    public void testVersionConstantMatchesExpectedValue() {
        assertThat(Clusterer.VERSION, is("1.1"));
    }

    @Test
    public void testClustererDeclaresExpectedMethodNames() {
        List<String> methodNames =
                Arrays.asList(
                        "initialize",
                        "determineClusters",
                        "parents",
                        "getClusteredDAG",
                        "description");

        for (String methodName : methodNames) {
            assertThat(
                    Arrays.stream(Clusterer.class.getDeclaredMethods())
                            .map(Method::getName)
                            .anyMatch(methodName::equals),
                    is(true));
        }
    }

    @Test
    public void testClustererMutatingMethodsDeclareClustererException() throws Exception {
        assertThrowsClustererException(
                Clusterer.class.getMethod(
                        "initialize",
                        edu.isi.pegasus.planner.classes.ADag.class,
                        edu.isi.pegasus.planner.classes.PegasusBag.class));
        assertThrowsClustererException(
                Clusterer.class.getMethod(
                        "determineClusters", edu.isi.pegasus.planner.partitioner.Partition.class));
        assertThrowsClustererException(
                Clusterer.class.getMethod("parents", String.class, List.class));
        assertThrowsClustererException(Clusterer.class.getMethod("getClusteredDAG"));
    }

    private void assertThrowsClustererException(Method method) {
        assertThat(
                Arrays.asList(method.getExceptionTypes()).contains(ClustererException.class),
                is(true));
    }
}
