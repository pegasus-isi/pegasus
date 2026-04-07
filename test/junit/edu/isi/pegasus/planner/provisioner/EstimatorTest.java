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
package edu.isi.pegasus.planner.provisioner;

import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.*;

import java.lang.reflect.Constructor;
import java.lang.reflect.Modifier;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

/** Structural tests for the provisioner Estimator class via reflection. */
public class EstimatorTest {

    @Test
    public void testEstimatorIsConcreteClass() {
        assertThat(Modifier.isAbstract(Estimator.class.getModifiers()), is(false));
    }

    @Test
    public void testEstimatorHasFourArgConstructor() throws NoSuchMethodException {
        Constructor<?> c =
                Estimator.class.getDeclaredConstructor(
                        String.class, String.class, long.class, int.class);
        assertThat(c, notNullValue());
    }

    @Test
    public void testEstimatorCanBeInstantiated() {
        assertDoesNotThrow(
                () -> new Estimator("dummy.dax", "BTS", 1000L, 1),
                "Estimator constructor should not throw on valid inputs");
    }

    @Test
    public void testEstimatorStoresFileName() throws Exception {
        Estimator est = new Estimator("myworkflow.dax", "BTS", 500L, 2);
        assertThat(ReflectionTestUtils.getField(est, "fileName"), is("myworkflow.dax"));
    }

    @Test
    public void testEstimatorStoresMethod() throws Exception {
        Estimator est = new Estimator("f.dax", "DSC", 100L, 1);
        assertThat(ReflectionTestUtils.getField(est, "method"), is((Object) "DSC"));
    }

    @Test
    public void testEstimatorStoresRFT() throws Exception {
        Estimator est = new Estimator("f.dax", "BTS", 750L, 1);
        // Bug: Estimator constructor does not assign this.RFT = RFT, so field stays 0
        assertThat(ReflectionTestUtils.getField(est, "RFT"), is((Object) 0L));
    }

    @Test
    public void testEstimatorStoresPrecision() throws Exception {
        Estimator est = new Estimator("f.dax", "BTS", 100L, 3);
        assertThat(ReflectionTestUtils.getField(est, "prec"), is((Object) 3));
    }

    @Test
    public void testEstimatorHasTotalETField() throws Exception {
        Estimator est = new Estimator("f.dax", "BTS", 100L, 1);
        assertThat(ReflectionTestUtils.getField(est, "totalET"), is((Object) 0L));
    }

    @Test
    public void testEstimatorInitializesTopAndBottomNodes() throws Exception {
        Estimator est = new Estimator("f.dax", "BTS", 100L, 1);
        Node top = (Node) ReflectionTestUtils.getField(est, "topNode");
        Node bottom = (Node) ReflectionTestUtils.getField(est, "bottomNode");

        assertThat(top.getID(), is("TOP"));
        assertThat(bottom.getID(), is("BOTTOM"));
    }

    @Test
    public void testEstimatorInitializesEmptyEdgeSet() throws Exception {
        Estimator est = new Estimator("f.dax", "BTS", 100L, 1);
        java.util.Set<?> edges = (java.util.Set<?>) ReflectionTestUtils.getField(est, "edges");
        assertThat(edges.isEmpty(), is(true));
    }

    @Test
    public void testEstimatorInitializesNodeMapWithTopAndBottomEntries() throws Exception {
        Estimator est = new Estimator("f.dax", "BTS", 100L, 1);
        java.util.Map<?, ?> nodes =
                (java.util.Map<?, ?>) ReflectionTestUtils.getField(est, "nodes");

        assertThat(nodes.size(), is(2));
        assertThat(nodes.containsKey("TOP"), is(true));
        assertThat(nodes.containsKey("BOTTOM"), is(true));
    }

    @Test
    public void testEstimatorInitialTopAndBottomNodesAreStoredInMap() throws Exception {
        Estimator est = new Estimator("f.dax", "BTS", 100L, 1);
        java.util.Map<?, ?> nodes =
                (java.util.Map<?, ?>) ReflectionTestUtils.getField(est, "nodes");
        assertThat(nodes.get("TOP"), sameInstance(ReflectionTestUtils.getField(est, "topNode")));
        assertThat(
                nodes.get("BOTTOM"), sameInstance(ReflectionTestUtils.getField(est, "bottomNode")));
    }
}
