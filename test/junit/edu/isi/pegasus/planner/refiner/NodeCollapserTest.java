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
package edu.isi.pegasus.planner.refiner;

import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.*;

import edu.isi.pegasus.planner.classes.ADag;
import edu.isi.pegasus.planner.classes.PegasusBag;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import org.junit.jupiter.api.Test;

/** Structural tests for NodeCollapser. */
public class NodeCollapserTest {

    @Test
    public void testExtendsEngine() {
        assertThat(Engine.class.isAssignableFrom(NodeCollapser.class), is(true));
    }

    @Test
    public void testHasClusterMethod() throws Exception {
        // The method is named cluster(), not collapse()
        assertThat(
                NodeCollapser.class.getMethod(
                        "cluster", edu.isi.pegasus.planner.classes.ADag.class),
                notNullValue());
    }

    @Test
    public void testHasPegasusBagConstructor() throws Exception {
        Constructor<NodeCollapser> constructor =
                NodeCollapser.class.getDeclaredConstructor(PegasusBag.class);
        assertThat(constructor, notNullValue());
    }

    @Test
    public void testOverloadedClusterMethodSignature() throws Exception {
        Method method = NodeCollapser.class.getMethod("cluster", ADag.class, String.class);
        assertThat((Object) method.getReturnType(), is((Object) ADag.class));
    }

    @Test
    public void testSetDirectoryMethodSignature() throws Exception {
        Method method = NodeCollapser.class.getMethod("setDirectory", String.class);
        assertThat((Object) method.getReturnType(), is((Object) void.class));
    }

    @Test
    public void testEdgeListToGraphMethodExists() throws Exception {
        assertThat(
                NodeCollapser.class.getDeclaredMethod(
                        "edgeList2Graph", ADag.class, java.util.Map.class),
                notNullValue());
    }
}
