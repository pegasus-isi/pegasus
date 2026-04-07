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
package edu.isi.pegasus.planner.partitioner.graph;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.*;

import java.lang.reflect.Method;
import org.junit.jupiter.api.Test;

/**
 * Structural tests for the Graph interface via reflection.
 *
 * @author Rajiv Mayani
 */
public class GraphTest {

    @Test
    public void testGraphHasAddNodeMethod() throws NoSuchMethodException {
        Method m = Graph.class.getMethod("addNode", GraphNode.class);
        assertThat(m, is(notNullValue()));
    }

    @Test
    public void testGraphHasGetNodeMethod() throws NoSuchMethodException {
        Method m = Graph.class.getMethod("getNode", String.class);
        assertThat(m, is(notNullValue()));
    }

    @Test
    public void testGraphHasAddEdgeMethod() throws NoSuchMethodException {
        Method m = Graph.class.getMethod("addEdge", String.class, String.class);
        assertThat(m, is(notNullValue()));
    }

    @Test
    public void testGraphHasSizeMethod() throws NoSuchMethodException {
        Method m = Graph.class.getMethod("size");
        assertThat(m, is(notNullValue()));
    }

    @Test
    public void testGraphHasNodeIteratorMethod() throws NoSuchMethodException {
        Method m = Graph.class.getMethod("nodeIterator");
        assertThat(m, is(notNullValue()));
    }

    @Test
    public void testMapGraphImplementsGraph() {
        assertThat(Graph.class.isAssignableFrom(MapGraph.class), is(true));
    }

    @Test
    public void testGraphHasAddRootMethod() throws NoSuchMethodException {
        Method m = Graph.class.getMethod("addRoot", GraphNode.class);
        assertThat(m, is(notNullValue()));
    }

    @Test
    public void testGraphIsInterface() {
        assertThat(Graph.class.isInterface(), is(true));
    }

    @Test
    public void testGraphExtendsGraphNodeContent() {
        assertThat(GraphNodeContent.class.isAssignableFrom(Graph.class), is(true));
    }

    @Test
    public void testVersionConstant() {
        assertThat(Graph.VERSION, is("1.6"));
    }

    @Test
    public void testSelectedMethodReturnTypes() throws NoSuchMethodException {
        assertThat(
                Graph.class.getMethod("getNode", String.class).getReturnType(),
                is(GraphNode.class));
        assertThat(Graph.class.getMethod("size").getReturnType(), is(int.class));
        assertThat(Graph.class.getMethod("isEmpty").getReturnType(), is(boolean.class));
        assertThat(Graph.class.getMethod("hasCycles").getReturnType(), is(boolean.class));
    }
}
