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
    public void testGraphIsInterface() {
        assertTrue(Graph.class.isInterface(), "Graph should be an interface");
    }

    @Test
    public void testGraphHasAddNodeMethod() throws NoSuchMethodException {
        Method m = Graph.class.getMethod("addNode", GraphNode.class);
        assertNotNull(m, "Graph should have an addNode(GraphNode) method");
    }

    @Test
    public void testGraphHasGetNodeMethod() throws NoSuchMethodException {
        Method m = Graph.class.getMethod("getNode", String.class);
        assertNotNull(m, "Graph should have a getNode(String) method");
    }

    @Test
    public void testGraphHasAddEdgeMethod() throws NoSuchMethodException {
        Method m = Graph.class.getMethod("addEdge", String.class, String.class);
        assertNotNull(m, "Graph should have an addEdge(String, String) method");
    }

    @Test
    public void testGraphHasSizeMethod() throws NoSuchMethodException {
        Method m = Graph.class.getMethod("size");
        assertNotNull(m, "Graph should have a size() method");
    }

    @Test
    public void testGraphHasNodeIteratorMethod() throws NoSuchMethodException {
        Method m = Graph.class.getMethod("nodeIterator");
        assertNotNull(m, "Graph should have a nodeIterator() method");
    }

    @Test
    public void testMapGraphImplementsGraph() {
        assertTrue(Graph.class.isAssignableFrom(MapGraph.class), "MapGraph should implement Graph");
    }

    @Test
    public void testGraphHasAddRootMethod() throws NoSuchMethodException {
        Method m = Graph.class.getMethod("addRoot", GraphNode.class);
        assertNotNull(m, "Graph should have an addRoot(GraphNode) method");
    }
}
