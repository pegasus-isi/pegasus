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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Unit tests for the MapGraph class. */
public class MapGraphTest {

    private MapGraph mGraph;

    @BeforeEach
    public void setUp() {
        mGraph = new MapGraph();
    }

    @Test
    public void testNewGraphIsEmpty() {
        assertFalse(mGraph.nodeIterator().hasNext(), "A new MapGraph should have no nodes");
    }

    @Test
    public void testAddNodeIncreasesSize() {
        mGraph.addNode(new GraphNode("A", "jobA"));
        assertTrue(mGraph.nodeIterator().hasNext(), "Graph should have nodes after addNode");
    }

    @Test
    public void testGetNodeReturnsAddedNode() {
        GraphNode node = new GraphNode("A", "jobA");
        mGraph.addNode(node);
        GraphNode retrieved = mGraph.getNode("A");
        assertNotNull(retrieved, "getNode should return the added node");
        assertEquals("A", retrieved.getID(), "Retrieved node should have the same ID");
    }

    @Test
    public void testGetNodeReturnsNullForMissingNode() {
        assertNull(
                mGraph.getNode("nonexistent"),
                "getNode should return null for a node that does not exist");
    }

    @Test
    public void testAddEdgeConnectsNodes() {
        mGraph.addNode(new GraphNode("A", "jobA"));
        mGraph.addNode(new GraphNode("B", "jobB"));
        mGraph.addEdge("A", "B");
        GraphNode nodeA = mGraph.getNode("A");
        assertFalse(nodeA.getChildren().isEmpty(), "Node A should have children after addEdge");
    }

    @Test
    public void testImplementsGraphInterface() {
        assertTrue(mGraph instanceof Graph, "MapGraph should implement the Graph interface");
    }

    @Test
    public void testAddSelfEdgeThrowsException() {
        mGraph.addNode(new GraphNode("A", "jobA"));
        assertThrows(
                IllegalArgumentException.class,
                () -> mGraph.addEdge("A", "A"),
                "Adding a self-edge should throw an exception");
    }

    @Test
    public void testSizeReturnsCorrectCount() {
        mGraph.addNode(new GraphNode("A", "jobA"));
        mGraph.addNode(new GraphNode("B", "jobB"));
        assertEquals(2, mGraph.size(), "size() should return 2 after adding 2 nodes");
    }
}
