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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.junit.jupiter.api.Test;

/** Unit tests for the TopologicalSortIterator class. */
public class TopologicalSortIteratorTest {

    @Test
    public void testEmptyGraphHasNoElements() {
        Graph g = new MapGraph();
        TopologicalSortIterator it = new TopologicalSortIterator(g);
        assertFalse(it.hasNext(), "Empty graph should produce an iterator with no elements");
    }

    @Test
    public void testSingleNodeGraphReturnsOneElement() {
        Graph g = new MapGraph();
        g.addNode(new GraphNode("A", "jobA"));
        TopologicalSortIterator it = new TopologicalSortIterator(g);
        assertTrue(it.hasNext(), "Single-node graph should have one element");
        GraphNode node = (GraphNode) it.next();
        assertEquals("A", node.getID(), "Single node should have ID 'A'");
        assertFalse(it.hasNext(), "After consuming the only node, hasNext should be false");
    }

    @Test
    public void testLinearChainOrderIsCorrect() {
        Graph g = new MapGraph();
        g.addNode(new GraphNode("A", "jobA"));
        g.addNode(new GraphNode("B", "jobB"));
        g.addNode(new GraphNode("C", "jobC"));
        g.addEdge("A", "B");
        g.addEdge("B", "C");

        TopologicalSortIterator it = new TopologicalSortIterator(g);
        List<String> order = new ArrayList<>();
        while (it.hasNext()) {
            order.add(((GraphNode) it.next()).getID());
        }
        assertEquals(3, order.size(), "Linear chain should produce 3 nodes");
        assertEquals("A", order.get(0), "First node in topological order should be A");
        assertEquals("C", order.get(2), "Last node in topological order should be C");
    }

    @Test
    public void testIteratorImplementsIterator() {
        Graph g = new MapGraph();
        TopologicalSortIterator it = new TopologicalSortIterator(g);
        assertTrue(it instanceof Iterator, "TopologicalSortIterator should implement Iterator");
    }

    @Test
    public void testAllNodesVisited() {
        Graph g = new MapGraph();
        g.addNode(new GraphNode("A", "jobA"));
        g.addNode(new GraphNode("B", "jobB"));
        g.addNode(new GraphNode("C", "jobC"));
        g.addEdge("A", "B");
        g.addEdge("A", "C");

        TopologicalSortIterator it = new TopologicalSortIterator(g);
        int count = 0;
        while (it.hasNext()) {
            it.next();
            count++;
        }
        assertEquals(3, count, "All 3 nodes should be visited");
    }

    @Test
    public void testRootNodeComesFirstInDiamondGraph() {
        Graph g = new MapGraph();
        g.addNode(new GraphNode("root", "root"));
        g.addNode(new GraphNode("left", "left"));
        g.addNode(new GraphNode("right", "right"));
        g.addNode(new GraphNode("leaf", "leaf"));
        g.addEdge("root", "left");
        g.addEdge("root", "right");
        g.addEdge("left", "leaf");
        g.addEdge("right", "leaf");

        TopologicalSortIterator it = new TopologicalSortIterator(g);
        assertTrue(it.hasNext(), "Diamond graph should have nodes");
        GraphNode first = (GraphNode) it.next();
        assertEquals("root", first.getID(), "Root node should come first in topological order");
    }
}
