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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

/** Unit tests for the TopologicalSortIterator class. */
public class TopologicalSortIteratorTest {

    @Test
    public void testEmptyGraphHasNoElements() {
        Graph g = new MapGraph();
        TopologicalSortIterator it = new TopologicalSortIterator(g);
        assertThat(it.hasNext(), is(false));
    }

    @Test
    public void testSingleNodeGraphReturnsOneElement() {
        Graph g = new MapGraph();
        g.addNode(new GraphNode("A", "jobA"));
        TopologicalSortIterator it = new TopologicalSortIterator(g);
        assertThat(it.hasNext(), is(true));
        GraphNode node = (GraphNode) it.next();
        assertThat(node.getID(), is("A"));
        assertThat(it.hasNext(), is(false));
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
        assertThat(order.size(), is(3));
        assertThat(order.get(0), is("A"));
        assertThat(order.get(2), is("C"));
    }

    @Test
    public void testIteratorImplementsIterator() {
        Graph g = new MapGraph();
        TopologicalSortIterator it = new TopologicalSortIterator(g);
        assertThat(it instanceof Iterator, is(true));
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
        assertThat(count, is(3));
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
        assertThat(it.hasNext(), is(true));
        GraphNode first = (GraphNode) it.next();
        assertThat(first.getID(), is("root"));
    }

    @Test
    public void testRemoveThrowsUnsupportedOperationException() {
        TopologicalSortIterator it = new TopologicalSortIterator(new MapGraph());
        assertThrows(UnsupportedOperationException.class, it::remove);
    }

    @Test
    public void testNextOnEmptyIteratorCurrentlyThrowsIndexOutOfBoundsException() {
        TopologicalSortIterator it = new TopologicalSortIterator(new MapGraph());
        assertThrows(IndexOutOfBoundsException.class, it::next);
    }

    @Test
    public void testInitializeBuildsExpectedIndegreeArrayForSimpleEdge() throws Exception {
        Graph g = new MapGraph();
        g.addNode(new GraphNode("A", "jobA"));
        g.addNode(new GraphNode("B", "jobB"));
        g.addEdge("A", "B");

        TopologicalSortIterator it = new TopologicalSortIterator(g);

        int[] inDegree = (int[]) ReflectionTestUtils.getField(it, "mInDegree");
        java.util.Map<?, ?> indexMap =
                (java.util.Map<?, ?>) ReflectionTestUtils.getField(it, "mIndexMap");

        assertThat(inDegree.length, is(2));
        assertThat(inDegree[((Integer) indexMap.get("A")).intValue()], is(0));
        assertThat(inDegree[((Integer) indexMap.get("B")).intValue()], is(1));
    }
}
