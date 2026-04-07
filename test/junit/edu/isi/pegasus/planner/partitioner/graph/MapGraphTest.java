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
        assertThat(mGraph.nodeIterator().hasNext(), is(false));
    }

    @Test
    public void testAddNodeIncreasesSize() {
        mGraph.addNode(new GraphNode("A", "jobA"));
        assertThat(mGraph.nodeIterator().hasNext(), is(true));
    }

    @Test
    public void testGetNodeReturnsAddedNode() {
        GraphNode node = new GraphNode("A", "jobA");
        mGraph.addNode(node);
        GraphNode retrieved = mGraph.getNode("A");
        assertThat(retrieved, is(notNullValue()));
        assertThat(retrieved.getID(), is("A"));
    }

    @Test
    public void testGetNodeReturnsNullForMissingNode() {
        assertThat(mGraph.getNode("nonexistent"), is(nullValue()));
    }

    @Test
    public void testAddEdgeConnectsNodes() {
        mGraph.addNode(new GraphNode("A", "jobA"));
        mGraph.addNode(new GraphNode("B", "jobB"));
        mGraph.addEdge("A", "B");
        GraphNode nodeA = mGraph.getNode("A");
        assertThat(nodeA.getChildren().isEmpty(), is(false));
    }

    @Test
    public void testImplementsGraphInterface() {
        assertThat(mGraph instanceof Graph, is(true));
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
        assertThat(mGraph.size(), is(2));
    }

    @Test
    public void testAddRootMakesExistingRootsChildren() {
        GraphNode a = new GraphNode("A", "jobA");
        GraphNode b = new GraphNode("B", "jobB");
        GraphNode root = new GraphNode("ROOT", "root");
        mGraph.addNode(a);
        mGraph.addNode(b);

        mGraph.addRoot(root);

        assertThat(root.getChildren().contains(a), is(true));
        assertThat(root.getChildren().contains(b), is(true));
        assertThat(a.getParents().contains(root), is(true));
        assertThat(b.getParents().contains(root), is(true));
    }

    @Test
    public void testResetEdgesPreservesNodesButClearsDependencies() {
        mGraph.addNode(new GraphNode("A", "jobA"));
        mGraph.addNode(new GraphNode("B", "jobB"));
        mGraph.addEdge("A", "B");

        mGraph.resetEdges();

        assertThat(mGraph.size(), is(2));
        assertThat(mGraph.getNode("A").getChildren().isEmpty(), is(true));
        assertThat(mGraph.getNode("B").getParents().isEmpty(), is(true));
    }

    @Test
    public void testRemoveReconnectsParentsToChildren() {
        GraphNode a = new GraphNode("A", "jobA");
        GraphNode b = new GraphNode("B", "jobB");
        GraphNode c = new GraphNode("C", "jobC");
        mGraph.addNode(a);
        mGraph.addNode(b);
        mGraph.addNode(c);
        mGraph.addEdge("A", "B");
        mGraph.addEdge("B", "C");

        assertThat(mGraph.remove("B"), is(true));

        assertThat(mGraph.getNode("B"), is(nullValue()));
        assertThat(a.getChildren().contains(c), is(true));
        assertThat(c.getParents().contains(a), is(true));
    }

    @Test
    public void testCloneReturnsCloneNotSupportedExceptionInstance() {
        Object cloned = mGraph.clone();
        assertThat(cloned, instanceOf(CloneNotSupportedException.class));
        assertThat(
                ((CloneNotSupportedException) cloned).getMessage(),
                is("Clone() not implemented in GraphNode"));
    }
}
