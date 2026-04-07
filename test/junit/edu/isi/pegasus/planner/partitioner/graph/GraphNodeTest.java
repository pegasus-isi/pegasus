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

import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Unit tests for the GraphNode class. */
public class GraphNodeTest {

    private GraphNode mNode;

    @BeforeEach
    public void setUp() {
        mNode = new GraphNode("node1", "job1");
    }

    @Test
    public void testGetIDReturnsConstructorValue() {
        assertThat(mNode.getID(), is("node1"));
    }

    @Test
    public void testGetNameReturnsConstructorValue() {
        assertThat(mNode.getName(), is("job1"));
    }

    @Test
    public void testDefaultColorIsWhite() {
        assertThat(mNode.getColor(), is(GraphNode.WHITE_COLOR));
    }

    @Test
    public void testDefaultDepthIsNegativeOne() {
        assertThat(mNode.getDepth(), is(-1));
    }

    @Test
    public void testSetAndGetDepth() {
        mNode.setDepth(3);
        assertThat(mNode.getDepth(), is(3));
    }

    @Test
    public void testSetAndGetColor() {
        mNode.setColor(GraphNode.GRAY_COLOR);
        assertThat(mNode.getColor(), is(GraphNode.GRAY_COLOR));
    }

    @Test
    public void testInitialParentsIsEmpty() {
        assertThat(mNode.getParents().isEmpty(), is(true));
    }

    @Test
    public void testInitialChildrenIsEmpty() {
        assertThat(mNode.getChildren().isEmpty(), is(true));
    }

    @Test
    public void testSetContentStoresValueAndSetsBackReference() {
        AtomicReference<GraphNode> reference = new AtomicReference<>();
        GraphNodeContent content = reference::set;

        mNode.setContent(content);

        assertThat(mNode.getContent(), is(sameInstance(content)));
        assertThat(reference.get(), is(sameInstance(mNode)));
    }

    @Test
    public void testAddAndRemoveParentAndChild() {
        GraphNode parent = new GraphNode("parent", "job-parent");
        GraphNode child = new GraphNode("child", "job-child");

        mNode.addParent(parent);
        mNode.addChild(child);
        assertThat(mNode.getParents().contains(parent), is(true));
        assertThat(mNode.getChildren().contains(child), is(true));

        mNode.removeParent(parent);
        mNode.removeChild(child);
        assertThat(mNode.getParents().contains(parent), is(false));
        assertThat(mNode.getChildren().contains(child), is(false));
    }

    @Test
    public void testResetEdgesClearsParentsAndChildren() {
        mNode.addParent(new GraphNode("parent", "job-parent"));
        mNode.addChild(new GraphNode("child", "job-child"));

        mNode.resetEdges();

        assertThat(mNode.getParents().isEmpty(), is(true));
        assertThat(mNode.getChildren().isEmpty(), is(true));
    }

    @Test
    public void testSetAndGetBag() {
        Bag bag = new LabelBag();
        mNode.setBag(bag);
        assertThat(mNode.getBag(), is(sameInstance(bag)));
    }
}
