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

/** Unit tests for the GraphNode class. */
public class GraphNodeTest {

    private GraphNode mNode;

    @BeforeEach
    public void setUp() {
        mNode = new GraphNode("node1", "job1");
    }

    @Test
    public void testGetIDReturnsConstructorValue() {
        assertEquals("node1", mNode.getID(), "getID should return the ID set in constructor");
    }

    @Test
    public void testGetNameReturnsConstructorValue() {
        assertEquals("job1", mNode.getName(), "getName should return the name set in constructor");
    }

    @Test
    public void testDefaultColorIsWhite() {
        assertEquals(
                GraphNode.WHITE_COLOR, mNode.getColor(), "Default color should be WHITE_COLOR");
    }

    @Test
    public void testDefaultDepthIsNegativeOne() {
        assertEquals(-1, mNode.getDepth(), "Default depth should be -1");
    }

    @Test
    public void testSetAndGetDepth() {
        mNode.setDepth(3);
        assertEquals(3, mNode.getDepth(), "Depth should reflect the set value");
    }

    @Test
    public void testSetAndGetColor() {
        mNode.setColor(GraphNode.GRAY_COLOR);
        assertEquals(GraphNode.GRAY_COLOR, mNode.getColor(), "Color should reflect the set value");
    }

    @Test
    public void testInitialParentsIsEmpty() {
        assertTrue(mNode.getParents().isEmpty(), "A new node should have no parents");
    }

    @Test
    public void testInitialChildrenIsEmpty() {
        assertTrue(mNode.getChildren().isEmpty(), "A new node should have no children");
    }
}
