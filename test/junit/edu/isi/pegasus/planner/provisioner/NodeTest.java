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

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Unit tests for the provisioner Node class. */
public class NodeTest {

    private Node mNode;

    @BeforeEach
    public void setUp() {
        mNode = new Node("n1", "myTask", 100L);
    }

    @Test
    public void testDefaultWeightConstant() {
        assertEquals(1L, Node.DEFAULT_WEIGHT, "DEFAULT_WEIGHT should be 1");
    }

    @Test
    public void testEvalWeightReturnsConfiguredWeight() {
        assertEquals(100L, mNode.evalWeight(), "evalWeight should return the configured weight");
    }

    @Test
    public void testGetInReturnsEmptyListInitially() {
        assertNotNull(mNode.getIn(), "getIn() should not return null");
        assertTrue(mNode.getIn().isEmpty(), "New node should have no incoming edges");
    }

    @Test
    public void testGetOutReturnsEmptyListInitially() {
        assertNotNull(mNode.getOut(), "getOut() should not return null");
        assertTrue(mNode.getOut().isEmpty(), "New node should have no outgoing edges");
    }

    @Test
    public void testCheckInReturnsTrueWhenNoIncomingEdges() {
        assertTrue(
                mNode.checkIn(), "checkIn() should return true when there are no incoming edges");
    }

    @Test
    public void testSetWeight() {
        mNode.setWeight(50L);
        assertEquals(
                50L, mNode.evalWeight(), "evalWeight should return the new weight after setWeight");
    }

    @Test
    public void testNodeWithStringIdOnlyConstructor() {
        Node n = new Node("myId");
        assertNotNull(n, "Node(String) constructor should create a node");
        assertEquals(
                Node.DEFAULT_WEIGHT,
                n.evalWeight(),
                "Default constructor should use DEFAULT_WEIGHT");
    }

    @Test
    public void testAddInEdge() {
        Node other = new Node("n2");
        Edge edge = new Edge(other, mNode, "file.txt", 100L);
        mNode.addIn(edge);
        assertFalse(mNode.getIn().isEmpty(), "Node should have one incoming edge after addIn");
    }
}
