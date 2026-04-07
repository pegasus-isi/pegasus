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

import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;
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
        assertThat(Node.DEFAULT_WEIGHT, is(1L));
    }

    @Test
    public void testEvalWeightReturnsConfiguredWeight() {
        assertThat(mNode.evalWeight(), is(100L));
    }

    @Test
    public void testGetInReturnsEmptyListInitially() {
        assertThat(mNode.getIn(), notNullValue());
        assertThat(mNode.getIn().isEmpty(), is(true));
    }

    @Test
    public void testGetOutReturnsEmptyListInitially() {
        assertThat(mNode.getOut(), notNullValue());
        assertThat(mNode.getOut().isEmpty(), is(true));
    }

    @Test
    public void testCheckInReturnsTrueWhenNoIncomingEdges() {
        assertThat(mNode.checkIn(), is(true));
    }

    @Test
    public void testSetWeight() {
        mNode.setWeight(50L);
        assertThat(mNode.evalWeight(), is(50L));
    }

    @Test
    public void testNodeWithStringIdOnlyConstructor() {
        Node n = new Node("myId");
        assertThat(n, notNullValue());
        assertThat(n.evalWeight(), is(Node.DEFAULT_WEIGHT));
    }

    @Test
    public void testAddInEdge() {
        Node other = new Node("n2");
        Edge edge = new Edge(other, mNode, "file.txt", 100L);
        mNode.addIn(edge);
        assertThat(mNode.getIn().isEmpty(), is(false));
    }

    @Test
    public void testGetIDReturnsConfiguredIdentifier() {
        assertThat(mNode.getID(), is("n1"));
    }

    @Test
    public void testIsTopAndIsBottomForIsolatedNode() {
        assertThat(mNode.isTop(), is(true));
        assertThat(mNode.isBottom(), is(true));
    }

    @Test
    public void testAddOutDoesNotDuplicateSameEdge() {
        Node child = new Node("n2");
        Edge edge = new Edge(mNode, child, "file.txt", 100L);
        mNode.addOut(edge);
        mNode.addOut(edge);
        assertThat(mNode.getOut().size(), is(1));
    }

    @Test
    public void testInitOutMarksOutgoingEdgesCompleteAndSetsCompTime() {
        Node child = new Node("n2");
        Edge edge = new Edge(mNode, child, "file.txt", 100L);
        mNode.addOut(edge);

        mNode.initOut(true, 42L);

        assertThat(edge.complete, is(true));
        assertThat(edge.compTime, is(42L));
    }
}
