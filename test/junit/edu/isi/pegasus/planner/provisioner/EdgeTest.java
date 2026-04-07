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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Unit tests for the provisioner Edge class. */
public class EdgeTest {

    private Node mNodeA;
    private Node mNodeB;
    private Edge mEdge;

    @BeforeEach
    public void setUp() {
        mNodeA = new Node("A", "jobA", 10L);
        mNodeB = new Node("B", "jobB", 20L);
        mEdge = new Edge(mNodeA, mNodeB, "data.txt", 1000L);
    }

    @Test
    public void testDefaultSizeConstant() {
        assertThat(Edge.DEFAULT_SIZE, is(0L));
    }

    @Test
    public void testDefaultBpsConstant() {
        assertThat(Edge.DEFAULT_BPS, is(1L));
    }

    @Test
    public void testDefaultLatencyConstant() {
        assertThat(Edge.DEFAULT_LATENCY, is(0L));
    }

    @Test
    public void testGetFromReturnsCorrectNode() {
        assertThat(mEdge.getFrom(), is(mNodeA));
    }

    @Test
    public void testGetToReturnsCorrectNode() {
        assertThat(mEdge.getTo(), is(mNodeB));
    }

    @Test
    public void testGetCostReturnsExpectedValue() {
        // cost = fileSize / DEFAULT_BPS + DEFAULT_LATENCY = 1000 / 1 + 0 = 1000
        assertThat(mEdge.getCost(), is(1000L));
    }

    @Test
    public void testInitResetsCompletionFlag() {
        mEdge.complete = true;
        mEdge.init();
        assertThat(mEdge.complete, is(false));
    }

    @Test
    public void testInitResetsCompTime() {
        mEdge.compTime = 500L;
        mEdge.init();
        assertThat(mEdge.compTime, is(0L));
    }

    @Test
    public void testGetIDReturnsFileName() {
        assertThat(mEdge.getID(), is("data.txt"));
    }

    @Test
    public void testSetFromAndSetToUpdateEndpoints() {
        Node newFrom = new Node("C", "jobC", 30L);
        Node newTo = new Node("D", "jobD", 40L);

        mEdge.setFrom(newFrom);
        mEdge.setTo(newTo);

        assertThat(mEdge.getFrom(), is(sameInstance(newFrom)));
        assertThat(mEdge.getTo(), is(sameInstance(newTo)));
    }

    @Test
    public void testGetCostWithBandwidthAndLatencyUsesProvidedValues() {
        assertThat(mEdge.getCost(5L, 5L), is(205L));
    }

    @Test
    public void testSetCostOverridesStoredCost() {
        mEdge.setCost(77L);
        assertThat(mEdge.getCost(), is(77L));
    }
}
