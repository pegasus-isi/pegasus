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
        assertEquals(0L, Edge.DEFAULT_SIZE, "DEFAULT_SIZE should be 0");
    }

    @Test
    public void testDefaultBpsConstant() {
        assertEquals(1L, Edge.DEFAULT_BPS, "DEFAULT_BPS should be 1");
    }

    @Test
    public void testDefaultLatencyConstant() {
        assertEquals(0L, Edge.DEFAULT_LATENCY, "DEFAULT_LATENCY should be 0");
    }

    @Test
    public void testGetFromReturnsCorrectNode() {
        assertEquals(mNodeA, mEdge.getFrom(), "getFrom() should return the 'from' node");
    }

    @Test
    public void testGetToReturnsCorrectNode() {
        assertEquals(mNodeB, mEdge.getTo(), "getTo() should return the 'to' node");
    }

    @Test
    public void testGetCostReturnsExpectedValue() {
        // cost = fileSize / DEFAULT_BPS + DEFAULT_LATENCY = 1000 / 1 + 0 = 1000
        assertEquals(1000L, mEdge.getCost(), "getCost should return fileSize / BPS + latency");
    }

    @Test
    public void testInitResetsCompletionFlag() {
        mEdge.complete = true;
        mEdge.init();
        assertFalse(mEdge.complete, "After init(), complete flag should be false");
    }

    @Test
    public void testInitResetsCompTime() {
        mEdge.compTime = 500L;
        mEdge.init();
        assertEquals(0L, mEdge.compTime, "After init(), compTime should be 0");
    }
}
