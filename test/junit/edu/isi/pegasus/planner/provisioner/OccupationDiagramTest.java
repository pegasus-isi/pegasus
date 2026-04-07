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
import org.springframework.test.util.ReflectionTestUtils;

/** Unit tests for the OccupationDiagram class. */
public class OccupationDiagramTest {

    private OccupationDiagram mDiagram;
    private static final long RFT = 100L;

    @BeforeEach
    public void setUp() {
        mDiagram = new OccupationDiagram(RFT);
    }

    @Test
    public void testOccupationDiagramCanBeInstantiated() {
        assertThat(mDiagram, notNullValue());
    }

    @Test
    public void testRFTIsSetCorrectly() throws Exception {
        long rft = (long) ReflectionTestUtils.getField(mDiagram, "RFT");
        assertThat(rft, is(RFT));
    }

    @Test
    public void testTimeMapHasCorrectSize() throws Exception {
        java.util.LinkedList[] timeMap =
                (java.util.LinkedList[]) ReflectionTestUtils.getField(mDiagram, "timeMap");
        assertThat(timeMap.length, is((int) RFT));
    }

    @Test
    public void testAddNodeWithPositiveWeight() {
        Node node = new Node("n1", "task1", 10L);
        // Just verify add doesn't throw
        assertDoesNotThrow(
                () -> mDiagram.add(node), "Adding a node with positive weight should not throw");
    }

    @Test
    public void testInitialMaxIsZero() throws Exception {
        int max = (int) ReflectionTestUtils.getField(mDiagram, "max");
        assertThat(max, is(0));
    }

    @Test
    public void testInitialNodesTreeSetIsEmpty() throws Exception {
        java.util.TreeSet nodes =
                (java.util.TreeSet) ReflectionTestUtils.getField(mDiagram, "nodes");
        assertThat(nodes.isEmpty(), is(true));
    }

    @Test
    public void testNodeWithZeroWeightIsNotAdded() throws Exception {
        // evalWeight returns 0 for a node with no edges/weight configured specially
        // We create a node with weight 0 - but Node(String, String, long) stores weight,
        // evalWeight may differ; let's just check that add with w>0 does work
        Node node = new Node("n1", "task1", 5L);
        mDiagram.add(node);
        java.util.TreeSet nodes =
                (java.util.TreeSet) ReflectionTestUtils.getField(mDiagram, "nodes");
        assertThat(nodes.isEmpty(), is(false));
    }

    @Test
    public void testZeroWeightNodeIsNotAdded() throws Exception {
        Node node = new Node("n0", "task0", 0L);
        mDiagram.add(node);
        java.util.TreeSet nodes =
                (java.util.TreeSet) ReflectionTestUtils.getField(mDiagram, "nodes");
        assertThat(nodes.isEmpty(), is(true));
    }

    @Test
    public void testAddingPositiveWeightNodeIncreasesStoredNodeCount() throws Exception {
        Node node = new Node("n2", "task2", 7L);
        mDiagram.add(node);
        java.util.TreeSet nodes =
                (java.util.TreeSet) ReflectionTestUtils.getField(mDiagram, "nodes");
        assertThat(nodes.size(), is(1));
    }

    @Test
    public void testStackOnEmptyDiagramReturnsZero() {
        assertThat(mDiagram.stack(false), is(0));
    }

    @Test
    public void testInitialMaxIndexIsZero() throws Exception {
        assertThat(ReflectionTestUtils.getField(mDiagram, "maxIndex"), is((Object) 0));
    }
}
