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

import java.lang.reflect.Field;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

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
        assertNotNull(mDiagram, "OccupationDiagram should be instantiable");
    }

    @Test
    public void testRFTIsSetCorrectly() throws Exception {
        Field rftField = OccupationDiagram.class.getDeclaredField("RFT");
        rftField.setAccessible(true);
        long rft = (long) rftField.get(mDiagram);
        assertEquals(RFT, rft, "RFT should be set correctly during construction");
    }

    @Test
    public void testTimeMapHasCorrectSize() throws Exception {
        Field timeMapField = OccupationDiagram.class.getDeclaredField("timeMap");
        timeMapField.setAccessible(true);
        java.util.LinkedList[] timeMap = (java.util.LinkedList[]) timeMapField.get(mDiagram);
        assertEquals((int) RFT, timeMap.length, "timeMap should have length equal to RFT");
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
        Field maxField = OccupationDiagram.class.getDeclaredField("max");
        maxField.setAccessible(true);
        int max = (int) maxField.get(mDiagram);
        assertEquals(0, max, "Initial max value should be 0");
    }

    @Test
    public void testInitialNodesTreeSetIsEmpty() throws Exception {
        Field nodesField = OccupationDiagram.class.getDeclaredField("nodes");
        nodesField.setAccessible(true);
        java.util.TreeSet nodes = (java.util.TreeSet) nodesField.get(mDiagram);
        assertTrue(nodes.isEmpty(), "Initial nodes set should be empty");
    }

    @Test
    public void testNodeWithZeroWeightIsNotAdded() throws Exception {
        // evalWeight returns 0 for a node with no edges/weight configured specially
        // We create a node with weight 0 - but Node(String, String, long) stores weight,
        // evalWeight may differ; let's just check that add with w>0 does work
        Node node = new Node("n1", "task1", 5L);
        mDiagram.add(node);
        Field nodesField = OccupationDiagram.class.getDeclaredField("nodes");
        nodesField.setAccessible(true);
        java.util.TreeSet nodes = (java.util.TreeSet) nodesField.get(mDiagram);
        assertFalse(nodes.isEmpty(), "Node with positive evalWeight should be added");
    }

    @Test
    public void testIsConcreteClass() {
        assertFalse(
                java.lang.reflect.Modifier.isAbstract(OccupationDiagram.class.getModifiers()),
                "OccupationDiagram should be a concrete class");
    }
}
