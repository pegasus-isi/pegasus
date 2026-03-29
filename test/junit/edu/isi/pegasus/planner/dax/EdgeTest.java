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
package edu.isi.pegasus.planner.dax;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Tests for the Edge class. */
public class EdgeTest {

    private Edge mEdge;

    @BeforeEach
    public void setUp() {
        mEdge = new Edge("parent1", "child1");
    }

    @Test
    public void testConstructorSetsParent() {
        assertEquals("parent1", mEdge.getParent(), "Parent should be set by constructor");
    }

    @Test
    public void testConstructorSetsChild() {
        assertEquals("child1", mEdge.getChild(), "Child should be set by constructor");
    }

    @Test
    public void testConstructorWithLabel() {
        Edge edge = new Edge("parent1", "child1", "my-label");
        assertEquals("my-label", edge.getLabel(), "Label should be set by constructor");
    }

    @Test
    public void testInitialLabelIsNull() {
        assertNull(mEdge.getLabel(), "Label should be null initially");
    }

    @Test
    public void testSetParent() {
        mEdge.setParent("new-parent");
        assertEquals("new-parent", mEdge.getParent(), "Parent should be updated");
    }

    @Test
    public void testSetChild() {
        mEdge.setChild("new-child");
        assertEquals("new-child", mEdge.getChild(), "Child should be updated");
    }

    @Test
    public void testSetLabel() {
        mEdge.setLabel("edge-label");
        assertEquals("edge-label", mEdge.getLabel(), "Label should be updated");
    }

    @Test
    public void testCopyConstructor() {
        Edge withLabel = new Edge("p", "c", "lbl");
        Edge copy = new Edge(withLabel);
        assertEquals("p", copy.getParent(), "Copy should have same parent");
        assertEquals("c", copy.getChild(), "Copy should have same child");
        assertEquals("lbl", copy.getLabel(), "Copy should have same label");
    }

    @Test
    public void testClone() {
        Edge withLabel = new Edge("p", "c", "lbl");
        Edge cloned = withLabel.clone();
        assertEquals(withLabel.getParent(), cloned.getParent(), "Clone should have same parent");
        assertEquals(withLabel.getChild(), cloned.getChild(), "Clone should have same child");
        assertEquals(withLabel.getLabel(), cloned.getLabel(), "Clone should have same label");
        assertNotSame(withLabel, cloned, "Clone should be a different object");
    }

    @Test
    public void testHashCode() {
        Edge e1 = new Edge("p", "c", "lbl");
        Edge e2 = new Edge("p", "c", "lbl");
        assertEquals(e1.hashCode(), e2.hashCode(), "Equal edges should have same hash code");
    }

    @Test
    public void testEqualsWithSameValues() {
        Edge e1 = new Edge("p", "c", "lbl");
        Edge e2 = new Edge("p", "c", "lbl");
        assertTrue(e1.equals(e2), "Edges with same values should be equal");
    }
}
