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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.*;

import edu.isi.pegasus.common.util.XMLWriter;
import java.io.StringWriter;
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
        assertThat(mEdge.getParent(), is("parent1"));
    }

    @Test
    public void testConstructorSetsChild() {
        assertThat(mEdge.getChild(), is("child1"));
    }

    @Test
    public void testConstructorWithLabel() {
        Edge edge = new Edge("parent1", "child1", "my-label");
        assertThat(edge.getLabel(), is("my-label"));
    }

    @Test
    public void testInitialLabelIsNull() {
        assertThat(mEdge.getLabel(), nullValue());
    }

    @Test
    public void testSetParent() {
        mEdge.setParent("new-parent");
        assertThat(mEdge.getParent(), is("new-parent"));
    }

    @Test
    public void testSetChild() {
        mEdge.setChild("new-child");
        assertThat(mEdge.getChild(), is("new-child"));
    }

    @Test
    public void testSetLabel() {
        mEdge.setLabel("edge-label");
        assertThat(mEdge.getLabel(), is("edge-label"));
    }

    @Test
    public void testCopyConstructor() {
        Edge withLabel = new Edge("p", "c", "lbl");
        Edge copy = new Edge(withLabel);
        assertThat(copy.getParent(), is("p"));
        assertThat(copy.getChild(), is("c"));
        assertThat(copy.getLabel(), is("lbl"));
    }

    @Test
    public void testClone() {
        Edge withLabel = new Edge("p", "c", "lbl");
        Edge cloned = withLabel.clone();
        assertThat(cloned.getParent(), is(withLabel.getParent()));
        assertThat(cloned.getChild(), is(withLabel.getChild()));
        assertThat(cloned.getLabel(), is(withLabel.getLabel()));
        assertThat(cloned, not(sameInstance(withLabel)));
    }

    @Test
    public void testHashCode() {
        Edge e1 = new Edge("p", "c", "lbl");
        Edge e2 = new Edge("p", "c", "lbl");
        assertThat(e1.hashCode(), is(e2.hashCode()));
    }

    @Test
    public void testEqualsWithSameValues() {
        Edge e1 = new Edge("p", "c", "lbl");
        Edge e2 = new Edge("p", "c", "lbl");
        assertThat(e1.equals(e2), is(true));
    }

    @Test
    public void testEqualsReturnsFalseForDifferentType() {
        assertThat(mEdge.equals("not-an-edge"), is(false));
    }

    @Test
    public void testEqualsWithNullLabelsCurrentlyThrowsNullPointerException() {
        Edge first = new Edge("p", "c");
        Edge second = new Edge("p", "c");

        assertThrows(NullPointerException.class, () -> first.equals(second));
    }

    @Test
    public void testToStringCurrentBehaviorWithoutLabel() {
        assertThat(mEdge.toString(), is("(parent1->:null)"));
    }

    @Test
    public void testToXMLWithoutLabelOmitsEdgeLabelAttribute() {
        StringWriter sw = new StringWriter();
        XMLWriter writer = new XMLWriter(sw);

        mEdge.toXML(writer, 0);
        String result = sw.toString();

        assertThat(
                result,
                allOf(
                        containsString("<child"),
                        containsString("<parent"),
                        containsString("ref=\"parent1\"")));
        assertThat(result.contains("edge-label="), is(false));
    }

    @Test
    public void testToXMLParentWithLabelIncludesEdgeLabelAttribute() {
        Edge edge = new Edge("parent1", "child1", "edge-a");
        StringWriter sw = new StringWriter();
        XMLWriter writer = new XMLWriter(sw);

        edge.toXMLParent(writer, 1);
        String result = sw.toString();

        assertThat(
                result,
                allOf(
                        containsString("<parent"),
                        containsString("ref=\"parent1\""),
                        containsString("edge-label=\"edge-a\"")));
    }
}
