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

/** Tests for the DAX job class. */
public class DAXTest {

    private DAX mDax;

    @BeforeEach
    public void setUp() {
        mDax = new DAX("DAX001", "subworkflow.dax");
    }

    @Test
    public void testInstantiation() {
        assertThat(mDax, notNullValue());
    }

    @Test
    public void testExtendsAbstractJob() {
        assertThat(mDax, instanceOf(AbstractJob.class));
    }

    @Test
    public void testIsDAXReturnsTrue() {
        assertThat(mDax.isDAX(), is(true));
    }

    @Test
    public void testIsDAGReturnsFalse() {
        assertThat(mDax.isDAG(), is(false));
    }

    @Test
    public void testConstructorWithLabel() {
        DAX dax = new DAX("DAX002", "subworkflow2.dax", "my-dax");
        assertThat(dax, notNullValue());
    }

    @Test
    public void testCopyConstructorThrowsDueToNullStdin() {
        // AbstractJob copy constructor calls new File(a.mStdin) but mStdin starts null,
        // causing NPE inside File copy constructor. Document this limitation.
        assertThrows(NullPointerException.class, () -> new DAX(mDax));
    }

    @Test
    public void testXMLSerialization() {
        StringWriter sw = new StringWriter();
        XMLWriter writer = new XMLWriter(sw);
        mDax.toXML(writer, 0);
        String result = sw.toString();
        assertThat(
                result,
                allOf(
                        containsString("dax"),
                        containsString("DAX001"),
                        containsString("subworkflow.dax")));
    }

    @Test
    public void testAddArgument() {
        mDax.addArgument("--sites condorpool");
        assertThat(mDax.getArguments().isEmpty(), is(false));
    }

    @Test
    public void testAddProfile() {
        mDax.addProfile("pegasus", "runtime", "600");
        assertThat(mDax.getProfiles().isEmpty(), is(false));
    }

    @Test
    public void testConstructorWithLabelStoresFields() {
        DAX dax = new DAX("DAX002", "subworkflow2.dax", "my-dax");

        assertThat(dax.getId(), is("DAX002"));
        assertThat(dax.getName(), is("subworkflow2.dax"));
        assertThat(dax.getNodeLabel(), is("my-dax"));
    }

    @Test
    public void testCopyConstructorCopiesFieldsWhenStdioInitialized() {
        DAX original = new DAX("DAX003", "copyable.dax", "label-copy");
        original.setStdin("in.txt");
        original.setStdout("out.txt");
        original.setStderr("err.txt");
        original.addArgument("--sites local");
        original.addProfile("pegasus", "runtime", "60");

        DAX copy = new DAX(original);

        assertThat(copy.getId(), is(original.getId()));
        assertThat(copy.getName(), is(original.getName()));
        assertThat(copy.getNodeLabel(), is(original.getNodeLabel()));
        assertThat(copy.getArguments(), is(original.getArguments()));
        assertThat(copy.getProfiles().size(), is(original.getProfiles().size()));
        assertThat(copy.getStdin().getName(), is("in.txt"));
        assertThat(copy.getStdout().getName(), is("out.txt"));
        assertThat(copy.getStderr().getName(), is("err.txt"));
    }

    @Test
    public void testXMLSerializationContainsExpectedAttributes() {
        StringWriter sw = new StringWriter();
        XMLWriter writer = new XMLWriter(sw);

        mDax.toXML(writer, 0);
        String result = sw.toString();

        assertThat(
                result,
                allOf(
                        containsString("<dax"),
                        containsString("id=\"DAX001\""),
                        containsString("file=\"subworkflow.dax\"")));
    }
}
