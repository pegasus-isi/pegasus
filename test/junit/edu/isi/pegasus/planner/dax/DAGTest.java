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

/** Tests for the DAG job class. */
public class DAGTest {

    private DAG mDag;

    @BeforeEach
    public void setUp() {
        mDag = new DAG("DAG001", "workflow.dag");
    }

    @Test
    public void testInstantiation() {
        assertThat(mDag, notNullValue());
    }

    @Test
    public void testExtendsAbstractJob() {
        assertThat(mDag, instanceOf(AbstractJob.class));
    }

    @Test
    public void testIsDAGReturnsTrue() {
        assertThat(mDag.isDAG(), is(true));
    }

    @Test
    public void testIsDAXReturnsFalse() {
        assertThat(mDag.isDAX(), is(false));
    }

    @Test
    public void testConstructorWithLabel() {
        DAG dag = new DAG("DAG002", "workflow2.dag", "my-dag");
        assertThat(dag, notNullValue());
    }

    @Test
    public void testCopyConstructorThrowsDueToNullStdin() {
        // AbstractJob copy constructor calls new File(a.mStdin) but mStdin starts null,
        // causing NPE inside File copy constructor. Document this limitation.
        assertThrows(NullPointerException.class, () -> new DAG(mDag));
    }

    @Test
    public void testXMLSerialization() {
        StringWriter sw = new StringWriter();
        XMLWriter writer = new XMLWriter(sw);
        mDag.toXML(writer, 0);
        String result = sw.toString();
        assertThat(
                result,
                allOf(
                        containsString("dag"),
                        containsString("DAG001"),
                        containsString("workflow.dag")));
    }

    @Test
    public void testAddArgument() {
        mDag.addArgument("--plan");
        assertThat(mDag.getArguments().isEmpty(), is(false));
    }

    @Test
    public void testAddProfile() {
        mDag.addProfile("pegasus", "runtime", "300");
        assertThat(mDag.getProfiles().isEmpty(), is(false));
    }

    @Test
    public void testConstructorWithLabelStoresFields() {
        DAG dag = new DAG("DAG002", "workflow2.dag", "my-dag");

        assertThat(dag.getId(), is("DAG002"));
        assertThat(dag.getName(), is("workflow2.dag"));
        assertThat(dag.getNodeLabel(), is("my-dag"));
    }

    @Test
    public void testCopyConstructorCopiesFieldsWhenStdioInitialized() {
        DAG original = new DAG("DAG003", "copyable.dag", "label-copy");
        original.setStdin("in.txt");
        original.setStdout("out.txt");
        original.setStderr("err.txt");
        original.addArgument("--plan");
        original.addProfile("pegasus", "runtime", "60");

        DAG copy = new DAG(original);

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

        mDag.toXML(writer, 0);
        String result = sw.toString();

        assertThat(
                result,
                allOf(
                        containsString("<dag"),
                        containsString("id=\"DAG001\""),
                        containsString("file=\"workflow.dag\"")));
    }
}
