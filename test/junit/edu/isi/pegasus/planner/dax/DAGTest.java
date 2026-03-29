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
        assertNotNull(mDag, "DAG should be instantiatable");
    }

    @Test
    public void testExtendsAbstractJob() {
        assertInstanceOf(AbstractJob.class, mDag, "DAG should extend AbstractJob");
    }

    @Test
    public void testIsDAGReturnsTrue() {
        assertTrue(mDag.isDAG(), "isDAG() should return true for a DAG object");
    }

    @Test
    public void testIsDAXReturnsFalse() {
        assertFalse(mDag.isDAX(), "isDAX() should return false for a DAG object");
    }

    @Test
    public void testConstructorWithLabel() {
        DAG dag = new DAG("DAG002", "workflow2.dag", "my-dag");
        assertNotNull(dag, "DAG with label should be constructable");
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
        assertTrue(result.contains("dag"), "XML should contain 'dag' element");
        assertTrue(result.contains("DAG001"), "XML should contain the job ID");
        assertTrue(result.contains("workflow.dag"), "XML should contain the dag filename");
    }

    @Test
    public void testAddArgument() {
        mDag.addArgument("--plan");
        assertFalse(mDag.getArguments().isEmpty(), "Should have arguments after addArgument");
    }

    @Test
    public void testAddProfile() {
        mDag.addProfile("pegasus", "runtime", "300");
        assertFalse(mDag.getProfiles().isEmpty(), "Should have profiles after addProfile");
    }
}
