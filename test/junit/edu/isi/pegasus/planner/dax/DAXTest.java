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

/** Tests for the DAX job class. */
public class DAXTest {

    private DAX mDax;

    @BeforeEach
    public void setUp() {
        mDax = new DAX("DAX001", "subworkflow.dax");
    }

    @Test
    public void testInstantiation() {
        assertNotNull(mDax, "DAX should be instantiatable");
    }

    @Test
    public void testExtendsAbstractJob() {
        assertInstanceOf(AbstractJob.class, mDax, "DAX should extend AbstractJob");
    }

    @Test
    public void testIsDAXReturnsTrue() {
        assertTrue(mDax.isDAX(), "isDAX() should return true for a DAX object");
    }

    @Test
    public void testIsDAGReturnsFalse() {
        assertFalse(mDax.isDAG(), "isDAG() should return false for a DAX object");
    }

    @Test
    public void testConstructorWithLabel() {
        DAX dax = new DAX("DAX002", "subworkflow2.dax", "my-dax");
        assertNotNull(dax, "DAX with label should be constructable");
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
        assertTrue(result.contains("dax"), "XML should contain 'dax' element");
        assertTrue(result.contains("DAX001"), "XML should contain the job ID");
        assertTrue(result.contains("subworkflow.dax"), "XML should contain the dax filename");
    }

    @Test
    public void testAddArgument() {
        mDax.addArgument("--sites condorpool");
        assertFalse(mDax.getArguments().isEmpty(), "Should have arguments after addArgument");
    }

    @Test
    public void testAddProfile() {
        mDax.addProfile("pegasus", "runtime", "600");
        assertFalse(mDax.getProfiles().isEmpty(), "Should have profiles after addProfile");
    }
}
