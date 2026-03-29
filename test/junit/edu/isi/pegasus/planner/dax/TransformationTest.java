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

/** Tests for the Transformation class. */
public class TransformationTest {

    private Transformation mTransformation;

    @BeforeEach
    public void setUp() {
        mTransformation = new Transformation("pegasus", "preprocess", "1.0");
    }

    @Test
    public void testInstantiation() {
        assertNotNull(mTransformation, "Transformation should be instantiatable");
    }

    @Test
    public void testGetName() {
        assertEquals("preprocess", mTransformation.getName(), "getName() should return the name");
    }

    @Test
    public void testGetNamespace() {
        assertEquals(
                "pegasus",
                mTransformation.getNamespace(),
                "getNamespace() should return namespace");
    }

    @Test
    public void testGetVersion() {
        assertEquals("1.0", mTransformation.getVersion(), "getVersion() should return the version");
    }

    @Test
    public void testSimpleNameConstructor() {
        Transformation t = new Transformation("my-transform");
        assertEquals("my-transform", t.getName(), "Simple name constructor should set name");
        assertEquals("", t.getNamespace(), "Simple name constructor should have empty namespace");
        assertEquals("", t.getVersion(), "Simple name constructor should have empty version");
    }

    @Test
    public void testNullNamespaceNormalizedToEmpty() {
        Transformation t = new Transformation(null, "tool", null);
        assertEquals("", t.getNamespace(), "Null namespace should be normalized to empty string");
        assertEquals("", t.getVersion(), "Null version should be normalized to empty string");
    }

    @Test
    public void testUsesFile() {
        File f = new File("input.txt");
        mTransformation.uses(f);
        assertEquals(1, mTransformation.getUses().size(), "uses() should add a file");
        assertSame(f, mTransformation.getUses().get(0), "Added file should be in the uses list");
    }

    @Test
    public void testUsesExecutable() {
        Executable e = new Executable("pegasus", "preprocess", "1.0");
        mTransformation.uses(e);
        assertEquals(1, mTransformation.getUses().size(), "uses() should add an executable");
    }

    @Test
    public void testUsesMultiple() {
        mTransformation.uses(new File("a.txt"));
        mTransformation.uses(new File("b.txt"));
        assertEquals(2, mTransformation.getUses().size(), "Should have two entries in uses");
    }

    @Test
    public void testInitialUsesEmpty() {
        assertTrue(
                mTransformation.getUses().isEmpty(), "New transformation should have empty uses");
    }

    @Test
    public void testAddInvokeWhenWhat() {
        mTransformation.addInvoke(Invoke.WHEN.start, "/bin/notify.sh");
        assertFalse(
                mTransformation.getInvoke().isEmpty(),
                "Should have an invoke after addInvoke(when, what)");
        assertEquals(1, mTransformation.getInvoke().size(), "Should have exactly one invoke");
    }

    @Test
    public void testAddInvokeObject() {
        Invoke invoke = new Invoke(Invoke.WHEN.end, "/bin/cleanup.sh");
        mTransformation.addInvoke(invoke);
        assertEquals(1, mTransformation.getInvoke().size(), "Should have one invoke");
    }

    @Test
    public void testGetNotificationIsSameAsGetInvoke() {
        mTransformation.addInvoke(Invoke.WHEN.start, "/bin/notify.sh");
        assertEquals(
                mTransformation.getInvoke(),
                mTransformation.getNotification(),
                "getNotification() should return same list as getInvoke()");
    }

    @Test
    public void testAddNotificationIsSameAsAddInvoke() {
        mTransformation.addNotification(Invoke.WHEN.start, "/bin/notify.sh");
        assertEquals(1, mTransformation.getInvoke().size(), "addNotification should add an invoke");
    }

    @Test
    public void testEquals() {
        Transformation t1 = new Transformation("pegasus", "preprocess", "1.0");
        Transformation t2 = new Transformation("pegasus", "preprocess", "1.0");
        assertTrue(t1.equals(t2), "Transformations with same values should be equal");
    }

    @Test
    public void testNotEqualsOnName() {
        Transformation t1 = new Transformation("pegasus", "preprocess", "1.0");
        Transformation t2 = new Transformation("pegasus", "other", "1.0");
        assertFalse(t1.equals(t2), "Transformations with different names should not be equal");
    }

    @Test
    public void testHashCode() {
        Transformation t1 = new Transformation("pegasus", "preprocess", "1.0");
        Transformation t2 = new Transformation("pegasus", "preprocess", "1.0");
        assertEquals(
                t1.hashCode(), t2.hashCode(), "Equal transformations should have same hash code");
    }

    @Test
    public void testToString() {
        String str = mTransformation.toString();
        assertTrue(str.contains("pegasus"), "toString should contain namespace");
        assertTrue(str.contains("preprocess"), "toString should contain name");
        assertTrue(str.contains("1.0"), "toString should contain version");
    }

    @Test
    public void testCopyConstructor() {
        mTransformation.uses(new File("input.txt"));
        mTransformation.addInvoke(Invoke.WHEN.start, "/bin/notify.sh");
        Transformation copy = new Transformation(mTransformation);
        assertEquals(mTransformation.getName(), copy.getName(), "Copy should have same name");
        assertEquals(
                mTransformation.getNamespace(),
                copy.getNamespace(),
                "Copy should have same namespace");
        assertEquals(1, copy.getUses().size(), "Copy should have same uses");
        assertEquals(1, copy.getInvoke().size(), "Copy should have same invokes");
    }

    @Test
    public void testXMLSerializationWithUses() {
        mTransformation.uses(new Executable("pegasus", "preprocess", "1.0"));
        StringWriter sw = new StringWriter();
        XMLWriter writer = new XMLWriter(sw);
        mTransformation.toXML(writer, 0);
        String result = sw.toString();
        assertTrue(
                result.contains("transformation"), "XML should contain 'transformation' element");
        assertTrue(result.contains("preprocess"), "XML should contain the name");
    }

    @Test
    public void testXMLSerializationEmptyUsesProducesNoOutput() {
        StringWriter sw = new StringWriter();
        XMLWriter writer = new XMLWriter(sw);
        mTransformation.toXML(writer, 0);
        String result = sw.toString();
        assertFalse(
                result.contains("transformation"),
                "XML should not contain 'transformation' element when uses is empty");
    }
}
