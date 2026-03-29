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

/** Tests for the Invoke class. */
public class InvokeTest {

    private Invoke mInvoke;

    @BeforeEach
    public void setUp() {
        mInvoke = new Invoke(Invoke.WHEN.start, "/bin/notify.sh");
    }

    @Test
    public void testInstantiation() {
        assertNotNull(mInvoke, "Invoke should be instantiatable");
    }

    @Test
    public void testGetWhen() {
        assertEquals("start", mInvoke.getWhen(), "getWhen() should return 'start'");
    }

    @Test
    public void testGetWhat() {
        assertEquals("/bin/notify.sh", mInvoke.getWhat(), "getWhat() should return the command");
    }

    @Test
    public void testSetWhenNormalizesAtEnd() {
        mInvoke.setWhen(Invoke.WHEN.at_end);
        assertEquals("end", mInvoke.getWhen(), "at_end should be normalized to 'end'");
    }

    @Test
    public void testSetWhenNormalizesOnError() {
        mInvoke.setWhen(Invoke.WHEN.on_error);
        assertEquals("error", mInvoke.getWhen(), "on_error should be normalized to 'error'");
    }

    @Test
    public void testSetWhenNormalizesOnSuccess() {
        mInvoke.setWhen(Invoke.WHEN.on_success);
        assertEquals("success", mInvoke.getWhen(), "on_success should be normalized to 'success'");
    }

    @Test
    public void testSetWhenNever() {
        mInvoke.setWhen(Invoke.WHEN.never);
        assertEquals("never", mInvoke.getWhen(), "WHEN.never should remain 'never'");
    }

    @Test
    public void testSetWhenAll() {
        mInvoke.setWhen(Invoke.WHEN.all);
        assertEquals("all", mInvoke.getWhen(), "WHEN.all should remain 'all'");
    }

    @Test
    public void testSetWhat() {
        mInvoke.setWhat("/usr/local/bin/other.sh");
        assertEquals(
                "/usr/local/bin/other.sh", mInvoke.getWhat(), "setWhat should update the command");
    }

    @Test
    public void testClone() {
        Invoke clone = mInvoke.clone();
        assertNotSame(mInvoke, clone, "Clone should be a different object");
        assertEquals(mInvoke.getWhen(), clone.getWhen(), "Clone should have same when");
        assertEquals(mInvoke.getWhat(), clone.getWhat(), "Clone should have same what");
    }

    @Test
    public void testCopyConstructor() {
        Invoke copy = new Invoke(mInvoke);
        assertEquals(mInvoke.getWhen(), copy.getWhen(), "Copy should have same when");
        assertEquals(mInvoke.getWhat(), copy.getWhat(), "Copy should have same what");
    }

    @Test
    public void testEqualsWithSameValues() {
        Invoke i1 = new Invoke(Invoke.WHEN.start, "/bin/notify.sh");
        Invoke i2 = new Invoke(Invoke.WHEN.start, "/bin/notify.sh");
        assertTrue(i1.equals(i2), "Invokes with same values should be equal");
    }

    @Test
    public void testEqualsWithDifferentValues() {
        Invoke i1 = new Invoke(Invoke.WHEN.start, "/bin/notify.sh");
        Invoke i2 = new Invoke(Invoke.WHEN.end, "/bin/notify.sh");
        assertFalse(i1.equals(i2), "Invokes with different when should not be equal");
    }

    @Test
    public void testToString() {
        String str = mInvoke.toString();
        assertTrue(str.contains("start"), "toString should contain the when value");
        assertTrue(str.contains("/bin/notify.sh"), "toString should contain the what value");
    }

    @Test
    public void testXMLSerialization() {
        StringWriter sw = new StringWriter();
        XMLWriter writer = new XMLWriter(sw);
        mInvoke.toXML(writer);
        String result = sw.toString();
        assertTrue(result.contains("invoke"), "XML should contain 'invoke' element");
        assertTrue(result.contains("start"), "XML should contain the when value");
        assertTrue(result.contains("/bin/notify.sh"), "XML should contain the what value");
    }

    @Test
    public void testWhenEnumValues() {
        assertNotNull(Invoke.WHEN.never);
        assertNotNull(Invoke.WHEN.start);
        assertNotNull(Invoke.WHEN.success);
        assertNotNull(Invoke.WHEN.error);
        assertNotNull(Invoke.WHEN.end);
        assertNotNull(Invoke.WHEN.all);
    }
}
