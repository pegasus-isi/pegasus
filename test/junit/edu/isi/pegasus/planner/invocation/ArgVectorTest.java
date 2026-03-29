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
package edu.isi.pegasus.planner.invocation;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

/** Tests for ArgVector invocation class. */
public class ArgVectorTest {

    @Test
    public void testExtendsArguments() {
        assertTrue(Arguments.class.isAssignableFrom(ArgVector.class));
    }

    @Test
    public void testDefaultConstructorEmptyValue() {
        ArgVector av = new ArgVector();
        assertEquals("", av.getValue());
    }

    @Test
    public void testConstructorWithExecutable() {
        ArgVector av = new ArgVector("/bin/echo");
        assertEquals("/bin/echo", av.getExecutable());
        assertEquals("", av.getValue());
    }

    @Test
    public void testSetValueAtPosition() {
        ArgVector av = new ArgVector();
        av.setValue(0, "first");
        av.setValue(1, "second");
        String val = av.getValue();
        assertTrue(val.contains("first"));
        assertTrue(val.contains("second"));
    }

    @Test
    public void testSetValueNegativePositionIgnored() {
        ArgVector av = new ArgVector();
        av.setValue(-1, "ignored");
        assertEquals("", av.getValue());
    }

    @Test
    public void testSetValueNullBecomesEmpty() {
        ArgVector av = new ArgVector();
        av.setValue(0, null);
        // null value stored as empty string, getValue joins non-null values
        String val = av.getValue();
        assertNotNull(val);
    }

    @Test
    public void testToXMLWithEntries() {
        ArgVector av = new ArgVector("/bin/prog");
        av.setValue(0, "arg0");
        String xml = av.toXML("");
        assertTrue(xml.contains("<argument-vector"));
        assertTrue(xml.contains("arg0"));
    }

    @Test
    public void testToXMLNoEntriesSelfClosing() {
        ArgVector av = new ArgVector("/bin/prog");
        String xml = av.toXML("");
        assertTrue(xml.contains("/>"));
    }
}
