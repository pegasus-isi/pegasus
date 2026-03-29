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

import java.io.StringWriter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Tests for ArgEntry invocation class. */
public class ArgEntryTest {

    private ArgEntry mEntry;

    @BeforeEach
    public void setUp() {
        mEntry = new ArgEntry();
    }

    @AfterEach
    public void tearDown() {
        mEntry = null;
    }

    @Test
    public void testImplementsHasText() {
        assertTrue(HasText.class.isAssignableFrom(ArgEntry.class));
    }

    @Test
    public void testDefaultConstructorPosition() {
        assertEquals(-1, mEntry.getPosition());
    }

    @Test
    public void testDefaultConstructorNullValue() {
        assertNull(mEntry.getValue());
    }

    @Test
    public void testConstructorWithPosition() {
        ArgEntry e = new ArgEntry(3);
        assertEquals(3, e.getPosition());
        assertNull(e.getValue());
    }

    @Test
    public void testConstructorWithPositionAndValue() {
        ArgEntry e = new ArgEntry(1, "hello");
        assertEquals(1, e.getPosition());
        assertEquals("hello", e.getValue());
    }

    @Test
    public void testSetAndGetPosition() {
        mEntry.setPosition(5);
        assertEquals(5, mEntry.getPosition());
    }

    @Test
    public void testSetAndGetValue() {
        mEntry.setValue("myarg");
        assertEquals("myarg", mEntry.getValue());
    }

    @Test
    public void testAppendValue() {
        mEntry.appendValue("foo");
        mEntry.appendValue("bar");
        assertEquals("foobar", mEntry.getValue());
    }

    @Test
    public void testAppendNullIsNoop() {
        mEntry.setValue("test");
        mEntry.appendValue(null);
        assertEquals("test", mEntry.getValue());
    }

    @Test
    public void testToXMLContainsIdAndValue() throws Exception {
        mEntry.setPosition(2);
        mEntry.setValue("argval");
        StringWriter sw = new StringWriter();
        mEntry.toXML(sw, "", null);
        String xml = sw.toString();
        assertTrue(xml.contains("id=\"2\""));
        assertTrue(xml.contains("argval"));
    }
}
