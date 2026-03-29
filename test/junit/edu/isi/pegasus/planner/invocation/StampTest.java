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

/** Tests for Stamp invocation class. */
public class StampTest {

    @Test
    public void testExtendsMachineInfo() {
        assertTrue(MachineInfo.class.isAssignableFrom(Stamp.class));
    }

    @Test
    public void testImplementsHasText() {
        assertTrue(HasText.class.isAssignableFrom(Stamp.class));
    }

    @Test
    public void testElementName() {
        assertEquals("stamp", Stamp.ELEMENT_NAME);
    }

    @Test
    public void testDefaultConstructorNullValue() {
        Stamp s = new Stamp();
        assertNull(s.getValue());
    }

    @Test
    public void testConstructorWithValue() {
        Stamp s = new Stamp("2024-01-01T00:00:00");
        assertEquals("2024-01-01T00:00:00", s.getValue());
    }

    @Test
    public void testConstructorNullThrows() {
        assertThrows(NullPointerException.class, () -> new Stamp(null));
    }

    @Test
    public void testSetAndGetValue() {
        Stamp s = new Stamp();
        s.setValue("2024-06-15T12:00:00");
        assertEquals("2024-06-15T12:00:00", s.getValue());
    }

    @Test
    public void testAppendValue() {
        Stamp s = new Stamp("2024-");
        s.appendValue("01-01");
        assertEquals("2024-01-01", s.getValue());
    }

    @Test
    public void testGetElementName() {
        Stamp s = new Stamp();
        assertEquals("stamp", s.getElementName());
    }
}
