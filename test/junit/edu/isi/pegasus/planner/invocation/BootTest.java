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

/** Tests for Boot invocation class. */
public class BootTest {

    @Test
    public void testExtendsMachineInfo() {
        assertTrue(MachineInfo.class.isAssignableFrom(Boot.class));
    }

    @Test
    public void testImplementsHasText() {
        assertTrue(HasText.class.isAssignableFrom(Boot.class));
    }

    @Test
    public void testElementName() {
        assertEquals("boot", Boot.ELEMENT_NAME);
    }

    @Test
    public void testDefaultConstructorNullValue() {
        Boot b = new Boot();
        assertNull(b.getValue());
    }

    @Test
    public void testConstructorWithValue() {
        Boot b = new Boot("2024-01-01T00:00:00");
        assertEquals("2024-01-01T00:00:00", b.getValue());
    }

    @Test
    public void testConstructorNullThrows() {
        assertThrows(NullPointerException.class, () -> new Boot(null));
    }

    @Test
    public void testAppendValue() {
        Boot b = new Boot();
        b.appendValue("2024");
        b.appendValue("-01-01");
        assertEquals("2024-01-01", b.getValue());
    }

    @Test
    public void testGetElementName() {
        Boot b = new Boot();
        assertEquals("boot", b.getElementName());
    }
}
