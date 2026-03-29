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

/** Tests for CPU invocation class. */
public class CPUTest {

    @Test
    public void testExtendsMachineInfo() {
        assertTrue(MachineInfo.class.isAssignableFrom(CPU.class));
    }

    @Test
    public void testImplementsHasText() {
        assertTrue(HasText.class.isAssignableFrom(CPU.class));
    }

    @Test
    public void testElementName() {
        assertEquals("cpu", CPU.ELEMENT_NAME);
    }

    @Test
    public void testDefaultConstructorNullValue() {
        CPU cpu = new CPU();
        assertNull(cpu.getValue());
    }

    @Test
    public void testConstructorWithValue() {
        CPU cpu = new CPU("GenuineIntel");
        assertEquals("GenuineIntel", cpu.getValue());
    }

    @Test
    public void testConstructorNullThrows() {
        assertThrows(NullPointerException.class, () -> new CPU(null));
    }

    @Test
    public void testAppendValue() {
        CPU cpu = new CPU();
        cpu.appendValue("Intel");
        cpu.appendValue(" Core i7");
        assertEquals("Intel Core i7", cpu.getValue());
    }

    @Test
    public void testGetElementName() {
        CPU cpu = new CPU();
        assertEquals("cpu", cpu.getElementName());
    }

    @Test
    public void testSetValue() {
        CPU cpu = new CPU();
        cpu.setValue("AuthenticAMD");
        assertEquals("AuthenticAMD", cpu.getValue());
    }
}
