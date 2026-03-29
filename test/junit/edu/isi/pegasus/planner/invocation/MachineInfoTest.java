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

import java.lang.reflect.Modifier;
import org.junit.jupiter.api.Test;

/** Tests for MachineInfo abstract class. */
public class MachineInfoTest {

    @Test
    public void testIsAbstract() {
        assertTrue(Modifier.isAbstract(MachineInfo.class.getModifiers()));
    }

    @Test
    public void testExtendsInvocation() {
        assertTrue(Invocation.class.isAssignableFrom(MachineInfo.class));
    }

    @Test
    public void testLoadExtendsMachineInfo() {
        assertTrue(MachineInfo.class.isAssignableFrom(Load.class));
    }

    @Test
    public void testRAMExtendsMachineInfo() {
        assertTrue(MachineInfo.class.isAssignableFrom(RAM.class));
    }

    @Test
    public void testCPUExtendsMachineInfo() {
        assertTrue(MachineInfo.class.isAssignableFrom(CPU.class));
    }

    @Test
    public void testAddAndGetAttribute() {
        // Use RAM (concrete subclass) to test MachineInfo's addAttribute/get
        RAM ram = new RAM();
        ram.addAttribute("total", "8192");
        assertEquals("8192", ram.get("total"));
    }

    @Test
    public void testGetMissingAttributeReturnsNull() {
        RAM ram = new RAM();
        assertNull(ram.get("nonexistent"));
    }

    @Test
    public void testGetElementNameIsAbstractMethod() throws Exception {
        java.lang.reflect.Method m = MachineInfo.class.getDeclaredMethod("getElementName");
        assertTrue(Modifier.isAbstract(m.getModifiers()));
    }
}
