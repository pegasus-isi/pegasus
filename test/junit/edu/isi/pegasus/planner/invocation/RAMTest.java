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

/** Tests for RAM invocation class. */
public class RAMTest {

    @Test
    public void testExtendsMachineInfo() {
        assertTrue(MachineInfo.class.isAssignableFrom(RAM.class));
    }

    @Test
    public void testElementName() {
        assertEquals("ram", RAM.ELEMENT_NAME);
    }

    @Test
    public void testDefaultConstructor() {
        RAM r = new RAM();
        assertNotNull(r);
    }

    @Test
    public void testGetElementName() {
        RAM r = new RAM();
        assertEquals("ram", r.getElementName());
    }

    @Test
    public void testAddAndGetAttribute() {
        RAM r = new RAM();
        r.addAttribute("total", "16384");
        assertEquals("16384", r.get("total"));
    }

    @Test
    public void testGetMissingAttributeReturnsNull() {
        RAM r = new RAM();
        assertNull(r.get("nonexistent"));
    }

    @Test
    public void testIsNotAbstract() {
        assertFalse(java.lang.reflect.Modifier.isAbstract(RAM.class.getModifiers()));
    }
}
