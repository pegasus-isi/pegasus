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

/** Tests for Swap invocation class. */
public class SwapTest {

    @Test
    public void testExtendsMachineInfo() {
        assertTrue(MachineInfo.class.isAssignableFrom(Swap.class));
    }

    @Test
    public void testElementName() {
        assertEquals("swap", Swap.ELEMENT_NAME);
    }

    @Test
    public void testDefaultConstructor() {
        Swap s = new Swap();
        assertNotNull(s);
    }

    @Test
    public void testGetElementName() {
        Swap s = new Swap();
        assertEquals("swap", s.getElementName());
    }

    @Test
    public void testAddAndGetAttribute() {
        Swap s = new Swap();
        s.addAttribute("total", "8192");
        assertEquals("8192", s.get("total"));
    }

    @Test
    public void testGetMissingAttributeReturnsNull() {
        Swap s = new Swap();
        assertNull(s.get("nosuchattr"));
    }

    @Test
    public void testIsNotAbstract() {
        assertFalse(java.lang.reflect.Modifier.isAbstract(Swap.class.getModifiers()));
    }
}
