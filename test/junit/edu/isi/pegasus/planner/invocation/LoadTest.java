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

/** Tests for Load invocation class. */
public class LoadTest {

    @Test
    public void testExtendsMachineInfo() {
        assertTrue(MachineInfo.class.isAssignableFrom(Load.class));
    }

    @Test
    public void testElementName() {
        assertEquals("load", Load.ELEMENT_NAME);
    }

    @Test
    public void testDefaultConstructor() {
        Load l = new Load();
        assertNotNull(l);
    }

    @Test
    public void testGetElementName() {
        Load l = new Load();
        assertEquals("load", l.getElementName());
    }

    @Test
    public void testNotHasText() {
        // Load does not implement HasText (unlike Boot/CPU/Stamp)
        assertFalse(HasText.class.isAssignableFrom(Load.class));
    }

    @Test
    public void testIsNotAbstract() {
        assertFalse(java.lang.reflect.Modifier.isAbstract(Load.class.getModifiers()));
    }
}
