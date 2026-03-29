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

/** Tests for Machine invocation class. */
public class MachineTest {

    @Test
    public void testExtendsInvocation() {
        assertTrue(Invocation.class.isAssignableFrom(Machine.class));
    }

    @Test
    public void testElementName() {
        assertEquals("machine", Machine.ELEMENT_NAME);
    }

    @Test
    public void testDefaultConstructorZeroPageSize() {
        Machine m = new Machine();
        assertEquals(0L, m.getPageSize());
    }

    @Test
    public void testSetAndGetPageSize() {
        Machine m = new Machine();
        m.setPageSize(4096L);
        assertEquals(4096L, m.getPageSize());
    }

    @Test
    public void testNullUnameByDefault() {
        Machine m = new Machine();
        assertNull(m.getUname());
    }

    @Test
    public void testNullStampByDefault() {
        Machine m = new Machine();
        assertNull(m.getStamp());
    }

    @Test
    public void testSetAndGetUname() {
        Machine m = new Machine();
        Uname u = new Uname();
        m.setUname(u);
        assertNotNull(m.getUname());
    }

    @Test
    public void testSetAndGetStamp() {
        Machine m = new Machine();
        Stamp s = new Stamp("2024-01-01T00:00:00");
        m.setStamp(s);
        assertNotNull(m.getStamp());
    }
}
