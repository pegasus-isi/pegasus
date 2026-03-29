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

import java.lang.reflect.Method;
import org.junit.jupiter.api.Test;

/** Tests for HasDescriptor interface structure. */
public class HasDescriptorTest {

    @Test
    public void testIsInterface() {
        assertTrue(HasDescriptor.class.isInterface());
    }

    @Test
    public void testHasGetDescriptorMethod() throws Exception {
        Method m = HasDescriptor.class.getMethod("getDescriptor");
        assertNotNull(m);
        assertEquals(int.class, m.getReturnType());
    }

    @Test
    public void testHasSetDescriptorMethod() throws Exception {
        Method m = HasDescriptor.class.getMethod("setDescriptor", int.class);
        assertNotNull(m);
    }

    @Test
    public void testDescriptorImplementsInterface() {
        assertTrue(HasDescriptor.class.isAssignableFrom(Descriptor.class));
    }

    @Test
    public void testFifoImplementsInterface() {
        assertTrue(HasDescriptor.class.isAssignableFrom(Fifo.class));
    }

    @Test
    public void testDescriptorGetDescriptorDefault() {
        Descriptor d = new Descriptor();
        assertEquals(-1, d.getDescriptor());
    }

    @Test
    public void testDescriptorSetDescriptor() {
        Descriptor d = new Descriptor();
        d.setDescriptor(7);
        assertEquals(7, d.getDescriptor());
    }
}
