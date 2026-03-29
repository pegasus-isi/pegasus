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

/** Tests for Descriptor invocation class. */
public class DescriptorTest {

    @Test
    public void testExtendsFile() {
        assertTrue(File.class.isAssignableFrom(Descriptor.class));
    }

    @Test
    public void testImplementsHasDescriptor() {
        assertTrue(HasDescriptor.class.isAssignableFrom(Descriptor.class));
    }

    @Test
    public void testDefaultConstructorDescriptorIsMinusOne() {
        Descriptor d = new Descriptor();
        assertEquals(-1, d.getDescriptor());
    }

    @Test
    public void testConstructorWithDescriptor() {
        Descriptor d = new Descriptor(2);
        assertEquals(2, d.getDescriptor());
    }

    @Test
    public void testSetAndGetDescriptor() {
        Descriptor d = new Descriptor();
        d.setDescriptor(3);
        assertEquals(3, d.getDescriptor());
    }

    @Test
    public void testAppendValue() {
        Descriptor d = new Descriptor();
        d.appendValue("hexdata");
        assertEquals("hexdata", d.getValue());
    }

    @Test
    public void testAppendNullIsNoop() {
        Descriptor d = new Descriptor();
        d.appendValue(null);
        assertNull(d.getValue());
    }
}
