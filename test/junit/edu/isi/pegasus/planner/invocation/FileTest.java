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

/** Tests for File abstract invocation class structure. */
public class FileTest {

    @Test
    public void testIsAbstract() {
        assertTrue(Modifier.isAbstract(File.class.getModifiers()));
    }

    @Test
    public void testExtendsInvocation() {
        assertTrue(Invocation.class.isAssignableFrom(File.class));
    }

    @Test
    public void testImplementsHasText() {
        assertTrue(HasText.class.isAssignableFrom(File.class));
    }

    @Test
    public void testDescriptorIsConcreteSubclass() {
        assertTrue(File.class.isAssignableFrom(Descriptor.class));
        assertFalse(Modifier.isAbstract(Descriptor.class.getModifiers()));
    }

    @Test
    public void testTemporaryExtendsFile() {
        assertTrue(File.class.isAssignableFrom(Temporary.class));
    }

    @Test
    public void testRegularExtendsFile() {
        assertTrue(File.class.isAssignableFrom(Regular.class));
    }

    @Test
    public void testDescriptorAppendValueBuildsHexbyte() {
        Descriptor d = new Descriptor();
        d.appendValue("deadbeef");
        assertEquals("deadbeef", d.getValue());
    }

    @Test
    public void testDescriptorAppendNullIsNoop() {
        Descriptor d = new Descriptor();
        d.appendValue(null);
        assertNull(d.getValue());
    }
}
