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

/** Tests for HasText interface structure. */
public class HasTextTest {

    @Test
    public void testIsInterface() {
        assertTrue(HasText.class.isInterface());
    }

    @Test
    public void testHasAppendValueMethod() throws Exception {
        Method m = HasText.class.getMethod("appendValue", String.class);
        assertNotNull(m);
    }

    @Test
    public void testHasGetValueMethod() throws Exception {
        Method m = HasText.class.getMethod("getValue");
        assertNotNull(m);
        assertEquals(String.class, m.getReturnType());
    }

    @Test
    public void testHasSetValueMethod() throws Exception {
        Method m = HasText.class.getMethod("setValue", String.class);
        assertNotNull(m);
    }

    @Test
    public void testDataImplementsInterface() {
        assertTrue(HasText.class.isAssignableFrom(Data.class));
    }

    @Test
    public void testArgEntryImplementsInterface() {
        assertTrue(HasText.class.isAssignableFrom(ArgEntry.class));
    }

    @Test
    public void testArchitectureImplementsInterface() {
        assertTrue(HasText.class.isAssignableFrom(Architecture.class));
    }

    @Test
    public void testDataAppendAndGetValue() {
        Data d = new Data();
        d.appendValue("hello");
        d.appendValue(" world");
        assertEquals("hello world", d.getValue());
    }
}
