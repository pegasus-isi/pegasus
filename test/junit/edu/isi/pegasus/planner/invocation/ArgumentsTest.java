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

/** Tests for the Arguments abstract class structure. */
public class ArgumentsTest {

    @Test
    public void testIsAbstract() {
        assertTrue(Modifier.isAbstract(Arguments.class.getModifiers()));
    }

    @Test
    public void testExtendsInvocation() {
        assertTrue(Invocation.class.isAssignableFrom(Arguments.class));
    }

    @Test
    public void testArgStringIsConcreteSubtype() {
        assertTrue(Arguments.class.isAssignableFrom(ArgString.class));
        assertFalse(Modifier.isAbstract(ArgString.class.getModifiers()));
    }

    @Test
    public void testArgVectorIsConcreteSubtype() {
        assertTrue(Arguments.class.isAssignableFrom(ArgVector.class));
        assertFalse(Modifier.isAbstract(ArgVector.class.getModifiers()));
    }

    @Test
    public void testArgStringSetExecutable() {
        ArgString as = new ArgString();
        as.setExecutable("/bin/bash");
        assertEquals("/bin/bash", as.getExecutable());
    }

    @Test
    public void testArgVectorGetValueWithEntries() {
        ArgVector av = new ArgVector("/bin/test");
        av.setValue(0, "arg0");
        av.setValue(1, "arg1");
        String value = av.getValue();
        assertTrue(value.contains("arg0"));
        assertTrue(value.contains("arg1"));
    }

    @Test
    public void testArgVectorDefaultConstructor() {
        ArgVector av = new ArgVector();
        assertNull(av.getExecutable());
        assertEquals("", av.getValue());
    }

    @Test
    public void testGetValueIsAbstractlyDeclared() throws Exception {
        java.lang.reflect.Method m = Arguments.class.getDeclaredMethod("getValue");
        assertTrue(Modifier.isAbstract(m.getModifiers()));
    }
}
