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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Tests for ArgString invocation class. */
public class ArgStringTest {

    @BeforeEach
    public void setUp() {}

    @AfterEach
    public void tearDown() {}

    @Test
    public void testExtendsArguments() {
        assertTrue(Arguments.class.isAssignableFrom(ArgString.class));
    }

    @Test
    public void testImplementsHasText() {
        assertTrue(HasText.class.isAssignableFrom(ArgString.class));
    }

    @Test
    public void testIsNotAbstract() {
        assertFalse(Modifier.isAbstract(ArgString.class.getModifiers()));
    }

    @Test
    public void testDefaultConstructorNullValue() {
        ArgString as = new ArgString();
        assertNull(as.getValue());
        assertNull(as.getExecutable());
    }

    @Test
    public void testConstructorWithExecutable() {
        ArgString as = new ArgString("/bin/ls");
        assertEquals("/bin/ls", as.getExecutable());
        assertNull(as.getValue());
    }

    @Test
    public void testConstructorWithExecutableAndValue() {
        ArgString as = new ArgString("/bin/ls", "-la /tmp");
        assertEquals("/bin/ls", as.getExecutable());
        assertEquals("-la /tmp", as.getValue());
    }

    @Test
    public void testAppendValue() {
        ArgString as = new ArgString();
        as.appendValue("-a");
        as.appendValue(" -b");
        assertEquals("-a -b", as.getValue());
    }

    @Test
    public void testSetValue() {
        ArgString as = new ArgString();
        as.setValue("--verbose");
        assertEquals("--verbose", as.getValue());
    }

    @Test
    public void testToXMLContainsArgumentsTag() {
        ArgString as = new ArgString("/usr/bin/myprog", "--flag");
        String xml = as.toXML("");
        assertTrue(xml.contains("<arguments"));
        assertTrue(xml.contains("--flag"));
    }

    @Test
    public void testToXMLNoContentWhenValueNull() {
        ArgString as = new ArgString("/usr/bin/myprog");
        String xml = as.toXML("");
        assertTrue(xml.contains("/>"));
    }
}
