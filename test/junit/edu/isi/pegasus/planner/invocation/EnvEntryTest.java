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

import java.io.StringWriter;
import org.junit.jupiter.api.Test;

/** Tests for EnvEntry invocation class. */
public class EnvEntryTest {

    @Test
    public void testExtendsInvocation() {
        assertTrue(Invocation.class.isAssignableFrom(EnvEntry.class));
    }

    @Test
    public void testImplementsHasText() {
        assertTrue(HasText.class.isAssignableFrom(EnvEntry.class));
    }

    @Test
    public void testDefaultConstructorNullKeyAndValue() {
        EnvEntry e = new EnvEntry();
        assertNull(e.getKey());
        assertNull(e.getValue());
    }

    @Test
    public void testConstructorWithKey() {
        EnvEntry e = new EnvEntry("PATH");
        assertEquals("PATH", e.getKey());
        assertNull(e.getValue());
    }

    @Test
    public void testConstructorWithKeyAndValue() {
        EnvEntry e = new EnvEntry("HOME", "/home/user");
        assertEquals("HOME", e.getKey());
        assertEquals("/home/user", e.getValue());
    }

    @Test
    public void testSetAndGetKey() {
        EnvEntry e = new EnvEntry();
        e.setKey("JAVA_HOME");
        assertEquals("JAVA_HOME", e.getKey());
    }

    @Test
    public void testSetAndGetValue() {
        EnvEntry e = new EnvEntry();
        e.setValue("/usr/lib/jvm/java-8");
        assertEquals("/usr/lib/jvm/java-8", e.getValue());
    }

    @Test
    public void testAppendValue() {
        EnvEntry e = new EnvEntry("VAR");
        e.appendValue("/usr");
        e.appendValue("/local");
        assertEquals("/usr/local", e.getValue());
    }

    @Test
    public void testToXMLContainsKeyAttribute() throws Exception {
        EnvEntry e = new EnvEntry("MYVAR", "myval");
        StringWriter sw = new StringWriter();
        e.toXML(sw, "", null);
        String xml = sw.toString();
        assertTrue(xml.contains("key=\"MYVAR\""));
        assertTrue(xml.contains("myval"));
    }
}
