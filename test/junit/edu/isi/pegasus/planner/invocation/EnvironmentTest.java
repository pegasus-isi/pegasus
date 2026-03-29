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

/** Tests for Environment invocation class. */
public class EnvironmentTest {

    @Test
    public void testExtendsInvocation() {
        assertTrue(Invocation.class.isAssignableFrom(Environment.class));
    }

    @Test
    public void testDefaultConstructor() {
        Environment env = new Environment();
        assertNotNull(env);
    }

    @Test
    public void testAddEntryByKeyValue() {
        Environment env = new Environment();
        env.addEntry("PATH", "/usr/bin");
        // get() is the accessor method on Environment
        assertEquals("/usr/bin", env.get("PATH"));
    }

    @Test
    public void testAddEntryByEnvEntry() {
        Environment env = new Environment();
        EnvEntry e = new EnvEntry("HOME", "/home/user");
        env.addEntry(e);
        assertEquals("/home/user", env.get("HOME"));
    }

    @Test
    public void testGetMissingKeyReturnsNull() {
        Environment env = new Environment();
        assertNull(env.get("NONEXISTENT"));
    }

    @Test
    public void testAddMultipleEntries() {
        Environment env = new Environment();
        env.addEntry("A", "1");
        env.addEntry("B", "2");
        assertEquals("1", env.get("A"));
        assertEquals("2", env.get("B"));
    }

    @Test
    public void testToXMLContainsEnvEntries() throws Exception {
        Environment env = new Environment();
        env.addEntry("MYKEY", "MYVAL");
        StringWriter sw = new StringWriter();
        env.toXML(sw, "", null);
        String xml = sw.toString();
        assertTrue(xml.contains("<environment"));
    }

    @Test
    public void testAddEntryReturnsOldValue() {
        Environment env = new Environment();
        env.addEntry("KEY", "first");
        String old = env.addEntry("KEY", "second");
        assertEquals("first", old);
        assertEquals("second", env.get("KEY"));
    }

    @Test
    public void testIteratorReturnsKeys() {
        Environment env = new Environment();
        env.addEntry("Z", "26");
        env.addEntry("A", "1");
        java.util.Iterator it = env.iterator();
        assertTrue(it.hasNext(), "Iterator should have entries");
    }
}
