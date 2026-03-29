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

/** Tests for CommandLine invocation class. */
public class CommandLineTest {

    @Test
    public void testExtendsInvocation() {
        assertTrue(Invocation.class.isAssignableFrom(CommandLine.class));
    }

    @Test
    public void testImplementsHasText() {
        assertTrue(HasText.class.isAssignableFrom(CommandLine.class));
    }

    @Test
    public void testDefaultConstructorNullValues() {
        CommandLine cl = new CommandLine();
        assertNull(cl.getExecutable());
        assertNull(cl.getValue());
    }

    @Test
    public void testConstructorWithExecutable() {
        CommandLine cl = new CommandLine("/bin/ls");
        assertEquals("/bin/ls", cl.getExecutable());
        assertNull(cl.getValue());
    }

    @Test
    public void testConstructorWithExecutableAndValue() {
        CommandLine cl = new CommandLine("/bin/ls", "-la /tmp");
        assertEquals("/bin/ls", cl.getExecutable());
        assertEquals("-la /tmp", cl.getValue());
    }

    @Test
    public void testSetAndGetExecutable() {
        CommandLine cl = new CommandLine();
        cl.setExecutable("/usr/bin/grep");
        assertEquals("/usr/bin/grep", cl.getExecutable());
    }

    @Test
    public void testSetAndGetValue() {
        CommandLine cl = new CommandLine();
        cl.setValue("--help");
        assertEquals("--help", cl.getValue());
    }

    @Test
    public void testAppendValue() {
        CommandLine cl = new CommandLine();
        cl.appendValue("-a");
        cl.appendValue(" -b");
        assertEquals("-a -b", cl.getValue());
    }

    @Test
    public void testToXMLWithExecutableAndValue() throws Exception {
        CommandLine cl = new CommandLine("/bin/echo", "hello world");
        StringWriter sw = new StringWriter();
        cl.toXML(sw, "", null);
        String xml = sw.toString();
        assertTrue(xml.contains("executable="));
        assertTrue(xml.contains("hello world"));
    }
}
