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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.io.StringWriter;
import org.junit.jupiter.api.Test;

/** Tests for CommandLine invocation class. */
public class CommandLineTest {

    @Test
    public void testExtendsInvocation() {
        assertThat(Invocation.class.isAssignableFrom(CommandLine.class), is(true));
    }

    @Test
    public void testImplementsHasText() {
        assertThat(HasText.class.isAssignableFrom(CommandLine.class), is(true));
    }

    @Test
    public void testDefaultConstructorNullValues() {
        CommandLine cl = new CommandLine();
        assertThat(cl.getExecutable(), nullValue());
        assertThat(cl.getValue(), nullValue());
    }

    @Test
    public void testConstructorWithExecutable() {
        CommandLine cl = new CommandLine("/bin/ls");
        assertThat(cl.getExecutable(), is("/bin/ls"));
        assertThat(cl.getValue(), nullValue());
    }

    @Test
    public void testConstructorWithExecutableAndValue() {
        CommandLine cl = new CommandLine("/bin/ls", "-la /tmp");
        assertThat(cl.getExecutable(), is("/bin/ls"));
        assertThat(cl.getValue(), is("-la /tmp"));
    }

    @Test
    public void testSetAndGetExecutable() {
        CommandLine cl = new CommandLine();
        cl.setExecutable("/usr/bin/grep");
        assertThat(cl.getExecutable(), is("/usr/bin/grep"));
    }

    @Test
    public void testSetAndGetValue() {
        CommandLine cl = new CommandLine();
        cl.setValue("--help");
        assertThat(cl.getValue(), is("--help"));
    }

    @Test
    public void testAppendValue() {
        CommandLine cl = new CommandLine();
        cl.appendValue("-a");
        cl.appendValue(" -b");
        assertThat(cl.getValue(), is("-a -b"));
    }

    @Test
    public void testAppendNullIsNoop() {
        CommandLine cl = new CommandLine("/bin/echo", "hello");
        cl.appendValue(null);

        assertThat(cl.getValue(), is("hello"));
    }

    @Test
    public void testSetValueReplacesPreviouslyAppendedText() {
        CommandLine cl = new CommandLine();
        cl.appendValue("old");
        cl.setValue("new");

        assertThat(cl.getValue(), is("new"));
    }

    @Test
    public void testToXMLWithExecutableAndValue() throws Exception {
        CommandLine cl = new CommandLine("/bin/echo", "hello world");
        StringWriter sw = new StringWriter();
        cl.toXML(sw, "", null);
        String xml = sw.toString();
        assertThat(xml, containsString("executable="));
        assertThat(xml, containsString("hello world"));
    }

    @Test
    public void testBaseToStringWriterThrowsIOException() {
        CommandLine cl = new CommandLine("/bin/echo", "hello");
        StringWriter sw = new StringWriter();

        IOException exception = assertThrows(IOException.class, () -> cl.toString(sw));
        assertThat(exception.getMessage(), containsString("method not implemented"));
    }

    @Test
    public void testToXMLStringEscapesExecutableAndValue() {
        CommandLine cl = new CommandLine("/bin/a&b", "1 < 2");

        String xml = cl.toXML("");

        assertThat(xml, containsString("executable=\"/bin/a&amp;b\""));
        assertThat(xml, containsString("1 &lt; 2"));
    }

    @Test
    public void testToXMLWriterUsesNamespaceAndSelfClosingTag() throws Exception {
        CommandLine cl = new CommandLine("/bin/echo");
        StringWriter sw = new StringWriter();

        cl.toXML(sw, null, "inv");

        String xml = sw.toString();
        assertThat(xml, containsString("<inv:command-line"));
        assertThat(xml, containsString("executable=\"/bin/echo\""));
        assertThat(xml, containsString("/>"));
    }
}
