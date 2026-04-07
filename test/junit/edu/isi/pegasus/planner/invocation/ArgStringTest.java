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

import java.io.StringWriter;
import org.junit.jupiter.api.Test;

/** Tests for ArgString invocation class. */
public class ArgStringTest {

    @Test
    public void testExtendsArguments() {
        assertThat(Arguments.class.isAssignableFrom(ArgString.class), is(true));
    }

    @Test
    public void testImplementsHasText() {
        assertThat(HasText.class.isAssignableFrom(ArgString.class), is(true));
    }

    @Test
    public void testDefaultConstructorNullValue() {
        ArgString as = new ArgString();
        assertThat(as.getValue(), nullValue());
        assertThat(as.getExecutable(), nullValue());
    }

    @Test
    public void testConstructorWithExecutable() {
        ArgString as = new ArgString("/bin/ls");
        assertThat(as.getExecutable(), is("/bin/ls"));
        assertThat(as.getValue(), nullValue());
    }

    @Test
    public void testConstructorWithExecutableAndValue() {
        ArgString as = new ArgString("/bin/ls", "-la /tmp");
        assertThat(as.getExecutable(), is("/bin/ls"));
        assertThat(as.getValue(), is("-la /tmp"));
    }

    @Test
    public void testAppendValue() {
        ArgString as = new ArgString();
        as.appendValue("-a");
        as.appendValue(" -b");
        assertThat(as.getValue(), is("-a -b"));
    }

    @Test
    public void testSetValue() {
        ArgString as = new ArgString();
        as.setValue("--verbose");
        assertThat(as.getValue(), is("--verbose"));
    }

    @Test
    public void testSetValueReplacesPreviouslyAppendedText() {
        ArgString as = new ArgString();
        as.appendValue("--old");
        as.setValue("--new");

        assertThat(as.getValue(), is("--new"));
    }

    @Test
    public void testAppendNullIsNoop() {
        ArgString as = new ArgString("/bin/echo", "hello");
        as.appendValue(null);

        assertThat(as.getValue(), is("hello"));
    }

    @Test
    public void testToXMLContainsArgumentsTag() {
        ArgString as = new ArgString("/usr/bin/myprog", "--flag");
        String xml = as.toXML("");
        assertThat(xml, containsString("<arguments"));
        assertThat(xml, containsString("--flag"));
    }

    @Test
    public void testToXMLNoContentWhenValueNull() {
        ArgString as = new ArgString("/usr/bin/myprog");
        String xml = as.toXML("");
        assertThat(xml, containsString("/>"));
    }

    @Test
    public void testToXMLStringEscapesExecutableAndValue() {
        ArgString as = new ArgString("/usr/bin/a&b", "1 < 2");

        String xml = as.toXML("");

        assertThat(xml, containsString("executable=\"/usr/bin/a&amp;b\""));
        assertThat(xml, containsString("1 &lt; 2"));
    }

    @Test
    public void testToXMLWriterUsesNamespaceAndSelfClosingTag() throws Exception {
        ArgString as = new ArgString("/usr/bin/myprog");
        StringWriter sw = new StringWriter();

        as.toXML(sw, null, "inv");

        String xml = sw.toString();
        assertThat(xml, containsString("<inv:arguments"));
        assertThat(xml, containsString("executable=\"/usr/bin/myprog\""));
        assertThat(xml, containsString("/>"));
    }
}
