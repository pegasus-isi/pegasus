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
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.io.StringWriter;
import org.junit.jupiter.api.Test;

/** Tests for Temporary invocation class. */
public class TemporaryTest {

    @Test
    public void testExtendsFile() {
        assertThat(File.class.isAssignableFrom(Temporary.class), is(true));
    }

    @Test
    public void testImplementsHasDescriptor() {
        assertThat(HasDescriptor.class.isAssignableFrom(Temporary.class), is(true));
    }

    @Test
    public void testImplementsHasFilename() {
        assertThat(HasFilename.class.isAssignableFrom(Temporary.class), is(true));
    }

    @Test
    public void testDefaultConstructorNullFilename() {
        Temporary t = new Temporary();
        assertThat(t.getFilename(), is(nullValue()));
    }

    @Test
    public void testDefaultConstructorDescriptorMinusOne() {
        Temporary t = new Temporary();
        assertThat(t.getDescriptor(), is(-1));
    }

    @Test
    public void testConstructorWithFilenameAndDescriptor() {
        Temporary t = new Temporary("/tmp/work.tmp", 3);
        assertThat(t.getFilename(), is("/tmp/work.tmp"));
        assertThat(t.getDescriptor(), is(3));
    }

    @Test
    public void testSetAndGetFilename() {
        Temporary t = new Temporary();
        t.setFilename("/var/tmp/data.tmp");
        assertThat(t.getFilename(), is("/var/tmp/data.tmp"));
    }

    @Test
    public void testSetAndGetDescriptor() {
        Temporary t = new Temporary();
        t.setDescriptor(5);
        assertThat(t.getDescriptor(), is(5));
    }

    @Test
    public void testToXMLContainsTemporaryTag() throws Exception {
        Temporary t = new Temporary("/tmp/out.tmp", 2);
        StringWriter sw = new StringWriter();
        t.toXML(sw, "", null);
        String xml = sw.toString();
        assertThat(xml, containsString("<temporary"));
        assertThat(xml, containsString("name=\"/tmp/out.tmp\""));
        assertThat(xml, containsString("descriptor=\"2\""));
    }

    @Test
    public void testSetValueReplacesHexContent() {
        Temporary t = new Temporary("/tmp/out.tmp", 2);
        t.appendValue("abcd");

        t.setValue("deadbeef");

        assertThat(t.getValue(), is("deadbeef"));
    }

    @Test
    public void testToStringWriterThrowsIOException() {
        Temporary t = new Temporary();

        IOException exception =
                assertThrows(IOException.class, () -> t.toString(new StringWriter()));

        assertThat(
                exception.getMessage(),
                is("method not implemented, please contact vds-support@griphyn.org"));
    }

    @Test
    public void testToXMLWithContentUsesNamespaceAndClosingTag() throws Exception {
        Temporary t = new Temporary("/tmp/out.tmp", 2);
        t.setValue("deadbeef");

        StringWriter sw = new StringWriter();
        t.toXML(sw, "  ", "inv");

        String xml = sw.toString();
        assertThat(xml.startsWith("  <inv:temporary"), is(true));
        assertThat(xml, containsString("name=\"/tmp/out.tmp\""));
        assertThat(xml, containsString("descriptor=\"2\""));
        assertThat(xml, containsString(">deadbeef</inv:temporary>"));
        assertThat(xml.endsWith(System.lineSeparator()), is(true));
    }
}
