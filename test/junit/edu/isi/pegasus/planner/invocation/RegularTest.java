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

/** Tests for Regular invocation class. */
public class RegularTest {

    @Test
    public void testExtendsFile() {
        assertThat(File.class.isAssignableFrom(Regular.class), is(true));
    }

    @Test
    public void testImplementsHasFilename() {
        assertThat(HasFilename.class.isAssignableFrom(Regular.class), is(true));
    }

    @Test
    public void testDefaultConstructorNullFilename() {
        Regular r = new Regular();
        assertThat(r.getFilename(), is(nullValue()));
    }

    @Test
    public void testConstructorWithFilename() {
        Regular r = new Regular("/tmp/output.txt");
        assertThat(r.getFilename(), is("/tmp/output.txt"));
    }

    @Test
    public void testSetAndGetFilename() {
        Regular r = new Regular();
        r.setFilename("/data/file.dat");
        assertThat(r.getFilename(), is("/data/file.dat"));
    }

    @Test
    public void testToXMLContainsNameAttribute() throws Exception {
        Regular r = new Regular("/tmp/myfile.txt");
        StringWriter sw = new StringWriter();
        r.toXML(sw, "", null);
        String xml = sw.toString();
        assertThat(xml, containsString("name=\"/tmp/myfile.txt\""));
        assertThat(xml, containsString("<file"));
    }

    @Test
    public void testToXMLSelfClosingWhenNoContent() throws Exception {
        Regular r = new Regular("/tmp/empty.txt");
        StringWriter sw = new StringWriter();
        r.toXML(sw, "", null);
        assertThat(sw.toString(), containsString("/>"));
    }

    @Test
    public void testSetValueReplacesHexContent() {
        Regular r = new Regular("/tmp/data.bin");
        r.appendValue("abcd");

        r.setValue("deadbeef");

        assertThat(r.getValue(), is("deadbeef"));
    }

    @Test
    public void testToStringWriterThrowsIOException() {
        Regular r = new Regular("/tmp/data.bin");

        IOException exception =
                assertThrows(IOException.class, () -> r.toString(new StringWriter()));

        assertThat(
                exception.getMessage(),
                is("method not implemented, please contact vds-support@griphyn.org"));
    }

    @Test
    public void testToXMLWithContentUsesNamespaceAndClosingTag() throws Exception {
        Regular r = new Regular("/tmp/data.bin");
        r.setValue("deadbeef");

        StringWriter sw = new StringWriter();
        r.toXML(sw, "  ", "inv");

        String xml = sw.toString();
        assertThat(xml.startsWith("  <inv:file"), is(true));
        assertThat(xml, containsString("name=\"/tmp/data.bin\""));
        assertThat(xml, containsString(">deadbeef</inv:file>"));
        assertThat(xml.endsWith(System.lineSeparator()), is(true));
    }
}
