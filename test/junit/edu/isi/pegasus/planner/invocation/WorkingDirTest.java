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

/** Tests for WorkingDir invocation class. */
public class WorkingDirTest {

    @Test
    public void testExtendsInvocation() {
        assertThat(Invocation.class.isAssignableFrom(WorkingDir.class), is(true));
    }

    @Test
    public void testImplementsHasText() {
        assertThat(HasText.class.isAssignableFrom(WorkingDir.class), is(true));
    }

    @Test
    public void testDefaultConstructorNullValue() {
        WorkingDir wd = new WorkingDir();
        assertThat(wd.getValue(), is(nullValue()));
    }

    @Test
    public void testConstructorWithValue() {
        WorkingDir wd = new WorkingDir("/scratch/run1");
        assertThat(wd.getValue(), is("/scratch/run1"));
    }

    @Test
    public void testConstructorNullThrows() {
        assertThrows(NullPointerException.class, () -> new WorkingDir(null));
    }

    @Test
    public void testSetAndGetValue() {
        WorkingDir wd = new WorkingDir();
        wd.setValue("/home/user/jobs");
        assertThat(wd.getValue(), is("/home/user/jobs"));
    }

    @Test
    public void testAppendValue() {
        WorkingDir wd = new WorkingDir("/home/");
        wd.appendValue("user");
        assertThat(wd.getValue(), is("/home/user"));
    }

    @Test
    public void testToXMLStringContainsCwd() {
        WorkingDir wd = new WorkingDir("/scratch/workflow");
        String xml = wd.toXML("");
        assertThat(xml, containsString("<cwd>"));
        assertThat(xml, containsString("/scratch/workflow"));
        assertThat(xml, containsString("</cwd>"));
    }

    @Test
    public void testToXMLStringEmptyWhenNullValue() {
        WorkingDir wd = new WorkingDir();
        String xml = wd.toXML("");
        assertThat(xml, is(""));
    }

    @Test
    public void testAppendNullIsNoop() {
        WorkingDir wd = new WorkingDir("/scratch");

        wd.appendValue(null);

        assertThat(wd.getValue(), is("/scratch"));
    }

    @Test
    public void testSetValueNullClearsValue() {
        WorkingDir wd = new WorkingDir("/scratch");

        wd.setValue(null);

        assertThat(wd.getValue(), is(nullValue()));
    }

    @Test
    public void testToStringWriterThrowsIOException() {
        WorkingDir wd = new WorkingDir();

        IOException exception =
                assertThrows(IOException.class, () -> wd.toString(new StringWriter()));

        assertThat(
                exception.getMessage(),
                is("method not implemented, please contact vds-support@griphyn.org"));
    }

    @Test
    public void testToXMLWriterUsesNamespaceAndEscapesValue() throws IOException {
        WorkingDir wd = new WorkingDir("/scratch/<run>");
        StringWriter writer = new StringWriter();

        wd.toXML(writer, "  ", "inv");

        String xml = writer.toString();
        assertThat(xml.startsWith("  <inv:cwd>"), is(true));
        assertThat(xml, containsString("/scratch/&lt;run&gt;"));
        assertThat(xml.endsWith("</inv:cwd>" + System.lineSeparator()), is(true));
    }

    @Test
    public void testToXMLWriterWithNullValueWritesNothing() throws IOException {
        WorkingDir wd = new WorkingDir();
        StringWriter writer = new StringWriter();

        wd.toXML(writer, "", "inv");

        assertThat(writer.toString(), is(""));
    }
}
