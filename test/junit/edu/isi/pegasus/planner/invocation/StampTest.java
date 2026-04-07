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

/** Tests for Stamp invocation class. */
public class StampTest {

    @Test
    public void testExtendsMachineInfo() {
        assertThat(MachineInfo.class.isAssignableFrom(Stamp.class), is(true));
    }

    @Test
    public void testImplementsHasText() {
        assertThat(HasText.class.isAssignableFrom(Stamp.class), is(true));
    }

    @Test
    public void testElementName() {
        assertThat(Stamp.ELEMENT_NAME, is("stamp"));
    }

    @Test
    public void testDefaultConstructorNullValue() {
        Stamp s = new Stamp();
        assertThat(s.getValue(), is(nullValue()));
    }

    @Test
    public void testConstructorWithValue() {
        Stamp s = new Stamp("2024-01-01T00:00:00");
        assertThat(s.getValue(), is("2024-01-01T00:00:00"));
    }

    @Test
    public void testConstructorNullThrows() {
        assertThrows(NullPointerException.class, () -> new Stamp(null));
    }

    @Test
    public void testSetAndGetValue() {
        Stamp s = new Stamp();
        s.setValue("2024-06-15T12:00:00");
        assertThat(s.getValue(), is("2024-06-15T12:00:00"));
    }

    @Test
    public void testAppendValue() {
        Stamp s = new Stamp("2024-");
        s.appendValue("01-01");
        assertThat(s.getValue(), is("2024-01-01"));
    }

    @Test
    public void testGetElementName() {
        Stamp s = new Stamp();
        assertThat(s.getElementName(), is("stamp"));
    }

    @Test
    public void testAppendNullIsNoop() {
        Stamp s = new Stamp("2024-01-01");

        s.appendValue(null);

        assertThat(s.getValue(), is("2024-01-01"));
    }

    @Test
    public void testSetValueNullClearsValue() {
        Stamp s = new Stamp("2024-01-01T00:00:00");

        s.setValue(null);

        assertThat(s.getValue(), is(nullValue()));
    }

    @Test
    public void testToStringWriterThrowsIOException() {
        Stamp s = new Stamp();

        IOException exception =
                assertThrows(IOException.class, () -> s.toString(new StringWriter()));

        assertThat(
                exception.getMessage(),
                is("method not implemented, please contact pegasus-support@isi.edu"));
    }

    @Test
    public void testToXMLUsesNamespaceAndEscapesValue() throws IOException {
        Stamp s = new Stamp("2024-01-01T00:00:00<UTC>");
        s.addAttribute("source", "clock");

        StringWriter writer = new StringWriter();
        s.toXML(writer, "  ", "inv");

        String xml = writer.toString();
        assertThat(xml.startsWith("  <inv:stamp"), is(true));
        assertThat(xml, containsString("source=\"clock\""));
        assertThat(xml, containsString(">2024-01-01T00:00:00&lt;UTC&gt;</inv:stamp>"));
        assertThat(xml.endsWith(System.lineSeparator()), is(true));
    }
}
