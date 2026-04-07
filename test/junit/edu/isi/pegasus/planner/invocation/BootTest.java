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

/** Tests for Boot invocation class. */
public class BootTest {

    @Test
    public void testExtendsMachineInfo() {
        assertThat(MachineInfo.class.isAssignableFrom(Boot.class), is(true));
    }

    @Test
    public void testImplementsHasText() {
        assertThat(HasText.class.isAssignableFrom(Boot.class), is(true));
    }

    @Test
    public void testElementName() {
        assertThat(Boot.ELEMENT_NAME, is("boot"));
    }

    @Test
    public void testDefaultConstructorNullValue() {
        Boot b = new Boot();
        assertThat(b.getValue(), nullValue());
    }

    @Test
    public void testConstructorWithValue() {
        Boot b = new Boot("2024-01-01T00:00:00");
        assertThat(b.getValue(), is("2024-01-01T00:00:00"));
    }

    @Test
    public void testConstructorNullThrows() {
        assertThrows(NullPointerException.class, () -> new Boot(null));
    }

    @Test
    public void testAppendValue() {
        Boot b = new Boot();
        b.appendValue("2024");
        b.appendValue("-01-01");
        assertThat(b.getValue(), is("2024-01-01"));
    }

    @Test
    public void testAppendNullIsNoop() {
        Boot b = new Boot("2024-01-01");
        b.appendValue(null);

        assertThat(b.getValue(), is("2024-01-01"));
    }

    @Test
    public void testSetValueReplacesAppendedContent() {
        Boot b = new Boot();
        b.appendValue("old");
        b.setValue("new");

        assertThat(b.getValue(), is("new"));
    }

    @Test
    public void testSetValueNullClearsValue() {
        Boot b = new Boot("2024-01-01");
        b.setValue(null);

        assertThat(b.getValue(), nullValue());
    }

    @Test
    public void testGetElementName() {
        Boot b = new Boot();
        assertThat(b.getElementName(), is("boot"));
    }

    @Test
    public void testInheritedToXMLIncludesAttributesAndEscapedValue() throws Exception {
        Boot b = new Boot("a&b");
        b.addAttribute("source", "bios&firmware");
        StringWriter sw = new StringWriter();

        b.toXML(sw, null, "inv");

        String xml = sw.toString();
        assertThat(xml, containsString("<inv:boot"));
        assertThat(xml, containsString("source=\"bios&amp;amp;firmware\""));
        assertThat(xml, containsString(">a&amp;b</inv:boot>"));
    }
}
