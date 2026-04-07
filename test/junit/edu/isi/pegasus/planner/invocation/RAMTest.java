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
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import org.junit.jupiter.api.Test;

/** Tests for RAM invocation class. */
public class RAMTest {

    @Test
    public void testExtendsMachineInfo() {
        assertThat(MachineInfo.class.isAssignableFrom(RAM.class), is(true));
    }

    @Test
    public void testElementName() {
        assertThat(RAM.ELEMENT_NAME, is("ram"));
    }

    @Test
    public void testDefaultConstructor() {
        RAM r = new RAM();
        assertThat(r, is(org.hamcrest.Matchers.notNullValue()));
    }

    @Test
    public void testGetElementName() {
        RAM r = new RAM();
        assertThat(r.getElementName(), is("ram"));
    }

    @Test
    public void testAddAndGetAttribute() {
        RAM r = new RAM();
        r.addAttribute("total", "16384");
        assertThat(r.get("total"), is("16384"));
    }

    @Test
    public void testGetMissingAttributeReturnsNull() {
        RAM r = new RAM();
        assertThat(r.get("nonexistent"), is(nullValue()));
    }

    @Test
    public void testAddAttributesAddsMultipleEntries() {
        RAM r = new RAM();

        r.addAttributes(Arrays.asList("total", "free"), Arrays.asList("16384", "8192"));

        assertThat(r.get("total"), is("16384"));
        assertThat(r.get("free"), is("8192"));
    }

    @Test
    public void testGetAttributeKeysIteratorIncludesAddedKeys() {
        RAM r = new RAM();
        r.addAttribute("total", "16384");
        r.addAttribute("free", "8192");

        Set<String> keys = new HashSet<String>();
        for (Iterator<String> it = r.getAttributeKeysIterator(); it.hasNext(); ) {
            keys.add(it.next());
        }

        assertThat(keys, is(new HashSet<String>(Arrays.asList("total", "free"))));
    }

    @Test
    public void testToStringWriterThrowsIOException() {
        RAM r = new RAM();

        IOException exception =
                assertThrows(IOException.class, () -> r.toString(new StringWriter()));

        assertThat(
                exception.getMessage(),
                is("method not implemented, please contact pegasus-support@isi.edu"));
    }

    @Test
    public void testToXMLUsesNamespaceAndSelfClosingTag() throws IOException {
        RAM r = new RAM();
        r.addAttribute("total", "16384");

        StringWriter writer = new StringWriter();
        r.toXML(writer, "  ", "inv");

        String xml = writer.toString();
        assertThat(xml.startsWith("  <inv:ram"), is(true));
        assertThat(xml, containsString("total=\"16384\""));
        assertThat(xml.endsWith("/>" + System.lineSeparator()), is(true));
    }
}
