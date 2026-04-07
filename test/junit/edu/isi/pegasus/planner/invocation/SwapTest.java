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

/** Tests for Swap invocation class. */
public class SwapTest {

    @Test
    public void testExtendsMachineInfo() {
        assertThat(MachineInfo.class.isAssignableFrom(Swap.class), is(true));
    }

    @Test
    public void testElementName() {
        assertThat(Swap.ELEMENT_NAME, is("swap"));
    }

    @Test
    public void testDefaultConstructor() {
        Swap s = new Swap();
        assertThat(s, is(org.hamcrest.Matchers.notNullValue()));
    }

    @Test
    public void testGetElementName() {
        Swap s = new Swap();
        assertThat(s.getElementName(), is("swap"));
    }

    @Test
    public void testAddAndGetAttribute() {
        Swap s = new Swap();
        s.addAttribute("total", "8192");
        assertThat(s.get("total"), is("8192"));
    }

    @Test
    public void testGetMissingAttributeReturnsNull() {
        Swap s = new Swap();
        assertThat(s.get("nosuchattr"), is(nullValue()));
    }

    @Test
    public void testAddAttributesAddsMultipleEntries() {
        Swap s = new Swap();

        s.addAttributes(Arrays.asList("total", "free"), Arrays.asList("8192", "4096"));

        assertThat(s.get("total"), is("8192"));
        assertThat(s.get("free"), is("4096"));
    }

    @Test
    public void testGetAttributeKeysIteratorIncludesAddedKeys() {
        Swap s = new Swap();
        s.addAttribute("total", "8192");
        s.addAttribute("free", "4096");

        Set<String> keys = new HashSet<String>();
        for (Iterator<String> it = s.getAttributeKeysIterator(); it.hasNext(); ) {
            keys.add(it.next());
        }

        assertThat(keys, is(new HashSet<String>(Arrays.asList("total", "free"))));
    }

    @Test
    public void testToStringWriterThrowsIOException() {
        Swap s = new Swap();

        IOException exception =
                assertThrows(IOException.class, () -> s.toString(new StringWriter()));

        assertThat(
                exception.getMessage(),
                is("method not implemented, please contact pegasus-support@isi.edu"));
    }

    @Test
    public void testToXMLUsesNamespaceAndSelfClosingTag() throws IOException {
        Swap s = new Swap();
        s.addAttribute("total", "8192");

        StringWriter writer = new StringWriter();
        s.toXML(writer, "  ", "inv");

        String xml = writer.toString();
        assertThat(xml.startsWith("  <inv:swap"), is(true));
        assertThat(xml, containsString("total=\"8192\""));
        assertThat(xml.endsWith("/>" + System.lineSeparator()), is(true));
    }
}
