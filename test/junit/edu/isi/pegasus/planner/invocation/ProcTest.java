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

/** Tests for Proc invocation class. */
public class ProcTest {

    @Test
    public void testExtendsMachineInfo() {
        assertThat(MachineInfo.class.isAssignableFrom(Proc.class), is(true));
    }

    @Test
    public void testElementName() {
        assertThat(Proc.ELEMENT_NAME, is("proc"));
    }

    @Test
    public void testDefaultConstructor() {
        Proc p = new Proc();
        assertThat(p, is(org.hamcrest.Matchers.notNullValue()));
    }

    @Test
    public void testGetElementName() {
        Proc p = new Proc();
        assertThat(p.getElementName(), is("proc"));
    }

    @Test
    public void testAddAndGetAttribute() {
        Proc p = new Proc();
        p.addAttribute("count", "4");
        assertThat(p.get("count"), is("4"));
    }

    @Test
    public void testGetMissingAttributeReturnsNull() {
        Proc p = new Proc();
        assertThat(p.get("missing"), is(nullValue()));
    }

    @Test
    public void testAddAttributesAddsMultipleEntries() {
        Proc p = new Proc();

        p.addAttributes(Arrays.asList("count", "model"), Arrays.asList("4", "x86_64"));

        assertThat(p.get("count"), is("4"));
        assertThat(p.get("model"), is("x86_64"));
    }

    @Test
    public void testGetAttributeKeysIteratorIncludesAddedKeys() {
        Proc p = new Proc();
        p.addAttribute("count", "4");
        p.addAttribute("model", "x86_64");

        Set<String> keys = new HashSet<String>();
        for (Iterator<String> it = p.getAttributeKeysIterator(); it.hasNext(); ) {
            keys.add(it.next());
        }

        assertThat(keys, is(new HashSet<String>(Arrays.asList("count", "model"))));
    }

    @Test
    public void testToStringWriterThrowsIOException() {
        Proc p = new Proc();

        IOException exception =
                assertThrows(IOException.class, () -> p.toString(new StringWriter()));

        assertThat(
                exception.getMessage(),
                is("method not implemented, please contact pegasus-support@isi.edu"));
    }

    @Test
    public void testToXMLUsesNamespaceAndSelfClosingTag() throws IOException {
        Proc p = new Proc();
        p.addAttribute("count", "4");

        StringWriter writer = new StringWriter();
        p.toXML(writer, "  ", "inv");

        String xml = writer.toString();
        assertThat(xml.startsWith("  <inv:proc"), is(true));
        assertThat(xml, containsString("count=\"4\""));
        assertThat(xml.endsWith("/>" + System.lineSeparator()), is(true));
    }
}
