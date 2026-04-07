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

/** Tests for Task invocation class. */
public class TaskTest {

    @Test
    public void testExtendsMachineInfo() {
        assertThat(MachineInfo.class.isAssignableFrom(Task.class), is(true));
    }

    @Test
    public void testElementName() {
        assertThat(Task.ELEMENT_NAME, is("task"));
    }

    @Test
    public void testDefaultConstructor() {
        Task t = new Task();
        assertThat(t, is(org.hamcrest.Matchers.notNullValue()));
    }

    @Test
    public void testGetElementName() {
        Task t = new Task();
        assertThat(t.getElementName(), is("task"));
    }

    @Test
    public void testAddAndGetAttribute() {
        Task t = new Task();
        t.addAttribute("count", "8");
        assertThat(t.get("count"), is("8"));
    }

    @Test
    public void testGetMissingAttributeReturnsNull() {
        Task t = new Task();
        assertThat(t.get("missing"), is(nullValue()));
    }

    @Test
    public void testAddAttributesAddsMultipleEntries() {
        Task t = new Task();

        t.addAttributes(Arrays.asList("count", "state"), Arrays.asList("8", "ready"));

        assertThat(t.get("count"), is("8"));
        assertThat(t.get("state"), is("ready"));
    }

    @Test
    public void testGetAttributeKeysIteratorIncludesAddedKeys() {
        Task t = new Task();
        t.addAttribute("count", "8");
        t.addAttribute("state", "ready");

        Set<String> keys = new HashSet<String>();
        for (Iterator<String> it = t.getAttributeKeysIterator(); it.hasNext(); ) {
            keys.add(it.next());
        }

        assertThat(keys, is(new HashSet<String>(Arrays.asList("count", "state"))));
    }

    @Test
    public void testToStringWriterThrowsIOException() {
        Task t = new Task();

        IOException exception =
                assertThrows(IOException.class, () -> t.toString(new StringWriter()));

        assertThat(
                exception.getMessage(),
                is("method not implemented, please contact pegasus-support@isi.edu"));
    }

    @Test
    public void testToXMLUsesNamespaceAndSelfClosingTag() throws IOException {
        Task t = new Task();
        t.addAttribute("count", "8");

        StringWriter writer = new StringWriter();
        t.toXML(writer, "  ", "inv");

        String xml = writer.toString();
        assertThat(xml.startsWith("  <inv:task"), is(true));
        assertThat(xml, containsString("count=\"8\""));
        assertThat(xml.endsWith("/>" + System.lineSeparator()), is(true));
    }
}
