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
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.io.StringWriter;
import org.junit.jupiter.api.Test;

/** Tests for Machine invocation class. */
public class MachineTest {

    @Test
    public void testExtendsInvocation() {
        assertThat(Invocation.class.isAssignableFrom(Machine.class), is(true));
    }

    @Test
    public void testElementName() {
        assertThat(Machine.ELEMENT_NAME, is("machine"));
    }

    @Test
    public void testDefaultConstructorZeroPageSize() {
        Machine m = new Machine();
        assertThat(m.getPageSize(), is(0L));
    }

    @Test
    public void testSetAndGetPageSize() {
        Machine m = new Machine();
        m.setPageSize(4096L);
        assertThat(m.getPageSize(), is(4096L));
    }

    @Test
    public void testNullUnameByDefault() {
        Machine m = new Machine();
        assertThat(m.getUname(), is(nullValue()));
    }

    @Test
    public void testNullStampByDefault() {
        Machine m = new Machine();
        assertThat(m.getStamp(), is(nullValue()));
    }

    @Test
    public void testSetAndGetUname() {
        Machine m = new Machine();
        Uname u = new Uname();
        m.setUname(u);
        assertThat(m.getUname(), is(notNullValue()));
    }

    @Test
    public void testSetAndGetStamp() {
        Machine m = new Machine();
        Stamp s = new Stamp("2024-01-01T00:00:00");
        m.setStamp(s);
        assertThat(m.getStamp(), is(notNullValue()));
    }

    @Test
    public void testMachineSpecificIsNullByDefault() {
        Machine m = new Machine();
        assertThat(m.getMachineSpecific(), is(nullValue()));
    }

    @Test
    public void testSetAndGetMachineSpecific() {
        Machine m = new Machine();
        MachineSpecific specific = new MachineSpecific("linux");

        m.setMachineSpecific(specific);

        assertThat(m.getMachineSpecific(), is(sameInstance(specific)));
    }

    @Test
    public void testGetElementNameReturnsConstant() {
        assertThat(new Machine().getElementName(), is(Machine.ELEMENT_NAME));
    }

    @Test
    public void testToStringWriterThrowsIOException() {
        Machine m = new Machine();

        IOException exception =
                assertThrows(IOException.class, () -> m.toString(new StringWriter()));

        assertThat(
                exception.getMessage(),
                is("method not implemented, please contact pegasus-support@isi.edu"));
    }

    @Test
    public void testToXMLUsesNamespaceAndNestedElements() throws IOException {
        Machine machine = new Machine();
        machine.setPageSize(4096L);

        Stamp stamp = new Stamp("2024-01-01T00:00:00");
        machine.setStamp(stamp);

        Uname uname = new Uname();
        uname.addAttribute(Uname.SYSTEM_ATTRIBUTE_KEY, "linux");
        machine.setUname(uname);

        MachineSpecific specific = new MachineSpecific("basic");
        machine.setMachineSpecific(specific);

        StringWriter writer = new StringWriter();
        machine.toXML(writer, "  ", "inv");

        String xml = writer.toString();
        assertThat(
                xml.startsWith("  <inv:machine page-size=\"4096\">" + System.lineSeparator()),
                is(true));
        assertThat(xml, containsString("<inv:stamp"));
        assertThat(xml, containsString("<inv:uname"));
        assertThat(xml, containsString("<inv:basic/>"));
        assertThat(xml.endsWith("</inv:machine>" + System.lineSeparator()), is(true));
    }
}
