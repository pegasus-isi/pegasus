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
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.io.StringWriter;
import java.util.Iterator;
import org.junit.jupiter.api.Test;

/** Tests for MachineSpecific invocation class. */
public class MachineSpecificTest {

    @Test
    public void testExtendsInvocation() {
        assertThat(Invocation.class.isAssignableFrom(MachineSpecific.class), is(true));
    }

    @Test
    public void testConstructorWithTag() {
        MachineSpecific ms = new MachineSpecific("linux");
        assertThat(ms.getTag(), is("linux"));
    }

    @Test
    public void testSetAndGetTag() {
        MachineSpecific ms = new MachineSpecific("darwin");
        ms.setTag("sunos");
        assertThat(ms.getTag(), is("sunos"));
    }

    @Test
    public void testAddMachineInfo() {
        MachineSpecific ms = new MachineSpecific("linux");
        RAM ram = new RAM();
        ram.addAttribute("total", "16384");
        ms.addMachineInfo(ram);
        // iterator should have at least one entry
        Iterator<MachineInfo> it = ms.getMachineInfoIterator();
        assertThat(it.hasNext(), is(true));
    }

    @Test
    public void testEmptyContentsInitially() {
        MachineSpecific ms = new MachineSpecific("basic");
        Iterator<MachineInfo> it = ms.getMachineInfoIterator();
        assertThat(it.hasNext(), is(false));
    }

    @Test
    public void testAddMultipleMachineInfoElements() {
        MachineSpecific ms = new MachineSpecific("linux");
        ms.addMachineInfo(new RAM());
        ms.addMachineInfo(new Load());
        ms.addMachineInfo(new Swap());
        // count by draining the iterator
        int count = 0;
        for (Iterator<MachineInfo> it = ms.getMachineInfoIterator(); it.hasNext(); it.next()) {
            count++;
        }
        assertThat(count, is(3));
    }

    @Test
    public void testGetElementNameMatchesTag() {
        MachineSpecific ms = new MachineSpecific("linux");
        assertThat(ms.getElementName(), is("linux"));
    }

    @Test
    public void testToStringWriterThrowsIOException() {
        MachineSpecific ms = new MachineSpecific("linux");

        IOException exception =
                assertThrows(IOException.class, () -> ms.toString(new StringWriter()));

        assertThat(
                exception.getMessage(),
                is("method not implemented, please contact pegasus-support@isi.edu"));
    }

    @Test
    public void testMachineInfoIteratorPreservesInsertionOrder() {
        MachineSpecific ms = new MachineSpecific("linux");
        RAM ram = new RAM();
        Load load = new Load();
        Swap swap = new Swap();

        ms.addMachineInfo(ram);
        ms.addMachineInfo(load);
        ms.addMachineInfo(swap);

        Iterator<MachineInfo> iterator = ms.getMachineInfoIterator();
        assertThat(iterator.next(), is(sameInstance(ram)));
        assertThat(iterator.next(), is(sameInstance(load)));
        assertThat(iterator.next(), is(sameInstance(swap)));
        assertThat(iterator.hasNext(), is(false));
    }

    @Test
    public void testToXMLWithoutMachineInfoUsesSelfClosingTag() throws IOException {
        MachineSpecific ms = new MachineSpecific("basic");
        StringWriter writer = new StringWriter();

        ms.toXML(writer, "", "inv");

        assertThat(writer.toString(), is("<inv:basic/>" + System.lineSeparator()));
    }

    @Test
    public void testToXMLWithMachineInfoUsesNamespaceAndNestedContent() throws IOException {
        MachineSpecific ms = new MachineSpecific("linux");
        Boot boot = new Boot();
        boot.addAttribute("method", "bios");
        boot.setValue("usable<15GB");
        ms.addMachineInfo(boot);

        StringWriter writer = new StringWriter();
        ms.toXML(writer, "  ", "inv");

        String xml = writer.toString();
        assertThat(xml.startsWith("  <inv:linux>" + System.lineSeparator()), is(true));
        assertThat(xml, containsString("<inv:boot"));
        assertThat(xml, containsString("method=\"bios\""));
        assertThat(xml, containsString("usable&lt;15GB</inv:boot>"));
        assertThat(xml.endsWith("  </inv:linux>" + System.lineSeparator()), is(true));
    }
}
