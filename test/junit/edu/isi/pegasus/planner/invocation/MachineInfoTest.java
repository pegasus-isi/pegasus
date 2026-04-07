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
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.io.StringWriter;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Iterator;
import org.junit.jupiter.api.Test;

/** Tests for MachineInfo abstract class. */
public class MachineInfoTest {

    @Test
    public void testExtendsInvocation() {
        assertThat(Invocation.class.isAssignableFrom(MachineInfo.class), is(true));
    }

    @Test
    public void testMachineInfoIsAbstract() {
        assertThat(Modifier.isAbstract(MachineInfo.class.getModifiers()), is(true));
    }

    @Test
    public void testLoadExtendsMachineInfo() {
        assertThat(MachineInfo.class.isAssignableFrom(Load.class), is(true));
    }

    @Test
    public void testRAMExtendsMachineInfo() {
        assertThat(MachineInfo.class.isAssignableFrom(RAM.class), is(true));
    }

    @Test
    public void testCPUExtendsMachineInfo() {
        assertThat(MachineInfo.class.isAssignableFrom(CPU.class), is(true));
    }

    @Test
    public void testAddAndGetAttribute() {
        // Use RAM (concrete subclass) to test MachineInfo's addAttribute/get
        RAM ram = new RAM();
        ram.addAttribute("total", "8192");
        assertThat(ram.get("total"), is("8192"));
    }

    @Test
    public void testGetMissingAttributeReturnsNull() {
        RAM ram = new RAM();
        assertThat(ram.get("nonexistent"), is(nullValue()));
    }

    @Test
    public void testGetElementNameIsAbstractMethod() throws Exception {
        java.lang.reflect.Method m = MachineInfo.class.getDeclaredMethod("getElementName");
        assertThat(Modifier.isAbstract(m.getModifiers()), is(true));
    }

    @Test
    public void testAddAttributesAddsMultipleEntries() {
        RAM ram = new RAM();
        ram.addAttributes(Arrays.asList("total", "free"), Arrays.asList("8192", "4096"));

        assertThat(ram.get("total"), is("8192"));
        assertThat(ram.get("free"), is("4096"));
    }

    @Test
    public void testGetAttributeKeysIteratorIncludesAddedKeys() {
        RAM ram = new RAM();
        ram.addAttribute("a", "1");
        ram.addAttribute("b", "2");

        Iterator<String> iterator = ram.getAttributeKeysIterator();
        assertThat(iterator.hasNext(), is(true));
        String first = iterator.next();
        String second = iterator.next();
        assertThat(first, not(second));
        assertThat(Arrays.asList("a", "b").contains(first), is(true));
        assertThat(Arrays.asList("a", "b").contains(second), is(true));
    }

    @Test
    public void testToStringWriterThrowsIOException() {
        RAM ram = new RAM();
        StringWriter sw = new StringWriter();

        IOException exception = assertThrows(IOException.class, () -> ram.toString(sw));
        assertThat(exception.getMessage(), containsString("method not implemented"));
    }

    @Test
    public void testToXMLForNonHasTextSubclassUsesSelfClosingTag() throws Exception {
        Load load = new Load();
        load.addAttribute("avg", "0.5");
        StringWriter sw = new StringWriter();

        load.toXML(sw, null, "inv");

        String xml = sw.toString();
        assertThat(xml, containsString("<inv:load"));
        assertThat(xml, containsString("avg=\"0.5\""));
        assertThat(xml, containsString("/>"));
    }

    @Test
    public void testToXMLForHasTextSubclassIncludesEscapedText() throws Exception {
        Boot boot = new Boot("a&b");
        boot.addAttribute("source", "bios");
        StringWriter sw = new StringWriter();

        boot.toXML(sw, null, "inv");

        String xml = sw.toString();
        assertThat(xml, containsString("<inv:boot"));
        assertThat(xml, containsString(">a&amp;b</inv:boot>"));
    }
}
