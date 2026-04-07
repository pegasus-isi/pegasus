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

/** Tests for CPU invocation class. */
public class CPUTest {

    @Test
    public void testExtendsMachineInfo() {
        assertThat(MachineInfo.class.isAssignableFrom(CPU.class), is(true));
    }

    @Test
    public void testImplementsHasText() {
        assertThat(HasText.class.isAssignableFrom(CPU.class), is(true));
    }

    @Test
    public void testElementName() {
        assertThat(CPU.ELEMENT_NAME, is("cpu"));
    }

    @Test
    public void testDefaultConstructorNullValue() {
        CPU cpu = new CPU();
        assertThat(cpu.getValue(), nullValue());
    }

    @Test
    public void testConstructorWithValue() {
        CPU cpu = new CPU("GenuineIntel");
        assertThat(cpu.getValue(), is("GenuineIntel"));
    }

    @Test
    public void testConstructorNullThrows() {
        assertThrows(NullPointerException.class, () -> new CPU(null));
    }

    @Test
    public void testAppendValue() {
        CPU cpu = new CPU();
        cpu.appendValue("Intel");
        cpu.appendValue(" Core i7");
        assertThat(cpu.getValue(), is("Intel Core i7"));
    }

    @Test
    public void testGetElementName() {
        CPU cpu = new CPU();
        assertThat(cpu.getElementName(), is("cpu"));
    }

    @Test
    public void testSetValue() {
        CPU cpu = new CPU();
        cpu.setValue("AuthenticAMD");
        assertThat(cpu.getValue(), is("AuthenticAMD"));
    }

    @Test
    public void testAppendNullIsNoop() {
        CPU cpu = new CPU("GenuineIntel");
        cpu.appendValue(null);

        assertThat(cpu.getValue(), is("GenuineIntel"));
    }

    @Test
    public void testSetValueReplacesPreviouslyAppendedContent() {
        CPU cpu = new CPU();
        cpu.appendValue("Intel");
        cpu.setValue("AMD");

        assertThat(cpu.getValue(), is("AMD"));
    }

    @Test
    public void testSetValueNullClearsValue() {
        CPU cpu = new CPU("GenuineIntel");
        cpu.setValue(null);

        assertThat(cpu.getValue(), nullValue());
    }

    @Test
    public void testInheritedToXMLIncludesAttributesAndEscapedValue() throws Exception {
        CPU cpu = new CPU("Intel & AMD");
        cpu.addAttribute("vendor", "x86&x64");
        StringWriter sw = new StringWriter();

        cpu.toXML(sw, null, "inv");

        String xml = sw.toString();
        assertThat(xml, containsString("<inv:cpu"));
        assertThat(xml, containsString("vendor=\"x86&amp;amp;x64\""));
        assertThat(xml, containsString(">Intel &amp; AMD</inv:cpu>"));
    }
}
