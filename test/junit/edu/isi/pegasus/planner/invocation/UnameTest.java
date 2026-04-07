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

/** Tests for Uname invocation class. */
public class UnameTest {

    @Test
    public void testExtendsMachineInfo() {
        assertThat(MachineInfo.class.isAssignableFrom(Uname.class), is(true));
    }

    @Test
    public void testImplementsHasText() {
        assertThat(HasText.class.isAssignableFrom(Uname.class), is(true));
    }

    @Test
    public void testElementName() {
        assertThat(Uname.ELEMENT_NAME, is("uname"));
    }

    @Test
    public void testDefaultConstructorNullValue() {
        Uname u = new Uname();
        assertThat(u.getValue(), is(nullValue()));
    }

    @Test
    public void testConstructorWithValue() {
        Uname u = new Uname("Linux host 5.4");
        assertThat(u.getValue(), is("Linux host 5.4"));
    }

    @Test
    public void testConstructorNullThrows() {
        assertThrows(NullPointerException.class, () -> new Uname(null));
    }

    @Test
    public void testSetAndGetValue() {
        Uname u = new Uname();
        u.setValue("Darwin kernel");
        assertThat(u.getValue(), is("Darwin kernel"));
    }

    @Test
    public void testAppendValue() {
        Uname u = new Uname("Linux ");
        u.appendValue("x86_64");
        assertThat(u.getValue(), is("Linux x86_64"));
    }

    @Test
    public void testToArchitectureNotNull() {
        Uname u = new Uname();
        u.addAttribute(Uname.SYSTEM_ATTRIBUTE_KEY, "Linux");
        u.addAttribute(Uname.MACHINE_ATTRIBUTE_KEY, "x86_64");
        u.addAttribute(Uname.NODENAME_ATTRIBUTE_KEY, "host1");
        u.addAttribute(Uname.RELEASE_ATTRIBUTE_KEY, "5.4.0");
        Architecture arch = u.toArchitecture();
        assertThat(arch, is(org.hamcrest.Matchers.notNullValue()));
        assertThat(arch.getSystemName(), is("Linux"));
    }

    @Test
    public void testUnameToArchitectureStaticMethod() {
        Uname u = new Uname();
        u.addAttribute(Uname.SYSTEM_ATTRIBUTE_KEY, "Darwin");
        u.addAttribute(Uname.NODENAME_ATTRIBUTE_KEY, "myhost");
        Architecture arch = Uname.unameToArchitecture(u);
        assertThat(arch.getSystemName(), is("Darwin"));
    }

    @Test
    public void testAppendNullIsNoop() {
        Uname u = new Uname("Linux");

        u.appendValue(null);

        assertThat(u.getValue(), is("Linux"));
    }

    @Test
    public void testSetValueNullClearsValue() {
        Uname u = new Uname("Linux");

        u.setValue(null);

        assertThat(u.getValue(), is(nullValue()));
    }

    @Test
    public void testUnameToArchitectureDefaultsArchModeWhenMissing() {
        Uname u = new Uname();
        u.addAttribute(Uname.SYSTEM_ATTRIBUTE_KEY, "Linux");
        u.addAttribute(Uname.NODENAME_ATTRIBUTE_KEY, "host1.example.com");
        u.addAttribute(Uname.MACHINE_ATTRIBUTE_KEY, "x86_64");
        u.addAttribute(Uname.RELEASE_ATTRIBUTE_KEY, "5.4.0");

        Architecture arch = Uname.unameToArchitecture(u);

        assertThat(arch.getArchMode(), is(Uname.UNDEFINED_ARCHMODE_VALUE));
    }

    @Test
    public void testToXMLUsesNamespaceAndEscapesValue() throws IOException {
        Uname u = new Uname("Linux <x86_64>");
        u.addAttribute(Uname.SYSTEM_ATTRIBUTE_KEY, "Linux");
        u.addAttribute(Uname.MACHINE_ATTRIBUTE_KEY, "x86_64");

        StringWriter writer = new StringWriter();
        u.toXML(writer, "  ", "inv");

        String xml = writer.toString();
        assertThat(xml.startsWith("  <inv:uname"), is(true));
        assertThat(xml, containsString("system=\"Linux\""));
        assertThat(xml, containsString("machine=\"x86_64\""));
        assertThat(xml, containsString(">Linux &lt;x86_64&gt;</inv:uname>"));
        assertThat(xml.endsWith(System.lineSeparator()), is(true));
    }
}
