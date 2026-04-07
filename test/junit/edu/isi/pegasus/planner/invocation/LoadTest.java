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
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.io.StringWriter;
import org.junit.jupiter.api.Test;

/** Tests for Load invocation class. */
public class LoadTest {

    @Test
    public void testExtendsMachineInfo() {
        assertThat(MachineInfo.class.isAssignableFrom(Load.class), is(true));
    }

    @Test
    public void testElementName() {
        assertThat(Load.ELEMENT_NAME, is("load"));
    }

    @Test
    public void testDefaultConstructor() {
        Load l = new Load();
        assertThat(l, is(notNullValue()));
    }

    @Test
    public void testGetElementName() {
        Load l = new Load();
        assertThat(l.getElementName(), is("load"));
    }

    @Test
    public void testNotHasText() {
        // Load does not implement HasText (unlike Boot/CPU/Stamp)
        assertThat(HasText.class.isAssignableFrom(Load.class), is(false));
    }

    @Test
    public void testInheritedToStringWriterThrowsIOException() {
        Load load = new Load();
        StringWriter sw = new StringWriter();

        IOException exception = assertThrows(IOException.class, () -> load.toString(sw));
        assertThat(exception.getMessage(), containsString("method not implemented"));
    }

    @Test
    public void testInheritedToXMLUsesSelfClosingTagAndAttributes() throws Exception {
        Load load = new Load();
        load.addAttribute("one", "1");
        load.addAttribute("two", "2&3");
        StringWriter sw = new StringWriter();

        load.toXML(sw, null, "inv");

        String xml = sw.toString();
        assertThat(xml, containsString("<inv:load"));
        assertThat(xml, containsString("one=\"1\""));
        assertThat(xml, containsString("two=\"2&amp;amp;3\""));
        assertThat(xml, containsString("/>"));
    }
}
