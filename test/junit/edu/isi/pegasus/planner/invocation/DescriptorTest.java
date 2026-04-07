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

/** Tests for Descriptor invocation class. */
public class DescriptorTest {

    @Test
    public void testExtendsFile() {
        assertThat(File.class.isAssignableFrom(Descriptor.class), is(true));
    }

    @Test
    public void testImplementsHasDescriptor() {
        assertThat(HasDescriptor.class.isAssignableFrom(Descriptor.class), is(true));
    }

    @Test
    public void testDefaultConstructorDescriptorIsMinusOne() {
        Descriptor d = new Descriptor();
        assertThat(d.getDescriptor(), is(-1));
    }

    @Test
    public void testConstructorWithDescriptor() {
        Descriptor d = new Descriptor(2);
        assertThat(d.getDescriptor(), is(2));
    }

    @Test
    public void testSetAndGetDescriptor() {
        Descriptor d = new Descriptor();
        d.setDescriptor(3);
        assertThat(d.getDescriptor(), is(3));
    }

    @Test
    public void testAppendValue() {
        Descriptor d = new Descriptor();
        d.appendValue("hexdata");
        assertThat(d.getValue(), is("hexdata"));
    }

    @Test
    public void testAppendNullIsNoop() {
        Descriptor d = new Descriptor();
        d.appendValue(null);
        assertThat(d.getValue(), nullValue());
    }

    @Test
    public void testSetValueReplacesPreviouslyAppendedContent() {
        Descriptor d = new Descriptor();
        d.appendValue("dead");
        d.setValue("beef");

        assertThat(d.getValue(), is("beef"));
    }

    @Test
    public void testSetValueNullClearsValue() {
        Descriptor d = new Descriptor();
        d.setValue("hexdata");
        d.setValue(null);

        assertThat(d.getValue(), nullValue());
    }

    @Test
    public void testToXMLWithoutValueUsesSelfClosingTag() throws Exception {
        Descriptor d = new Descriptor(5);
        StringWriter sw = new StringWriter();

        d.toXML(sw, null, "inv");

        String xml = sw.toString();
        assertThat(xml, containsString("<inv:descriptor"));
        assertThat(xml, containsString("number=\"5\""));
        assertThat(xml, containsString("/>"));
    }

    @Test
    public void testToXMLWithValueIncludesContentAndClosingTag() throws Exception {
        Descriptor d = new Descriptor(2);
        d.setValue("deadbeef");
        StringWriter sw = new StringWriter();

        d.toXML(sw, "", null);

        String xml = sw.toString();
        assertThat(xml, containsString("<descriptor"));
        assertThat(xml, containsString("number=\"2\""));
        assertThat(xml, containsString(">deadbeef</descriptor>"));
    }
}
