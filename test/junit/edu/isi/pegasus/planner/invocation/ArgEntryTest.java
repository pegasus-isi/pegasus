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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Tests for ArgEntry invocation class. */
public class ArgEntryTest {

    private ArgEntry mEntry;

    @BeforeEach
    public void setUp() {
        mEntry = new ArgEntry();
    }

    @AfterEach
    public void tearDown() {
        mEntry = null;
    }

    @Test
    public void testImplementsHasText() {
        assertThat(HasText.class.isAssignableFrom(ArgEntry.class), is(true));
    }

    @Test
    public void testExtendsInvocation() {
        assertThat(Invocation.class.isAssignableFrom(ArgEntry.class), is(true));
    }

    @Test
    public void testDefaultConstructorPosition() {
        assertThat(mEntry.getPosition(), is(-1));
    }

    @Test
    public void testDefaultConstructorNullValue() {
        assertThat(mEntry.getValue(), nullValue());
    }

    @Test
    public void testConstructorWithPosition() {
        ArgEntry e = new ArgEntry(3);
        assertThat(e.getPosition(), is(3));
        assertThat(e.getValue(), nullValue());
    }

    @Test
    public void testConstructorWithPositionAndValue() {
        ArgEntry e = new ArgEntry(1, "hello");
        assertThat(e.getPosition(), is(1));
        assertThat(e.getValue(), is("hello"));
    }

    @Test
    public void testSetAndGetPosition() {
        mEntry.setPosition(5);
        assertThat(mEntry.getPosition(), is(5));
    }

    @Test
    public void testSetAndGetValue() {
        mEntry.setValue("myarg");
        assertThat(mEntry.getValue(), is("myarg"));
    }

    @Test
    public void testAppendValue() {
        mEntry.appendValue("foo");
        mEntry.appendValue("bar");
        assertThat(mEntry.getValue(), is("foobar"));
    }

    @Test
    public void testAppendNullIsNoop() {
        mEntry.setValue("test");
        mEntry.appendValue(null);
        assertThat(mEntry.getValue(), is("test"));
    }

    @Test
    public void testSetValueReplacesPreviouslyAppendedText() {
        mEntry.appendValue("foo");
        mEntry.setValue("bar");

        assertThat(mEntry.getValue(), is("bar"));
    }

    @Test
    public void testToXMLContainsIdAndValue() throws Exception {
        mEntry.setPosition(2);
        mEntry.setValue("argval");
        StringWriter sw = new StringWriter();
        mEntry.toXML(sw, "", null);
        String xml = sw.toString();
        assertThat(xml, containsString("id=\"2\""));
        assertThat(xml, containsString("argval"));
    }

    @Test
    public void testToXMLWithNamespaceEscapesValue() throws Exception {
        mEntry.setPosition(7);
        mEntry.setValue("a&b");
        StringWriter sw = new StringWriter();

        mEntry.toXML(sw, null, "inv");

        String xml = sw.toString();
        assertThat(xml, containsString("<inv:arg"));
        assertThat(xml, containsString("id=\"7\""));
        assertThat(xml, containsString("a&amp;b"));
    }

    @Test
    public void testToStringWriterCurrentPositionRenderingBehavior() throws Exception {
        ArgEntry entry = new ArgEntry(49, "hello");
        StringWriter sw = new StringWriter();

        entry.toString(sw);

        assertThat(sw.toString(), is("[1]=hello"));
    }
}
