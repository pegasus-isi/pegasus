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

/** Tests for EnvEntry invocation class. */
public class EnvEntryTest {

    @Test
    public void testExtendsInvocation() {
        assertThat(Invocation.class.isAssignableFrom(EnvEntry.class), is(true));
    }

    @Test
    public void testImplementsHasText() {
        assertThat(HasText.class.isAssignableFrom(EnvEntry.class), is(true));
    }

    @Test
    public void testDefaultConstructorNullKeyAndValue() {
        EnvEntry e = new EnvEntry();
        assertThat(e.getKey(), nullValue());
        assertThat(e.getValue(), nullValue());
    }

    @Test
    public void testConstructorWithKey() {
        EnvEntry e = new EnvEntry("PATH");
        assertThat(e.getKey(), is("PATH"));
        assertThat(e.getValue(), nullValue());
    }

    @Test
    public void testConstructorWithKeyAndValue() {
        EnvEntry e = new EnvEntry("HOME", "/home/user");
        assertThat(e.getKey(), is("HOME"));
        assertThat(e.getValue(), is("/home/user"));
    }

    @Test
    public void testSetAndGetKey() {
        EnvEntry e = new EnvEntry();
        e.setKey("JAVA_HOME");
        assertThat(e.getKey(), is("JAVA_HOME"));
    }

    @Test
    public void testSetAndGetValue() {
        EnvEntry e = new EnvEntry();
        e.setValue("/usr/lib/jvm/java-8");
        assertThat(e.getValue(), is("/usr/lib/jvm/java-8"));
    }

    @Test
    public void testAppendValue() {
        EnvEntry e = new EnvEntry("VAR");
        e.appendValue("/usr");
        e.appendValue("/local");
        assertThat(e.getValue(), is("/usr/local"));
    }

    @Test
    public void testAppendNullIsNoop() {
        EnvEntry e = new EnvEntry("VAR", "value");
        e.appendValue(null);

        assertThat(e.getValue(), is("value"));
    }

    @Test
    public void testSetValueReplacesPreviouslyAppendedContent() {
        EnvEntry e = new EnvEntry("VAR");
        e.appendValue("old");
        e.setValue("new");

        assertThat(e.getValue(), is("new"));
    }

    @Test
    public void testToXMLContainsKeyAttribute() throws Exception {
        EnvEntry e = new EnvEntry("MYVAR", "myval");
        StringWriter sw = new StringWriter();
        e.toXML(sw, "", null);
        String xml = sw.toString();
        assertThat(xml, containsString("key=\"MYVAR\""));
        assertThat(xml, containsString("myval"));
    }

    @Test
    public void testToStringWriterFormatsKeyValuePair() throws Exception {
        EnvEntry e = new EnvEntry("HOME", "/home/user");
        StringWriter sw = new StringWriter();

        e.toString(sw);

        assertThat(sw.toString(), is("HOME=/home/user"));
    }

    @Test
    public void testToXMLWriterUsesNamespaceAndEscapesKeyAndValue() throws Exception {
        EnvEntry e = new EnvEntry("A&B", "1 < 2");
        StringWriter sw = new StringWriter();

        e.toXML(sw, null, "inv");

        String xml = sw.toString();
        assertThat(xml, containsString("<inv:env"));
        assertThat(xml, containsString("key=\"A&amp;amp;B\""));
        assertThat(xml, containsString(">1 &lt; 2</inv:env>"));
    }
}
