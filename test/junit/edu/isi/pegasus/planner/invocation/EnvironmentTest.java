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

import java.io.IOException;
import java.io.StringWriter;
import java.util.Iterator;
import org.junit.jupiter.api.Test;

/** Tests for Environment invocation class. */
public class EnvironmentTest {

    @Test
    public void testExtendsInvocation() {
        assertThat(Invocation.class.isAssignableFrom(Environment.class), is(true));
    }

    @Test
    public void testDefaultConstructor() {
        Environment env = new Environment();
        assertThat(env, notNullValue());
    }

    @Test
    public void testAddEntryByKeyValue() {
        Environment env = new Environment();
        env.addEntry("PATH", "/usr/bin");
        // get() is the accessor method on Environment
        assertThat(env.get("PATH"), is("/usr/bin"));
    }

    @Test
    public void testAddEntryByEnvEntry() {
        Environment env = new Environment();
        EnvEntry e = new EnvEntry("HOME", "/home/user");
        env.addEntry(e);
        assertThat(env.get("HOME"), is("/home/user"));
    }

    @Test
    public void testGetMissingKeyReturnsNull() {
        Environment env = new Environment();
        assertThat(env.get("NONEXISTENT"), nullValue());
    }

    @Test
    public void testAddMultipleEntries() {
        Environment env = new Environment();
        env.addEntry("A", "1");
        env.addEntry("B", "2");
        assertThat(env.get("A"), is("1"));
        assertThat(env.get("B"), is("2"));
    }

    @Test
    public void testToXMLContainsEnvEntries() throws Exception {
        Environment env = new Environment();
        env.addEntry("MYKEY", "MYVAL");
        StringWriter sw = new StringWriter();
        env.toXML(sw, "", null);
        String xml = sw.toString();
        assertThat(xml, containsString("<environment"));
    }

    @Test
    public void testAddEntryReturnsOldValue() {
        Environment env = new Environment();
        env.addEntry("KEY", "first");
        String old = env.addEntry("KEY", "second");
        assertThat(old, is("first"));
        assertThat(env.get("KEY"), is("second"));
    }

    @Test
    public void testIteratorReturnsKeys() {
        Environment env = new Environment();
        env.addEntry("Z", "26");
        env.addEntry("A", "1");
        java.util.Iterator it = env.iterator();
        assertThat(it.hasNext(), is(true));
    }

    @Test
    public void testAddEntryWithNullKeyReturnsNullAndDoesNotInsert() {
        Environment env = new Environment();

        assertThat(env.addEntry((String) null, "value"), nullValue());
        assertThat(env.get(null), nullValue());
    }

    @Test
    public void testAddEntryWithNullValueNormalizesToEmptyString() {
        Environment env = new Environment();
        env.addEntry("EMPTY", null);

        assertThat(env.get("EMPTY"), is(""));
    }

    @Test
    public void testIteratorReturnsKeysInSortedOrder() {
        Environment env = new Environment();
        env.addEntry("Z", "26");
        env.addEntry("A", "1");
        env.addEntry("M", "13");

        Iterator it = env.iterator();
        assertThat(it.next(), is("A"));
        assertThat(it.next(), is("M"));
        assertThat(it.next(), is("Z"));
    }

    @Test
    public void testToStringWriterThrowsIOException() {
        Environment env = new Environment();
        StringWriter sw = new StringWriter();

        IOException exception = assertThrows(IOException.class, () -> env.toString(sw));
        assertThat(exception.getMessage(), containsString("method not implemented"));
    }

    @Test
    public void testToXMLUsesNamespaceAndEscapesValues() throws Exception {
        Environment env = new Environment();
        env.addEntry("A&B", "1 < 2");
        StringWriter sw = new StringWriter();

        env.toXML(sw, null, "inv");

        String xml = sw.toString();
        assertThat(xml, containsString("<inv:environment>"));
        assertThat(xml, containsString("<inv:env"));
        assertThat(xml, containsString("key=\"A&amp;B\""));
        assertThat(xml, containsString(">1 &lt; 2</inv:env>"));
    }
}
