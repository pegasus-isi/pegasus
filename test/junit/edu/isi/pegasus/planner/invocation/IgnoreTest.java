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

/** Tests for Ignore invocation class. */
public class IgnoreTest {

    @Test
    public void testExtendsInvocation() {
        assertThat(Invocation.class.isAssignableFrom(Ignore.class), is(true));
    }

    @Test
    public void testImplementsHasText() {
        assertThat(HasText.class.isAssignableFrom(Ignore.class), is(true));
    }

    @Test
    public void testDefaultConstructorCreates() {
        Ignore ig = new Ignore();
        assertThat(ig, is(notNullValue()));
    }

    @Test
    public void testGetValueReturnsNull() {
        // Ignore.getValue() returns "" (empty string), not null — it is a no-op store
        Ignore ig = new Ignore();
        assertThat(ig.getValue(), is(""));
    }

    @Test
    public void testAppendValueIsNoop() {
        Ignore ig = new Ignore();
        ig.appendValue("anything");
        // getValue() still returns "" because Ignore discards all data
        assertThat(ig.getValue(), is(""));
    }

    @Test
    public void testSetValueIsNoop() {
        Ignore ig = new Ignore();
        ig.setValue("something");
        assertThat(ig.getValue(), is(""));
    }

    @Test
    public void testAppendNullIsNoop() {
        Ignore ig = new Ignore();
        ig.appendValue(null);
        assertThat(ig.getValue(), is(""));
    }

    @Test
    public void testConstructorWithValueStillDiscardsContent() {
        Ignore ig = new Ignore("ignored");

        assertThat(ig.getValue(), is(""));
    }

    @Test
    public void testToStringWriterThrowsIOException() {
        Ignore ig = new Ignore();
        StringWriter sw = new StringWriter();

        IOException exception = assertThrows(IOException.class, () -> ig.toString(sw));
        assertThat(exception.getMessage(), containsString("method not implemented"));
    }

    @Test
    public void testToXMLWriterThrowsIOException() {
        Ignore ig = new Ignore();
        StringWriter sw = new StringWriter();

        IOException exception = assertThrows(IOException.class, () -> ig.toXML(sw, null, "inv"));
        assertThat(exception.getMessage(), containsString("method not implemented"));
    }
}
