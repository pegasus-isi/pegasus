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
import org.junit.jupiter.api.Test;

/** Tests for Data invocation class. */
public class DataTest {

    @Test
    public void testExtendsInvocation() {
        assertThat(Invocation.class.isAssignableFrom(Data.class), is(true));
    }

    @Test
    public void testImplementsHasText() {
        assertThat(HasText.class.isAssignableFrom(Data.class), is(true));
    }

    @Test
    public void testDefaultConstructorNullValue() {
        Data d = new Data();
        assertThat(d.getValue(), nullValue());
        assertThat(d.getTruncated(), is(false));
    }

    @Test
    public void testConstructorWithValue() {
        Data d = new Data("content");
        assertThat(d.getValue(), is("content"));
        assertThat(d.getTruncated(), is(false));
    }

    @Test
    public void testConstructorWithValueAndTruncated() {
        Data d = new Data("partial", true);
        assertThat(d.getValue(), is("partial"));
        assertThat(d.getTruncated(), is(true));
    }

    @Test
    public void testConstructorNullValueThrows() {
        assertThrows(NullPointerException.class, () -> new Data(null));
    }

    @Test
    public void testSetAndGetTruncated() {
        Data d = new Data();
        d.setTruncated(true);
        assertThat(d.getTruncated(), is(true));
    }

    @Test
    public void testAppendValue() {
        Data d = new Data();
        d.appendValue("hello");
        d.appendValue(" world");
        assertThat(d.getValue(), is("hello world"));
    }

    @Test
    public void testAppendNullIsNoop() {
        Data d = new Data("content");
        d.appendValue(null);

        assertThat(d.getValue(), is("content"));
    }

    @Test
    public void testSetValueReplacesPreviouslyAppendedContent() {
        Data d = new Data();
        d.appendValue("old");
        d.setValue("new");

        assertThat(d.getValue(), is("new"));
    }

    @Test
    public void testSetValueNullClearsValue() {
        Data d = new Data("content");
        d.setValue(null);

        assertThat(d.getValue(), nullValue());
    }

    @Test
    public void testToXMLContainsTruncatedAttribute() {
        Data d = new Data("some content", false);
        String xml = d.toXML("");
        assertThat(xml, containsString("truncated=\"false\""));
        assertThat(xml, containsString("some content"));
    }

    @Test
    public void testToXMLEmptyWhenValueNull() {
        Data d = new Data();
        String xml = d.toXML("");
        assertThat(xml, is(""));
    }

    @Test
    public void testToStringWriterThrowsIOException() {
        Data d = new Data("content");
        StringWriter sw = new StringWriter();

        IOException exception = assertThrows(IOException.class, () -> d.toString(sw));
        assertThat(exception.getMessage(), containsString("method not implemented"));
    }

    @Test
    public void testToXMLWriterUsesNamespaceAndEscapesValue() throws Exception {
        Data d = new Data("a&b", true);
        StringWriter sw = new StringWriter();

        d.toXML(sw, null, "inv");

        String xml = sw.toString();
        assertThat(xml, containsString("<inv:data"));
        assertThat(xml, containsString("truncated=\"true\""));
        assertThat(xml, containsString(">a&amp;b</inv:data>"));
    }
}
