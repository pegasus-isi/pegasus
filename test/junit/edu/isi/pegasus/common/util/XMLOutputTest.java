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
package edu.isi.pegasus.common.util;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import org.junit.jupiter.api.Test;

/** @author Rajiv Mayani */
public class XMLOutputTest {

    // Concrete subclass for testing the abstract XMLOutput
    private static class ConcreteXMLOutput extends XMLOutput {
        @Override
        public void toXML(Writer stream, String indent, String namespace) throws IOException {
            stream.write("<test/>");
        }
    }

    @Test
    public void testEscape_null() {
        assertNull(XMLOutput.escape(null));
    }

    @Test
    public void testEscape_plainString() {
        assertThat(XMLOutput.escape("hello"), is("hello"));
    }

    @Test
    public void testEscape_carriageReturn() {
        assertThat(XMLOutput.escape("a\rb"), is("a\\rb"));
    }

    @Test
    public void testEscape_newline() {
        assertThat(XMLOutput.escape("a\nb"), is("a\\nb"));
    }

    @Test
    public void testEscape_tab() {
        assertThat(XMLOutput.escape("a\tb"), is("a\\tb"));
    }

    @Test
    public void testEscape_doubleQuote() {
        assertThat(XMLOutput.escape("a\"b"), is("a\\\"b"));
    }

    @Test
    public void testEscape_backslash() {
        assertThat(XMLOutput.escape("a\\b"), is("a\\\\b"));
    }

    @Test
    public void testQuote_null() {
        assertNull(XMLOutput.quote(null, false));
        assertNull(XMLOutput.quote(null, true));
    }

    @Test
    public void testQuote_lessThan_content() {
        assertThat(XMLOutput.quote("<", false), is("&lt;"));
    }

    @Test
    public void testQuote_lessThan_attribute() {
        assertThat(XMLOutput.quote("<", true), is("&#60;"));
    }

    @Test
    public void testQuote_ampersand_content() {
        assertThat(XMLOutput.quote("&", false), is("&amp;"));
    }

    @Test
    public void testQuote_ampersand_attribute() {
        assertThat(XMLOutput.quote("&", true), is("&#38;"));
    }

    @Test
    public void testQuote_greaterThan_content() {
        assertThat(XMLOutput.quote(">", false), is("&gt;"));
    }

    @Test
    public void testQuote_singleQuote_attribute() {
        assertThat(XMLOutput.quote("'", true), is("&#39;"));
    }

    @Test
    public void testQuote_doubleQuote_attribute() {
        assertThat(XMLOutput.quote("\"", true), is("&#34;"));
    }

    @Test
    public void testQuote_plainText_unchanged() {
        assertThat(XMLOutput.quote("hello world", false), is("hello world"));
    }

    @Test
    public void testWriteAttribute_writesAttributeWhenValueNotNull() throws IOException {
        ConcreteXMLOutput xml = new ConcreteXMLOutput();
        StringWriter sw = new StringWriter();
        xml.writeAttribute(sw, " key=\"", "value");
        assertThat(sw.toString(), is(" key=\"value\""));
    }

    @Test
    public void testWriteAttribute_skipsWhenValueNull() throws IOException {
        ConcreteXMLOutput xml = new ConcreteXMLOutput();
        StringWriter sw = new StringWriter();
        xml.writeAttribute(sw, " key=\"", null);
        assertThat(sw.toString(), is(""));
    }

    @Test
    public void testToXML_returnsString() throws IOException {
        ConcreteXMLOutput xml = new ConcreteXMLOutput();
        String result = xml.toXML("", null);
        assertThat(result, is("<test/>"));
    }
}
