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
import java.lang.reflect.Modifier;
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

    private static class CapturingXMLOutput extends XMLOutput {
        private String indent;
        private String namespace;

        @Override
        public void toXML(Writer stream, String indent, String namespace) throws IOException {
            this.indent = indent;
            this.namespace = namespace;
            stream.write("<captured/>");
        }
    }

    @Test
    public void testEscape_null() {
        assertThat(XMLOutput.escape(null), is(nullValue()));
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
        assertThat(XMLOutput.quote(null, false), is(nullValue()));
        assertThat(XMLOutput.quote(null, true), is(nullValue()));
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
    public void testEscape_emptyString() {
        assertThat(XMLOutput.escape(""), is(""));
    }

    @Test
    public void testEscape_apostropheNotEscaped() {
        // Apostrophe must NOT be escaped per the method's contract
        assertThat(XMLOutput.escape("it's"), is("it's"));
    }

    @Test
    public void testEscape_mixedSpecialChars() {
        assertThat(XMLOutput.escape("a\r\n\t\"\\b"), is("a\\r\\n\\t\\\"\\\\b"));
    }

    @Test
    public void testQuote_emptyString() {
        assertThat(XMLOutput.quote("", false), is(""));
        assertThat(XMLOutput.quote("", true), is(""));
    }

    @Test
    public void testQuote_greaterThan_attribute() {
        assertThat(XMLOutput.quote(">", true), is("&#62;"));
    }

    @Test
    public void testQuote_singleQuote_content() {
        assertThat(XMLOutput.quote("'", false), is("&apos;"));
    }

    @Test
    public void testQuote_doubleQuote_content() {
        assertThat(XMLOutput.quote("\"", false), is("&quot;"));
    }

    @Test
    public void testQuote_mixedChars_content() {
        assertThat(XMLOutput.quote("a<b>&c", false), is("a&lt;b&gt;&amp;c"));
    }

    @Test
    public void testQuote_mixedChars_attribute() {
        assertThat(XMLOutput.quote("a<b>&c", true), is("a&#60;b&#62;&#38;c"));
    }

    @Test
    public void testWriteAttribute_escapesSpecialCharsInValue() throws IOException {
        ConcreteXMLOutput xml = new ConcreteXMLOutput();
        StringWriter sw = new StringWriter();
        xml.writeAttribute(sw, " key=\"", "a<b>&c");
        assertThat(sw.toString(), is(" key=\"a&#60;b&#62;&#38;c\""));
    }

    @Test
    public void testWriteAttribute2_writesAttributeWhenBothNotNull() throws IOException {
        ConcreteXMLOutput xml = new ConcreteXMLOutput();
        StringWriter sw = new StringWriter();
        xml.writeAttribute2(sw, " key", "value");
        assertThat(sw.toString(), is(" key=\"value\""));
    }

    @Test
    public void testWriteAttribute2_skipsWhenKeyNull() throws IOException {
        ConcreteXMLOutput xml = new ConcreteXMLOutput();
        StringWriter sw = new StringWriter();
        xml.writeAttribute2(sw, null, "value");
        assertThat(sw.toString(), is(""));
    }

    @Test
    public void testWriteAttribute2_skipsWhenValueNull() throws IOException {
        ConcreteXMLOutput xml = new ConcreteXMLOutput();
        StringWriter sw = new StringWriter();
        xml.writeAttribute2(sw, " key", null);
        assertThat(sw.toString(), is(""));
    }

    @Test
    public void testWriteAttribute2_escapesSpecialCharsInValue() throws IOException {
        ConcreteXMLOutput xml = new ConcreteXMLOutput();
        StringWriter sw = new StringWriter();
        xml.writeAttribute2(sw, " key", "a<b>&\"c");
        assertThat(sw.toString(), is(" key=\"a&#60;b&#62;&#38;&#34;c\""));
    }

    @Test
    public void testToXML_returnsString() throws IOException {
        ConcreteXMLOutput xml = new ConcreteXMLOutput();
        String result = xml.toXML("", null);
        assertThat(result, is("<test/>"));
    }

    @Test
    public void testToXML_twoArgDelegatesToThreeArg() throws IOException {
        ConcreteXMLOutput xml = new ConcreteXMLOutput();
        StringWriter sw = new StringWriter();
        xml.toXML(sw, "  ");
        assertThat(sw.toString(), is("<test/>"));
    }

    @Test
    public void testXMLOutputIsAbstractClass() {
        assertThat(Modifier.isAbstract(XMLOutput.class.getModifiers()), is(true));
        assertThat(XMLOutput.class.isInterface(), is(false));
    }

    @Test
    public void testQuote_attributeEscapesBothQuoteTypes() {
        assertThat(XMLOutput.quote("'\"", true), is("&#39;&#34;"));
    }

    @Test
    public void testToXML_stringFormPassesIndentAndNamespace() throws IOException {
        CapturingXMLOutput xml = new CapturingXMLOutput();
        String result = xml.toXML("    ", "ns");
        assertThat(result, is("<captured/>"));
        assertThat(xml.indent, is("    "));
        assertThat(xml.namespace, is("ns"));
    }

    @Test
    public void testToXML_twoArgPassesNullNamespace() throws IOException {
        CapturingXMLOutput xml = new CapturingXMLOutput();
        StringWriter sw = new StringWriter();
        xml.toXML(sw, "\t");
        assertThat(sw.toString(), is("<captured/>"));
        assertThat(xml.indent, is("\t"));
        assertThat(xml.namespace, is(nullValue()));
    }
}
