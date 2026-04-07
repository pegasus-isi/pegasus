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

import java.io.StringWriter;
import org.junit.jupiter.api.Test;

/** @author Rajiv Mayani */
public class XMLWriterTest {

    @Test
    public void testStartAndEndElement_producesEmptyElement() {
        StringWriter sw = new StringWriter();
        XMLWriter xw = new XMLWriter(sw);
        xw.startElement("job").endElement();
        xw.close();
        assertThat(sw.toString(), containsString("<job/>"));
    }

    @Test
    public void testStartElement_withAttribute() {
        StringWriter sw = new StringWriter();
        XMLWriter xw = new XMLWriter(sw);
        xw.startElement("file").writeAttribute("name", "test.txt").endElement();
        xw.close();
        assertThat(sw.toString(), containsString("name=\"test.txt\""));
    }

    @Test
    public void testWriteData_producesTextContent() {
        StringWriter sw = new StringWriter();
        XMLWriter xw = new XMLWriter(sw);
        xw.startElement("desc").writeData("hello world").endElement();
        xw.close();
        assertThat(sw.toString(), containsString("hello world"));
    }

    @Test
    public void testWriteData_escapesXmlSpecialChars() {
        StringWriter sw = new StringWriter();
        XMLWriter xw = new XMLWriter(sw);
        xw.startElement("desc").writeData("<value>&</value>").endElement();
        xw.close();
        String out = sw.toString();
        assertThat(out, containsString("&lt;"));
        assertThat(out, containsString("&amp;"));
    }

    @Test
    public void testXmlHeader_isPresent() {
        StringWriter sw = new StringWriter();
        new XMLWriter(sw).close();
        assertThat(sw.toString(), containsString("<?xml version=\"1.0\" encoding=\"UTF-8\"?>"));
    }

    @Test
    public void testNamespacedElement() {
        StringWriter sw = new StringWriter();
        XMLWriter xw = new XMLWriter(sw, "wf");
        xw.startElement("workflow").endElement();
        xw.close();
        assertThat(sw.toString(), containsString("<wf:workflow"));
    }

    @Test
    public void testWriteXMLComment() {
        StringWriter sw = new StringWriter();
        XMLWriter xw = new XMLWriter(sw);
        xw.writeXMLComment("this is a comment");
        xw.close();
        assertThat(sw.toString(), containsString("<!-- this is a comment -->"));
    }

    @Test
    public void testWriteCData() {
        StringWriter sw = new StringWriter();
        XMLWriter xw = new XMLWriter(sw);
        xw.startElement("script").writeCData("<![CDATA[data]]>").endElement();
        xw.close();
        assertThat(sw.toString(), containsString("<![CDATA["));
    }

    @Test
    public void testNested_elements() {
        StringWriter sw = new StringWriter();
        XMLWriter xw = new XMLWriter(sw);
        xw.startElement("workflow");
        xw.startElement("job").writeAttribute("id", "j1").endElement();
        xw.endElement();
        xw.close();
        String out = sw.toString();
        assertThat(out, containsString("<job"));
        assertThat(out, containsString("id=\"j1\""));
        assertThat(out, containsString("</workflow>"));
    }

    @Test
    public void testEndElement_nonEmptyElement_producesClosingTag() {
        StringWriter sw = new StringWriter();
        XMLWriter xw = new XMLWriter(sw);
        xw.startElement("desc").writeData("text").endElement();
        xw.close();
        assertThat(sw.toString(), containsString("</desc>"));
        assertThat(sw.toString(), not(containsString("<desc/>")));
    }

    @Test
    public void testWriteData_escapesGreaterThan() {
        StringWriter sw = new StringWriter();
        XMLWriter xw = new XMLWriter(sw);
        xw.startElement("v").writeData(">").endElement();
        xw.close();
        assertThat(sw.toString(), containsString("&gt;"));
    }

    @Test
    public void testWriteData_escapesDoubleQuote() {
        StringWriter sw = new StringWriter();
        XMLWriter xw = new XMLWriter(sw);
        xw.startElement("v").writeData("\"hello\"").endElement();
        xw.close();
        assertThat(sw.toString(), containsString("&quot;"));
    }

    @Test
    public void testWriteData_escapesSingleQuote() {
        StringWriter sw = new StringWriter();
        XMLWriter xw = new XMLWriter(sw);
        xw.startElement("v").writeData("it's").endElement();
        xw.close();
        assertThat(sw.toString(), containsString("&apos;"));
    }

    @Test
    public void testWriteUnEscapedData_writesVerbatim() {
        StringWriter sw = new StringWriter();
        XMLWriter xw = new XMLWriter(sw);
        xw.startElement("v").writeUnEscapedData("<b>bold</b>").endElement();
        xw.close();
        assertThat(sw.toString(), containsString("<b>bold</b>"));
    }

    @Test
    public void testWriteAttribute_escapesXmlSpecialChars() {
        StringWriter sw = new StringWriter();
        XMLWriter xw = new XMLWriter(sw);
        xw.startElement("file").writeAttribute("path", "a<b>&\"c'").endElement();
        xw.close();
        String out = sw.toString();
        assertThat(out, containsString("&lt;"));
        assertThat(out, containsString("&amp;"));
        assertThat(out, containsString("&quot;"));
        assertThat(out, containsString("&apos;"));
    }

    @Test
    public void testWriteAttribute_multipleAttributes() {
        StringWriter sw = new StringWriter();
        XMLWriter xw = new XMLWriter(sw);
        xw.startElement("job")
                .writeAttribute("id", "j1")
                .writeAttribute("name", "myjob")
                .endElement();
        xw.close();
        String out = sw.toString();
        assertThat(out, containsString("id=\"j1\""));
        assertThat(out, containsString("name=\"myjob\""));
    }

    @Test
    public void testNamespacedAttribute() {
        StringWriter sw = new StringWriter();
        XMLWriter xw = new XMLWriter(sw, "wf");
        xw.startElement("job").writeAttribute("id", "j1").endElement();
        xw.close();
        assertThat(sw.toString(), containsString("wf:id=\"j1\""));
    }

    @Test
    public void testNamespacedEndElement() {
        StringWriter sw = new StringWriter();
        XMLWriter xw = new XMLWriter(sw, "wf");
        xw.startElement("workflow").writeData("x").endElement();
        xw.close();
        assertThat(sw.toString(), containsString("</wf:workflow>"));
    }

    @Test
    public void testNullNamespace_treatedAsEmpty() {
        StringWriter sw = new StringWriter();
        XMLWriter xw = new XMLWriter(sw, null);
        xw.startElement("job").endElement();
        xw.close();
        assertThat(sw.toString(), containsString("<job/>"));
    }

    @Test
    public void testNoLine_suppressesTrailingNewlineForOneEndElement() {
        StringWriter sw = new StringWriter();
        XMLWriter xw = new XMLWriter(sw);
        xw.startElement("a").noLine().endElement();
        xw.close();
        String afterHeader = sw.toString().replaceFirst(".*\\?>\\s*<!--.*-->\\s*<!--.*-->\\s*", "");
        assertThat(afterHeader.startsWith(System.lineSeparator()), is(false));
    }

    @Test
    public void testWriteXMLComment_linePadded() {
        StringWriter sw = new StringWriter();
        XMLWriter xw = new XMLWriter(sw);
        xw.writeXMLComment("padded comment", true);
        xw.close();
        String out = sw.toString();
        assertThat(out, containsString("<!-- padded comment -->"));
    }

    @Test
    public void testWriteXMLComment_notLinePadded() {
        StringWriter sw = new StringWriter();
        XMLWriter xw = new XMLWriter(sw);
        xw.writeXMLComment("plain comment", false);
        xw.close();
        assertThat(sw.toString(), containsString("<!-- plain comment -->"));
    }

    @Test
    public void testStartElement_withIndentPrefixesElement() {
        StringWriter sw = new StringWriter();
        XMLWriter xw = new XMLWriter(sw);
        xw.startElement("job", 2).endElement();
        xw.close();

        assertThat(sw.toString(), containsString(System.lineSeparator() + "      <job/>"));
    }

    @Test
    public void testWriteXMLCommentInsideOpenElementLeavesTrailingBareEmptyCloseMarker() {
        StringWriter sw = new StringWriter();
        XMLWriter xw = new XMLWriter(sw);
        xw.startElement("job").writeXMLComment("after-open-tag").endElement();
        xw.close();

        String out = sw.toString();
        assertThat(
                out, containsString("<job>" + System.lineSeparator() + "<!-- after-open-tag -->"));
        assertThat(out, containsString(System.lineSeparator() + "/>" + System.lineSeparator()));
    }

    @Test
    public void testEndElementOnEmptyStackThrowsEmptyStackException() {
        StringWriter sw = new StringWriter();
        XMLWriter xw = new XMLWriter(sw);

        assertThrows(java.util.EmptyStackException.class, xw::endElement);
    }

    @Test
    public void testWriteXMLCommentWithLinePaddingAddsBlankLineBeforeAndAfter() {
        StringWriter sw = new StringWriter();
        XMLWriter xw = new XMLWriter(sw);
        xw.writeXMLComment("spaced", true);
        xw.close();

        String paddedComment =
                System.lineSeparator()
                        + "<!-- spaced -->"
                        + System.lineSeparator()
                        + System.lineSeparator();
        assertThat(sw.toString(), containsString(paddedComment));
    }
}
