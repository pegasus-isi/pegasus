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
        assertThat(out, anyOf(containsString("</wf:workflow>"), containsString("</workflow>")));
    }
}
