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
package edu.isi.pegasus.planner.parser;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.*;

import edu.isi.pegasus.common.logging.LogFormatter;
import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.common.PegasusProperties;
import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.util.Properties;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;

/** @author Rajiv Mayani */
public class XMLParserTest {

    /*
    @Test
    public void testSomeMethod() {
        org.hamcrest.MatcherAssert.assertThat(1, org.hamcrest.Matchers.is(1));
    }
    */

    @Test
    public void testMakeDAGManCompliant() {
        assertThat(
                "Dots and plus signs should be replaced for DAGMan compliance",
                XMLParser.makeDAGManCompliant("a.b+c"),
                is("a_b_c"));
        assertThat("Null input should stay null", XMLParser.makeDAGManCompliant(null), nullValue());
    }

    @Test
    public void testIgnoreWhitespaceAndCharactersBuffering() {
        TestXMLParser parser = new TestXMLParser(createBag());

        assertThat(
                "Whitespace should be normalized",
                parser.ignoreWhitespace(" \tvalue\n"),
                is(" value "));
        assertThat(
                "Preserve-line-break mode should keep leading and trailing newlines",
                parser.ignoreWhitespace("\nvalue\n", true),
                is("\n value \n"));

        parser.characters("  hello  ".toCharArray(), 0, "  hello  ".length());
        assertThat(
                "characters(...) should append normalized text",
                parser.textContent(),
                is(" hello "));
    }

    @Test
    public void testConstructorInitializesParserAndLoggerState() throws Exception {
        TestXMLParser parser = new TestXMLParser(createBag());

        assertThat(
                "Constructor should create an XMLReader",
                getField(parser, "mParser"),
                notNullValue());
        assertThat(
                "Constructor should store the logger", getField(parser, "mLogger"), notNullValue());
        assertThat(
                "Constructor should initialize text buffering",
                getField(parser, "mTextContent"),
                notNullValue());
        assertThat("Initial text buffer should be empty", parser.textContent(), is(""));
    }

    @Test
    public void testTestForFileThrowsForMissingPath() {
        TestXMLParser parser = new TestXMLParser(createBag());

        FileNotFoundException exception =
                assertThrows(
                        FileNotFoundException.class,
                        () -> parser.testForFile("test/junit/definitely-missing.xml"),
                        "Missing files should raise FileNotFoundException");

        assertThat(
                "Exception message should mention the missing file",
                exception.getMessage(),
                containsString("specified does not exist"));
    }

    @Test
    public void testSetParserFeatureRejectsUnknownFeature() {
        TestXMLParser parser = new TestXMLParser(createBag());

        assertThat(
                "Unknown parser features should be rejected",
                parser.setParserFeature("urn:pegasus:test:missing-feature", true),
                is(false));
    }

    private PegasusBag createBag() {
        PegasusBag bag = new PegasusBag();
        bag.add(PegasusBag.PEGASUS_PROPERTIES, PegasusProperties.nonSingletonInstance());
        bag.add(PegasusBag.PEGASUS_LOGMANAGER, new NoOpLogManager());
        return bag;
    }

    private Object getField(Object target, String name) throws Exception {
        return ReflectionTestUtils.getField(target, name);
    }

    private static final class TestXMLParser extends XMLParser {

        TestXMLParser(PegasusBag bag) {
            super(bag);
        }

        String textContent() {
            return this.mTextContent.toString();
        }

        @Override
        public void startElement(String uri, String local, String raw, Attributes attrs)
                throws SAXException {}

        @Override
        public void endElement(String uri, String localName, String qName) throws SAXException {}

        @Override
        public void endDocument() {}

        @Override
        public void startParser(String file) {}

        @Override
        public String getSchemaLocation() {
            return null;
        }

        @Override
        public String getSchemaNamespace() {
            return "urn:test";
        }
    }

    private static final class NoOpLogManager extends LogManager {

        @Override
        public void initialize(LogFormatter formatter, Properties properties) {
            this.mLogFormatter = formatter;
        }

        @Override
        public void configure(boolean prefixTimestamp) {}

        @Override
        protected void setLevel(int level, boolean info) {}

        @Override
        public int getLevel() {
            return LogManager.DEBUG_MESSAGE_LEVEL;
        }

        @Override
        public void setWriters(String out) {}

        @Override
        public void setWriter(STREAM_TYPE type, PrintStream ps) {}

        @Override
        public PrintStream getWriter(STREAM_TYPE type) {
            return null;
        }

        @Override
        public void log(String message, int level) {}

        @Override
        public void log(String message, Exception e, int level) {}

        @Override
        protected void logAlreadyFormattedMessage(String message, int level) {}

        @Override
        public void logEventCompletion(int level) {}
    }
}
