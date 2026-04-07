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
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.*;

import edu.isi.pegasus.common.logging.LogFormatter;
import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.common.PegasusProperties;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;
import org.xml.sax.Attributes;
import org.xml.sax.Locator;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.AttributesImpl;
import org.xml.sax.helpers.LocatorImpl;

/** @author Rajiv Mayani */
public class StackBasedXMLParserTest {

    /*
    @Test
    public void testSomeMethod() {
        org.hamcrest.MatcherAssert.assertThat(1, org.hamcrest.Matchers.is(1));
    }
    */

    @Test
    public void testConstructorInitializesStateAndEndDocumentMarksParsingDone() throws Exception {
        TestStackBasedXMLParser parser = new TestStackBasedXMLParser(createBag(), true);

        assertThat("Initial depth should be zero", getIntField(parser, "mDepth"), is(0));
        assertThat(
                "Constructor should initialize the stack",
                getField(parser, "mStack"),
                notNullValue());
        assertThat(
                "Constructor should initialize the unsupported-attribute set",
                getField(parser, "mUnsupportedElementAttributes"),
                notNullValue());
        assertThat(
                "Parsing should not start as done",
                getBooleanField(parser, "mParsingDone"),
                is(false));

        parser.endDocument();
        assertThat(
                "endDocument should mark parsing as done",
                getBooleanField(parser, "mParsingDone"),
                is(true));
    }

    @Test
    public void testStartAndEndElementMaintainStackAndInvokeRelation() throws Exception {
        TestStackBasedXMLParser parser = new TestStackBasedXMLParser(createBag(), true);
        parser.setDocumentLocator(locatorAt(12, 4));

        Attributes attributes = new AttributesImpl();
        parser.startElement("", "root", "root", attributes);
        parser.startElement("", "child", "child", attributes);

        assertThat(
                "Depth should increase for nested elements", getIntField(parser, "mDepth"), is(2));
        assertThat(
                "Each recognized element should be pushed on the stack", parser.stackSize(), is(2));

        parser.endElement("", "child", "child");

        assertThat("Depth should decrease after endElement", getIntField(parser, "mDepth"), is(1));
        assertThat("Completed child should be popped off the stack", parser.stackSize(), is(1));
        assertThat(
                "Relation callback should receive the child element name",
                parser.lastChildElement,
                is("child"));
        assertThat(
                "Parent object should come from the remaining stack top",
                parser.lastParentObject,
                is("root-object"));
        assertThat(
                "Child object should be passed to the relation callback",
                parser.lastChildObject,
                is("child-object"));
    }

    @Test
    public void testStartElementRejectsUnknownObjects() {
        TestStackBasedXMLParser parser = new TestStackBasedXMLParser(createBag(), false);

        SAXException exception =
                assertThrows(
                        SAXException.class,
                        () -> parser.startElement("", "unknown", "unknown", new AttributesImpl()),
                        "Null objects from createObject should be rejected");

        assertThat(
                "Unknown-element message should match",
                exception.getMessage(),
                is("Unknown or Empty element while parsing "));
    }

    @Test
    public void testEndElementRejectsMismatchedClosingTag() throws Exception {
        TestStackBasedXMLParser parser = new TestStackBasedXMLParser(createBag(), true);
        parser.setDocumentLocator(locatorAt(8, 2));
        parser.startElement("", "root", "root", new AttributesImpl());

        SAXException exception =
                assertThrows(
                        SAXException.class,
                        () -> parser.endElement("", "other", "other"),
                        "Mismatched closing tags should be rejected");

        assertThat(
                "Mismatch message should mention both the stack top and closing element",
                exception.getMessage(),
                containsString("Top of Stack root does not mactch other"));
    }

    @Test
    public void testAttributeNotSupportedLogsOnlyOncePerUniqueCombination() {
        TestStackBasedXMLParser parser = new TestStackBasedXMLParser(createBag(), true);
        CapturingLogManager logger = parser.capturingLogger();

        parser.attributeNotSupported("site", "foo", "bar");
        parser.attributeNotSupported("site", "foo", "bar");
        parser.attributeNotSupported("site", "foo", "baz");

        assertThat(
                "Duplicate unsupported attributes should only log once",
                logger.messages.size(),
                is(2));
        assertThat(
                "Unsupported attributes log at warning level",
                logger.levels.get(0),
                is(LogManager.WARNING_MESSAGE_LEVEL));
        assertThat(
                "Warning message should mention unsupported attributes",
                logger.messages.get(0),
                containsString("currently not supported"));
    }

    private PegasusBag createBag() {
        PegasusBag bag = new PegasusBag();
        bag.add(PegasusBag.PEGASUS_PROPERTIES, PegasusProperties.nonSingletonInstance());
        bag.add(PegasusBag.PEGASUS_LOGMANAGER, new CapturingLogManager());
        return bag;
    }

    private Locator locatorAt(int line, int column) {
        LocatorImpl locator = new LocatorImpl();
        locator.setLineNumber(line);
        locator.setColumnNumber(column);
        return locator;
    }

    private Object getField(Object target, String name) throws Exception {
        return ReflectionTestUtils.getField(target, name);
    }

    private int getIntField(Object target, String name) throws Exception {
        return ((Integer) getField(target, name)).intValue();
    }

    private boolean getBooleanField(Object target, String name) throws Exception {
        return ((Boolean) getField(target, name)).booleanValue();
    }

    private static final class TestStackBasedXMLParser extends StackBasedXMLParser {
        private final boolean createObjects;
        private final CapturingLogManager logger;
        private String lastChildElement;
        private Object lastParentObject;
        private Object lastChildObject;

        TestStackBasedXMLParser(PegasusBag bag, boolean createObjects) {
            super(bag);
            this.createObjects = createObjects;
            this.logger = (CapturingLogManager) bag.getLogger();
        }

        int stackSize() {
            return this.mStack.size();
        }

        CapturingLogManager capturingLogger() {
            return this.logger;
        }

        @Override
        public Object createObject(String element, List names, List values) {
            return createObjects ? element + "-object" : null;
        }

        @Override
        public boolean setElementRelation(String childElement, Object parent, Object child) {
            this.lastChildElement = childElement;
            this.lastParentObject = parent;
            this.lastChildObject = child;
            return true;
        }

        @Override
        public void startParser(String file) {}

        @Override
        public String getSchemaLocation() {
            return "/tmp/test.xsd";
        }

        @Override
        public String getSchemaNamespace() {
            return "urn:test";
        }
    }

    private static final class CapturingLogManager extends LogManager {
        private final List<String> messages = new ArrayList<String>();
        private final List<Integer> levels = new ArrayList<Integer>();

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
        public void log(String message, int level) {
            messages.add(message);
            levels.add(level);
        }

        @Override
        public void log(String message, Exception e, int level) {
            messages.add(message);
            levels.add(level);
        }

        @Override
        protected void logAlreadyFormattedMessage(String message, int level) {
            messages.add(message);
            levels.add(level);
        }

        @Override
        public void logEventCompletion(int level) {}
    }
}
