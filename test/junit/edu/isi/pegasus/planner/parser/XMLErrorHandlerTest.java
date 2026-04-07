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
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.*;

import edu.isi.pegasus.common.logging.LogFormatter;
import edu.isi.pegasus.common.logging.LogManager;
import java.io.PrintStream;
import java.util.Properties;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;
import org.xml.sax.SAXParseException;

/** @author Rajiv Mayani */
public class XMLErrorHandlerTest {

    /*
    @Test
    public void testSomeMethod() {
        org.hamcrest.MatcherAssert.assertThat(1, org.hamcrest.Matchers.is(1));
    }
    */

    @Test
    public void testConstructorInitializesLogger() throws Exception {
        XMLErrorHandler handler = new XMLErrorHandler();

        Object logger = getField(handler, "mLogger");
        assertThat(
                "Constructor should initialize the logger", logger, instanceOf(LogManager.class));
    }

    @Test
    public void testWarningLogsExpectedMessageAndLevel() throws Exception {
        XMLErrorHandler handler = new XMLErrorHandler();
        CapturingLogManager logger = new CapturingLogManager();
        setField(handler, "mLogger", logger);
        SAXParseException exception = new SAXParseException("warning-text", null, null, 7, 3);

        handler.warning(exception);

        assertThat(
                "Warning should log at warning level",
                logger.lastLevel,
                is(LogManager.WARNING_MESSAGE_LEVEL));
        assertThat(
                "Warning log should include the line number",
                logger.lastMessage,
                containsString("Line: 7"));
        assertThat(
                "Warning log should include the exception rendering",
                logger.lastMessage,
                containsString("[org.xml.sax.SAXParseException"));
    }

    @Test
    public void testErrorLogsExpectedMessageAndLevel() throws Exception {
        XMLErrorHandler handler = new XMLErrorHandler();
        CapturingLogManager logger = new CapturingLogManager();
        setField(handler, "mLogger", logger);
        SAXParseException exception = new SAXParseException("error-text", null, null, 11, 2);

        handler.error(exception);

        assertThat(
                "Error should log at error level",
                logger.lastLevel,
                is(LogManager.ERROR_MESSAGE_LEVEL));
        assertThat(
                "Error log should include the line number",
                logger.lastMessage,
                containsString("Line: 11"));
        assertThat(
                "Error log should use the non-fatal prefix",
                logger.lastMessage.startsWith("**Parsing **"),
                is(true));
    }

    @Test
    public void testFatalErrorLogsExpectedMessageAndLevel() throws Exception {
        XMLErrorHandler handler = new XMLErrorHandler();
        CapturingLogManager logger = new CapturingLogManager();
        setField(handler, "mLogger", logger);
        SAXParseException exception = new SAXParseException("fatal-text", null, null, 13, 9);

        handler.fatalError(exception);

        assertThat(
                "Fatal errors should log at fatal level",
                logger.lastLevel,
                is(LogManager.FATAL_MESSAGE_LEVEL));
        assertThat(
                "Fatal log should include the line number",
                logger.lastMessage,
                containsString("Line: 13"));
        assertThat(
                "Fatal log should use the fatal prefix",
                logger.lastMessage.startsWith("\n** Parsing **"),
                is(true));
    }

    private Object getField(Object target, String name) throws Exception {
        return ReflectionTestUtils.getField(target, name);
    }

    private void setField(Object target, String name, Object value) throws Exception {
        ReflectionTestUtils.setField(target, name, value);
    }

    private static final class CapturingLogManager extends LogManager {
        private String lastMessage;
        private int lastLevel = -1;

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
            this.lastMessage = message;
            this.lastLevel = level;
        }

        @Override
        public void log(String message, Exception e, int level) {
            this.lastMessage = message;
            this.lastLevel = level;
        }

        @Override
        protected void logAlreadyFormattedMessage(String message, int level) {
            this.lastMessage = message;
            this.lastLevel = level;
        }

        @Override
        public void logEventCompletion(int level) {}
    }
}
