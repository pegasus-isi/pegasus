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
package edu.isi.pegasus.common.logging.logger;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.*;

import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.common.logging.format.Simple;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
import org.apache.logging.log4j.Level;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

/** Tests for the Log4j LogManager implementation. */
public class Log4jTest {

    private Log4j mLogger;

    @BeforeEach
    public void setUp() {
        mLogger = new Log4j();
        mLogger.initialize(new Simple(), new Properties());
        // Current formatter-backed log/add/hierarchy paths require an event context.
        mLogger.logEventStart("test.log4j.logger", "logger", "log4j");
    }

    // -----------------------------------------------------------------------
    // Class-level contract
    // -----------------------------------------------------------------------

    @Test
    public void testLog4jIsConcreteClass() {
        assertThat(Modifier.isAbstract(Log4j.class.getModifiers()), is(false));
    }

    @Test
    public void testLog4jExtendsLogManager() {
        assertThat(mLogger, instanceOf(LogManager.class));
    }

    // -----------------------------------------------------------------------
    // Static level map — log4jIntValueTolog4jLevel()
    // -----------------------------------------------------------------------

    @Test
    public void testLog4jIntValueTolog4jLevelNotNull() {
        Map<Integer, Level> map = Log4j.log4jIntValueTolog4jLevel();
        assertThat(map, is(notNullValue()));
        assertThat(map.isEmpty(), is(false));
    }

    @Test
    public void testLevelMapContainsSevenEntries() {
        // FATAL, ERROR, WARN, INFO, DEBUG, TRACE, ALL
        assertThat(Log4j.log4jIntValueTolog4jLevel().size(), is(7));
    }

    @Test
    public void testLog4jIntValueTolog4jLevelContainsFatal() {
        assertThat(Log4j.log4jIntValueTolog4jLevel().containsValue(Level.FATAL), is(true));
    }

    @Test
    public void testLog4jIntValueTolog4jLevelContainsError() {
        assertThat(Log4j.log4jIntValueTolog4jLevel().containsValue(Level.ERROR), is(true));
    }

    @Test
    public void testLog4jIntValueTolog4jLevelContainsWarn() {
        assertThat(Log4j.log4jIntValueTolog4jLevel().containsValue(Level.WARN), is(true));
    }

    @Test
    public void testLog4jIntValueTolog4jLevelContainsInfo() {
        assertThat(Log4j.log4jIntValueTolog4jLevel().containsValue(Level.INFO), is(true));
    }

    @Test
    public void testLog4jIntValueTolog4jLevelContainsDebug() {
        assertThat(Log4j.log4jIntValueTolog4jLevel().containsValue(Level.DEBUG), is(true));
    }

    @Test
    public void testLog4jIntValueTolog4jLevelContainsTrace() {
        assertThat(Log4j.log4jIntValueTolog4jLevel().containsValue(Level.TRACE), is(true));
    }

    @Test
    public void testLog4jIntValueTolog4jLevelContainsAll() {
        assertThat(Log4j.log4jIntValueTolog4jLevel().containsValue(Level.ALL), is(true));
    }

    @Test
    public void testLevelMapReturnsSameInstanceOnRepeatCall() {
        Map<Integer, Level> first = Log4j.log4jIntValueTolog4jLevel();
        Map<Integer, Level> second = Log4j.log4jIntValueTolog4jLevel();
        assertThat(second, is(sameInstance(first)));
    }

    // -----------------------------------------------------------------------
    // getLevel — initial state
    // -----------------------------------------------------------------------

    @Test
    public void testGetLevelAfterConstructionIsZero() {
        assertThat(new Log4j().getLevel(), is(0));
    }

    // -----------------------------------------------------------------------
    // setLevel(Level) — updates mDebugLevel via setLevel(Level, boolean)
    // -----------------------------------------------------------------------

    @Test
    public void testSetLevelLog4jDebugUpdatesGetLevel() {
        mLogger.setLevel(Level.DEBUG);
        assertThat(mLogger.getLevel(), is(LogManager.DEBUG_MESSAGE_LEVEL));
    }

    @Test
    public void testSetLevelLog4jInfoUpdatesGetLevel() {
        mLogger.setLevel(Level.INFO);
        assertThat(mLogger.getLevel(), is(LogManager.INFO_MESSAGE_LEVEL));
    }

    @Test
    public void testSetLevelLog4jWarnUpdatesGetLevel() {
        mLogger.setLevel(Level.WARN);
        assertThat(mLogger.getLevel(), is(LogManager.WARNING_MESSAGE_LEVEL));
    }

    @Test
    public void testSetLevelLog4jFatalUpdatesGetLevel() {
        mLogger.setLevel(Level.FATAL);
        assertThat(mLogger.getLevel(), is(LogManager.FATAL_MESSAGE_LEVEL));
    }

    @Test
    public void testSetLevelLog4jErrorUpdatesGetLevel() {
        mLogger.setLevel(Level.ERROR);
        assertThat(mLogger.getLevel(), is(LogManager.ERROR_MESSAGE_LEVEL));
    }

    // -----------------------------------------------------------------------
    // setLevel(int) — only calls Configurator, does NOT update mDebugLevel
    // -----------------------------------------------------------------------

    @Test
    public void testSetLevelIntDoesNotThrow() {
        assertDoesNotThrow(
                () -> mLogger.setLevel(LogManager.DEBUG_MESSAGE_LEVEL),
                "setLevel(int) should not throw");
    }

    @Test
    public void testSetLevelIntDoesNotUpdateGetLevel() {
        // setLevel(int) calls setLevel(int, boolean) which only calls Configurator.setLevel
        // and does NOT assign mDebugLevel — getLevel() stays at 0
        mLogger.setLevel(LogManager.DEBUG_MESSAGE_LEVEL);
        assertThat(mLogger.getLevel(), is(0));
    }

    @Test
    public void testSetLevelAllIntValuesDoNotThrow() {
        int[] levels = {
            LogManager.FATAL_MESSAGE_LEVEL,
            LogManager.ERROR_MESSAGE_LEVEL,
            LogManager.CONSOLE_MESSAGE_LEVEL,
            LogManager.WARNING_MESSAGE_LEVEL,
            LogManager.INFO_MESSAGE_LEVEL,
            LogManager.CONFIG_MESSAGE_LEVEL,
            LogManager.DEBUG_MESSAGE_LEVEL,
            LogManager.TRACE_MESSAGE_LEVEL
        };
        for (int level : levels) {
            final int l = level;
            assertDoesNotThrow(() -> mLogger.setLevel(l), "setLevel(" + l + ") should not throw");
        }
    }

    // -----------------------------------------------------------------------
    // configure() — no-op implementation
    // -----------------------------------------------------------------------

    @Test
    public void testConfigureTrueDoesNotThrow() {
        assertDoesNotThrow(
                () -> mLogger.configure(true),
                "configure(true) should not throw (no-op implementation)");
    }

    @Test
    public void testConfigureFalseDoesNotThrow() {
        assertDoesNotThrow(
                () -> mLogger.configure(false),
                "configure(false) should not throw (no-op implementation)");
    }

    // -----------------------------------------------------------------------
    // Unsupported stream operations
    // -----------------------------------------------------------------------

    @Test
    public void testSetWritersStringThrowsUnsupportedOperation() {
        assertThrows(
                UnsupportedOperationException.class,
                () -> mLogger.setWriters("stdout"),
                "setWriters(String) should throw UnsupportedOperationException for Log4j");
    }

    @Test
    public void testSetWriterPrintStreamThrowsUnsupportedOperation() {
        assertThrows(
                UnsupportedOperationException.class,
                () ->
                        mLogger.setWriter(
                                LogManager.STREAM_TYPE.stdout,
                                new PrintStream(new ByteArrayOutputStream())),
                "setWriter(STREAM_TYPE, PrintStream) should throw UnsupportedOperationException");
    }

    @Test
    public void testGetWriterThrowsUnsupportedOperation() {
        assertThrows(
                UnsupportedOperationException.class,
                () -> mLogger.getWriter(LogManager.STREAM_TYPE.stdout),
                "getWriter should throw UnsupportedOperationException for Log4j");
    }

    @Test
    public void testGetWriterStderrThrowsUnsupportedOperation() {
        assertThrows(
                UnsupportedOperationException.class,
                () -> mLogger.getWriter(LogManager.STREAM_TYPE.stderr),
                "getWriter(stderr) should throw UnsupportedOperationException for Log4j");
    }

    // -----------------------------------------------------------------------
    // log(String, int) — smoke tests at each level
    // -----------------------------------------------------------------------

    @Test
    public void testLogAtFatalLevelDoesNotThrow() {
        assertDoesNotThrow(() -> mLogger.log("fatal-msg", LogManager.FATAL_MESSAGE_LEVEL));
    }

    @Test
    public void testLogAtErrorLevelDoesNotThrow() {
        assertDoesNotThrow(() -> mLogger.log("error-msg", LogManager.ERROR_MESSAGE_LEVEL));
    }

    @Test
    public void testLogAtWarningLevelDoesNotThrow() {
        assertDoesNotThrow(() -> mLogger.log("warn-msg", LogManager.WARNING_MESSAGE_LEVEL));
    }

    @Test
    public void testLogAtInfoLevelDoesNotThrow() {
        assertDoesNotThrow(() -> mLogger.log("info-msg", LogManager.INFO_MESSAGE_LEVEL));
    }

    @Test
    public void testLogAtConfigLevelDoesNotThrow() {
        assertDoesNotThrow(() -> mLogger.log("config-msg", LogManager.CONFIG_MESSAGE_LEVEL));
    }

    @Test
    public void testLogAtDebugLevelDoesNotThrow() {
        mLogger.setLevel(Level.DEBUG);
        assertDoesNotThrow(() -> mLogger.log("debug-msg", LogManager.DEBUG_MESSAGE_LEVEL));
    }

    @Test
    public void testLogAtTraceLevelDoesNotThrow() {
        mLogger.setLevel(Level.TRACE);
        assertDoesNotThrow(() -> mLogger.log("trace-msg", LogManager.TRACE_MESSAGE_LEVEL));
    }

    // -----------------------------------------------------------------------
    // log(String, Exception, int) — smoke tests at each level
    // -----------------------------------------------------------------------

    @Test
    public void testLogExceptionAtFatalLevelDoesNotThrow() {
        assertDoesNotThrow(
                () ->
                        mLogger.log(
                                "fatal-ex",
                                new RuntimeException("boom"),
                                LogManager.FATAL_MESSAGE_LEVEL));
    }

    @Test
    public void testLogExceptionAtErrorLevelDoesNotThrow() {
        assertDoesNotThrow(
                () ->
                        mLogger.log(
                                "error-ex",
                                new RuntimeException("boom"),
                                LogManager.ERROR_MESSAGE_LEVEL));
    }

    @Test
    public void testLogExceptionAtWarningLevelDoesNotThrow() {
        assertDoesNotThrow(
                () ->
                        mLogger.log(
                                "warn-ex",
                                new RuntimeException("boom"),
                                LogManager.WARNING_MESSAGE_LEVEL));
    }

    @Test
    public void testLogExceptionAtInfoLevelDoesNotThrow() {
        assertDoesNotThrow(
                () ->
                        mLogger.log(
                                "info-ex",
                                new RuntimeException("boom"),
                                LogManager.INFO_MESSAGE_LEVEL));
    }

    @Test
    public void testLogExceptionAtDebugLevelDoesNotThrow() {
        mLogger.setLevel(Level.DEBUG);
        assertDoesNotThrow(
                () ->
                        mLogger.log(
                                "debug-ex",
                                new RuntimeException("boom"),
                                LogManager.DEBUG_MESSAGE_LEVEL));
    }

    @Test
    public void testLogExceptionAtTraceLevelDoesNotThrow() {
        mLogger.setLevel(Level.TRACE);
        assertDoesNotThrow(
                () ->
                        mLogger.log(
                                "trace-ex",
                                new RuntimeException("boom"),
                                LogManager.TRACE_MESSAGE_LEVEL));
    }

    // -----------------------------------------------------------------------
    // add() + log(int) — buffered logging path
    // -----------------------------------------------------------------------

    @Test
    public void testAddAndLogDoesNotThrow() {
        assertDoesNotThrow(
                () -> {
                    mLogger.add("buffered-value");
                    mLogger.log(LogManager.INFO_MESSAGE_LEVEL);
                },
                "add() followed by log(level) should not throw");
    }

    @Test
    public void testAddKeyValueAndLogDoesNotThrow() {
        assertDoesNotThrow(
                () -> {
                    mLogger.add("k", "v");
                    mLogger.log(LogManager.WARNING_MESSAGE_LEVEL);
                },
                "add(key,value) followed by log(level) should not throw");
    }

    @Test
    public void testAddReturnsLogManagerForChaining() {
        LogManager result = mLogger.add("some-value");
        assertThat(result, is(sameInstance(mLogger)));
    }

    // -----------------------------------------------------------------------
    // logEventStart / logEventCompletion
    // -----------------------------------------------------------------------

    @Test
    public void testLogEventStartDoesNotThrow() {
        assertDoesNotThrow(
                () -> mLogger.logEventStart("event.test", "workflow", "wf-001"),
                "logEventStart(name, entity, id) should not throw");
    }

    @Test
    public void testLogEventStartWithLevelDoesNotThrow() {
        assertDoesNotThrow(
                () ->
                        mLogger.logEventStart(
                                "event.test", "workflow", "wf-001", LogManager.INFO_MESSAGE_LEVEL),
                "logEventStart with explicit level should not throw");
    }

    @Test
    public void testLogEventCompletionDoesNotThrow() {
        mLogger.logEventStart("event.test", "workflow", "wf-001");
        assertDoesNotThrow(
                () -> mLogger.logEventCompletion(), "logEventCompletion() should not throw");
    }

    @Test
    public void testLogEventCompletionWithLevelDoesNotThrow() {
        mLogger.logEventStart("event.test", "workflow", "wf-001", LogManager.INFO_MESSAGE_LEVEL);
        assertDoesNotThrow(
                () -> mLogger.logEventCompletion(LogManager.INFO_MESSAGE_LEVEL),
                "logEventCompletion(level) should not throw");
    }

    // -----------------------------------------------------------------------
    // logEntityHierarchyMessage
    // -----------------------------------------------------------------------

    @Test
    public void testLogEntityHierarchyMessageDoesNotThrow() {
        assertDoesNotThrow(
                () ->
                        mLogger.logEntityHierarchyMessage(
                                "workflow", "wf-001", "job", Arrays.asList("job-1", "job-2")),
                "logEntityHierarchyMessage should not throw");
    }

    @Test
    public void testLogEntityHierarchyMessageWithLevelDoesNotThrow() {
        assertDoesNotThrow(
                () ->
                        mLogger.logEntityHierarchyMessage(
                                "workflow",
                                "wf-001",
                                "job",
                                Arrays.asList("job-1"),
                                LogManager.INFO_MESSAGE_LEVEL),
                "logEntityHierarchyMessage with explicit level should not throw");
    }

    // -----------------------------------------------------------------------
    // initialize()
    // -----------------------------------------------------------------------

    @Test
    public void testInitializeWithLog4jConfPropertyDoesNotThrow() {
        Properties props = new Properties();
        props.setProperty("log4j.conf", "/nonexistent/path/log4j2.xml");
        // Even with a bad path, initialize should not throw — Configurator silently ignores it
        assertDoesNotThrow(
                () -> mLogger.initialize(new Simple(), props),
                "initialize() with a log4j.conf property should not throw");
    }

    @Test
    public void testInitializeWithEmptyPropertiesDoesNotThrow() {
        assertDoesNotThrow(
                () -> mLogger.initialize(new Simple(), new Properties()),
                "initialize() with empty properties should not throw");
    }

    @Test
    public void testConstructorInitializesLoggerFields() throws Exception {
        Log4j logger = new Log4j();
        assertThat(ReflectionTestUtils.getField(logger, "mLogger"), is(notNullValue()));
        assertThat(ReflectionTestUtils.getField(Log4j.class, "mRoot"), is(notNullValue()));
    }

    @Test
    public void testInitializeSetsFormatterProgramNameToPegasus() {
        Simple formatter = new Simple();
        formatter.setProgramName("custom-name");

        mLogger.initialize(formatter, new Properties());

        assertThat(formatter.getProgramName("ignored"), is("pegasus"));
    }

    @Test
    public void testInitializeStoresPropertiesReference() throws Exception {
        Properties props = new Properties();
        props.setProperty("sample", "value");

        mLogger.initialize(new Simple(), props);

        assertThat(ReflectionTestUtils.getField(mLogger, "mProperties"), is(sameInstance(props)));
    }

    @Test
    public void testLevelMapContainsExpectedIntKeyMappings() {
        Map<Integer, Level> map = Log4j.log4jIntValueTolog4jLevel();
        assertThat(map.get(Level.INFO.intLevel()), is(Level.INFO));
        assertThat(map.get(Level.ALL.intLevel()), is(Level.ALL));
    }
}
