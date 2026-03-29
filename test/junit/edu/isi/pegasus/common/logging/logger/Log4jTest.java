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

/** Tests for the Log4j LogManager implementation. */
public class Log4jTest {

    private Log4j mLogger;

    @BeforeEach
    public void setUp() {
        mLogger = new Log4j();
        mLogger.initialize(new Simple(), new Properties());
    }

    // -----------------------------------------------------------------------
    // Class-level contract
    // -----------------------------------------------------------------------

    @Test
    public void testLog4jIsConcreteClass() {
        assertFalse(
                Modifier.isAbstract(Log4j.class.getModifiers()),
                "Log4j should be a concrete class");
    }

    @Test
    public void testLog4jExtendsLogManager() {
        assertTrue(mLogger instanceof LogManager, "Log4j should extend LogManager");
    }

    // -----------------------------------------------------------------------
    // Static level map — log4jIntValueTolog4jLevel()
    // -----------------------------------------------------------------------

    @Test
    public void testLog4jIntValueTolog4jLevelNotNull() {
        Map<Integer, Level> map = Log4j.log4jIntValueTolog4jLevel();
        assertNotNull(map, "log4jIntValueTolog4jLevel() should not return null");
        assertFalse(map.isEmpty(), "The level map should not be empty");
    }

    @Test
    public void testLevelMapContainsSevenEntries() {
        // FATAL, ERROR, WARN, INFO, DEBUG, TRACE, ALL
        assertEquals(
                7,
                Log4j.log4jIntValueTolog4jLevel().size(),
                "Level map should contain exactly 7 entries");
    }

    @Test
    public void testLog4jIntValueTolog4jLevelContainsFatal() {
        assertTrue(
                Log4j.log4jIntValueTolog4jLevel().containsValue(Level.FATAL),
                "Level map should contain FATAL");
    }

    @Test
    public void testLog4jIntValueTolog4jLevelContainsError() {
        assertTrue(
                Log4j.log4jIntValueTolog4jLevel().containsValue(Level.ERROR),
                "Level map should contain ERROR");
    }

    @Test
    public void testLog4jIntValueTolog4jLevelContainsWarn() {
        assertTrue(
                Log4j.log4jIntValueTolog4jLevel().containsValue(Level.WARN),
                "Level map should contain WARN");
    }

    @Test
    public void testLog4jIntValueTolog4jLevelContainsInfo() {
        assertTrue(
                Log4j.log4jIntValueTolog4jLevel().containsValue(Level.INFO),
                "Level map should contain INFO");
    }

    @Test
    public void testLog4jIntValueTolog4jLevelContainsDebug() {
        assertTrue(
                Log4j.log4jIntValueTolog4jLevel().containsValue(Level.DEBUG),
                "Level map should contain DEBUG");
    }

    @Test
    public void testLog4jIntValueTolog4jLevelContainsTrace() {
        assertTrue(
                Log4j.log4jIntValueTolog4jLevel().containsValue(Level.TRACE),
                "Level map should contain TRACE");
    }

    @Test
    public void testLog4jIntValueTolog4jLevelContainsAll() {
        assertTrue(
                Log4j.log4jIntValueTolog4jLevel().containsValue(Level.ALL),
                "Level map should contain ALL");
    }

    @Test
    public void testLevelMapReturnsSameInstanceOnRepeatCall() {
        Map<Integer, Level> first = Log4j.log4jIntValueTolog4jLevel();
        Map<Integer, Level> second = Log4j.log4jIntValueTolog4jLevel();
        assertSame(
                first, second, "log4jIntValueTolog4jLevel() should return the same singleton map");
    }

    // -----------------------------------------------------------------------
    // getLevel — initial state
    // -----------------------------------------------------------------------

    @Test
    public void testGetLevelAfterConstructionIsZero() {
        assertEquals(
                0,
                new Log4j().getLevel(),
                "mDebugLevel should be 0 immediately after construction");
    }

    // -----------------------------------------------------------------------
    // setLevel(Level) — updates mDebugLevel via setLevel(Level, boolean)
    // -----------------------------------------------------------------------

    @Test
    public void testSetLevelLog4jDebugUpdatesGetLevel() {
        mLogger.setLevel(Level.DEBUG);
        assertEquals(
                LogManager.DEBUG_MESSAGE_LEVEL,
                mLogger.getLevel(),
                "setLevel(Level.DEBUG) should update getLevel() to DEBUG_MESSAGE_LEVEL");
    }

    @Test
    public void testSetLevelLog4jInfoUpdatesGetLevel() {
        mLogger.setLevel(Level.INFO);
        assertEquals(
                LogManager.INFO_MESSAGE_LEVEL,
                mLogger.getLevel(),
                "setLevel(Level.INFO) should update getLevel() to INFO_MESSAGE_LEVEL");
    }

    @Test
    public void testSetLevelLog4jWarnUpdatesGetLevel() {
        mLogger.setLevel(Level.WARN);
        assertEquals(
                LogManager.WARNING_MESSAGE_LEVEL,
                mLogger.getLevel(),
                "setLevel(Level.WARN) should update getLevel() to WARNING_MESSAGE_LEVEL");
    }

    @Test
    public void testSetLevelLog4jFatalUpdatesGetLevel() {
        mLogger.setLevel(Level.FATAL);
        assertEquals(
                LogManager.FATAL_MESSAGE_LEVEL,
                mLogger.getLevel(),
                "setLevel(Level.FATAL) should update getLevel() to FATAL_MESSAGE_LEVEL");
    }

    @Test
    public void testSetLevelLog4jErrorUpdatesGetLevel() {
        mLogger.setLevel(Level.ERROR);
        assertEquals(
                LogManager.ERROR_MESSAGE_LEVEL,
                mLogger.getLevel(),
                "setLevel(Level.ERROR) should update getLevel() to ERROR_MESSAGE_LEVEL");
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
        assertEquals(
                0,
                mLogger.getLevel(),
                "setLevel(int) does not update mDebugLevel; getLevel() should remain 0");
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
        assertSame(mLogger, result, "add(value) should return self-reference");
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
}
