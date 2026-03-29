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
import java.util.Properties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Tests for the Default LogManager implementation. */
public class DefaultTest {

    private Default mLogger;

    /** Captures output written to the configured stdout stream. */
    private ByteArrayOutputStream mOutCapture;

    /** Captures output written to the configured stderr stream. */
    private ByteArrayOutputStream mErrCapture;

    @BeforeEach
    public void setUp() {
        mLogger = new Default();
        mLogger.initialize(new Simple(), new Properties());

        // Disable timestamps so output is deterministic in assertions
        mLogger.configure(false);

        mOutCapture = new ByteArrayOutputStream();
        mErrCapture = new ByteArrayOutputStream();
        mLogger.setWriter(LogManager.STREAM_TYPE.stdout, new PrintStream(mOutCapture, true));
        mLogger.setWriter(LogManager.STREAM_TYPE.stderr, new PrintStream(mErrCapture, true));
    }

    // -----------------------------------------------------------------------
    // Class-level contract
    // -----------------------------------------------------------------------

    @Test
    public void testDefaultIsConcreteClass() {
        assertFalse(
                Modifier.isAbstract(Default.class.getModifiers()),
                "Default should be a concrete class");
    }

    @Test
    public void testDefaultExtendsLogManager() {
        assertTrue(mLogger instanceof LogManager, "Default should extend LogManager");
    }

    // -----------------------------------------------------------------------
    // Level get / set
    // -----------------------------------------------------------------------

    @Test
    public void testDefaultLevelAfterConstruction() {
        assertEquals(
                0, new Default().getLevel(), "Default debug level after construction should be 0");
    }

    @Test
    public void testSetLevelAndGetLevel() {
        mLogger.setLevel(LogManager.DEBUG_MESSAGE_LEVEL);
        assertEquals(
                LogManager.DEBUG_MESSAGE_LEVEL,
                mLogger.getLevel(),
                "getLevel should return the level that was set");
    }

    @Test
    public void testSetLevelFatal() {
        mLogger.setLevel(LogManager.FATAL_MESSAGE_LEVEL);
        assertEquals(LogManager.FATAL_MESSAGE_LEVEL, mLogger.getLevel());
    }

    @Test
    public void testSetLevelTrace() {
        mLogger.setLevel(LogManager.TRACE_MESSAGE_LEVEL);
        assertEquals(LogManager.TRACE_MESSAGE_LEVEL, mLogger.getLevel());
    }

    @Test
    public void testSetLevelInfo() {
        mLogger.setLevel(LogManager.INFO_MESSAGE_LEVEL);
        assertEquals(LogManager.INFO_MESSAGE_LEVEL, mLogger.getLevel());
    }

    // -----------------------------------------------------------------------
    // configure — timestamp prefix
    // -----------------------------------------------------------------------

    @Test
    public void testConfigureFalseOmitsTimestampFromOutput() {
        mLogger.configure(false);
        mLogger.setLevel(LogManager.WARNING_MESSAGE_LEVEL);
        mLogger.log("no-ts-message", LogManager.WARNING_MESSAGE_LEVEL);

        String output = mErrCapture.toString();
        // With configure(false) the line must start with the prefix bracket, not a date
        assertTrue(
                output.startsWith("[WARNING]"),
                "With configure(false) output should start immediately with the level prefix");
    }

    @Test
    public void testConfigureTrueAddsTimestampToOutput() {
        mLogger.configure(true);
        mLogger.setLevel(LogManager.WARNING_MESSAGE_LEVEL);
        mLogger.log("ts-message", LogManager.WARNING_MESSAGE_LEVEL);

        String output = mErrCapture.toString();
        // Timestamp format is "yyyy.MM.dd HH:mm:ss.SSS zzz: " — starts with a digit
        assertFalse(
                output.startsWith("[WARNING]"),
                "With configure(true) output should be prefixed by a timestamp before the level tag");
    }

    // -----------------------------------------------------------------------
    // setWriter / getWriter
    // -----------------------------------------------------------------------

    @Test
    public void testGetWriterStdoutReturnsNonNull() {
        assertNotNull(
                mLogger.getWriter(LogManager.STREAM_TYPE.stdout),
                "getWriter(stdout) should return a non-null PrintStream");
    }

    @Test
    public void testGetWriterStderrReturnsNonNull() {
        assertNotNull(
                mLogger.getWriter(LogManager.STREAM_TYPE.stderr),
                "getWriter(stderr) should return a non-null PrintStream");
    }

    @Test
    public void testSetWriterStdoutRoundTrip() {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        PrintStream ps = new PrintStream(bos);
        mLogger.setWriter(LogManager.STREAM_TYPE.stdout, ps);
        assertSame(
                ps,
                mLogger.getWriter(LogManager.STREAM_TYPE.stdout),
                "getWriter(stdout) should return the stream set via setWriter");
    }

    @Test
    public void testSetWriterStderrRoundTrip() {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        PrintStream ps = new PrintStream(bos);
        mLogger.setWriter(LogManager.STREAM_TYPE.stderr, ps);
        assertSame(
                ps,
                mLogger.getWriter(LogManager.STREAM_TYPE.stderr),
                "getWriter(stderr) should return the stream set via setWriter");
    }

    @Test
    public void testGetWriterUnsupportedTypeThrows() {
        assertThrows(
                RuntimeException.class,
                () -> mLogger.getWriter(null),
                "getWriter with null/unsupported type should throw RuntimeException");
    }

    @Test
    public void testSetWriterUnsupportedTypeThrows() {
        assertThrows(
                RuntimeException.class,
                () -> mLogger.setWriter(null, new PrintStream(new ByteArrayOutputStream())),
                "setWriter with null/unsupported type should throw RuntimeException");
    }

    // -----------------------------------------------------------------------
    // Stream routing — error-bound levels go to stderr, others to stdout
    // -----------------------------------------------------------------------

    @Test
    public void testFatalMessageRoutedToErrStream() {
        mLogger.setLevel(LogManager.FATAL_MESSAGE_LEVEL);
        mLogger.log("fatal-test", LogManager.FATAL_MESSAGE_LEVEL);
        assertTrue(
                mErrCapture.toString().contains("fatal-test"),
                "FATAL messages should be written to the error stream");
        assertFalse(
                mOutCapture.toString().contains("fatal-test"),
                "FATAL messages should not appear on the output stream");
    }

    @Test
    public void testErrorMessageRoutedToErrStream() {
        mLogger.setLevel(LogManager.ERROR_MESSAGE_LEVEL);
        mLogger.log("error-test", LogManager.ERROR_MESSAGE_LEVEL);
        assertTrue(
                mErrCapture.toString().contains("error-test"),
                "ERROR messages should be written to the error stream");
    }

    @Test
    public void testWarningMessageRoutedToErrStream() {
        // WARNING is in the default mask, no setLevel needed
        mLogger.log("warning-test", LogManager.WARNING_MESSAGE_LEVEL);
        assertTrue(
                mErrCapture.toString().contains("warning-test"),
                "WARNING messages should be written to the error stream");
    }

    @Test
    public void testInfoMessageRoutedToOutStream() {
        mLogger.setLevel(LogManager.INFO_MESSAGE_LEVEL);
        mLogger.log("info-test", LogManager.INFO_MESSAGE_LEVEL);
        assertTrue(
                mOutCapture.toString().contains("info-test"),
                "INFO messages should be written to the output stream");
        assertFalse(
                mErrCapture.toString().contains("info-test"),
                "INFO messages should not appear on the error stream");
    }

    @Test
    public void testDebugMessageRoutedToOutStream() {
        mLogger.setLevel(LogManager.DEBUG_MESSAGE_LEVEL);
        mLogger.log("debug-test", LogManager.DEBUG_MESSAGE_LEVEL);
        assertTrue(
                mOutCapture.toString().contains("debug-test"),
                "DEBUG messages should be written to the output stream");
    }

    @Test
    public void testConsoleMessageRoutedToOutStream() {
        mLogger.setLevel(LogManager.CONSOLE_MESSAGE_LEVEL);
        mLogger.log("console-test", LogManager.CONSOLE_MESSAGE_LEVEL);
        assertTrue(
                mOutCapture.toString().contains("console-test"),
                "CONSOLE messages should be written to the output stream");
    }

    // -----------------------------------------------------------------------
    // Level prefix in output
    // -----------------------------------------------------------------------

    @Test
    public void testFatalMessageContainsFatalPrefix() {
        mLogger.setLevel(LogManager.FATAL_MESSAGE_LEVEL);
        mLogger.log("msg", LogManager.FATAL_MESSAGE_LEVEL);
        assertTrue(
                mErrCapture.toString().contains("[FATAL ERROR]"),
                "FATAL output should contain '[FATAL ERROR]' prefix");
    }

    @Test
    public void testErrorMessageContainsErrorPrefix() {
        mLogger.setLevel(LogManager.ERROR_MESSAGE_LEVEL);
        mLogger.log("msg", LogManager.ERROR_MESSAGE_LEVEL);
        assertTrue(
                mErrCapture.toString().contains("[ERROR]"),
                "ERROR output should contain '[ERROR]' prefix");
    }

    @Test
    public void testWarningMessageContainsWarningPrefix() {
        mLogger.log("msg", LogManager.WARNING_MESSAGE_LEVEL);
        assertTrue(
                mErrCapture.toString().contains("[WARNING]"),
                "WARNING output should contain '[WARNING]' prefix");
    }

    @Test
    public void testInfoMessageContainsInfoPrefix() {
        mLogger.setLevel(LogManager.INFO_MESSAGE_LEVEL);
        mLogger.log("msg", LogManager.INFO_MESSAGE_LEVEL);
        assertTrue(
                mOutCapture.toString().contains("[INFO]"),
                "INFO output should contain '[INFO]' prefix");
    }

    @Test
    public void testDebugMessageContainsDebugPrefix() {
        mLogger.setLevel(LogManager.DEBUG_MESSAGE_LEVEL);
        mLogger.log("msg", LogManager.DEBUG_MESSAGE_LEVEL);
        assertTrue(
                mOutCapture.toString().contains("[DEBUG]"),
                "DEBUG output should contain '[DEBUG]' prefix");
    }

    // -----------------------------------------------------------------------
    // Message suppression when below configured level
    // -----------------------------------------------------------------------

    @Test
    public void testInfoMessageSuppressedAtDefaultLevel() {
        // Default mask covers only up to WARNING (level 3); INFO (level 4) must be suppressed
        mLogger.log("suppressed-info", LogManager.INFO_MESSAGE_LEVEL);
        assertFalse(
                mOutCapture.toString().contains("suppressed-info"),
                "INFO messages should be suppressed when level is at default (WARNING)");
    }

    @Test
    public void testDebugMessageSuppressedBelowDebugLevel() {
        mLogger.setLevel(LogManager.WARNING_MESSAGE_LEVEL);
        mLogger.log("suppressed-debug", LogManager.DEBUG_MESSAGE_LEVEL);
        assertFalse(
                mOutCapture.toString().contains("suppressed-debug"),
                "DEBUG messages should be suppressed when level is set to WARNING");
    }

    @Test
    public void testMessageAppearsAfterLevelIsRaised() {
        mLogger.setLevel(LogManager.DEBUG_MESSAGE_LEVEL);
        mLogger.log("now-visible", LogManager.DEBUG_MESSAGE_LEVEL);
        assertTrue(
                mOutCapture.toString().contains("now-visible"),
                "DEBUG message should appear after setLevel(DEBUG)");
    }

    // -----------------------------------------------------------------------
    // log(String, Exception, int)
    // -----------------------------------------------------------------------

    @Test
    public void testLogWithExceptionContainsMessage() {
        mLogger.log("ex-msg", new RuntimeException("boom"), LogManager.WARNING_MESSAGE_LEVEL);
        assertTrue(
                mErrCapture.toString().contains("ex-msg"),
                "log with exception should include the provided message");
    }

    @Test
    public void testLogWithExceptionContainsExceptionMessage() {
        mLogger.log("ex-msg", new RuntimeException("boom"), LogManager.WARNING_MESSAGE_LEVEL);
        assertTrue(
                mErrCapture.toString().contains("boom"),
                "log with exception should include the exception's message");
    }

    @Test
    public void testLogWithExceptionContainsExceptionClass() {
        mLogger.log(
                "ex-msg", new IllegalArgumentException("bad"), LogManager.WARNING_MESSAGE_LEVEL);
        assertTrue(
                mErrCapture.toString().contains("IllegalArgumentException"),
                "log with exception should include the exception class name");
    }

    // -----------------------------------------------------------------------
    // add() + log(int) — buffered logging path
    // -----------------------------------------------------------------------

    @Test
    public void testAddAndLogWritesValueToStream() {
        mLogger.setLevel(LogManager.WARNING_MESSAGE_LEVEL);
        mLogger.add("buffered-value");
        mLogger.log(LogManager.WARNING_MESSAGE_LEVEL);
        assertTrue(
                mErrCapture.toString().contains("buffered-value"),
                "add() followed by log(level) should write the buffered value");
    }

    @Test
    public void testAddKeyValueAndLogWritesValueToStream() {
        mLogger.setLevel(LogManager.WARNING_MESSAGE_LEVEL);
        mLogger.add("k", "kv-value");
        mLogger.log(LogManager.WARNING_MESSAGE_LEVEL);
        assertTrue(
                mErrCapture.toString().contains("kv-value"),
                "add(key,value) followed by log(level) should write the value");
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
    public void testLogEventStartWritesToStream() {
        mLogger.setLevel(LogManager.INFO_MESSAGE_LEVEL);
        mLogger.logEventStart("event.test", "workflow", "wf-001");
        assertFalse(
                mOutCapture.toString().isEmpty(),
                "logEventStart should produce output on the stream");
    }

    @Test
    public void testLogEventCompletionWritesToStream() {
        mLogger.setLevel(LogManager.INFO_MESSAGE_LEVEL);
        mLogger.logEventStart("event.test", "workflow", "wf-001");
        mOutCapture.reset(); // clear start output
        mLogger.logEventCompletion();
        assertFalse(
                mOutCapture.toString().isEmpty(),
                "logEventCompletion should produce output on the stream");
    }

    @Test
    public void testLogEntityHierarchyMessageDoesNotThrow() {
        mLogger.setLevel(LogManager.DEBUG_MESSAGE_LEVEL);
        assertDoesNotThrow(
                () ->
                        mLogger.logEntityHierarchyMessage(
                                "workflow", "wf-001", "job", Arrays.asList("job-1", "job-2")),
                "logEntityHierarchyMessage should not throw");
    }

    // -----------------------------------------------------------------------
    // getTimeStamp
    // -----------------------------------------------------------------------

    @Test
    public void testGetTimestampIsNotEmpty() {
        String ts = mLogger.getTimeStamp();
        assertNotNull(ts, "getTimeStamp should not return null");
        assertFalse(ts.isEmpty(), "getTimeStamp should not return empty string");
    }
}
