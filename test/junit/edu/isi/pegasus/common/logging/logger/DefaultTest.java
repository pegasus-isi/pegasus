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

        // Current formatter-backed logging paths require an event on the stack.
        mLogger.logEventStart("test.default.logger", "logger", "default");
        mOutCapture.reset();
        mErrCapture.reset();
    }

    // -----------------------------------------------------------------------
    // Class-level contract
    // -----------------------------------------------------------------------

    @Test
    public void testDefaultIsConcreteClass() {
        assertThat(Modifier.isAbstract(Default.class.getModifiers()), is(false));
    }

    @Test
    public void testDefaultExtendsLogManager() {
        assertThat(mLogger, instanceOf(LogManager.class));
    }

    // -----------------------------------------------------------------------
    // Level get / set
    // -----------------------------------------------------------------------

    @Test
    public void testDefaultLevelAfterConstruction() {
        assertThat(new Default().getLevel(), is(0));
    }

    @Test
    public void testSetLevelAndGetLevel() {
        mLogger.setLevel(LogManager.DEBUG_MESSAGE_LEVEL);
        assertThat(mLogger.getLevel(), is(LogManager.DEBUG_MESSAGE_LEVEL));
    }

    @Test
    public void testSetLevelFatal() {
        mLogger.setLevel(LogManager.FATAL_MESSAGE_LEVEL);
        assertThat(mLogger.getLevel(), is(LogManager.FATAL_MESSAGE_LEVEL));
    }

    @Test
    public void testSetLevelTrace() {
        mLogger.setLevel(LogManager.TRACE_MESSAGE_LEVEL);
        assertThat(mLogger.getLevel(), is(LogManager.TRACE_MESSAGE_LEVEL));
    }

    @Test
    public void testSetLevelInfo() {
        mLogger.setLevel(LogManager.INFO_MESSAGE_LEVEL);
        assertThat(mLogger.getLevel(), is(LogManager.INFO_MESSAGE_LEVEL));
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
        assertThat(output, startsWith("[WARNING]"));
    }

    @Test
    public void testConfigureTrueAddsTimestampToOutput() {
        mLogger.configure(true);
        mLogger.setLevel(LogManager.WARNING_MESSAGE_LEVEL);
        mLogger.log("ts-message", LogManager.WARNING_MESSAGE_LEVEL);

        String output = mErrCapture.toString();
        // Timestamp format is "yyyy.MM.dd HH:mm:ss.SSS zzz: " — starts with a digit
        assertThat(output, not(startsWith("[WARNING]")));
    }

    // -----------------------------------------------------------------------
    // setWriter / getWriter
    // -----------------------------------------------------------------------

    @Test
    public void testGetWriterStdoutReturnsNonNull() {
        assertThat(mLogger.getWriter(LogManager.STREAM_TYPE.stdout), is(notNullValue()));
    }

    @Test
    public void testGetWriterStderrReturnsNonNull() {
        assertThat(mLogger.getWriter(LogManager.STREAM_TYPE.stderr), is(notNullValue()));
    }

    @Test
    public void testSetWriterStdoutRoundTrip() {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        PrintStream ps = new PrintStream(bos);
        mLogger.setWriter(LogManager.STREAM_TYPE.stdout, ps);
        assertThat(mLogger.getWriter(LogManager.STREAM_TYPE.stdout), is(sameInstance(ps)));
    }

    @Test
    public void testSetWriterStderrRoundTrip() {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        PrintStream ps = new PrintStream(bos);
        mLogger.setWriter(LogManager.STREAM_TYPE.stderr, ps);
        assertThat(mLogger.getWriter(LogManager.STREAM_TYPE.stderr), is(sameInstance(ps)));
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
        assertThat(mErrCapture.toString(), containsString("fatal-test"));
        assertThat(mOutCapture.toString(), not(containsString("fatal-test")));
    }

    @Test
    public void testErrorMessageRoutedToErrStream() {
        mLogger.setLevel(LogManager.ERROR_MESSAGE_LEVEL);
        mLogger.log("error-test", LogManager.ERROR_MESSAGE_LEVEL);
        assertThat(mErrCapture.toString(), containsString("error-test"));
    }

    @Test
    public void testWarningMessageRoutedToErrStream() {
        // WARNING is in the default mask, no setLevel needed
        mLogger.log("warning-test", LogManager.WARNING_MESSAGE_LEVEL);
        assertThat(mErrCapture.toString(), containsString("warning-test"));
    }

    @Test
    public void testInfoMessageRoutedToOutStream() {
        mLogger.setLevel(LogManager.INFO_MESSAGE_LEVEL);
        mLogger.log("info-test", LogManager.INFO_MESSAGE_LEVEL);
        assertThat(mOutCapture.toString(), containsString("info-test"));
        assertThat(mErrCapture.toString(), not(containsString("info-test")));
    }

    @Test
    public void testDebugMessageRoutedToOutStream() {
        mLogger.setLevel(LogManager.DEBUG_MESSAGE_LEVEL);
        mLogger.log("debug-test", LogManager.DEBUG_MESSAGE_LEVEL);
        assertThat(mOutCapture.toString(), containsString("debug-test"));
    }

    @Test
    public void testConsoleMessageRoutedToOutStream() {
        mLogger.setLevel(LogManager.CONSOLE_MESSAGE_LEVEL);
        mLogger.log("console-test", LogManager.CONSOLE_MESSAGE_LEVEL);
        assertThat(mOutCapture.toString(), containsString("console-test"));
    }

    // -----------------------------------------------------------------------
    // Level prefix in output
    // -----------------------------------------------------------------------

    @Test
    public void testFatalMessageContainsFatalPrefix() {
        mLogger.setLevel(LogManager.FATAL_MESSAGE_LEVEL);
        mLogger.log("msg", LogManager.FATAL_MESSAGE_LEVEL);
        assertThat(mErrCapture.toString(), containsString("[FATAL ERROR]"));
    }

    @Test
    public void testErrorMessageContainsErrorPrefix() {
        mLogger.setLevel(LogManager.ERROR_MESSAGE_LEVEL);
        mLogger.log("msg", LogManager.ERROR_MESSAGE_LEVEL);
        assertThat(mErrCapture.toString(), containsString("[ERROR]"));
    }

    @Test
    public void testWarningMessageContainsWarningPrefix() {
        mLogger.log("msg", LogManager.WARNING_MESSAGE_LEVEL);
        assertThat(mErrCapture.toString(), containsString("[WARNING]"));
    }

    @Test
    public void testInfoMessageContainsInfoPrefix() {
        mLogger.setLevel(LogManager.INFO_MESSAGE_LEVEL);
        mLogger.log("msg", LogManager.INFO_MESSAGE_LEVEL);
        assertThat(mOutCapture.toString(), containsString("[INFO]"));
    }

    @Test
    public void testDebugMessageContainsDebugPrefix() {
        mLogger.setLevel(LogManager.DEBUG_MESSAGE_LEVEL);
        mLogger.log("msg", LogManager.DEBUG_MESSAGE_LEVEL);
        assertThat(mOutCapture.toString(), containsString("[DEBUG]"));
    }

    // -----------------------------------------------------------------------
    // Message suppression when below configured level
    // -----------------------------------------------------------------------

    @Test
    public void testInfoMessageSuppressedAtDefaultLevel() {
        // Default mask covers only up to WARNING (level 3); INFO (level 4) must be suppressed
        mLogger.log("suppressed-info", LogManager.INFO_MESSAGE_LEVEL);
        assertThat(mOutCapture.toString(), not(containsString("suppressed-info")));
    }

    @Test
    public void testDebugMessageSuppressedBelowDebugLevel() {
        mLogger.setLevel(LogManager.WARNING_MESSAGE_LEVEL);
        mLogger.log("suppressed-debug", LogManager.DEBUG_MESSAGE_LEVEL);
        assertThat(mOutCapture.toString(), not(containsString("suppressed-debug")));
    }

    @Test
    public void testMessageAppearsAfterLevelIsRaised() {
        mLogger.setLevel(LogManager.DEBUG_MESSAGE_LEVEL);
        mLogger.log("now-visible", LogManager.DEBUG_MESSAGE_LEVEL);
        assertThat(mOutCapture.toString(), containsString("now-visible"));
    }

    // -----------------------------------------------------------------------
    // log(String, Exception, int)
    // -----------------------------------------------------------------------

    @Test
    public void testLogWithExceptionContainsMessage() {
        mLogger.log("ex-msg", new RuntimeException("boom"), LogManager.WARNING_MESSAGE_LEVEL);
        assertThat(mErrCapture.toString(), containsString("ex-msg"));
    }

    @Test
    public void testLogWithExceptionContainsExceptionMessage() {
        mLogger.log("ex-msg", new RuntimeException("boom"), LogManager.WARNING_MESSAGE_LEVEL);
        assertThat(mErrCapture.toString(), containsString("boom"));
    }

    @Test
    public void testLogWithExceptionContainsExceptionClass() {
        mLogger.log(
                "ex-msg", new IllegalArgumentException("bad"), LogManager.WARNING_MESSAGE_LEVEL);
        assertThat(mErrCapture.toString(), containsString("IllegalArgumentException"));
    }

    // -----------------------------------------------------------------------
    // add() + log(int) — buffered logging path
    // -----------------------------------------------------------------------

    @Test
    public void testAddAndLogWritesValueToStream() {
        mLogger.setLevel(LogManager.WARNING_MESSAGE_LEVEL);
        mLogger.add("buffered-value");
        mLogger.log(LogManager.WARNING_MESSAGE_LEVEL);
        assertThat(mErrCapture.toString(), containsString("buffered-value"));
    }

    @Test
    public void testAddKeyValueAndLogWritesValueToStream() {
        mLogger.setLevel(LogManager.WARNING_MESSAGE_LEVEL);
        mLogger.add("k", "kv-value");
        mLogger.log(LogManager.WARNING_MESSAGE_LEVEL);
        assertThat(mErrCapture.toString(), containsString("kv-value"));
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
    public void testLogEventStartWritesToStream() {
        mLogger.setLevel(LogManager.INFO_MESSAGE_LEVEL);
        mLogger.logEventStart("event.test", "workflow", "wf-001");
        assertThat(mOutCapture.toString().isEmpty(), is(false));
    }

    @Test
    public void testLogEventCompletionWritesToStream() {
        mLogger.setLevel(LogManager.INFO_MESSAGE_LEVEL);
        mLogger.logEventStart("event.test", "workflow", "wf-001");
        mOutCapture.reset(); // clear start output
        mLogger.logEventCompletion();
        assertThat(mOutCapture.toString().isEmpty(), is(false));
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
        assertThat(ts, is(notNullValue()));
        assertThat(ts.isEmpty(), is(false));
    }

    @Test
    public void testGetTimestampLooksLikeIso8601() {
        String ts = mLogger.getTimeStamp();
        assertThat(ts, containsString("T"));
    }

    @Test
    public void testSetOutputWriterOutputStreamUpdatesStdoutWriter() {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        mLogger.setOutputWriter(bos);

        assertThat(mLogger.getWriter(LogManager.STREAM_TYPE.stdout), is(notNullValue()));
        mLogger.setLevel(LogManager.INFO_MESSAGE_LEVEL);
        mLogger.log("redirected-out", LogManager.INFO_MESSAGE_LEVEL);
        assertThat(bos.toString(), containsString("redirected-out"));
    }

    @Test
    public void testSetErrorWriterOutputStreamUpdatesStderrWriter() {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        mLogger.setErrorWriter(bos);

        assertThat(mLogger.getWriter(LogManager.STREAM_TYPE.stderr), is(notNullValue()));
        mLogger.log("redirected-err", LogManager.WARNING_MESSAGE_LEVEL);
        assertThat(bos.toString(), containsString("redirected-err"));
    }

    @Test
    public void testLogAlreadyFormattedMessageWritesWithoutFormatterBuffer() {
        mLogger.setLevel(LogManager.INFO_MESSAGE_LEVEL);
        mLogger.logAlreadyFormattedMessage("preformatted-message", LogManager.INFO_MESSAGE_LEVEL);
        assertThat(mOutCapture.toString(), containsString("preformatted-message"));
    }
}
