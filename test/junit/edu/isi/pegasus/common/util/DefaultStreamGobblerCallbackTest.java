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
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.*;

import edu.isi.pegasus.common.logging.LogFormatter;
import edu.isi.pegasus.common.logging.LogManager;
import java.io.PrintStream;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.test.util.ReflectionTestUtils;

/** @author Rajiv Mayani */
public class DefaultStreamGobblerCallbackTest {

    // -----------------------------------------------------------------------
    // Constructor — smoke test at every log level
    // -----------------------------------------------------------------------

    @ParameterizedTest
    @ValueSource(
            ints = {
                LogManager.FATAL_MESSAGE_LEVEL,
                LogManager.ERROR_MESSAGE_LEVEL,
                LogManager.CONSOLE_MESSAGE_LEVEL,
                LogManager.WARNING_MESSAGE_LEVEL,
                LogManager.INFO_MESSAGE_LEVEL,
                LogManager.CONFIG_MESSAGE_LEVEL,
                LogManager.DEBUG_MESSAGE_LEVEL,
                LogManager.TRACE_MESSAGE_LEVEL
            })
    public void testConstructor_allLogLevelsDoNotThrow(int level) {
        assertDoesNotThrow(
                () -> new DefaultStreamGobblerCallback(level),
                "Constructor with level " + level + " should not throw");
    }

    // -----------------------------------------------------------------------
    // work() — input variations at DEBUG level
    // -----------------------------------------------------------------------

    @Test
    public void testWork_emptyString_doesNotThrow() {
        DefaultStreamGobblerCallback cb = newCallback(LogManager.DEBUG_MESSAGE_LEVEL);
        assertDoesNotThrow(() -> cb.work(""), "work() with empty string should not throw");
    }

    @Test
    public void testWork_whitespaceOnlyString_doesNotThrow() {
        DefaultStreamGobblerCallback cb = newCallback(LogManager.DEBUG_MESSAGE_LEVEL);
        assertDoesNotThrow(
                () -> cb.work("   "), "work() with whitespace-only string should not throw");
    }

    @Test
    public void testWork_longLine_doesNotThrow() {
        DefaultStreamGobblerCallback cb = newCallback(LogManager.DEBUG_MESSAGE_LEVEL);
        String longLine = "x".repeat(10_000);
        assertDoesNotThrow(
                () -> cb.work(longLine), "work() with a very long line should not throw");
    }

    @Test
    public void testWork_specialCharacters_doesNotThrow() {
        DefaultStreamGobblerCallback cb = newCallback(LogManager.DEBUG_MESSAGE_LEVEL);
        assertDoesNotThrow(
                () -> cb.work("line with special chars: \t\n\r\\\"'<>&"),
                "work() with special characters should not throw");
    }

    @Test
    public void testWork_multipleSuccessiveCalls_doNotThrow() {
        DefaultStreamGobblerCallback cb = newCallback(LogManager.DEBUG_MESSAGE_LEVEL);
        assertDoesNotThrow(
                () -> {
                    cb.work("line one");
                    cb.work("line two");
                    cb.work("line three");
                },
                "Multiple successive work() calls should not throw");
    }

    @Test
    public void testWork_mixedNullAndNonNullLines_doNotThrow() {
        DefaultStreamGobblerCallback cb = newCallback(LogManager.DEBUG_MESSAGE_LEVEL);
        assertDoesNotThrow(
                () -> {
                    cb.work("first line");
                    cb.work(null);
                    cb.work("third line");
                },
                "Interleaved null and non-null work() calls should not throw");
    }

    // -----------------------------------------------------------------------
    // work() — smoke test at every log level
    // -----------------------------------------------------------------------

    @ParameterizedTest
    @ValueSource(
            ints = {
                LogManager.FATAL_MESSAGE_LEVEL,
                LogManager.ERROR_MESSAGE_LEVEL,
                LogManager.CONSOLE_MESSAGE_LEVEL,
                LogManager.WARNING_MESSAGE_LEVEL,
                LogManager.INFO_MESSAGE_LEVEL,
                LogManager.CONFIG_MESSAGE_LEVEL,
                LogManager.DEBUG_MESSAGE_LEVEL,
                LogManager.TRACE_MESSAGE_LEVEL
            })
    public void testWork_allLogLevelsDoNotThrow(int level) {
        DefaultStreamGobblerCallback cb = newCallback(level);
        assertDoesNotThrow(
                () -> cb.work("test line at level " + level),
                "work() at log level " + level + " should not throw");
    }

    @ParameterizedTest
    @ValueSource(
            ints = {
                LogManager.FATAL_MESSAGE_LEVEL,
                LogManager.ERROR_MESSAGE_LEVEL,
                LogManager.CONSOLE_MESSAGE_LEVEL,
                LogManager.WARNING_MESSAGE_LEVEL,
                LogManager.INFO_MESSAGE_LEVEL,
                LogManager.CONFIG_MESSAGE_LEVEL,
                LogManager.DEBUG_MESSAGE_LEVEL,
                LogManager.TRACE_MESSAGE_LEVEL
            })
    public void testWork_nullLineAllLogLevelsDoNotThrow(int level) {
        DefaultStreamGobblerCallback cb = newCallback(level);
        assertDoesNotThrow(
                () -> cb.work(null), "work(null) at log level " + level + " should not throw");
    }

    @Test
    public void testImplementsStreamGobblerCallback() {
        assertThat(
                StreamGobblerCallback.class.isAssignableFrom(DefaultStreamGobblerCallback.class),
                is(true));
    }

    @Test
    public void testConstructorStoresLevelAndLogger() throws Exception {
        DefaultStreamGobblerCallback cb = newCallback(LogManager.CONFIG_MESSAGE_LEVEL);

        assertThat(
                (Integer) ReflectionTestUtils.getField(cb, "mLevel"),
                is(LogManager.CONFIG_MESSAGE_LEVEL));
        assertThat(ReflectionTestUtils.getField(cb, "mLogger"), is(notNullValue()));
    }

    @Test
    public void testWorkMethodSignature() throws Exception {
        Method method = DefaultStreamGobblerCallback.class.getDeclaredMethod("work", String.class);
        assertThat(method.getReturnType(), is(Void.TYPE));
        assertThat(Modifier.isPublic(method.getModifiers()), is(true));
    }

    private DefaultStreamGobblerCallback newCallback(int level) {
        DefaultStreamGobblerCallback cb = new DefaultStreamGobblerCallback(level);
        ReflectionTestUtils.setField(cb, "mLogger", new NoOpLogManager());
        return cb;
    }

    private static final class NoOpLogManager extends LogManager {
        @Override
        public void initialize(LogFormatter formatter, java.util.Properties properties) {}

        @Override
        public void configure(boolean prefixTimestamp) {}

        @Override
        protected void setLevel(int level, boolean info) {}

        @Override
        public int getLevel() {
            return LogManager.INFO_MESSAGE_LEVEL;
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
        public void log(String message, Exception e, int level) {}

        @Override
        public void log(String message, int level) {}

        @Override
        protected void logAlreadyFormattedMessage(String message, int level) {}

        @Override
        public void logEventCompletion(int level) {}
    }
}
