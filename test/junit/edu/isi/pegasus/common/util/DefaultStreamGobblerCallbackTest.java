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

import static org.junit.jupiter.api.Assertions.*;

import edu.isi.pegasus.common.logging.LogManager;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

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
        DefaultStreamGobblerCallback cb =
                new DefaultStreamGobblerCallback(LogManager.DEBUG_MESSAGE_LEVEL);
        assertDoesNotThrow(() -> cb.work(""), "work() with empty string should not throw");
    }

    @Test
    public void testWork_whitespaceOnlyString_doesNotThrow() {
        DefaultStreamGobblerCallback cb =
                new DefaultStreamGobblerCallback(LogManager.DEBUG_MESSAGE_LEVEL);
        assertDoesNotThrow(
                () -> cb.work("   "), "work() with whitespace-only string should not throw");
    }

    @Test
    public void testWork_longLine_doesNotThrow() {
        DefaultStreamGobblerCallback cb =
                new DefaultStreamGobblerCallback(LogManager.DEBUG_MESSAGE_LEVEL);
        String longLine = "x".repeat(10_000);
        assertDoesNotThrow(
                () -> cb.work(longLine), "work() with a very long line should not throw");
    }

    @Test
    public void testWork_specialCharacters_doesNotThrow() {
        DefaultStreamGobblerCallback cb =
                new DefaultStreamGobblerCallback(LogManager.DEBUG_MESSAGE_LEVEL);
        assertDoesNotThrow(
                () -> cb.work("line with special chars: \t\n\r\\\"'<>&"),
                "work() with special characters should not throw");
    }

    @Test
    public void testWork_multipleSuccessiveCalls_doNotThrow() {
        DefaultStreamGobblerCallback cb =
                new DefaultStreamGobblerCallback(LogManager.DEBUG_MESSAGE_LEVEL);
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
        DefaultStreamGobblerCallback cb =
                new DefaultStreamGobblerCallback(LogManager.DEBUG_MESSAGE_LEVEL);
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
        DefaultStreamGobblerCallback cb = new DefaultStreamGobblerCallback(level);
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
        DefaultStreamGobblerCallback cb = new DefaultStreamGobblerCallback(level);
        assertDoesNotThrow(
                () -> cb.work(null), "work(null) at log level " + level + " should not throw");
    }
}
