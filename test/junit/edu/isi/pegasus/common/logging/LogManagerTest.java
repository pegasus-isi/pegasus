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
package edu.isi.pegasus.common.logging;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.*;

import java.lang.reflect.Modifier;
import java.util.Map;
import org.apache.logging.log4j.Level;
import org.junit.jupiter.api.Test;

/** Tests for LogManager constants and structural properties. */
public class LogManagerTest {

    // --- class structure ---

    @Test
    public void testLogManagerIsAbstractClass() {
        assertTrue(Modifier.isAbstract(LogManager.class.getModifiers()));
    }

    // --- string constants ---

    @Test
    public void testVersionConstant() {
        assertThat(LogManager.VERSION, is("2.1"));
    }

    @Test
    public void testPropertiesPrefixConstant() {
        assertThat(LogManager.PROPERTIES_PREFIX, is("pegasus.log.manager"));
    }

    @Test
    public void testMessageDonePrefixConstant() {
        assertThat(LogManager.MESSAGE_DONE_PREFIX, is(" -DONE"));
    }

    @Test
    public void testLoggerNameConstants() {
        assertThat(LogManager.DEFAULT_LOGGER, is("Default"));
        assertThat(LogManager.LOG4J_LOGGER, is("Log4j"));
    }

    // --- message level constants (ascending verbosity) ---

    @Test
    public void testMessageLevelConstants() {
        assertThat(LogManager.FATAL_MESSAGE_LEVEL, is(0));
        assertThat(LogManager.ERROR_MESSAGE_LEVEL, is(1));
        assertThat(LogManager.CONSOLE_MESSAGE_LEVEL, is(2));
        assertThat(LogManager.WARNING_MESSAGE_LEVEL, is(3));
        assertThat(LogManager.INFO_MESSAGE_LEVEL, is(4));
        assertThat(LogManager.CONFIG_MESSAGE_LEVEL, is(5));
        assertThat(LogManager.DEBUG_MESSAGE_LEVEL, is(6));
        assertThat(LogManager.TRACE_MESSAGE_LEVEL, is(7));
    }

    @Test
    public void testMessageLevelsAreStrictlyIncreasing() {
        assertTrue(LogManager.FATAL_MESSAGE_LEVEL < LogManager.ERROR_MESSAGE_LEVEL);
        assertTrue(LogManager.ERROR_MESSAGE_LEVEL < LogManager.CONSOLE_MESSAGE_LEVEL);
        assertTrue(LogManager.CONSOLE_MESSAGE_LEVEL < LogManager.WARNING_MESSAGE_LEVEL);
        assertTrue(LogManager.WARNING_MESSAGE_LEVEL < LogManager.INFO_MESSAGE_LEVEL);
        assertTrue(LogManager.INFO_MESSAGE_LEVEL < LogManager.CONFIG_MESSAGE_LEVEL);
        assertTrue(LogManager.CONFIG_MESSAGE_LEVEL < LogManager.DEBUG_MESSAGE_LEVEL);
        assertTrue(LogManager.DEBUG_MESSAGE_LEVEL < LogManager.TRACE_MESSAGE_LEVEL);
    }

    // --- STREAM_TYPE enum ---

    @Test
    public void testStreamTypeEnumValues() {
        assertThat(
                LogManager.STREAM_TYPE.values(),
                arrayContainingInAnyOrder(
                        LogManager.STREAM_TYPE.stdout, LogManager.STREAM_TYPE.stderr));
    }

    // --- original system streams ---

    @Test
    public void testOriginalSystemStreamsAreNotNull() {
        assertNotNull(LogManager.ORIGINAL_SYSTEM_OUT);
        assertNotNull(LogManager.ORIGINAL_SYSTEM_ERR);
    }

    // --- log4jLevelToInt() mapping ---

    @Test
    public void testLog4jLevelToIntMappings() {
        Map<Level, Integer> map = LogManager.log4jLevelToInt();
        assertThat(map.get(Level.FATAL), is(LogManager.FATAL_MESSAGE_LEVEL));
        assertThat(map.get(Level.ERROR), is(LogManager.ERROR_MESSAGE_LEVEL));
        assertThat(map.get(Level.WARN), is(LogManager.WARNING_MESSAGE_LEVEL));
        assertThat(map.get(Level.INFO), is(LogManager.INFO_MESSAGE_LEVEL));
        assertThat(map.get(Level.DEBUG), is(LogManager.DEBUG_MESSAGE_LEVEL));
        assertThat(map.get(Level.TRACE), is(LogManager.TRACE_MESSAGE_LEVEL));
        assertThat(map.get(Level.ALL), is(LogManager.TRACE_MESSAGE_LEVEL));
    }

    // --- intTolog4jLevel() mapping ---

    @Test
    public void testIntToLog4jLevelMappings() {
        Map<Integer, Level> map = LogManager.intTolog4jLevel();
        assertThat(map.get(LogManager.FATAL_MESSAGE_LEVEL), is(Level.FATAL));
        assertThat(map.get(LogManager.ERROR_MESSAGE_LEVEL), is(Level.ERROR));
        assertThat(map.get(LogManager.WARNING_MESSAGE_LEVEL), is(Level.WARN));
        assertThat(map.get(LogManager.INFO_MESSAGE_LEVEL), is(Level.INFO));
        assertThat(map.get(LogManager.CONFIG_MESSAGE_LEVEL), is(Level.INFO));
        assertThat(map.get(LogManager.CONSOLE_MESSAGE_LEVEL), is(Level.INFO));
        assertThat(map.get(LogManager.DEBUG_MESSAGE_LEVEL), is(Level.DEBUG));
        assertThat(map.get(LogManager.TRACE_MESSAGE_LEVEL), is(Level.TRACE));
    }
}
