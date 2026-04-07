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

import java.io.PrintStream;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Map;
import java.util.Properties;
import org.apache.logging.log4j.Level;
import org.junit.jupiter.api.Test;

/** Tests for LogManager constants and structural properties. */
public class LogManagerTest {

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
        assertThat(LogManager.FATAL_MESSAGE_LEVEL < LogManager.ERROR_MESSAGE_LEVEL, is(true));
        assertThat(LogManager.ERROR_MESSAGE_LEVEL < LogManager.CONSOLE_MESSAGE_LEVEL, is(true));
        assertThat(LogManager.CONSOLE_MESSAGE_LEVEL < LogManager.WARNING_MESSAGE_LEVEL, is(true));
        assertThat(LogManager.WARNING_MESSAGE_LEVEL < LogManager.INFO_MESSAGE_LEVEL, is(true));
        assertThat(LogManager.INFO_MESSAGE_LEVEL < LogManager.CONFIG_MESSAGE_LEVEL, is(true));
        assertThat(LogManager.CONFIG_MESSAGE_LEVEL < LogManager.DEBUG_MESSAGE_LEVEL, is(true));
        assertThat(LogManager.DEBUG_MESSAGE_LEVEL < LogManager.TRACE_MESSAGE_LEVEL, is(true));
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
        assertThat(LogManager.ORIGINAL_SYSTEM_OUT, is(notNullValue()));
        assertThat(LogManager.ORIGINAL_SYSTEM_ERR, is(notNullValue()));
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

    @Test
    public void testLogManagerIsAbstractClass() {
        assertThat(Modifier.isAbstract(LogManager.class.getModifiers()), is(true));
        assertThat(LogManager.class.isInterface(), is(false));
    }

    @Test
    public void testLevelMappingMethodsReturnCachedMaps() {
        assertThat(LogManager.log4jLevelToInt(), sameInstance(LogManager.log4jLevelToInt()));
        assertThat(LogManager.intTolog4jLevel(), sameInstance(LogManager.intTolog4jLevel()));
    }

    @Test
    public void testSelectedMethodReturnTypes() throws Exception {
        assertThat(
                LogManager.class.getMethod("initialize", LogFormatter.class, Properties.class),
                hasProperty("returnType", is((Object) Void.TYPE)));
        assertThat(
                LogManager.class.getMethod("configure", boolean.class),
                hasProperty("returnType", is((Object) Void.TYPE)));
        assertThat(
                LogManager.class.getMethod("getLevel"),
                hasProperty("returnType", is((Object) Integer.TYPE)));
        assertThat(
                LogManager.class.getMethod("setWriters", String.class),
                hasProperty("returnType", is((Object) Void.TYPE)));
        assertThat(
                LogManager.class.getMethod("getWriter", LogManager.STREAM_TYPE.class),
                hasProperty("returnType", is((Object) PrintStream.class)));
        assertThat(
                LogManager.class.getMethod("add", String.class),
                hasProperty("returnType", is((Object) LogManager.class)));
    }

    @Test
    public void testAbstractMethodsRemainAbstract() throws Exception {
        Method setLevel = LogManager.class.getDeclaredMethod("setLevel", int.class, boolean.class);
        Method getLevel = LogManager.class.getDeclaredMethod("getLevel");
        Method logEventCompletion =
                LogManager.class.getDeclaredMethod("logEventCompletion", int.class);

        assertThat(Modifier.isAbstract(setLevel.getModifiers()), is(true));
        assertThat(Modifier.isAbstract(getLevel.getModifiers()), is(true));
        assertThat(Modifier.isAbstract(logEventCompletion.getModifiers()), is(true));
    }
}
