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
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.*;

import edu.isi.pegasus.common.logging.LogFormatter;
import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.common.logging.format.Simple;
import edu.isi.pegasus.common.logging.logger.Default;
import java.io.PrintStream;
import java.util.Properties;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

/** @author Rajiv Mayani */
public class ShellCommandTest {

    @Test
    public void testGetInstance_returnsNonNull() {
        ShellCommand sc = ShellCommand.getInstance();
        assertThat(sc, is(notNullValue()));
    }

    @Test
    public void testGetInstance_withNullLogger_returnsNonNull() {
        ShellCommand sc = ShellCommand.getInstance(null);
        assertThat(sc, is(notNullValue()));
    }

    @Test
    public void testInitialState_stdoutIsNull() {
        ShellCommand sc = ShellCommand.getInstance();
        assertThat(sc.getSTDOut(), is(nullValue()));
    }

    @Test
    public void testInitialState_stderrIsNull() {
        ShellCommand sc = ShellCommand.getInstance();
        assertThat(sc.getSTDErr(), is(nullValue()));
    }

    @Test
    public void testExecute_echoCommand_capturesOutput() {
        ShellCommand sc = ShellCommand.getInstance();
        int exit = sc.execute("echo", "hello");
        assertThat(exit, is(0));
        assertThat(sc.getSTDOut(), is("hello"));
    }

    @Test
    public void testExecute_withNullArgs() {
        ShellCommand sc = ShellCommand.getInstance();
        int exit = sc.execute("true", null);
        assertThat(exit, is(0));
    }

    @Test
    public void testExecute_nonZeroExitCode() {
        ShellCommand sc = ShellCommand.getInstance(new NoOpLogManager());
        int exit = sc.execute("false", null);
        assertThat(exit, not(0));
    }

    @Test
    public void testGetInstance_withCustomLoggerStoresLogger() throws Exception {
        LogManager logger = new Default();
        ShellCommand sc = ShellCommand.getInstance(logger);
        assertThat(ReflectionTestUtils.getField(sc, "mLogger"), is(sameInstance(logger)));
    }

    @Test
    public void testExecute_echoWithSpacesPreservesOutput() {
        ShellCommand sc = ShellCommand.getInstance();
        int exit = sc.execute("echo", "hello world");
        assertThat(exit, is(0));
        assertThat(sc.getSTDOut(), is("hello world"));
    }

    @Test
    public void testExecute_nonexistentCommandReturnsMinusOne() {
        ShellCommand sc = ShellCommand.getInstance(createPreparedLogger());
        int exit = sc.execute("definitely_not_a_real_command_pegasus", null);
        assertThat(exit, is(-1));
    }

    @Test
    public void testExecute_failingCommandCapturesStdErr() {
        ShellCommand sc = ShellCommand.getInstance(createPreparedLogger());
        int exit = sc.execute("ls", "__definitely_missing_file__");
        assertThat(exit, not(0));
        assertThat(sc.getSTDErr(), containsString("__definitely_missing_file__"));
    }

    private LogManager createPreparedLogger() {
        Default logger = new Default();
        logger.initialize(new Simple(), new Properties());
        logger.logEventStart("shell", "test", "command");
        return logger;
    }

    private static class NoOpLogManager extends LogManager {

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
            return System.out;
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
