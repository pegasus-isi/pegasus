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

import org.junit.jupiter.api.Test;

/** @author Rajiv Mayani */
public class ShellCommandTest {

    @Test
    public void testGetInstance_returnsNonNull() {
        ShellCommand sc = ShellCommand.getInstance();
        assertNotNull(sc);
    }

    @Test
    public void testGetInstance_withNullLogger_returnsNonNull() {
        ShellCommand sc = ShellCommand.getInstance(null);
        assertNotNull(sc);
    }

    @Test
    public void testInitialState_stdoutIsNull() {
        ShellCommand sc = ShellCommand.getInstance();
        assertNull(sc.getSTDOut());
    }

    @Test
    public void testInitialState_stderrIsNull() {
        ShellCommand sc = ShellCommand.getInstance();
        assertNull(sc.getSTDErr());
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
        ShellCommand sc = ShellCommand.getInstance();
        int exit = sc.execute("false", null);
        assertThat(exit, not(0));
    }
}
