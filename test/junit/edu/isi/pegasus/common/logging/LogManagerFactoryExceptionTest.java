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

import edu.isi.pegasus.common.util.FactoryException;
import org.junit.jupiter.api.Test;

/** Unit tests for the LogManagerFactoryException class. */
public class LogManagerFactoryExceptionTest {

    // --- class structure ---

    @Test
    public void testExceptionExtendsFactoryException() {
        assertThat(new LogManagerFactoryException("test"), instanceOf(FactoryException.class));
    }

    // --- DEFAULT_NAME constant ---

    @Test
    public void testDefaultNameConstant() {
        assertThat(LogManagerFactoryException.DEFAULT_NAME, is("Logger Factory"));
    }

    // --- constructor: (String) ---

    @Test
    public void testMessageOnlyConstructor() {
        LogManagerFactoryException ex = new LogManagerFactoryException("log manager error");
        assertThat(ex.getMessage(), is("log manager error"));
        assertThat(ex.getClassname(), is(LogManagerFactoryException.DEFAULT_NAME));
        assertThat(ex.getCause(), is(nullValue()));
    }

    // --- constructor: (String, String) ---

    @Test
    public void testMessageAndClassnameConstructor() {
        LogManagerFactoryException ex =
                new LogManagerFactoryException("load error", "MyLoggerClass");
        assertThat(ex.getMessage(), is("load error"));
        assertThat(ex.getClassname(), is("MyLoggerClass"));
        assertThat(ex.getCause(), is(nullValue()));
    }

    // --- constructor: (String, Throwable) ---

    @Test
    public void testMessageAndCauseConstructor() {
        RuntimeException cause = new RuntimeException("root cause");
        LogManagerFactoryException ex = new LogManagerFactoryException("wrapper error", cause);
        assertThat(ex.getMessage(), is("wrapper error"));
        assertThat(ex.getCause(), is(cause));
        assertThat(ex.getClassname(), is(LogManagerFactoryException.DEFAULT_NAME));
    }

    // --- constructor: (String, String, Throwable) ---

    @Test
    public void testMessageClassnameAndCauseConstructor() {
        RuntimeException cause = new RuntimeException("root cause");
        LogManagerFactoryException ex =
                new LogManagerFactoryException("load error", "MyLoggerClass", cause);
        assertThat(ex.getMessage(), is("load error"));
        assertThat(ex.getClassname(), is("MyLoggerClass"));
        assertThat(ex.getCause(), is(cause));
    }

    // --- canBeThrown ---

    @Test
    public void testCanBeCaughtAsFactoryException() {
        assertThrows(
                FactoryException.class,
                () -> {
                    throw new LogManagerFactoryException("thrown");
                });
    }

    @Test
    public void testCanBeCaughtAsRuntimeException() {
        assertThrows(
                RuntimeException.class,
                () -> {
                    throw new LogManagerFactoryException("thrown");
                });
    }

    // --- convertException ---

    @Test
    public void testConvertExceptionContainsMessage() {
        LogManagerFactoryException ex = new LogManagerFactoryException("my error message");
        assertThat(ex.convertException(), containsString("my error message"));
    }

    @Test
    public void testConvertExceptionWithCauseContainsBothMessages() {
        RuntimeException cause = new RuntimeException("root cause message");
        LogManagerFactoryException ex = new LogManagerFactoryException("outer message", cause);
        String converted = ex.convertException();
        assertThat(converted, containsString("outer message"));
        assertThat(converted, containsString("root cause message"));
    }

    @Test
    public void testMessageAndNullCauseUsesDefaultClassname() {
        LogManagerFactoryException ex =
                new LogManagerFactoryException("wrapper error", (Throwable) null);

        assertThat(ex.getMessage(), is("wrapper error"));
        assertThat(ex.getCause(), is(nullValue()));
        assertThat(ex.getClassname(), is(LogManagerFactoryException.DEFAULT_NAME));
    }

    @Test
    public void testMessageAndNullClassnamePreservesNull() {
        LogManagerFactoryException ex = new LogManagerFactoryException("load error", (String) null);

        assertThat(ex.getMessage(), is("load error"));
        assertThat(ex.getClassname(), is(nullValue()));
        assertThat(ex.getCause(), is(nullValue()));
    }

    @Test
    public void testMessageNullClassnameAndNullCausePreservesNull() {
        LogManagerFactoryException ex = new LogManagerFactoryException("load error", null, null);

        assertThat(ex.getMessage(), is("load error"));
        assertThat(ex.getClassname(), is(nullValue()));
        assertThat(ex.getCause(), is(nullValue()));
    }
}
