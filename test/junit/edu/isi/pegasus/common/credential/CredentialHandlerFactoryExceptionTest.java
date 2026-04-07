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
package edu.isi.pegasus.common.credential;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.*;

import edu.isi.pegasus.common.util.FactoryException;
import org.junit.jupiter.api.Test;

/** Unit tests for the CredentialHandlerFactoryException class. */
public class CredentialHandlerFactoryExceptionTest {

    @Test
    public void testExceptionExtendsFactoryException() {
        CredentialHandlerFactoryException ex = new CredentialHandlerFactoryException("test");
        assertThat(ex, instanceOf(FactoryException.class));
    }

    @Test
    public void testDefaultNameConstant() {
        assertThat(CredentialHandlerFactoryException.DEFAULT_NAME, is("Credential Implementor"));
    }

    // --- constructor: (String) ---

    @Test
    public void testMessageOnlyConstructor() {
        CredentialHandlerFactoryException ex =
                new CredentialHandlerFactoryException("credential factory error");
        assertThat(ex.getMessage(), is("credential factory error"));
        assertThat(ex.getClassname(), is(CredentialHandlerFactoryException.DEFAULT_NAME));
        assertThat(ex.getCause(), is(nullValue()));
    }

    // --- constructor: (String, String) ---

    @Test
    public void testMessageAndClassnameConstructor() {
        CredentialHandlerFactoryException ex =
                new CredentialHandlerFactoryException("load error", "MyCredentialHandler");
        assertThat(ex.getMessage(), is("load error"));
        assertThat(ex.getClassname(), is("MyCredentialHandler"));
        assertThat(ex.getCause(), is(nullValue()));
    }

    // --- constructor: (String, Throwable) ---

    @Test
    public void testMessageAndCauseConstructor() {
        RuntimeException cause = new RuntimeException("root cause");
        CredentialHandlerFactoryException ex =
                new CredentialHandlerFactoryException("wrapper error", cause);
        assertThat(ex.getMessage(), is("wrapper error"));
        assertThat(ex.getCause(), is(cause));
        assertThat(ex.getClassname(), is(CredentialHandlerFactoryException.DEFAULT_NAME));
    }

    // --- constructor: (String, String, Throwable) ---

    @Test
    public void testMessageClassnameAndCauseConstructor() {
        RuntimeException cause = new RuntimeException("root cause");
        CredentialHandlerFactoryException ex =
                new CredentialHandlerFactoryException("load error", "MyHandler", cause);
        assertThat(ex.getMessage(), is("load error"));
        assertThat(ex.getClassname(), is("MyHandler"));
        assertThat(ex.getCause(), is(cause));
    }

    // --- canBeThrown ---

    @Test
    public void testCanBeCaughtAsFactoryException() {
        assertThrows(
                FactoryException.class,
                () -> {
                    throw new CredentialHandlerFactoryException("thrown");
                });
    }

    @Test
    public void testCanBeCaughtAsRuntimeException() {
        assertThrows(
                RuntimeException.class,
                () -> {
                    throw new CredentialHandlerFactoryException("thrown");
                });
    }

    // --- convertException ---

    @Test
    public void testConvertExceptionContainsMessage() {
        CredentialHandlerFactoryException ex =
                new CredentialHandlerFactoryException("my error message");
        assertThat(ex.convertException(), containsString("my error message"));
    }

    @Test
    public void testConvertExceptionWithCauseContainsBothMessages() {
        RuntimeException cause = new RuntimeException("root cause message");
        CredentialHandlerFactoryException ex =
                new CredentialHandlerFactoryException("outer message", cause);
        String converted = ex.convertException();
        assertThat(converted, containsString("outer message"));
        assertThat(converted, containsString("root cause message"));
    }

    @Test
    public void testMessageAndNullCauseUsesDefaultClassname() {
        CredentialHandlerFactoryException ex =
                new CredentialHandlerFactoryException("wrapper error", (Throwable) null);

        assertThat(ex.getMessage(), is("wrapper error"));
        assertThat(ex.getCause(), is(nullValue()));
        assertThat(ex.getClassname(), is(CredentialHandlerFactoryException.DEFAULT_NAME));
    }

    @Test
    public void testMessageAndNullClassnamePreservesNull() {
        CredentialHandlerFactoryException ex =
                new CredentialHandlerFactoryException("load error", (String) null);

        assertThat(ex.getMessage(), is("load error"));
        assertThat(ex.getClassname(), is(nullValue()));
        assertThat(ex.getCause(), is(nullValue()));
    }

    @Test
    public void testMessageNullClassnameAndNullCausePreservesNull() {
        CredentialHandlerFactoryException ex =
                new CredentialHandlerFactoryException("load error", null, null);

        assertThat(ex.getMessage(), is("load error"));
        assertThat(ex.getClassname(), is(nullValue()));
        assertThat(ex.getCause(), is(nullValue()));
    }
}
