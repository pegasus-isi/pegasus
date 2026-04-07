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
package edu.isi.pegasus.planner.cluster;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

/** Tests for the ClustererException class. */
public class ClustererExceptionTest {

    @Test
    public void testNoArgConstructor() {
        ClustererException ex = new ClustererException();
        assertThat(ex, notNullValue());
    }

    @Test
    public void testMessageConstructor() {
        ClustererException ex = new ClustererException("test error");
        assertThat(ex.getMessage(), is("test error"));
    }

    @Test
    public void testMessageAndCauseConstructor() {
        Throwable cause = new RuntimeException("root cause");
        ClustererException ex = new ClustererException("test error", cause);
        assertThat(ex.getMessage(), is("test error"));
        assertThat(ex.getCause(), is(cause));
    }

    @Test
    public void testCauseOnlyConstructor() {
        Throwable cause = new RuntimeException("root cause");
        ClustererException ex = new ClustererException(cause);
        assertThat(ex.getCause(), is(cause));
    }

    @Test
    public void testIsCheckedException() {
        ClustererException ex = new ClustererException("test");
        assertThat(ex, instanceOf(Exception.class));
    }

    @Test
    public void testNullMessageAllowed() {
        ClustererException ex = new ClustererException((String) null);
        assertThat(ex.getMessage(), nullValue());
    }

    @Test
    public void testNullCauseAllowed() {
        ClustererException ex = new ClustererException("msg", (Throwable) null);
        assertThat(ex.getCause(), nullValue());
    }

    @Test
    public void testDefaultConstructorHasNullMessageAndCause() {
        ClustererException ex = new ClustererException();

        assertThat(ex.getMessage(), nullValue());
        assertThat(ex.getCause(), nullValue());
    }

    @Test
    public void testCauseOnlyConstructorUsesCauseToBuildMessage() {
        Throwable cause = new IllegalStateException("broken");
        ClustererException ex = new ClustererException(cause);

        assertThat(ex.getMessage(), is(cause.toString()));
        assertThat(ex.getCause(), sameInstance(cause));
    }

    @Test
    public void testCauseOnlyConstructorAllowsNullCause() {
        ClustererException ex = new ClustererException((Throwable) null);

        assertThat(ex.getMessage(), nullValue());
        assertThat(ex.getCause(), nullValue());
    }
}
