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
package edu.isi.pegasus.planner.code.generator.condor;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

import edu.isi.pegasus.planner.code.CodeGeneratorException;
import org.junit.jupiter.api.Test;

/** Tests for the CondorStyleException class. */
public class CondorStyleExceptionTest {

    @Test
    public void testExceptionExtendsCodeGeneratorException() {
        assertThat(
                CodeGeneratorException.class.isAssignableFrom(CondorStyleException.class),
                is(true));
    }

    @Test
    public void testDefaultConstructor() {
        CondorStyleException ex = new CondorStyleException();
        assertThat(ex, notNullValue());
    }

    @Test
    public void testConstructorWithMessage() {
        CondorStyleException ex = new CondorStyleException("test message");
        assertThat(ex, notNullValue());
        assertThat(ex.getMessage(), is("test message"));
    }

    @Test
    public void testConstructorWithMessageAndCause() {
        Throwable cause = new RuntimeException("root cause");
        CondorStyleException ex = new CondorStyleException("test message", cause);
        assertThat(ex, notNullValue());
        assertThat(ex.getMessage(), is("test message"));
        assertThat(ex.getCause(), is(cause));
    }

    @Test
    public void testConstructorWithCauseOnly() {
        Throwable cause = new RuntimeException("root cause");
        CondorStyleException ex = new CondorStyleException(cause);
        assertThat(ex, notNullValue());
        assertThat(ex.getCause(), is(cause));
    }

    @Test
    public void testExceptionIsThrowable() {
        assertThat(Throwable.class.isAssignableFrom(CondorStyleException.class), is(true));
    }

    @Test
    public void testDefaultConstructorHasNullMessageAndCause() {
        CondorStyleException ex = new CondorStyleException();

        assertThat(ex.getMessage(), is((String) null));
        assertThat(ex.getCause(), is((Throwable) null));
    }

    @Test
    public void testConstructorWithMessageAndNullCause() {
        CondorStyleException ex = new CondorStyleException("test message", null);

        assertThat(ex.getMessage(), is("test message"));
        assertThat(ex.getCause(), is((Throwable) null));
    }

    @Test
    public void testConstructorWithNullCauseOnly() {
        CondorStyleException ex = new CondorStyleException((Throwable) null);

        assertThat(ex.getMessage(), is((String) null));
        assertThat(ex.getCause(), is((Throwable) null));
    }

    @Test
    public void testExceptionIsCheckedNotRuntime() {
        assertThat(RuntimeException.class.isAssignableFrom(CondorStyleException.class), is(false));
    }

    @Test
    public void testConstructorWithCauseOnlyUsesCauseToStringAsMessage() {
        IllegalStateException cause = new IllegalStateException("root cause");

        CondorStyleException ex = new CondorStyleException(cause);

        assertThat(ex.getMessage(), is(cause.toString()));
        assertThat(ex.getCause(), is(cause));
    }

    @Test
    public void testConstructorWithNullMessageAndCausePreservesCause() {
        IllegalArgumentException cause = new IllegalArgumentException("bad input");

        CondorStyleException ex = new CondorStyleException(null, cause);

        assertThat(ex.getMessage(), is((String) null));
        assertThat(ex.getCause(), is(cause));
    }

    @Test
    public void testConstructorWithNullMessageOnly() {
        CondorStyleException ex = new CondorStyleException((String) null);

        assertThat(ex.getMessage(), is((String) null));
        assertThat(ex.getCause(), is((Throwable) null));
    }
}
