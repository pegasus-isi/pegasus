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
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

/** Tests for the CondorQuoteParserException class. */
public class CondorQuoteParserExceptionTest {

    @Test
    public void testExceptionExtendsException() {
        assertThat(Exception.class.isAssignableFrom(CondorQuoteParserException.class), is(true));
    }

    @Test
    public void testConstructorWithMessageAndPosition() {
        CondorQuoteParserException ex = new CondorQuoteParserException("parse error", 5);
        assertThat(ex, notNullValue());
        assertThat(ex.getMessage(), is("parse error"));
        assertThat(ex.getPosition(), is(5));
    }

    @Test
    public void testConstructorWithMessagePositionAndCause() {
        Throwable cause = new RuntimeException("root cause");
        CondorQuoteParserException ex = new CondorQuoteParserException("parse error", 3, cause);
        assertThat(ex, notNullValue());
        assertThat(ex.getMessage(), is("parse error"));
        assertThat(ex.getPosition(), is(3));
        assertThat(ex.getCause(), is(cause));
    }

    @Test
    public void testGetPositionReturnsCorrectValue() {
        CondorQuoteParserException ex = new CondorQuoteParserException("error at position 10", 10);
        assertThat(ex.getPosition(), is(10));
    }

    @Test
    public void testExceptionIsThrowable() {
        assertThat(Throwable.class.isAssignableFrom(CondorQuoteParserException.class), is(true));
    }

    @Test
    public void testExceptionCanBeThrown() {
        assertThrows(
                CondorQuoteParserException.class,
                () -> {
                    throw new CondorQuoteParserException("test", 0);
                });
    }

    @Test
    public void testConstructorWithNullCausePreservesMessageAndPosition() {
        CondorQuoteParserException ex = new CondorQuoteParserException("parse error", 7, null);

        assertThat(ex.getMessage(), is("parse error"));
        assertThat(ex.getPosition(), is(7));
        assertThat(ex.getCause(), nullValue());
    }

    @Test
    public void testNegativePositionIsPreserved() {
        CondorQuoteParserException ex = new CondorQuoteParserException("parse error", -1);

        assertThat(ex.getPosition(), is(-1));
    }

    @Test
    public void testExceptionExtendsCheckedExceptionNotRuntimeException() {
        assertThat(
                RuntimeException.class.isAssignableFrom(CondorQuoteParserException.class),
                is(false));
    }
}
