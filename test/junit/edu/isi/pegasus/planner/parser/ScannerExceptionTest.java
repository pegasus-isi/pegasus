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
package edu.isi.pegasus.planner.parser;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.*;

import java.io.LineNumberReader;
import java.io.StringReader;
import org.junit.jupiter.api.Test;

/** Tests for {@link ScannerException}. */
public class ScannerExceptionTest {

    @Test
    public void testConstructorWithLinenoAndMessage() {
        ScannerException e = new ScannerException(42, "unexpected token");
        assertThat(e.getLineNumber(), is(42));
        assertThat(e.getMessage(), containsString("42"));
        assertThat(e.getMessage(), containsString("unexpected token"));
    }

    @Test
    public void testConstructorWithMessageOnly() {
        ScannerException e = new ScannerException("parse error");
        assertThat(e.getLineNumber(), is(-1));
        assertThat(e.getMessage(), is("parse error"));
    }

    @Test
    public void testConstructorWithMessageAndException() {
        Exception cause = new Exception("root cause");
        ScannerException e = new ScannerException("wrapped error", cause);
        assertThat(e.getLineNumber(), is(-1));
        assertThat(e.getMessage(), containsString("wrapped error"));
        assertThat(e.getCause(), sameInstance(cause));
    }

    @Test
    public void testConstructorWithLinenoAndException() {
        Exception cause = new Exception("cause");
        ScannerException e = new ScannerException(10, cause);
        assertThat(e.getLineNumber(), is(10));
        assertThat(e.getMessage(), containsString("10"));
    }

    @Test
    public void testConstructorWithLineNumberReaderAndMessage() throws Exception {
        LineNumberReader reader = new LineNumberReader(new StringReader("a\nb\nc"));
        reader.readLine();
        reader.readLine();

        ScannerException e = new ScannerException(reader, "reader failure");
        assertThat(e.getLineNumber(), is(2));
        assertThat(e.getMessage(), containsString("line 2"));
        assertThat(e.getMessage(), containsString("reader failure"));
    }

    @Test
    public void testConstructorWithNegativeLinenoPreservesValue() {
        ScannerException e = new ScannerException(-5, "unexpected token");
        assertThat(e.getLineNumber(), is(-5));
        assertThat(e.getMessage(), containsString("line -5"));
    }

    @Test
    public void testConstructorWithMessageAndNullException() {
        ScannerException e = new ScannerException("wrapped error", null);
        assertThat(e.getLineNumber(), is(-1));
        assertThat(e.getMessage(), is("message - wrapped error:"));
        assertThat(e.getCause(), nullValue());
    }

    @Test
    public void testIsRuntimeException() {
        ScannerException e = new ScannerException("test");
        assertThat(e, instanceOf(RuntimeException.class));
    }

    @Test
    public void testLineNumberNegativeOneWhenNoLine() {
        ScannerException e = new ScannerException("no line info");
        assertThat(e.getLineNumber(), is(-1));
    }
}
