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

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Tests for {@link ScannerException}. */
public class ScannerExceptionTest {

    @BeforeAll
    public static void setUpClass() {}

    @AfterAll
    public static void tearDownClass() {}

    @BeforeEach
    public void setUp() {}

    @AfterEach
    public void tearDown() {}

    @Test
    public void testConstructorWithLinenoAndMessage() {
        ScannerException e = new ScannerException(42, "unexpected token");
        assertEquals(42, e.getLineNumber());
        assertThat(e.getMessage(), containsString("42"));
        assertThat(e.getMessage(), containsString("unexpected token"));
    }

    @Test
    public void testConstructorWithMessageOnly() {
        ScannerException e = new ScannerException("parse error");
        assertEquals(-1, e.getLineNumber());
        assertEquals("parse error", e.getMessage());
    }

    @Test
    public void testConstructorWithMessageAndException() {
        Exception cause = new Exception("root cause");
        ScannerException e = new ScannerException("wrapped error", cause);
        assertEquals(-1, e.getLineNumber());
        assertThat(e.getMessage(), containsString("wrapped error"));
        assertSame(cause, e.getCause());
    }

    @Test
    public void testConstructorWithLinenoAndException() {
        Exception cause = new Exception("cause");
        ScannerException e = new ScannerException(10, cause);
        assertEquals(10, e.getLineNumber());
        assertThat(e.getMessage(), containsString("10"));
    }

    @Test
    public void testIsRuntimeException() {
        ScannerException e = new ScannerException("test");
        assertInstanceOf(RuntimeException.class, e);
    }

    @Test
    public void testLineNumberNegativeOneWhenNoLine() {
        ScannerException e = new ScannerException("no line info");
        assertEquals(-1, e.getLineNumber());
    }
}
