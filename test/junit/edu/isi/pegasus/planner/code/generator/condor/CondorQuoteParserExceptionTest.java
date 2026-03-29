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

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Tests for the CondorQuoteParserException class. */
public class CondorQuoteParserExceptionTest {
    @BeforeAll
    public static void setUpClass() {}

    @AfterAll
    public static void tearDownClass() {}

    @BeforeEach
    public void setUp() {}

    @AfterEach
    public void tearDown() {}

    @Test
    public void testExceptionExtendsException() {
        assertTrue(Exception.class.isAssignableFrom(CondorQuoteParserException.class));
    }

    @Test
    public void testConstructorWithMessageAndPosition() {
        CondorQuoteParserException ex = new CondorQuoteParserException("parse error", 5);
        assertNotNull(ex);
        assertEquals("parse error", ex.getMessage());
        assertEquals(5, ex.getPosition());
    }

    @Test
    public void testConstructorWithMessagePositionAndCause() {
        Throwable cause = new RuntimeException("root cause");
        CondorQuoteParserException ex = new CondorQuoteParserException("parse error", 3, cause);
        assertNotNull(ex);
        assertEquals("parse error", ex.getMessage());
        assertEquals(3, ex.getPosition());
        assertEquals(cause, ex.getCause());
    }

    @Test
    public void testGetPositionReturnsCorrectValue() {
        CondorQuoteParserException ex = new CondorQuoteParserException("error at position 10", 10);
        assertEquals(10, ex.getPosition());
    }

    @Test
    public void testExceptionIsThrowable() {
        assertTrue(Throwable.class.isAssignableFrom(CondorQuoteParserException.class));
    }

    @Test
    public void testExceptionCanBeThrown() {
        assertThrows(
                CondorQuoteParserException.class,
                () -> {
                    throw new CondorQuoteParserException("test", 0);
                });
    }
}
