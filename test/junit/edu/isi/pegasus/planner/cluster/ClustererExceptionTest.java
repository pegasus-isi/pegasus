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

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

/** Tests for the ClustererException class. */
public class ClustererExceptionTest {

    @Test
    public void testNoArgConstructor() {
        ClustererException ex = new ClustererException();
        assertNotNull(ex, "ClustererException should be instantiatable with no-arg constructor");
    }

    @Test
    public void testMessageConstructor() {
        ClustererException ex = new ClustererException("test error");
        assertEquals("test error", ex.getMessage(), "Message should match constructor argument");
    }

    @Test
    public void testMessageAndCauseConstructor() {
        Throwable cause = new RuntimeException("root cause");
        ClustererException ex = new ClustererException("test error", cause);
        assertEquals("test error", ex.getMessage(), "Message should match");
        assertEquals(cause, ex.getCause(), "Cause should match");
    }

    @Test
    public void testCauseOnlyConstructor() {
        Throwable cause = new RuntimeException("root cause");
        ClustererException ex = new ClustererException(cause);
        assertEquals(cause, ex.getCause(), "Cause should match");
    }

    @Test
    public void testIsCheckedException() {
        ClustererException ex = new ClustererException("test");
        assertInstanceOf(Exception.class, ex, "ClustererException should extend Exception");
    }

    @Test
    public void testNullMessageAllowed() {
        ClustererException ex = new ClustererException((String) null);
        assertNull(ex.getMessage(), "null message should be permitted");
    }

    @Test
    public void testNullCauseAllowed() {
        ClustererException ex = new ClustererException("msg", (Throwable) null);
        assertNull(ex.getCause(), "null cause should be permitted");
    }
}
