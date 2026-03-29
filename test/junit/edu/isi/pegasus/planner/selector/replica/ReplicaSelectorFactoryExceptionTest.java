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
package edu.isi.pegasus.planner.selector.replica;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

/** Tests for the ReplicaSelectorFactoryException. */
public class ReplicaSelectorFactoryExceptionTest {

    @Test
    public void testDefaultName() {
        assertEquals(
                "Replica Selector",
                ReplicaSelectorFactoryException.DEFAULT_NAME,
                "DEFAULT_NAME should be 'Replica Selector'");
    }

    @Test
    public void testConstructorWithMessage() {
        ReplicaSelectorFactoryException ex = new ReplicaSelectorFactoryException("test error");
        assertEquals("test error", ex.getMessage(), "Message should match");
    }

    @Test
    public void testConstructorWithMessageAndClassname() {
        ReplicaSelectorFactoryException ex =
                new ReplicaSelectorFactoryException("test error", "TestClass");
        assertNotNull(ex.getMessage(), "Message should not be null");
        assertEquals("TestClass", ex.getClassname(), "Classname should match");
    }

    @Test
    public void testConstructorWithMessageAndCause() {
        Throwable cause = new RuntimeException("root cause");
        ReplicaSelectorFactoryException ex =
                new ReplicaSelectorFactoryException("test error", cause);
        assertEquals(cause, ex.getCause(), "Cause should match");
        assertEquals(
                ReplicaSelectorFactoryException.DEFAULT_NAME,
                ex.getClassname(),
                "Classname should default to DEFAULT_NAME");
    }

    @Test
    public void testConstructorWithMessageClassnameAndCause() {
        Throwable cause = new RuntimeException("root cause");
        ReplicaSelectorFactoryException ex =
                new ReplicaSelectorFactoryException("test error", "TestClass", cause);
        assertEquals(cause, ex.getCause(), "Cause should match");
        assertEquals("TestClass", ex.getClassname(), "Classname should match");
    }

    @Test
    public void testDefaultClassname() {
        ReplicaSelectorFactoryException ex = new ReplicaSelectorFactoryException("test");
        assertEquals(
                ReplicaSelectorFactoryException.DEFAULT_NAME,
                ex.getClassname(),
                "Default classname should be set to DEFAULT_NAME");
    }

    @Test
    public void testIsFactoryException() {
        ReplicaSelectorFactoryException ex = new ReplicaSelectorFactoryException("test");
        assertInstanceOf(
                edu.isi.pegasus.common.util.FactoryException.class,
                ex,
                "Should extend FactoryException");
    }
}
