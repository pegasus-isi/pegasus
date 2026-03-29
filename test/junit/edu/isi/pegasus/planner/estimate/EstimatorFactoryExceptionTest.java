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
package edu.isi.pegasus.planner.estimate;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

/** Tests for the EstimatorFactoryException. */
public class EstimatorFactoryExceptionTest {

    @Test
    public void testDefaultName() {
        assertEquals(
                "Estimator",
                EstimatorFactoryException.DEFAULT_NAME,
                "DEFAULT_NAME should be 'Estimator'");
    }

    @Test
    public void testConstructorWithMessage() {
        EstimatorFactoryException ex = new EstimatorFactoryException("test error");
        assertEquals("test error", ex.getMessage(), "Message should match");
    }

    @Test
    public void testConstructorWithMessageAndClassname() {
        EstimatorFactoryException ex = new EstimatorFactoryException("test error", "TestEstimator");
        assertNotNull(ex.getMessage(), "Message should not be null");
        assertEquals("TestEstimator", ex.getClassname(), "Classname should match");
    }

    @Test
    public void testConstructorWithMessageAndCause() {
        Throwable cause = new RuntimeException("root cause");
        EstimatorFactoryException ex = new EstimatorFactoryException("test error", cause);
        assertEquals(cause, ex.getCause(), "Cause should match");
        assertEquals(
                EstimatorFactoryException.DEFAULT_NAME,
                ex.getClassname(),
                "Classname should default to DEFAULT_NAME");
    }

    @Test
    public void testConstructorWithAllParams() {
        Throwable cause = new RuntimeException("root cause");
        EstimatorFactoryException ex =
                new EstimatorFactoryException("test error", "TestClass", cause);
        assertEquals("TestClass", ex.getClassname(), "Classname should match");
        assertEquals(cause, ex.getCause(), "Cause should match");
    }

    @Test
    public void testIsFactoryException() {
        EstimatorFactoryException ex = new EstimatorFactoryException("test");
        assertInstanceOf(
                edu.isi.pegasus.common.util.FactoryException.class,
                ex,
                "Should extend FactoryException");
    }

    @Test
    public void testDefaultClassnameSet() {
        EstimatorFactoryException ex = new EstimatorFactoryException("test");
        assertEquals(
                EstimatorFactoryException.DEFAULT_NAME,
                ex.getClassname(),
                "Default classname should be set to DEFAULT_NAME");
    }
}
