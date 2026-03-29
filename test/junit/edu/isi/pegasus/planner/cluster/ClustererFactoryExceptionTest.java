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

import edu.isi.pegasus.common.util.FactoryException;
import org.junit.jupiter.api.Test;

/** Tests for the ClustererFactoryException class. */
public class ClustererFactoryExceptionTest {

    @Test
    public void testDefaultName() {
        assertEquals(
                "Clusterer",
                ClustererFactoryException.DEFAULT_NAME,
                "DEFAULT_NAME should be 'Clusterer'");
    }

    @Test
    public void testConstructorWithMessage() {
        ClustererFactoryException ex = new ClustererFactoryException("test error");
        assertNotNull(ex.getMessage(), "Message should not be null");
        assertEquals(
                ClustererFactoryException.DEFAULT_NAME,
                ex.getClassname(),
                "Classname should default to DEFAULT_NAME");
    }

    @Test
    public void testConstructorWithMessageAndClassname() {
        ClustererFactoryException ex = new ClustererFactoryException("test error", "MyClusterer");
        assertEquals("MyClusterer", ex.getClassname(), "Classname should match");
    }

    @Test
    public void testConstructorWithMessageAndCause() {
        Throwable cause = new RuntimeException("root cause");
        ClustererFactoryException ex = new ClustererFactoryException("test error", cause);
        assertEquals(cause, ex.getCause(), "Cause should match");
        assertEquals(
                ClustererFactoryException.DEFAULT_NAME,
                ex.getClassname(),
                "Classname should default to DEFAULT_NAME when not specified");
    }

    @Test
    public void testConstructorWithAllParams() {
        Throwable cause = new RuntimeException("root cause");
        ClustererFactoryException ex =
                new ClustererFactoryException("test error", "MyClusterer", cause);
        assertEquals("MyClusterer", ex.getClassname(), "Classname should match");
        assertEquals(cause, ex.getCause(), "Cause should match");
    }

    @Test
    public void testExtendsFactoryException() {
        ClustererFactoryException ex = new ClustererFactoryException("test");
        assertInstanceOf(FactoryException.class, ex, "Should extend FactoryException");
    }

    @Test
    public void testDefaultClassnameSet() {
        ClustererFactoryException ex = new ClustererFactoryException("test");
        assertEquals(
                ClustererFactoryException.DEFAULT_NAME,
                ex.getClassname(),
                "Default classname should be set to DEFAULT_NAME");
    }
}
