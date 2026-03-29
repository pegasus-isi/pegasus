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
package edu.isi.pegasus.planner.refiner.cleanup;

import static org.junit.jupiter.api.Assertions.*;

import edu.isi.pegasus.common.util.FactoryException;
import org.junit.jupiter.api.Test;

/** Tests for CleanupFactoryException. */
public class CleanupFactoryExceptionTest {

    @Test
    public void testExtendsFactoryException() {
        assertTrue(FactoryException.class.isAssignableFrom(CleanupFactoryException.class));
    }

    @Test
    public void testDefaultNameConstant() {
        assertEquals("File Cleanup", CleanupFactoryException.DEFAULT_NAME);
    }

    @Test
    public void testConstructorWithMessage() {
        CleanupFactoryException ex = new CleanupFactoryException("test error");
        assertEquals("test error", ex.getMessage());
    }

    @Test
    public void testConstructorWithMessageSetsDefaultClassname() {
        CleanupFactoryException ex = new CleanupFactoryException("test error");
        assertEquals("File Cleanup", ex.getClassname());
    }

    @Test
    public void testConstructorWithMessageAndClassname() {
        CleanupFactoryException ex = new CleanupFactoryException("error", "InPlace");
        assertEquals("InPlace", ex.getClassname());
    }

    @Test
    public void testConstructorWithMessageAndCause() {
        Throwable cause = new RuntimeException("root cause");
        CleanupFactoryException ex = new CleanupFactoryException("error", cause);
        assertEquals(cause, ex.getCause());
    }
}
