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

import edu.isi.pegasus.common.util.FactoryException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Tests for the CondorStyleFactoryException class. */
public class CondorStyleFactoryExceptionTest {
    @BeforeAll
    public static void setUpClass() {}

    @AfterAll
    public static void tearDownClass() {}

    @BeforeEach
    public void setUp() {}

    @AfterEach
    public void tearDown() {}

    @Test
    public void testExceptionExtendsFactoryException() {
        assertTrue(FactoryException.class.isAssignableFrom(CondorStyleFactoryException.class));
    }

    @Test
    public void testDefaultNameConstant() {
        assertEquals("Code Generator", CondorStyleFactoryException.DEFAULT_NAME);
    }

    @Test
    public void testConstructorWithMessageSetsDefaultName() {
        CondorStyleFactoryException ex = new CondorStyleFactoryException("test message");
        assertNotNull(ex);
        assertEquals("test message", ex.getMessage());
    }

    @Test
    public void testConstructorWithMessageAndClassname() {
        CondorStyleFactoryException ex = new CondorStyleFactoryException("test message", "MyClass");
        assertNotNull(ex);
        assertEquals("test message", ex.getMessage());
    }

    @Test
    public void testConstructorWithMessageAndCause() {
        Throwable cause = new RuntimeException("root cause");
        CondorStyleFactoryException ex = new CondorStyleFactoryException("test message", cause);
        assertNotNull(ex);
        assertEquals(cause, ex.getCause());
    }

    @Test
    public void testConstructorWithMessageClassnameAndCause() {
        Throwable cause = new RuntimeException("root cause");
        CondorStyleFactoryException ex =
                new CondorStyleFactoryException("test message", "MyClass", cause);
        assertNotNull(ex);
        assertEquals(cause, ex.getCause());
    }
}
