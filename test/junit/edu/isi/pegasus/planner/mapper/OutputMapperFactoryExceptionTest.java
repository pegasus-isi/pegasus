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
package edu.isi.pegasus.planner.mapper;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Tests for the OutputMapperFactoryException class. */
public class OutputMapperFactoryExceptionTest {

    @BeforeAll
    public static void setUpClass() {}

    @AfterAll
    public static void tearDownClass() {}

    @BeforeEach
    public void setUp() {}

    @AfterEach
    public void tearDown() {}

    @Test
    public void testExtendsFactoryException() {
        assertTrue(
                edu.isi.pegasus.common.util.FactoryException.class.isAssignableFrom(
                        OutputMapperFactoryException.class));
    }

    @Test
    public void testDefaultNameConstant() {
        assertEquals("Output Mapper", OutputMapperFactoryException.DEFAULT_NAME);
    }

    @Test
    public void testConstructWithMessageSetsDefaultClassname() {
        OutputMapperFactoryException ex = new OutputMapperFactoryException("test message");
        assertEquals("test message", ex.getMessage());
        assertEquals(OutputMapperFactoryException.DEFAULT_NAME, ex.getClassname());
    }

    @Test
    public void testConstructWithMessageAndClassname() {
        OutputMapperFactoryException ex =
                new OutputMapperFactoryException("test message", "MyClass");
        assertEquals("test message", ex.getMessage());
        assertEquals("MyClass", ex.getClassname());
    }

    @Test
    public void testConstructWithMessageAndCause() {
        RuntimeException cause = new RuntimeException("cause");
        OutputMapperFactoryException ex = new OutputMapperFactoryException("test message", cause);
        assertEquals("test message", ex.getMessage());
        assertEquals(cause, ex.getCause());
        assertEquals(OutputMapperFactoryException.DEFAULT_NAME, ex.getClassname());
    }

    @Test
    public void testConstructWithMessageClassnameAndCause() {
        RuntimeException cause = new RuntimeException("cause");
        OutputMapperFactoryException ex =
                new OutputMapperFactoryException("test message", "MyClass", cause);
        assertEquals("test message", ex.getMessage());
        assertEquals("MyClass", ex.getClassname());
        assertEquals(cause, ex.getCause());
    }
}
