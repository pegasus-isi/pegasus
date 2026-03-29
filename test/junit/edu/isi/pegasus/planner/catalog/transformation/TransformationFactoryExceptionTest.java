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
package edu.isi.pegasus.planner.catalog.transformation;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Tests for TransformationFactoryException. */
public class TransformationFactoryExceptionTest {

    @BeforeAll
    public static void setUpClass() {}

    @AfterAll
    public static void tearDownClass() {}

    @BeforeEach
    public void setUp() {}

    @AfterEach
    public void tearDown() {}

    @Test
    public void testDefaultNameConstantValue() {
        assertEquals("Transformation Catalog", TransformationFactoryException.DEFAULT_NAME);
    }

    @Test
    public void testSingleArgConstructorSetsMessage() {
        TransformationFactoryException ex =
                new TransformationFactoryException("catalog load failed");
        assertEquals("catalog load failed", ex.getMessage());
    }

    @Test
    public void testSingleArgConstructorSetsDefaultClassname() {
        TransformationFactoryException ex =
                new TransformationFactoryException("catalog load failed");
        assertEquals(TransformationFactoryException.DEFAULT_NAME, ex.getClassname());
    }

    @Test
    public void testTwoArgConstructorSetsMessageAndClassname() {
        TransformationFactoryException ex =
                new TransformationFactoryException("load failed", "MyTCClass");
        assertEquals("load failed", ex.getMessage());
        assertEquals("MyTCClass", ex.getClassname());
    }

    @Test
    public void testConstructorWithCauseSetsDefaultClassname() {
        RuntimeException cause = new RuntimeException("root cause");
        TransformationFactoryException ex = new TransformationFactoryException("outer", cause);
        assertEquals(TransformationFactoryException.DEFAULT_NAME, ex.getClassname());
        assertSame(cause, ex.getCause());
    }

    @Test
    public void testConstructorWithClassnameAndCause() {
        RuntimeException cause = new RuntimeException("root");
        TransformationFactoryException ex =
                new TransformationFactoryException("message", "SomeClass", cause);
        assertEquals("message", ex.getMessage());
        assertEquals("SomeClass", ex.getClassname());
        assertSame(cause, ex.getCause());
    }

    @Test
    public void testIsRuntimeException() {
        TransformationFactoryException ex = new TransformationFactoryException("test");
        assertTrue(ex instanceof RuntimeException);
    }

    @Test
    public void testConvertExceptionReturnsNonNullString() {
        TransformationFactoryException ex =
                new TransformationFactoryException("catalog load failed");
        String result = ex.convertException();
        assertNotNull(result);
        assertFalse(result.isEmpty());
    }

    @Test
    public void testConvertExceptionContainsMessage() {
        TransformationFactoryException ex =
                new TransformationFactoryException("unique-error-message");
        String result = ex.convertException();
        assertTrue(
                result.contains("unique-error-message"),
                "Expected exception message in result, got: " + result);
    }
}
