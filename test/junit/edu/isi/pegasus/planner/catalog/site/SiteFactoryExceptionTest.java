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
package edu.isi.pegasus.planner.catalog.site;

import static org.junit.jupiter.api.Assertions.*;

import edu.isi.pegasus.common.util.FactoryException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Tests for the SiteFactoryException class. */
public class SiteFactoryExceptionTest {
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
        assertTrue(FactoryException.class.isAssignableFrom(SiteFactoryException.class));
    }

    @Test
    public void testDefaultNameConstant() {
        assertEquals("Site Catalog", SiteFactoryException.DEFAULT_NAME);
    }

    @Test
    public void testConstructorWithMessage() {
        SiteFactoryException ex = new SiteFactoryException("test message");
        assertNotNull(ex);
        assertEquals("test message", ex.getMessage());
    }

    @Test
    public void testConstructorWithMessageAndClassname() {
        SiteFactoryException ex = new SiteFactoryException("test message", "MyClass");
        assertNotNull(ex);
        assertEquals("test message", ex.getMessage());
    }

    @Test
    public void testExceptionIsThrowable() {
        assertTrue(Throwable.class.isAssignableFrom(SiteFactoryException.class));
    }

    @Test
    public void testExceptionCanBeThrown() {
        assertThrows(
                SiteFactoryException.class,
                () -> {
                    throw new SiteFactoryException("test");
                });
    }
}
