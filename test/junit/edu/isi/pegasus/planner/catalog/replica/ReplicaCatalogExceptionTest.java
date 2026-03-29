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
package edu.isi.pegasus.planner.catalog.replica;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.*;

import edu.isi.pegasus.planner.catalog.CatalogException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** @author Rajiv Mayani */
public class ReplicaCatalogExceptionTest {

    @BeforeAll
    public static void setUpClass() {}

    @AfterAll
    public static void tearDownClass() {}

    @BeforeEach
    public void setUp() {}

    @AfterEach
    public void tearDown() {}

    @Test
    public void testDefaultConstructorCreatesException() {
        ReplicaCatalogException ex = new ReplicaCatalogException();
        assertNotNull(ex);
        assertNull(ex.getMessage());
    }

    @Test
    public void testStringConstructorSetsMessage() {
        ReplicaCatalogException ex = new ReplicaCatalogException("replica error");
        assertEquals("replica error", ex.getMessage());
    }

    @Test
    public void testStringCauseConstructorSetsBoth() {
        Throwable cause = new RuntimeException("root cause");
        ReplicaCatalogException ex = new ReplicaCatalogException("wrapper", cause);
        assertEquals("wrapper", ex.getMessage());
        assertSame(cause, ex.getCause());
    }

    @Test
    public void testCauseOnlyConstructor() {
        Throwable cause = new RuntimeException("root cause");
        ReplicaCatalogException ex = new ReplicaCatalogException(cause);
        assertSame(cause, ex.getCause());
    }

    @Test
    public void testIsInstanceOfCatalogException() {
        ReplicaCatalogException ex = new ReplicaCatalogException("test");
        assertThat(ex, instanceOf(CatalogException.class));
    }

    @Test
    public void testIsRuntimeException() {
        ReplicaCatalogException ex = new ReplicaCatalogException("test");
        assertThat(ex, instanceOf(RuntimeException.class));
    }

    @Test
    public void testExceptionCanBeThrown() {
        assertThrows(
                ReplicaCatalogException.class,
                () -> {
                    throw new ReplicaCatalogException("thrown");
                });
    }

    @Test
    public void testExceptionCaughtAsCatalogException() {
        assertThrows(
                CatalogException.class,
                () -> {
                    throw new ReplicaCatalogException("thrown as CatalogException");
                });
    }

    @Test
    public void testGetNextExceptionInherited() {
        ReplicaCatalogException first = new ReplicaCatalogException("first");
        ReplicaCatalogException second = new ReplicaCatalogException("second");
        first.setNextException(second);
        assertSame(second, first.getNextException());
    }

    @Test
    public void testChainedExceptions() {
        ReplicaCatalogException root = new ReplicaCatalogException("root");
        root.setNextException(new ReplicaCatalogException("child1"));
        root.setNextException(new ReplicaCatalogException("child2"));

        int count = 0;
        for (CatalogException ex = root; ex != null; ex = ex.getNextException()) {
            count++;
        }
        assertEquals(3, count);
    }
}
