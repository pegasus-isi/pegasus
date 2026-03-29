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
package edu.isi.pegasus.planner.catalog;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** @author Rajiv Mayani */
public class CatalogExceptionTest {

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
        CatalogException ex = new CatalogException();
        assertNotNull(ex);
        assertNull(ex.getMessage());
        assertNull(ex.getNextException());
    }

    @Test
    public void testStringConstructorSetsMessage() {
        CatalogException ex = new CatalogException("test error");
        assertEquals("test error", ex.getMessage());
    }

    @Test
    public void testStringCauseConstructorSetsBoth() {
        Throwable cause = new RuntimeException("root cause");
        CatalogException ex = new CatalogException("wrapper message", cause);
        assertEquals("wrapper message", ex.getMessage());
        assertSame(cause, ex.getCause());
    }

    @Test
    public void testCauseOnlyConstructor() {
        Throwable cause = new RuntimeException("root cause");
        CatalogException ex = new CatalogException(cause);
        assertSame(cause, ex.getCause());
    }

    @Test
    public void testGetNextExceptionInitiallyNull() {
        CatalogException ex = new CatalogException("first");
        assertNull(ex.getNextException());
    }

    @Test
    public void testSetNextExceptionChainsSingle() {
        CatalogException first = new CatalogException("first");
        CatalogException second = new CatalogException("second");
        first.setNextException(second);
        assertSame(second, first.getNextException());
    }

    @Test
    public void testSetNextExceptionChainsMultiple() {
        CatalogException first = new CatalogException("first");
        CatalogException second = new CatalogException("second");
        CatalogException third = new CatalogException("third");

        first.setNextException(second);
        first.setNextException(third); // should be appended to end of chain

        assertSame(second, first.getNextException());
        assertSame(third, first.getNextException().getNextException());
    }

    @Test
    public void testCatalogExceptionIsRuntimeException() {
        CatalogException ex = new CatalogException("test");
        assertThat(ex, instanceOf(RuntimeException.class));
    }

    @Test
    public void testExceptionChainIterationPattern() {
        CatalogException root = new CatalogException("first");
        root.setNextException(new CatalogException("second"));
        root.setNextException(new CatalogException("third"));

        int count = 0;
        for (CatalogException rce = root; rce != null; rce = rce.getNextException()) {
            count++;
        }
        assertEquals(3, count, "Chain should have 3 exceptions");
    }

    @Test
    public void testExceptionCanBeThrown() {
        assertThrows(
                CatalogException.class,
                () -> {
                    throw new CatalogException("thrown exception");
                });
    }

    @Test
    public void testSetNextExceptionOnSecondPositionAppends() {
        CatalogException first = new CatalogException("first");
        CatalogException second = new CatalogException("second");
        first.setNextException(second);

        CatalogException third = new CatalogException("third");
        first.setNextException(third);

        // third should be at the end of the chain
        assertNull(third.getNextException());
        assertSame(third, second.getNextException());
    }
}
