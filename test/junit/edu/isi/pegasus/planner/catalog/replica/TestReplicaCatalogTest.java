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

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for the TestReplicaCatalog driver class. Since TestReplicaCatalog is a CLI-only utility
 * with a single main() method, tests are limited to structural/reflection checks.
 */
public class TestReplicaCatalogTest {

    @BeforeAll
    public static void setUpClass() {}

    @AfterAll
    public static void tearDownClass() {}

    @BeforeEach
    public void setUp() {}

    @AfterEach
    public void tearDown() {}

    @Test
    public void testClassLoads() {
        assertNotNull(TestReplicaCatalog.class);
    }

    @Test
    public void testClassHasMainMethod() throws NoSuchMethodException {
        java.lang.reflect.Method main =
                TestReplicaCatalog.class.getDeclaredMethod("main", String[].class);
        assertNotNull(main);
    }

    @Test
    public void testMainMethodIsPublicStatic() throws NoSuchMethodException {
        java.lang.reflect.Method main =
                TestReplicaCatalog.class.getDeclaredMethod("main", String[].class);
        int modifiers = main.getModifiers();
        assertTrue(java.lang.reflect.Modifier.isPublic(modifiers));
        assertTrue(java.lang.reflect.Modifier.isStatic(modifiers));
    }

    @Test
    public void testMainMethodReturnTypeIsVoid() throws NoSuchMethodException {
        java.lang.reflect.Method main =
                TestReplicaCatalog.class.getDeclaredMethod("main", String[].class);
        assertEquals(void.class, main.getReturnType());
    }

    @Test
    public void testClassInCorrectPackage() {
        assertEquals(
                "edu.isi.pegasus.planner.catalog.replica",
                TestReplicaCatalog.class.getPackage().getName());
    }
}
