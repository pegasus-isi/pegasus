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

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

/**
 * Structural tests for the WorkCatalog interface via reflection.
 *
 * @author Rajiv Mayani
 */
public class WorkCatalogTest {

    @Test
    public void testWorkCatalogIsInterface() {
        assertTrue(WorkCatalog.class.isInterface(), "WorkCatalog should be an interface");
    }

    @Test
    public void testWorkCatalogExtendsCatalog() {
        assertTrue(
                Catalog.class.isAssignableFrom(WorkCatalog.class),
                "WorkCatalog should extend Catalog");
    }

    @Test
    public void testVersionConstant() {
        assertNotNull(WorkCatalog.VERSION, "WorkCatalog VERSION should not be null");
        assertEquals("1.0", WorkCatalog.VERSION, "WorkCatalog VERSION should be '1.0'");
    }

    @Test
    public void testPropertyPrefixConstant() {
        assertEquals(
                "pegasus.catalog.work",
                WorkCatalog.c_prefix,
                "WorkCatalog property prefix should be 'pegasus.catalog.work'");
    }

    @Test
    public void testDbPrefixConstant() {
        assertEquals(
                "pegasus.catalog.work.db",
                WorkCatalog.DB_PREFIX,
                "WorkCatalog DB_PREFIX should be 'pegasus.catalog.work.db'");
    }

    @Test
    public void testHasInsertMethod() throws NoSuchMethodException {
        java.lang.reflect.Method insert =
                WorkCatalog.class.getMethod(
                        "insert",
                        String.class,
                        String.class,
                        String.class,
                        String.class,
                        String.class,
                        java.util.Date.class,
                        java.util.Date.class,
                        int.class);
        assertNotNull(insert, "WorkCatalog should have an insert method");
    }

    @Test
    public void testWorkCatalogHasMethods() {
        java.lang.reflect.Method[] methods = WorkCatalog.class.getMethods();
        assertTrue(methods.length > 0, "WorkCatalog should declare methods");
    }

    @Test
    public void testWorkCatalogVersionNotEmpty() {
        assertFalse(WorkCatalog.VERSION.isEmpty(), "WorkCatalog VERSION should not be empty");
    }
}
