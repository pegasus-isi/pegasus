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

import java.lang.reflect.Method;
import org.junit.jupiter.api.Test;

/**
 * Structural tests for the SiteCatalog interface via reflection.
 *
 * @author Rajiv Mayani
 */
public class SiteCatalogTest {

    @Test
    public void testSiteCatalogIsInterface() {
        assertTrue(SiteCatalog.class.isInterface(), "SiteCatalog should be an interface");
    }

    @Test
    public void testSiteCatalogExtendsCatalog() {
        assertTrue(
                Catalog.class.isAssignableFrom(SiteCatalog.class),
                "SiteCatalog should extend Catalog");
    }

    @Test
    public void testVersionConstant() {
        assertNotNull(SiteCatalog.VERSION, "SiteCatalog VERSION should not be null");
        assertFalse(SiteCatalog.VERSION.isEmpty(), "SiteCatalog VERSION should not be empty");
    }

    @Test
    public void testPropertyPrefixConstant() {
        assertEquals(
                "pegasus.catalog.site",
                SiteCatalog.c_prefix,
                "SiteCatalog property prefix should be 'pegasus.catalog.site'");
    }

    @Test
    public void testFileKeyConstant() {
        assertEquals("file", SiteCatalog.FILE_KEY, "SiteCatalog FILE_KEY should be 'file'");
    }

    @Test
    public void testHasLoadMethod() throws NoSuchMethodException {
        Method load = SiteCatalog.class.getMethod("load", java.util.List.class);
        assertNotNull(load, "SiteCatalog should have a load(List) method");
    }

    @Test
    public void testHasInitializeMethod() throws NoSuchMethodException {
        Method init =
                SiteCatalog.class.getMethod(
                        "initialize", edu.isi.pegasus.planner.classes.PegasusBag.class);
        assertNotNull(init, "SiteCatalog should have an initialize(PegasusBag) method");
    }

    @Test
    public void testVariableExpansionKeyConstant() {
        assertEquals(
                "expand",
                SiteCatalog.VARIABLE_EXPANSION_KEY,
                "SiteCatalog VARIABLE_EXPANSION_KEY should be 'expand'");
    }
}
