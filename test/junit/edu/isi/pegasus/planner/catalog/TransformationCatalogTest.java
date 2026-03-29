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
 * Structural tests for the TransformationCatalog interface via reflection.
 *
 * @author Rajiv Mayani
 */
public class TransformationCatalogTest {

    @Test
    public void testTransformationCatalogIsInterface() {
        assertTrue(
                TransformationCatalog.class.isInterface(),
                "TransformationCatalog should be an interface");
    }

    @Test
    public void testTransformationCatalogExtendsCatalog() {
        assertTrue(
                Catalog.class.isAssignableFrom(TransformationCatalog.class),
                "TransformationCatalog should extend Catalog");
    }

    @Test
    public void testVersionConstant() {
        assertNotNull(
                TransformationCatalog.VERSION, "TransformationCatalog VERSION should not be null");
        assertFalse(
                TransformationCatalog.VERSION.isEmpty(),
                "TransformationCatalog VERSION should not be empty");
    }

    @Test
    public void testPropertyPrefixConstant() {
        assertEquals(
                "pegasus.catalog.transformation",
                TransformationCatalog.c_prefix,
                "TransformationCatalog property prefix should be 'pegasus.catalog.transformation'");
    }

    @Test
    public void testFileKeyConstant() {
        assertEquals(
                "file",
                TransformationCatalog.FILE_KEY,
                "TransformationCatalog FILE_KEY should be 'file'");
    }

    @Test
    public void testTransientKeyConstant() {
        assertEquals(
                "transient",
                TransformationCatalog.TRANSIENT_KEY,
                "TransformationCatalog TRANSIENT_KEY should be 'transient'");
    }

    @Test
    public void testHasInitializeMethod() throws NoSuchMethodException {
        Method init =
                TransformationCatalog.class.getMethod(
                        "initialize", edu.isi.pegasus.planner.classes.PegasusBag.class);
        assertNotNull(init, "TransformationCatalog should have an initialize(PegasusBag) method");
    }

    @Test
    public void testVariableExpansionKeyConstant() {
        assertEquals(
                "expand",
                TransformationCatalog.VARIABLE_EXPANSION_KEY,
                "TransformationCatalog VARIABLE_EXPANSION_KEY should be 'expand'");
    }
}
