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
package edu.isi.pegasus.planner.catalog.transformation.classes;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Tests for the TransformationCatalogKeywords enum. */
public class TransformationCatalogKeywordsTest {

    @BeforeAll
    public static void setUpClass() {}

    @AfterAll
    public static void tearDownClass() {}

    @BeforeEach
    public void setUp() {}

    @AfterEach
    public void tearDown() {}

    @Test
    public void testGetReservedNameForPegasus() {
        assertEquals("pegasus", TransformationCatalogKeywords.PEGASUS.getReservedName());
    }

    @Test
    public void testGetReservedNameForNamespace() {
        assertEquals("namespace", TransformationCatalogKeywords.NAMESPACE.getReservedName());
    }

    @Test
    public void testGetReservedNameForName() {
        assertEquals("name", TransformationCatalogKeywords.NAME.getReservedName());
    }

    @Test
    public void testGetReservedNameForTransformations() {
        assertEquals(
                "transformations", TransformationCatalogKeywords.TRANSFORMATIONS.getReservedName());
    }

    @Test
    public void testGetReservedNameForContainers() {
        assertEquals("containers", TransformationCatalogKeywords.CONTAINERS.getReservedName());
    }

    @Test
    public void testGetReservedNameForSites() {
        assertEquals("sites", TransformationCatalogKeywords.SITES.getReservedName());
    }

    @Test
    public void testGetReservedNameForType() {
        assertEquals("type", TransformationCatalogKeywords.TYPE.getReservedName());
    }

    @Test
    public void testGetReservedNameForPfn() {
        assertEquals("pfn", TransformationCatalogKeywords.SITE_PFN.getReservedName());
    }

    @Test
    public void testGetReservedNameForContainerImage() {
        assertEquals("image", TransformationCatalogKeywords.CONTAINER_IMAGE.getReservedName());
    }

    @Test
    public void testGetReservedKeyLookupByName() {
        assertSame(
                TransformationCatalogKeywords.NAME,
                TransformationCatalogKeywords.getReservedKey("name"));
    }

    @Test
    public void testGetReservedKeyLookupForPegasus() {
        assertSame(
                TransformationCatalogKeywords.PEGASUS,
                TransformationCatalogKeywords.getReservedKey("pegasus"));
    }

    @Test
    public void testGetReservedKeyLookupForTransformations() {
        assertSame(
                TransformationCatalogKeywords.TRANSFORMATIONS,
                TransformationCatalogKeywords.getReservedKey("transformations"));
    }

    @Test
    public void testGetReservedKeyReturnsNullForUnknownKey() {
        assertNull(TransformationCatalogKeywords.getReservedKey("unknown-key-xyz"));
    }

    @Test
    public void testAllEnumValuesHaveNonEmptyReservedNames() {
        for (TransformationCatalogKeywords keyword : TransformationCatalogKeywords.values()) {
            assertNotNull(
                    keyword.getReservedName(), "Reserved name should not be null for " + keyword);
            assertFalse(
                    keyword.getReservedName().isEmpty(),
                    "Reserved name should not be empty for " + keyword);
        }
    }

    @Test
    public void testGetReservedKeyForAllValues() {
        for (TransformationCatalogKeywords keyword : TransformationCatalogKeywords.values()) {
            assertSame(
                    keyword,
                    TransformationCatalogKeywords.getReservedKey(keyword.getReservedName()),
                    "getReservedKey should return the same constant for " + keyword);
        }
    }

    @Test
    public void testContainerMountKeyword() {
        assertEquals("mounts", TransformationCatalogKeywords.CONTAINER_MOUNT.getReservedName());
    }

    @Test
    public void testChecksumKeyword() {
        assertEquals("checksum", TransformationCatalogKeywords.CHECKSUM.getReservedName());
    }

    @Test
    public void testBypassKeyword() {
        assertEquals("bypass", TransformationCatalogKeywords.BYPASS.getReservedName());
    }
}
