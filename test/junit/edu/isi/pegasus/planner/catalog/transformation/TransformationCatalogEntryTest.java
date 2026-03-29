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

import edu.isi.pegasus.planner.catalog.transformation.classes.TCType;
import edu.isi.pegasus.planner.classes.Profile;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for TransformationCatalogEntry from the transformation package perspective. (Complements
 * the more detailed tests in classes.TransformationCatalogEntryTest.)
 */
public class TransformationCatalogEntryTest {

    @BeforeAll
    public static void setUpClass() {}

    @AfterAll
    public static void tearDownClass() {}

    @BeforeEach
    public void setUp() {}

    @AfterEach
    public void tearDown() {}

    @Test
    public void testDefaultConstructorCreatesEntry() {
        TransformationCatalogEntry entry = new TransformationCatalogEntry();
        assertNull(entry.getLogicalNamespace());
        assertNull(entry.getLogicalName());
        assertNull(entry.getLogicalVersion());
    }

    @Test
    public void testThreeArgConstructorSetsNamespaceNameVersion() {
        TransformationCatalogEntry entry = new TransformationCatalogEntry("pegasus", "keg", "1.0");
        assertEquals("pegasus", entry.getLogicalNamespace());
        assertEquals("keg", entry.getLogicalName());
        assertEquals("1.0", entry.getLogicalVersion());
    }

    @Test
    public void testDefaultTypeIsInstalled() {
        TransformationCatalogEntry entry = new TransformationCatalogEntry("ns", "name", "1.0");
        assertEquals(TCType.INSTALLED, entry.getType());
    }

    @Test
    public void testSetTypeToStageable() {
        TransformationCatalogEntry entry = new TransformationCatalogEntry("ns", "name", "1.0");
        entry.setType(TCType.STAGEABLE);
        assertEquals(TCType.STAGEABLE, entry.getType());
    }

    @Test
    public void testSetPhysicalTransformation() {
        TransformationCatalogEntry entry = new TransformationCatalogEntry("ns", "name", "1.0");
        entry.setPhysicalTransformation("/usr/bin/keg");
        assertEquals("/usr/bin/keg", entry.getPhysicalTransformation());
    }

    @Test
    public void testSetResourceId() {
        TransformationCatalogEntry entry = new TransformationCatalogEntry("ns", "name", "1.0");
        entry.setResourceId("isi");
        assertEquals("isi", entry.getResourceId());
    }

    @Test
    public void testBypassStagingDefaultIsFalse() {
        TransformationCatalogEntry entry = new TransformationCatalogEntry("ns", "name", "1.0");
        assertFalse(entry.bypassStaging());
    }

    @Test
    public void testSetBypassStaging() {
        TransformationCatalogEntry entry = new TransformationCatalogEntry("ns", "name", "1.0");
        entry.setForBypassStaging(true);
        assertTrue(entry.bypassStaging());
    }

    @Test
    public void testAddProfileIncreasesProfileCount() {
        TransformationCatalogEntry entry = new TransformationCatalogEntry("ns", "name", "1.0");
        entry.addProfile(new Profile("env", "JAVA_HOME", "/opt/java"));
        assertNotNull(entry.getProfiles());
        assertFalse(entry.getProfiles().isEmpty());
    }

    @Test
    public void testGetLogicalTransformationCombinesNamespaceNameVersion() {
        TransformationCatalogEntry entry = new TransformationCatalogEntry("pegasus", "keg", "1.0");
        String lt = entry.getLogicalTransformation();
        assertNotNull(lt);
        assertTrue(lt.contains("keg"));
    }

    @Test
    public void testCloneProducesEquivalentEntry() {
        TransformationCatalogEntry original =
                new TransformationCatalogEntry("pegasus", "keg", "1.0");
        original.setPhysicalTransformation("/usr/bin/keg");
        original.setResourceId("isi");
        original.setType(TCType.STAGEABLE);

        TransformationCatalogEntry clone = (TransformationCatalogEntry) original.clone();
        assertEquals(original.getLogicalNamespace(), clone.getLogicalNamespace());
        assertEquals(original.getLogicalName(), clone.getLogicalName());
        assertEquals(original.getLogicalVersion(), clone.getLogicalVersion());
        assertEquals(original.getPhysicalTransformation(), clone.getPhysicalTransformation());
        assertEquals(original.getResourceId(), clone.getResourceId());
        assertEquals(original.getType(), clone.getType());
    }

    @Test
    public void testToStringContainsLogicalName() {
        TransformationCatalogEntry entry = new TransformationCatalogEntry("pegasus", "keg", "1.0");
        String s = entry.toString();
        assertTrue(s.contains("keg"));
    }

    @Test
    public void testNullNamespaceAllowed() {
        TransformationCatalogEntry entry = new TransformationCatalogEntry(null, "keg", null);
        assertNull(entry.getLogicalNamespace());
        assertEquals("keg", entry.getLogicalName());
        assertNull(entry.getLogicalVersion());
    }
}
