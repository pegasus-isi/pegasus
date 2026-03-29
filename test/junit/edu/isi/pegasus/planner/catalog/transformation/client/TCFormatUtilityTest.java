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
package edu.isi.pegasus.planner.catalog.transformation.client;

import static org.junit.jupiter.api.Assertions.*;

import edu.isi.pegasus.planner.catalog.transformation.TransformationCatalogEntry;
import edu.isi.pegasus.planner.catalog.transformation.classes.TCType;
import edu.isi.pegasus.planner.catalog.transformation.classes.TransformationStore;
import edu.isi.pegasus.planner.classes.Profile;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Tests for TCFormatUtility. */
public class TCFormatUtilityTest {

    @BeforeAll
    public static void setUpClass() {}

    @AfterAll
    public static void tearDownClass() {}

    @BeforeEach
    public void setUp() {}

    @AfterEach
    public void tearDown() {}

    private TransformationStore buildStoreWithSingleEntry() {
        TransformationCatalogEntry entry = new TransformationCatalogEntry("pegasus", "keg", "1.0");
        entry.setPhysicalTransformation("/usr/bin/keg");
        entry.setResourceId("isi");
        entry.setType(TCType.INSTALLED);
        TransformationStore store = new TransformationStore();
        store.addEntry(entry);
        return store;
    }

    @Test
    public void testToTextFormatReturnsNonNullString() {
        TransformationStore store = buildStoreWithSingleEntry();
        String result = TCFormatUtility.toTextFormat(store);
        assertNotNull(result);
    }

    @Test
    public void testToTextFormatContainsTransformationName() {
        TransformationStore store = buildStoreWithSingleEntry();
        String result = TCFormatUtility.toTextFormat(store);
        assertTrue(result.contains("keg"), "Text format should contain transformation name 'keg'");
    }

    @Test
    public void testToTextFormatContainsSiteBlock() {
        TransformationStore store = buildStoreWithSingleEntry();
        String result = TCFormatUtility.toTextFormat(store);
        assertTrue(result.contains("site"), "Text format should contain 'site' keyword");
        assertTrue(result.contains("isi"), "Text format should contain site id 'isi'");
    }

    @Test
    public void testToTextFormatContainsPfn() {
        TransformationStore store = buildStoreWithSingleEntry();
        String result = TCFormatUtility.toTextFormat(store);
        assertTrue(result.contains("/usr/bin/keg"), "Text format should contain the pfn");
    }

    @Test
    public void testToTextFormatContainsTrKeyword() {
        TransformationStore store = buildStoreWithSingleEntry();
        String result = TCFormatUtility.toTextFormat(store);
        assertTrue(result.contains("tr "), "Text format should start transformation with 'tr'");
    }

    @Test
    public void testToTextFormatWithProfile() {
        TransformationCatalogEntry entry = new TransformationCatalogEntry("test", "app", null);
        entry.setPhysicalTransformation("/bin/app");
        entry.setResourceId("local");
        entry.setType(TCType.STAGEABLE);
        entry.addProfile(new Profile("env", "HOME", "/home/user"));

        TransformationStore store = new TransformationStore();
        store.addEntry(entry);

        String result = TCFormatUtility.toTextFormat(store);
        assertNotNull(result);
        assertTrue(result.contains("app"), "Result should contain transformation name");
        assertTrue(result.contains("profile"), "Result should contain profile keyword");
        assertTrue(result.contains("HOME"), "Result should contain profile key");
    }

    @Test
    public void testToTextFormatEmptyStoreReturnsHeaderOnly() {
        TransformationStore store = new TransformationStore();
        String result = TCFormatUtility.toTextFormat(store);
        assertNotNull(result);
        // An empty store should still produce at least a header comment
        assertFalse(result.isEmpty());
    }

    @Test
    public void testToTextFormatTypeAppears() {
        TransformationStore store = buildStoreWithSingleEntry();
        String result = TCFormatUtility.toTextFormat(store);
        // type INSTALLED should appear
        assertTrue(
                result.contains("INSTALLED") || result.contains("installed"),
                "Text format should mention the type");
    }

    @Test
    public void testToTextFormatWithMultipleEntries() {
        TransformationCatalogEntry entry1 = new TransformationCatalogEntry("ns", "app1", "1.0");
        entry1.setPhysicalTransformation("/bin/app1");
        entry1.setResourceId("site1");
        entry1.setType(TCType.INSTALLED);

        TransformationCatalogEntry entry2 = new TransformationCatalogEntry("ns", "app2", "1.0");
        entry2.setPhysicalTransformation("/bin/app2");
        entry2.setResourceId("site2");
        entry2.setType(TCType.STAGEABLE);

        TransformationStore store = new TransformationStore();
        store.addEntry(entry1);
        store.addEntry(entry2);

        String result = TCFormatUtility.toTextFormat(store);
        assertNotNull(result);
        assertTrue(result.contains("app1") || result.contains("app2"));
    }
}
