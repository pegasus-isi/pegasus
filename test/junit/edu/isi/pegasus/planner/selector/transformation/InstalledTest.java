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
package edu.isi.pegasus.planner.selector.transformation;

import static org.junit.jupiter.api.Assertions.*;

import edu.isi.pegasus.planner.catalog.transformation.TransformationCatalogEntry;
import edu.isi.pegasus.planner.catalog.transformation.classes.TCType;
import edu.isi.pegasus.planner.selector.TransformationSelector;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Tests for the Installed transformation selector. */
public class InstalledTest {

    private Installed mSelector;

    @BeforeEach
    public void setUp() {
        mSelector = new Installed();
    }

    @Test
    public void testInstantiation() {
        assertNotNull(mSelector, "Installed selector should be instantiatable");
    }

    @Test
    public void testExtendsTransformationSelector() {
        assertInstanceOf(
                TransformationSelector.class,
                mSelector,
                "Installed should extend TransformationSelector");
    }

    @Test
    public void testSelectsInstalledEntries() {
        List<TransformationCatalogEntry> entries = new ArrayList<>();
        TransformationCatalogEntry installed = new TransformationCatalogEntry("ns", "exe", "1.0");
        installed.setType(TCType.INSTALLED);
        installed.setResourceId("site1");
        entries.add(installed);

        List result = mSelector.getTCEntry(entries, "site1");
        assertNotNull(result, "Should return a list for installed entries");
        assertEquals(1, result.size(), "Should return 1 installed entry");
    }

    @Test
    public void testFiltersOutStageable() {
        List<TransformationCatalogEntry> entries = new ArrayList<>();
        TransformationCatalogEntry stageable = new TransformationCatalogEntry("ns", "exe", "1.0");
        stageable.setType(TCType.STAGEABLE);
        stageable.setResourceId("site1");
        entries.add(stageable);

        List result = mSelector.getTCEntry(entries, "site1");
        assertNull(result, "Should return null when no installed entries");
    }

    @Test
    public void testMultipleEntries() {
        List<TransformationCatalogEntry> entries = new ArrayList<>();

        TransformationCatalogEntry i1 = new TransformationCatalogEntry("ns", "exe", "1.0");
        i1.setType(TCType.INSTALLED);
        i1.setResourceId("site1");

        TransformationCatalogEntry i2 = new TransformationCatalogEntry("ns", "exe", "1.0");
        i2.setType(TCType.INSTALLED);
        i2.setResourceId("site2");

        TransformationCatalogEntry s1 = new TransformationCatalogEntry("ns", "exe", "1.0");
        s1.setType(TCType.STAGEABLE);
        s1.setResourceId("site3");

        entries.add(i1);
        entries.add(i2);
        entries.add(s1);

        List result = mSelector.getTCEntry(entries, "site1");
        assertNotNull(result);
        assertEquals(
                2, result.size(), "Should return 2 installed entries, filtering out stageable");
    }

    @Test
    public void testEmptyList() {
        List<TransformationCatalogEntry> entries = new ArrayList<>();
        List result = mSelector.getTCEntry(entries, "site1");
        assertNull(result, "Should return null for empty input");
    }

    @Test
    public void testReturnedEntriesAreInstalled() {
        List<TransformationCatalogEntry> entries = new ArrayList<>();
        TransformationCatalogEntry installed = new TransformationCatalogEntry("ns", "exe", "1.0");
        installed.setType(TCType.INSTALLED);
        installed.setResourceId("site1");
        entries.add(installed);

        List result = mSelector.getTCEntry(entries, "site1");
        TransformationCatalogEntry selected = (TransformationCatalogEntry) result.get(0);
        assertEquals(
                TCType.INSTALLED, selected.getType(), "Returned entry should be of type INSTALLED");
    }
}
