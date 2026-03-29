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

/** Tests for the Staged transformation selector. */
public class StagedTest {

    private Staged mSelector;

    @BeforeEach
    public void setUp() {
        mSelector = new Staged();
    }

    @Test
    public void testInstantiation() {
        assertNotNull(mSelector, "Staged selector should be instantiatable");
    }

    @Test
    public void testExtendsTransformationSelector() {
        assertInstanceOf(
                TransformationSelector.class,
                mSelector,
                "Staged should extend TransformationSelector");
    }

    @Test
    public void testSelectsStageableEntries() {
        List<TransformationCatalogEntry> entries = new ArrayList<>();
        TransformationCatalogEntry stageable = new TransformationCatalogEntry("ns", "exe", "1.0");
        stageable.setType(TCType.STAGEABLE);
        stageable.setResourceId("site1");
        entries.add(stageable);

        List result = mSelector.getTCEntry(entries, "site1");
        assertNotNull(result, "Should return a list for stageable entries");
        assertEquals(1, result.size(), "Should return 1 stageable entry");
    }

    @Test
    public void testFiltersOutInstalled() {
        List<TransformationCatalogEntry> entries = new ArrayList<>();
        TransformationCatalogEntry installed = new TransformationCatalogEntry("ns", "exe", "1.0");
        installed.setType(TCType.INSTALLED);
        installed.setResourceId("site1");
        entries.add(installed);

        List result = mSelector.getTCEntry(entries, "site1");
        assertNull(result, "Should return null when no stageable entries");
    }

    @Test
    public void testMultipleStageableEntries() {
        List<TransformationCatalogEntry> entries = new ArrayList<>();

        TransformationCatalogEntry s1 = new TransformationCatalogEntry("ns", "exe", "1.0");
        s1.setType(TCType.STAGEABLE);
        s1.setResourceId("site1");

        TransformationCatalogEntry s2 = new TransformationCatalogEntry("ns", "exe", "1.0");
        s2.setType(TCType.STAGEABLE);
        s2.setResourceId("site2");

        TransformationCatalogEntry i1 = new TransformationCatalogEntry("ns", "exe", "1.0");
        i1.setType(TCType.INSTALLED);
        i1.setResourceId("site3");

        entries.add(s1);
        entries.add(s2);
        entries.add(i1);

        List result = mSelector.getTCEntry(entries, "site1");
        assertNotNull(result);
        assertEquals(
                2, result.size(), "Should return 2 stageable entries, filtering out installed");
    }

    @Test
    public void testEmptyList() {
        List<TransformationCatalogEntry> entries = new ArrayList<>();
        List result = mSelector.getTCEntry(entries, "site1");
        assertNull(result, "Should return null for empty input");
    }

    @Test
    public void testReturnedEntriesAreStageable() {
        List<TransformationCatalogEntry> entries = new ArrayList<>();
        TransformationCatalogEntry stageable = new TransformationCatalogEntry("ns", "exe", "1.0");
        stageable.setType(TCType.STAGEABLE);
        stageable.setResourceId("site1");
        entries.add(stageable);

        List result = mSelector.getTCEntry(entries, "site1");
        TransformationCatalogEntry selected = (TransformationCatalogEntry) result.get(0);
        assertEquals(
                TCType.STAGEABLE, selected.getType(), "Returned entry should be of type STAGEABLE");
    }
}
