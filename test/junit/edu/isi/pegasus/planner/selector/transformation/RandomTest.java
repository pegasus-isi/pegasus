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

/** Tests for the Random transformation selector. */
public class RandomTest {

    private Random mSelector;

    @BeforeEach
    public void setUp() {
        mSelector = new Random();
    }

    @Test
    public void testInstantiation() {
        assertNotNull(mSelector, "Random selector should be instantiatable");
    }

    @Test
    public void testExtendsTransformationSelector() {
        assertInstanceOf(
                TransformationSelector.class,
                mSelector,
                "Random should extend TransformationSelector");
    }

    @Test
    public void testSelectsOneEntry() {
        List<TransformationCatalogEntry> entries = new ArrayList<>();
        TransformationCatalogEntry e1 = new TransformationCatalogEntry("ns", "exe", "1.0");
        e1.setType(TCType.INSTALLED);
        e1.setResourceId("site1");

        TransformationCatalogEntry e2 = new TransformationCatalogEntry("ns", "exe", "1.0");
        e2.setType(TCType.INSTALLED);
        e2.setResourceId("site2");

        entries.add(e1);
        entries.add(e2);

        List result = mSelector.getTCEntry(entries, "site1");
        assertNotNull(result, "Result should not be null");
        assertEquals(1, result.size(), "Random selector should return exactly one entry");
    }

    @Test
    public void testPrefersPreferredSite() {
        List<TransformationCatalogEntry> entries = new ArrayList<>();
        TransformationCatalogEntry e1 = new TransformationCatalogEntry("ns", "exe", "1.0");
        e1.setType(TCType.INSTALLED);
        e1.setResourceId("preferred");

        TransformationCatalogEntry e2 = new TransformationCatalogEntry("ns", "exe", "1.0");
        e2.setType(TCType.INSTALLED);
        e2.setResourceId("other");

        entries.add(e1);
        entries.add(e2);

        // Run multiple times to ensure preferred site is always selected
        for (int i = 0; i < 10; i++) {
            List result = mSelector.getTCEntry(entries, "preferred");
            assertEquals(1, result.size(), "Should return one entry");
            assertEquals(
                    "preferred",
                    ((TransformationCatalogEntry) result.get(0)).getResourceId(),
                    "Should always select preferred site when available");
        }
    }

    @Test
    public void testSingleEntryReturned() {
        List<TransformationCatalogEntry> entries = new ArrayList<>();
        TransformationCatalogEntry e1 = new TransformationCatalogEntry("ns", "exe", "1.0");
        e1.setType(TCType.INSTALLED);
        e1.setResourceId("site1");
        entries.add(e1);

        List result = mSelector.getTCEntry(entries, "site1");
        assertNotNull(result);
        assertEquals(1, result.size(), "Should return 1 entry when only 1 available");
        assertEquals(e1, result.get(0), "Should return the same entry");
    }

    @Test
    public void testFallsBackToAnyWhenPreferredNotFound() {
        List<TransformationCatalogEntry> entries = new ArrayList<>();
        TransformationCatalogEntry e1 = new TransformationCatalogEntry("ns", "exe", "1.0");
        e1.setType(TCType.INSTALLED);
        e1.setResourceId("other-site");
        entries.add(e1);

        List result = mSelector.getTCEntry(entries, "preferred-site");
        assertNotNull(result, "Should fall back to available entries when preferred not found");
        assertEquals(1, result.size(), "Should return 1 entry as fallback");
    }

    @Test
    public void testReturnedListSize() {
        List<TransformationCatalogEntry> entries = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            TransformationCatalogEntry e = new TransformationCatalogEntry("ns", "exe", "1.0");
            e.setType(TCType.INSTALLED);
            e.setResourceId("site" + i);
            entries.add(e);
        }

        List result = mSelector.getTCEntry(entries, "site3");
        assertEquals(1, result.size(), "Random selector must return exactly 1 entry");
    }
}
