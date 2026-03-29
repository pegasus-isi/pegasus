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

/** Tests for the Submit transformation selector. */
public class SubmitTest {

    private Submit mSelector;

    @BeforeEach
    public void setUp() {
        mSelector = new Submit();
    }

    @Test
    public void testInstantiation() {
        assertNotNull(mSelector, "Submit selector should be instantiatable");
    }

    @Test
    public void testExtendsTransformationSelector() {
        assertInstanceOf(
                TransformationSelector.class,
                mSelector,
                "Submit should extend TransformationSelector");
    }

    @Test
    public void testSelectsLocalStageableEntries() {
        List<TransformationCatalogEntry> entries = new ArrayList<>();
        TransformationCatalogEntry localStageable =
                new TransformationCatalogEntry("ns", "exe", "1.0");
        localStageable.setType(TCType.STAGEABLE);
        localStageable.setResourceId("local");
        entries.add(localStageable);

        List result = mSelector.getTCEntry(entries, "local");
        assertNotNull(result, "Should return a list for local stageable entries");
        assertEquals(1, result.size(), "Should return 1 local stageable entry");
    }

    @Test
    public void testFiltersOutNonLocalStageable() {
        List<TransformationCatalogEntry> entries = new ArrayList<>();
        TransformationCatalogEntry remoteStageable =
                new TransformationCatalogEntry("ns", "exe", "1.0");
        remoteStageable.setType(TCType.STAGEABLE);
        remoteStageable.setResourceId("remote-site");
        entries.add(remoteStageable);

        List result = mSelector.getTCEntry(entries, "remote-site");
        assertNull(result, "Should return null for non-local stageable entries");
    }

    @Test
    public void testFiltersOutInstalledEvenIfLocal() {
        List<TransformationCatalogEntry> entries = new ArrayList<>();
        TransformationCatalogEntry localInstalled =
                new TransformationCatalogEntry("ns", "exe", "1.0");
        localInstalled.setType(TCType.INSTALLED);
        localInstalled.setResourceId("local");
        entries.add(localInstalled);

        List result = mSelector.getTCEntry(entries, "local");
        assertNull(result, "Should filter out installed entries even from local site");
    }

    @Test
    public void testMixedEntries() {
        List<TransformationCatalogEntry> entries = new ArrayList<>();

        TransformationCatalogEntry localStageable =
                new TransformationCatalogEntry("ns", "exe", "1.0");
        localStageable.setType(TCType.STAGEABLE);
        localStageable.setResourceId("local");

        TransformationCatalogEntry remoteStageable =
                new TransformationCatalogEntry("ns", "exe", "1.0");
        remoteStageable.setType(TCType.STAGEABLE);
        remoteStageable.setResourceId("remote");

        TransformationCatalogEntry localInstalled =
                new TransformationCatalogEntry("ns", "exe", "1.0");
        localInstalled.setType(TCType.INSTALLED);
        localInstalled.setResourceId("local");

        entries.add(localStageable);
        entries.add(remoteStageable);
        entries.add(localInstalled);

        List result = mSelector.getTCEntry(entries, "site1");
        assertNotNull(result, "Should return the local stageable entry");
        assertEquals(1, result.size(), "Should return only 1 local stageable entry");
        assertEquals(
                "local",
                ((TransformationCatalogEntry) result.get(0)).getResourceId(),
                "Returned entry should be from local site");
    }

    @Test
    public void testEmptyList() {
        List<TransformationCatalogEntry> entries = new ArrayList<>();
        List result = mSelector.getTCEntry(entries, "site1");
        assertNull(result, "Should return null for empty input");
    }

    @Test
    public void testLocalCaseInsensitive() {
        List<TransformationCatalogEntry> entries = new ArrayList<>();
        // "LOCAL" in uppercase - should still match (uses equalsIgnoreCase)
        TransformationCatalogEntry localStageable =
                new TransformationCatalogEntry("ns", "exe", "1.0");
        localStageable.setType(TCType.STAGEABLE);
        localStageable.setResourceId("LOCAL");
        entries.add(localStageable);

        List result = mSelector.getTCEntry(entries, "site1");
        assertNotNull(result, "Should select LOCAL (uppercase) as it uses equalsIgnoreCase");
    }
}
