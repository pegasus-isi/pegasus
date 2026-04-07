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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

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
        assertThat(mSelector, notNullValue());
    }

    @Test
    public void testExtendsTransformationSelector() {
        assertThat(mSelector, instanceOf(TransformationSelector.class));
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
        assertThat(result, notNullValue());
        assertThat(result.size(), equalTo(1));
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
        assertThat(result, nullValue());
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
        assertThat(result, nullValue());
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
        assertThat(result, notNullValue());
        assertThat(result.size(), equalTo(1));
        assertThat(((TransformationCatalogEntry) result.get(0)).getResourceId(), equalTo("local"));
    }

    @Test
    public void testEmptyList() {
        List<TransformationCatalogEntry> entries = new ArrayList<>();
        List result = mSelector.getTCEntry(entries, "site1");
        assertThat(result, nullValue());
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
        assertThat(result, notNullValue());
    }

    @Test
    public void testPreferredSiteDoesNotAffectLocalSelection() {
        List<TransformationCatalogEntry> entries = new ArrayList<>();
        TransformationCatalogEntry localStageable =
                new TransformationCatalogEntry("ns", "exe", "1.0");
        localStageable.setType(TCType.STAGEABLE);
        localStageable.setResourceId("local");
        entries.add(localStageable);

        List result = mSelector.getTCEntry(entries, "preferred-remote-site");

        assertThat(result, notNullValue());
        assertThat(result.size(), equalTo(1));
        assertThat(result.get(0), sameInstance(localStageable));
    }

    @Test
    public void testReturnedEntriesPreserveInputOrderForMultipleLocalStageables() {
        List<TransformationCatalogEntry> entries = new ArrayList<>();

        TransformationCatalogEntry first = new TransformationCatalogEntry("ns", "exe", "1.0");
        first.setType(TCType.STAGEABLE);
        first.setResourceId("local");

        TransformationCatalogEntry second = new TransformationCatalogEntry("ns", "exe", "1.0");
        second.setType(TCType.STAGEABLE);
        second.setResourceId("LOCAL");

        entries.add(first);
        entries.add(second);

        List result = mSelector.getTCEntry(entries, "ignored-site");

        assertThat(result.size(), equalTo(2));
        assertThat(result.get(0), sameInstance(first));
        assertThat(result.get(1), sameInstance(second));
    }

    @Test
    public void testDeclaredGetTCEntryMethodReturnType() throws Exception {
        assertThat(
                Submit.class.getMethod("getTCEntry", List.class, String.class).getReturnType(),
                equalTo(List.class));
    }
}
