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

/** Tests for the Staged transformation selector. */
public class StagedTest {

    private Staged mSelector;

    @BeforeEach
    public void setUp() {
        mSelector = new Staged();
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
    public void testSelectsStageableEntries() {
        List<TransformationCatalogEntry> entries = new ArrayList<>();
        TransformationCatalogEntry stageable = new TransformationCatalogEntry("ns", "exe", "1.0");
        stageable.setType(TCType.STAGEABLE);
        stageable.setResourceId("site1");
        entries.add(stageable);

        List result = mSelector.getTCEntry(entries, "site1");
        assertThat(result, notNullValue());
        assertThat(result.size(), equalTo(1));
    }

    @Test
    public void testFiltersOutInstalled() {
        List<TransformationCatalogEntry> entries = new ArrayList<>();
        TransformationCatalogEntry installed = new TransformationCatalogEntry("ns", "exe", "1.0");
        installed.setType(TCType.INSTALLED);
        installed.setResourceId("site1");
        entries.add(installed);

        List result = mSelector.getTCEntry(entries, "site1");
        assertThat(result, nullValue());
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
        assertThat(result, notNullValue());
        assertThat(result.size(), equalTo(2));
    }

    @Test
    public void testEmptyList() {
        List<TransformationCatalogEntry> entries = new ArrayList<>();
        List result = mSelector.getTCEntry(entries, "site1");
        assertThat(result, nullValue());
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
        assertThat(selected.getType(), equalTo(TCType.STAGEABLE));
    }

    @Test
    public void testPreferredSiteDoesNotFilterStageableEntries() {
        List<TransformationCatalogEntry> entries = new ArrayList<>();
        TransformationCatalogEntry stageable = new TransformationCatalogEntry("ns", "exe", "1.0");
        stageable.setType(TCType.STAGEABLE);
        stageable.setResourceId("other-site");
        entries.add(stageable);

        List result = mSelector.getTCEntry(entries, "preferred-site");

        assertThat(result, notNullValue());
        assertThat(result.size(), equalTo(1));
        assertThat(result.get(0), sameInstance(stageable));
    }

    @Test
    public void testReturnedEntriesPreserveInputOrder() {
        List<TransformationCatalogEntry> entries = new ArrayList<>();

        TransformationCatalogEntry first = new TransformationCatalogEntry("ns", "exe", "1.0");
        first.setType(TCType.STAGEABLE);
        first.setResourceId("site1");

        TransformationCatalogEntry second = new TransformationCatalogEntry("ns", "exe", "1.0");
        second.setType(TCType.STAGEABLE);
        second.setResourceId("site2");

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
                Staged.class.getMethod("getTCEntry", List.class, String.class).getReturnType(),
                equalTo(List.class));
    }
}
