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

/** Tests for the Random transformation selector. */
public class RandomTest {

    private Random mSelector;

    @BeforeEach
    public void setUp() {
        mSelector = new Random();
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
        assertThat(result, notNullValue());
        assertThat(result.size(), equalTo(1));
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
            assertThat(result.size(), equalTo(1));
            assertThat(
                    ((TransformationCatalogEntry) result.get(0)).getResourceId(),
                    equalTo("preferred"));
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
        assertThat(result, notNullValue());
        assertThat(result.size(), equalTo(1));
        assertThat(result.get(0), sameInstance(e1));
    }

    @Test
    public void testFallsBackToAnyWhenPreferredNotFound() {
        List<TransformationCatalogEntry> entries = new ArrayList<>();
        TransformationCatalogEntry e1 = new TransformationCatalogEntry("ns", "exe", "1.0");
        e1.setType(TCType.INSTALLED);
        e1.setResourceId("other-site");
        entries.add(e1);

        List result = mSelector.getTCEntry(entries, "preferred-site");
        assertThat(result, notNullValue());
        assertThat(result.size(), equalTo(1));
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
        assertThat(result.size(), equalTo(1));
    }

    @Test
    public void testSelectionRemainsWithinPreferredSiteSubset() {
        List<TransformationCatalogEntry> entries = new ArrayList<>();
        TransformationCatalogEntry preferredOne =
                new TransformationCatalogEntry("ns", "exe", "1.0");
        preferredOne.setType(TCType.INSTALLED);
        preferredOne.setResourceId("preferred");

        TransformationCatalogEntry preferredTwo =
                new TransformationCatalogEntry("ns", "exe", "1.0");
        preferredTwo.setType(TCType.INSTALLED);
        preferredTwo.setResourceId("preferred");

        TransformationCatalogEntry other = new TransformationCatalogEntry("ns", "exe", "1.0");
        other.setType(TCType.INSTALLED);
        other.setResourceId("other");

        entries.add(preferredOne);
        entries.add(preferredTwo);
        entries.add(other);

        for (int i = 0; i < 10; i++) {
            TransformationCatalogEntry selected =
                    (TransformationCatalogEntry) mSelector.getTCEntry(entries, "preferred").get(0);
            assertThat(selected.getResourceId(), equalTo("preferred"));
            assertThat(selected == preferredOne || selected == preferredTwo, is(true));
        }
    }

    @Test
    public void testReturnedListIsNewSingletonContainer() {
        List<TransformationCatalogEntry> entries = new ArrayList<>();
        TransformationCatalogEntry entry = new TransformationCatalogEntry("ns", "exe", "1.0");
        entry.setType(TCType.INSTALLED);
        entry.setResourceId("site1");
        entries.add(entry);

        List result = mSelector.getTCEntry(entries, "site1");

        assertThat(result, not(sameInstance(entries)));
        assertThat(result.size(), equalTo(1));
        assertThat(result.get(0), sameInstance(entry));
    }

    @Test
    public void testDeclaredGetTCEntryMethodReturnType() throws Exception {
        assertThat(
                Random.class.getMethod("getTCEntry", List.class, String.class).getReturnType(),
                equalTo(List.class));
    }
}
