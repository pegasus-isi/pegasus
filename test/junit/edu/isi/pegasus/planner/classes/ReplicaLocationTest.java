/**
 * Copyright 2007-2020 University Of Southern California
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
package edu.isi.pegasus.planner.classes;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertEquals;
import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import edu.isi.pegasus.planner.catalog.replica.ReplicaCatalogEntry;
import edu.isi.pegasus.planner.dax.PFN;
import edu.isi.pegasus.planner.test.DefaultTestSetup;
import edu.isi.pegasus.planner.test.TestSetup;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** @author Karan Vahi */
public class ReplicaLocationTest {

    private TestSetup mTestSetup;

    @BeforeEach
    public void setUp() {
        mTestSetup = new DefaultTestSetup();

        mTestSetup.setInputDirectory(this.getClass());
    }

    @AfterAll
    public static void tearDownClass() {}

    @AfterEach
    public void tearDown() {}

    @Test
    public void serializerReplicaLocation() throws IOException {
        ObjectMapper mapper =
                new ObjectMapper(
                        new YAMLFactory().configure(YAMLGenerator.Feature.INDENT_ARRAYS, true));
        mapper.configure(MapperFeature.ALLOW_COERCION_OF_SCALARS, false);
        ReplicaLocation rl = new ReplicaLocation();
        rl.setLFN("f.a");
        rl.addMetadata("foo", "bar");
        rl.addPFN(new PFN("file:///scratch/f.a").setSite("local"));
        String expected =
                "---\n"
                        + "lfn: \"f.a\"\n"
                        + "pfns:\n"
                        + " -\n"
                        + "  pfn: \"file:///scratch/f.a\"\n"
                        + "  site: \"local\"\n"
                        + "metadata:\n"
                        + "  foo: \"bar\"\n";
        assertEquals(expected, mapper.writeValueAsString(rl));
    }

    // --- Construction tests ---

    @Test
    public void defaultConstructorCreatesEmptyLFN() {
        ReplicaLocation rl = new ReplicaLocation();
        assertThat(rl.getLFN(), is(""));
    }

    @Test
    public void defaultConstructorHasZeroPFNs() {
        ReplicaLocation rl = new ReplicaLocation();
        assertThat(rl.getPFNCount(), is(0));
    }

    @Test
    public void defaultConstructorIsNotRegex() {
        ReplicaLocation rl = new ReplicaLocation();
        assertThat(rl.isRegex(), is(false));
    }

    @Test
    public void constructorWithLFNSetsLFN() {
        List<ReplicaCatalogEntry> pfns = new ArrayList<>();
        ReplicaLocation rl = new ReplicaLocation("input.txt", pfns);
        assertThat(rl.getLFN(), is("input.txt"));
    }

    @Test
    public void constructorWithLFNAndPFNsPopulatesList() {
        List<ReplicaCatalogEntry> pfns = new ArrayList<>();
        pfns.add(new ReplicaCatalogEntry("file:///data/input.txt", "local"));
        pfns.add(new ReplicaCatalogEntry("gsiftp://remote/input.txt", "remote"));
        ReplicaLocation rl = new ReplicaLocation("input.txt", pfns);
        assertThat(rl.getPFNCount(), is(2));
    }

    @Test
    public void copyConstructorCopiesLFN() {
        List<ReplicaCatalogEntry> pfns = new ArrayList<>();
        pfns.add(new ReplicaCatalogEntry("file:///data/input.txt", "local"));
        ReplicaLocation original = new ReplicaLocation("f1", pfns);
        ReplicaLocation copy = new ReplicaLocation(original);
        assertThat(copy.getLFN(), is("f1"));
    }

    @Test
    public void copyConstructorCopiesPFNs() {
        List<ReplicaCatalogEntry> pfns = new ArrayList<>();
        pfns.add(new ReplicaCatalogEntry("file:///data/input.txt", "local"));
        ReplicaLocation original = new ReplicaLocation("f1", pfns);
        ReplicaLocation copy = new ReplicaLocation(original);
        assertThat(copy.getPFNCount(), is(1));
    }

    // --- setLFN / getLFN ---

    @Test
    public void setLFNUpdatesLFN() {
        ReplicaLocation rl = new ReplicaLocation();
        rl.setLFN("myfile.txt");
        assertThat(rl.getLFN(), is("myfile.txt"));
    }

    // --- setRegex / isRegex ---

    @Test
    public void setRegexToTrueReturnsTrue() {
        ReplicaLocation rl = new ReplicaLocation();
        rl.setRegex(true);
        assertThat(rl.isRegex(), is(true));
    }

    @Test
    public void setRegexToFalseReturnsFalse() {
        ReplicaLocation rl = new ReplicaLocation();
        rl.setRegex(true);
        rl.setRegex(false);
        assertThat(rl.isRegex(), is(false));
    }

    // --- addPFN (ReplicaCatalogEntry) ---

    @Test
    public void addPFNIncrementsPFNCount() {
        ReplicaLocation rl = new ReplicaLocation();
        rl.setLFN("f1");
        ReplicaCatalogEntry rce = new ReplicaCatalogEntry("file:///tmp/f1", "local");
        rl.addPFN(rce);
        assertThat(rl.getPFNCount(), is(1));
    }

    @Test
    public void addPFNWithNullSiteSetsSiteToUndefined() {
        ReplicaLocation rl = new ReplicaLocation();
        rl.setLFN("f1");
        ReplicaCatalogEntry rce = new ReplicaCatalogEntry("file:///tmp/f1");
        rl.addPFN(rce);
        assertThat(rl.getPFN(0).getResourceHandle(), is(ReplicaLocation.UNDEFINED_SITE_NAME));
    }

    @Test
    public void addPFNDuplicateReplacesPreviousEntry() {
        ReplicaLocation rl = new ReplicaLocation();
        rl.setLFN("f1");
        ReplicaCatalogEntry rce1 = new ReplicaCatalogEntry("file:///tmp/f1", "local");
        ReplicaCatalogEntry rce2 = new ReplicaCatalogEntry("file:///tmp/f1", "local");
        rl.addPFN(rce1);
        rl.addPFN(rce2);
        // duplicate pfn+site pair should replace, so count stays 1
        assertThat(rl.getPFNCount(), is(1));
    }

    @Test
    public void addPFNSamePFNDifferentSiteKeepsBoth() {
        ReplicaLocation rl = new ReplicaLocation();
        rl.setLFN("f1");
        ReplicaCatalogEntry rce1 = new ReplicaCatalogEntry("file:///tmp/f1", "local");
        ReplicaCatalogEntry rce2 = new ReplicaCatalogEntry("file:///tmp/f1", "condorpool");
        rl.addPFN(rce1);
        rl.addPFN(rce2);
        assertThat(rl.getPFNCount(), is(2));
    }

    // --- addPFN (PFN dax object) ---

    @Test
    public void addPFNFromDaxPFNObject() {
        ReplicaLocation rl = new ReplicaLocation();
        rl.setLFN("f2");
        rl.addPFN(new PFN("gsiftp://remote/f2").setSite("remote"));
        assertThat(rl.getPFNCount(), is(1));
        assertThat(rl.getPFN(0).getPFN(), is("gsiftp://remote/f2"));
    }

    // --- addPFN (Collection) ---

    @Test
    public void addPFNCollectionAddsAll() {
        ReplicaLocation rl = new ReplicaLocation();
        rl.setLFN("f3");
        List<ReplicaCatalogEntry> entries = new ArrayList<>();
        entries.add(new ReplicaCatalogEntry("file:///a", "siteA"));
        entries.add(new ReplicaCatalogEntry("file:///b", "siteB"));
        rl.addPFN(entries);
        assertThat(rl.getPFNCount(), is(2));
    }

    // --- getPFN(index) ---

    @Test
    public void getPFNByIndexReturnsCorrectEntry() {
        List<ReplicaCatalogEntry> pfns = new ArrayList<>();
        pfns.add(new ReplicaCatalogEntry("file:///data/f.txt", "local"));
        ReplicaLocation rl = new ReplicaLocation("f.txt", pfns);
        assertThat(rl.getPFN(0).getPFN(), is("file:///data/f.txt"));
    }

    @Test
    public void getPFNByIndexThrowsForOutOfBounds() {
        ReplicaLocation rl = new ReplicaLocation();
        assertThrows(IndexOutOfBoundsException.class, () -> rl.getPFN(0));
    }

    // --- getPFNList ---

    @Test
    public void getPFNListReturnsAllEntries() {
        List<ReplicaCatalogEntry> pfns = new ArrayList<>();
        pfns.add(new ReplicaCatalogEntry("file:///a", "local"));
        pfns.add(new ReplicaCatalogEntry("file:///b", "condorpool"));
        ReplicaLocation rl = new ReplicaLocation("f", pfns);
        assertThat(rl.getPFNList(), hasSize(2));
    }

    // --- addMetadata / getMetadata ---

    @Test
    public void addAndGetMetadata() {
        ReplicaLocation rl = new ReplicaLocation();
        rl.addMetadata("owner", "pegasus");
        assertThat(rl.getMetadata("owner"), is("pegasus"));
    }

    @Test
    public void getMetadataForMissingKeyReturnsNull() {
        ReplicaLocation rl = new ReplicaLocation();
        assertThat(rl.getMetadata("nonexistent"), is(nullValue()));
    }

    @Test
    public void getAllMetadataReturnsNonNull() {
        ReplicaLocation rl = new ReplicaLocation();
        assertThat(rl.getAllMetadata(), is(notNullValue()));
    }

    // --- sanitize behavior ---

    @Test
    public void constructorSanitizesNullSiteHandle() {
        List<ReplicaCatalogEntry> pfns = new ArrayList<>();
        pfns.add(new ReplicaCatalogEntry("file:///tmp/f.txt"));
        ReplicaLocation rl = new ReplicaLocation("f.txt", pfns, true);
        assertThat(rl.getPFN(0).getResourceHandle(), is(ReplicaLocation.UNDEFINED_SITE_NAME));
    }

    @Test
    public void constructorWithoutSanitizeKeepsNullSite() {
        List<ReplicaCatalogEntry> pfns = new ArrayList<>();
        pfns.add(new ReplicaCatalogEntry("file:///tmp/f.txt"));
        ReplicaLocation rl = new ReplicaLocation("f.txt", pfns, false);
        assertThat(rl.getPFN(0).getResourceHandle(), is(nullValue()));
    }

    // --- clone ---

    @Test
    public void cloneProducesEqualLFN() {
        List<ReplicaCatalogEntry> pfns = new ArrayList<>();
        pfns.add(new ReplicaCatalogEntry("file:///tmp/f.txt", "local"));
        ReplicaLocation rl = new ReplicaLocation("f.txt", pfns);
        ReplicaLocation cloned = (ReplicaLocation) rl.clone();
        assertThat(cloned.getLFN(), is(rl.getLFN()));
    }

    @Test
    public void cloneProducesSamePFNCount() {
        List<ReplicaCatalogEntry> pfns = new ArrayList<>();
        pfns.add(new ReplicaCatalogEntry("file:///tmp/f.txt", "local"));
        pfns.add(new ReplicaCatalogEntry("gsiftp://remote/f.txt", "remote"));
        ReplicaLocation rl = new ReplicaLocation("f.txt", pfns);
        ReplicaLocation cloned = (ReplicaLocation) rl.clone();
        assertThat(cloned.getPFNCount(), is(rl.getPFNCount()));
    }

    @Test
    public void cloneProducesIndependentPFNList() {
        List<ReplicaCatalogEntry> pfns = new ArrayList<>();
        pfns.add(new ReplicaCatalogEntry("file:///tmp/f.txt", "local"));
        ReplicaLocation rl = new ReplicaLocation("f.txt", pfns);
        ReplicaLocation cloned = (ReplicaLocation) rl.clone();
        // Mutating clone should not affect original
        cloned.addPFN(new ReplicaCatalogEntry("gsiftp://extra/f.txt", "extra"));
        assertThat(rl.getPFNCount(), is(1));
        assertThat(cloned.getPFNCount(), is(2));
    }

    @Test
    public void cloneCopiesRegexFlag() {
        ReplicaLocation rl = new ReplicaLocation();
        rl.setLFN("f");
        rl.setRegex(true);
        ReplicaLocation cloned = (ReplicaLocation) rl.clone();
        assertThat(cloned.isRegex(), is(true));
    }

    // --- merge ---

    @Test
    public void mergeSameLFNReturnsPFNCount() {
        List<ReplicaCatalogEntry> pfns1 = new ArrayList<>();
        pfns1.add(new ReplicaCatalogEntry("file:///a", "local"));
        ReplicaLocation rl1 = new ReplicaLocation("f1", pfns1);

        List<ReplicaCatalogEntry> pfns2 = new ArrayList<>();
        pfns2.add(new ReplicaCatalogEntry("gsiftp://b/f1", "remote"));
        ReplicaLocation rl2 = new ReplicaLocation("f1", pfns2);

        int inserted = rl1.merge(rl2);
        assertThat(inserted, is(1));
    }

    @Test
    public void mergeSameLFNAddsPFNs() {
        List<ReplicaCatalogEntry> pfns1 = new ArrayList<>();
        pfns1.add(new ReplicaCatalogEntry("file:///a", "local"));
        ReplicaLocation rl1 = new ReplicaLocation("f1", pfns1);

        List<ReplicaCatalogEntry> pfns2 = new ArrayList<>();
        pfns2.add(new ReplicaCatalogEntry("gsiftp://b/f1", "remote"));
        ReplicaLocation rl2 = new ReplicaLocation("f1", pfns2);

        rl1.merge(rl2);
        assertThat(rl1.getPFNCount(), is(2));
    }

    @Test
    public void mergeDifferentLFNReturnsZero() {
        ReplicaLocation rl1 = new ReplicaLocation("f1", new ArrayList<>());
        ReplicaLocation rl2 = new ReplicaLocation("f2", new ArrayList<>());

        int inserted = rl1.merge(rl2);
        assertThat(inserted, is(0));
    }

    @Test
    public void mergeSameLFNWithOverlappingPFNReplacesEntry() {
        List<ReplicaCatalogEntry> pfns1 = new ArrayList<>();
        pfns1.add(new ReplicaCatalogEntry("file:///a", "local"));
        ReplicaLocation rl1 = new ReplicaLocation("f1", pfns1);

        List<ReplicaCatalogEntry> pfns2 = new ArrayList<>();
        pfns2.add(new ReplicaCatalogEntry("file:///a", "local"));
        ReplicaLocation rl2 = new ReplicaLocation("f1", pfns2);

        rl1.merge(rl2);
        // the old entry is removed and new one is inserted → still 1
        assertThat(rl1.getPFNCount(), is(1));
    }

    // --- toString ---

    @Test
    public void toStringContainsLFN() {
        ReplicaLocation rl = new ReplicaLocation();
        rl.setLFN("mylfn");
        assertThat(rl.toString(), containsString("mylfn"));
    }

    @Test
    public void toStringContainsRegexFlag() {
        ReplicaLocation rl = new ReplicaLocation();
        rl.setLFN("x");
        rl.setRegex(true);
        assertThat(rl.toString(), containsString("true"));
    }

    // --- UNDEFINED_SITE_NAME constant ---

    @Test
    public void undefinedSiteNameConstantValue() {
        assertThat(ReplicaLocation.UNDEFINED_SITE_NAME, is("UNDEFINED_SITE"));
    }
}
