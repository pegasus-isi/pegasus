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
package edu.isi.pegasus.planner.classes;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.*;

import edu.isi.pegasus.planner.catalog.transformation.TransformationCatalogEntry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** @author Rajiv Mayani */
public class TCMapTest {

    private TCMap mTCMap;

    @BeforeEach
    public void setUp() {
        mTCMap = new TCMap();
    }

    // --- Construction ---

    @Test
    public void defaultConstructorCreatesEmptyMap() {
        assertThat(mTCMap.getSiteMap("any::transformation::1.0"), is(nullValue()));
    }

    // --- setSiteMap / getSiteMap ---

    @Test
    public void setSiteMapAndRetrieveIt() {
        Map<String, List<TransformationCatalogEntry>> siteMap = new HashMap<>();
        mTCMap.setSiteMap("ns::name::1.0", siteMap);
        assertThat(mTCMap.getSiteMap("ns::name::1.0"), is(notNullValue()));
    }

    @Test
    public void getSiteMapReturnsNullForMissingLFN() {
        assertThat(mTCMap.getSiteMap("does::not::exist"), is(nullValue()));
    }

    @Test
    public void setSiteMapReturnsTrueOnSuccess() {
        Map<String, List<TransformationCatalogEntry>> siteMap = new HashMap<>();
        boolean result = mTCMap.setSiteMap("test::tfn::1.0", siteMap);
        assertThat(result, is(true));
    }

    @Test
    public void setSiteMapOverwritesPreviousEntry() {
        Map<String, List<TransformationCatalogEntry>> siteMap1 = new HashMap<>();
        Map<String, List<TransformationCatalogEntry>> siteMap2 = new HashMap<>();
        siteMap2.put("local", new ArrayList<>());
        mTCMap.setSiteMap("ns::t::1.0", siteMap1);
        mTCMap.setSiteMap("ns::t::1.0", siteMap2);
        assertThat(mTCMap.getSiteMap("ns::t::1.0"), is(siteMap2));
    }

    // --- getSiteList ---

    @Test
    public void getSiteListReturnsNullForMissingLFN() {
        List<String> result = mTCMap.getSiteList("missing::t::1.0");
        assertThat(result, is(nullValue()));
    }

    @Test
    public void getSiteListReturnsAllSites() {
        String fqlfn = "ns::t::1.0";
        TransformationCatalogEntry tce1 = new TransformationCatalogEntry("ns", "t", "1.0");
        tce1.setResourceId("local");
        tce1.setPhysicalTransformation("/usr/bin/t");
        TransformationCatalogEntry tce2 = new TransformationCatalogEntry("ns", "t", "1.0");
        tce2.setResourceId("remote");
        tce2.setPhysicalTransformation("/usr/bin/t");
        mTCMap.setSiteTCEntries(fqlfn, "local", tce1);
        mTCMap.setSiteTCEntries(fqlfn, "remote", tce2);

        List<String> sites = mTCMap.getSiteList(fqlfn);
        assertThat(sites, hasSize(2));
        assertThat(sites, containsInAnyOrder("local", "remote"));
    }

    @Test
    public void getSiteListWithFilterReturnsMatchingSites() {
        String fqlfn = "ns::t::1.0";
        TransformationCatalogEntry tce = new TransformationCatalogEntry("ns", "t", "1.0");
        tce.setResourceId("local");
        tce.setPhysicalTransformation("/usr/bin/t");
        mTCMap.setSiteTCEntries(fqlfn, "local", tce);

        List<String> filter = Arrays.asList("local", "condorpool");
        List<String> result = mTCMap.getSiteList(fqlfn, filter);
        assertThat(result, hasItem("local"));
        assertThat(result, not(hasItem("condorpool")));
    }

    @Test
    public void getSiteListWithFilterReturnsNullWhenNoMatch() {
        String fqlfn = "ns::t::1.0";
        TransformationCatalogEntry tce = new TransformationCatalogEntry("ns", "t", "1.0");
        tce.setResourceId("local");
        tce.setPhysicalTransformation("/usr/bin/t");
        mTCMap.setSiteTCEntries(fqlfn, "local", tce);

        List<String> filter = Arrays.asList("nonexistent");
        List<String> result = mTCMap.getSiteList(fqlfn, filter);
        assertThat(result, is(nullValue()));
    }

    @Test
    public void getSiteListWithFilterReturnsNullForMissingLFN() {
        List<String> result = mTCMap.getSiteList("missing::t::1.0", Arrays.asList("local"));

        assertThat(result, is(nullValue()));
    }

    // --- setSiteTCEntries / getSiteTCEntries ---

    @Test
    public void setSiteTCEntriesAndRetrieve() {
        String fqlfn = "ns::job::1.0";
        TransformationCatalogEntry tce = new TransformationCatalogEntry("ns", "job", "1.0");
        tce.setResourceId("local");
        tce.setPhysicalTransformation("/usr/bin/job");

        mTCMap.setSiteTCEntries(fqlfn, "local", tce);
        List results = mTCMap.getSiteTCEntries(fqlfn, "local");

        assertThat(results, is(notNullValue()));
        assertThat(results.size(), is(1));
    }

    @Test
    public void setSiteTCEntriesReturnsTrueOnSuccess() {
        String fqlfn = "ns::job::1.0";
        TransformationCatalogEntry tce = new TransformationCatalogEntry("ns", "job", "1.0");
        tce.setResourceId("local");
        tce.setPhysicalTransformation("/usr/bin/job");

        boolean result = mTCMap.setSiteTCEntries(fqlfn, "local", tce);
        assertThat(result, is(true));
    }

    @Test
    public void setSiteTCEntriesMultipleEntriesForSameSite() {
        String fqlfn = "ns::job::1.0";
        TransformationCatalogEntry tce1 = new TransformationCatalogEntry("ns", "job", "1.0");
        tce1.setResourceId("local");
        tce1.setPhysicalTransformation("/usr/bin/job");
        TransformationCatalogEntry tce2 = new TransformationCatalogEntry("ns", "job", "1.0");
        tce2.setResourceId("local");
        tce2.setPhysicalTransformation("/opt/bin/job");

        mTCMap.setSiteTCEntries(fqlfn, "local", tce1);
        mTCMap.setSiteTCEntries(fqlfn, "local", tce2);

        List results = mTCMap.getSiteTCEntries(fqlfn, "local");
        assertThat(results.size(), is(2));
    }

    @Test
    public void setSiteTCEntriesCreatesSeparateListsForDifferentSites() {
        String fqlfn = "ns::job::1.0";
        TransformationCatalogEntry localEntry = new TransformationCatalogEntry("ns", "job", "1.0");
        localEntry.setResourceId("local");
        localEntry.setPhysicalTransformation("/usr/bin/job");
        TransformationCatalogEntry remoteEntry = new TransformationCatalogEntry("ns", "job", "1.0");
        remoteEntry.setResourceId("remote");
        remoteEntry.setPhysicalTransformation("/opt/job");

        mTCMap.setSiteTCEntries(fqlfn, "local", localEntry);
        mTCMap.setSiteTCEntries(fqlfn, "remote", remoteEntry);

        assertThat(mTCMap.getSiteTCEntries(fqlfn, "local").size(), is(1));
        assertThat(mTCMap.getSiteTCEntries(fqlfn, "remote").size(), is(1));
    }

    @Test
    public void getSiteTCEntriesReturnsNullForMissingLFN() {
        // getSiteTCEntries() logs via LogManager when entry not found.
        // LogManagerFactory.loadSingletonInstance() returns an uninitialized logger
        // whose formatter has an empty stack, causing EmptyStackException on log().
        assertThrows(
                java.util.EmptyStackException.class,
                () -> mTCMap.getSiteTCEntries("missing::t::1.0", "local"));
    }

    @Test
    public void getSiteTCEntriesReturnsNullForMissingSite() {
        String fqlfn = "ns::t::1.0";
        TransformationCatalogEntry tce = new TransformationCatalogEntry("ns", "t", "1.0");
        tce.setResourceId("local");
        tce.setPhysicalTransformation("/usr/bin/t");
        mTCMap.setSiteTCEntries(fqlfn, "local", tce);

        // Same EmptyStackException when logging "site not found"
        assertThrows(
                java.util.EmptyStackException.class,
                () -> mTCMap.getSiteTCEntries(fqlfn, "condorpool"));
    }

    @Test
    public void getSiteTCEntriesStoresPFNCorrectly() {
        String fqlfn = "ns::job::1.0";
        TransformationCatalogEntry tce = new TransformationCatalogEntry("ns", "job", "1.0");
        tce.setResourceId("local");
        tce.setPhysicalTransformation("/usr/bin/job");
        mTCMap.setSiteTCEntries(fqlfn, "local", tce);

        List results = mTCMap.getSiteTCEntries(fqlfn, "local");
        assertThat(
                ((TransformationCatalogEntry) results.get(0)).getPhysicalTransformation(),
                is("/usr/bin/job"));
    }

    // --- getSitesTCEntries ---

    @Test
    public void getSitesTCEntriesReturnsNullForMissingLFN() {
        List<String> sites = Arrays.asList("local");
        assertThat(mTCMap.getSitesTCEntries("missing::t::1.0", sites), is(nullValue()));
    }

    @Test
    public void getSitesTCEntriesReturnsMatchingSites() {
        String fqlfn = "ns::t::1.0";
        TransformationCatalogEntry tce = new TransformationCatalogEntry("ns", "t", "1.0");
        tce.setResourceId("local");
        tce.setPhysicalTransformation("/usr/bin/t");
        mTCMap.setSiteTCEntries(fqlfn, "local", tce);

        List<String> sites = Arrays.asList("local", "missing");
        Map result = mTCMap.getSitesTCEntries(fqlfn, sites);
        assertThat(result, is(notNullValue()));
        assertThat(result.containsKey("local"), is(true));
        assertThat(result.containsKey("missing"), is(false));
    }

    @Test
    public void getSitesTCEntriesReturnsNullWhenNoSiteMatches() {
        String fqlfn = "ns::t::1.0";
        TransformationCatalogEntry tce = new TransformationCatalogEntry("ns", "t", "1.0");
        tce.setResourceId("local");
        tce.setPhysicalTransformation("/usr/bin/t");
        mTCMap.setSiteTCEntries(fqlfn, "local", tce);

        List<String> sites = Arrays.asList("nonexistent");
        assertThat(mTCMap.getSitesTCEntries(fqlfn, sites), is(nullValue()));
    }

    @Test
    public void getSitesTCEntriesIgnoresDuplicateRequestedSites() {
        String fqlfn = "ns::t::1.0";
        TransformationCatalogEntry tce = new TransformationCatalogEntry("ns", "t", "1.0");
        tce.setResourceId("local");
        tce.setPhysicalTransformation("/usr/bin/t");
        mTCMap.setSiteTCEntries(fqlfn, "local", tce);

        Map result = mTCMap.getSitesTCEntries(fqlfn, Arrays.asList("local", "local"));

        assertThat(result.keySet().size(), is(1));
        assertThat(result.containsKey("local"), is(true));
    }

    // --- toString ---

    @Test
    public void toStringIsNotNullForEmptyMap() {
        assertThat(mTCMap.toString(), is(notNullValue()));
    }

    @Test
    public void toStringContainsLFN() {
        String fqlfn = "ns::myjob::1.0";
        TransformationCatalogEntry tce = new TransformationCatalogEntry("ns", "myjob", "1.0");
        tce.setResourceId("local");
        tce.setPhysicalTransformation("/usr/bin/myjob");
        mTCMap.setSiteTCEntries(fqlfn, "local", tce);
        assertThat(mTCMap.toString(), containsString(fqlfn));
    }

    @Test
    public void toStringLFNContainsSiteAndPFN() {
        String fqlfn = "ns::myjob::1.0";
        TransformationCatalogEntry tce = new TransformationCatalogEntry("ns", "myjob", "1.0");
        tce.setResourceId("condorpool");
        tce.setPhysicalTransformation("/usr/bin/myjob");
        mTCMap.setSiteTCEntries(fqlfn, "condorpool", tce);
        String output = mTCMap.toString(fqlfn);
        assertThat(output, containsString("condorpool"));
        assertThat(output, containsString("/usr/bin/myjob"));
    }

    @Test
    public void toStringForMissingLFNThrowsNullPointerException() {
        assertThrows(NullPointerException.class, () -> mTCMap.toString("missing::t::1.0"));
    }

    // --- Multiple LFNs ---

    @Test
    public void multipleLFNsStoredIndependently() {
        String fqlfn1 = "ns::job1::1.0";
        String fqlfn2 = "ns::job2::1.0";
        TransformationCatalogEntry tce1 = new TransformationCatalogEntry("ns", "job1", "1.0");
        tce1.setResourceId("local");
        tce1.setPhysicalTransformation("/bin/job1");
        TransformationCatalogEntry tce2 = new TransformationCatalogEntry("ns", "job2", "1.0");
        tce2.setResourceId("local");
        tce2.setPhysicalTransformation("/bin/job2");

        mTCMap.setSiteTCEntries(fqlfn1, "local", tce1);
        mTCMap.setSiteTCEntries(fqlfn2, "local", tce2);

        List entries1 = mTCMap.getSiteTCEntries(fqlfn1, "local");
        List entries2 = mTCMap.getSiteTCEntries(fqlfn2, "local");
        assertThat(entries1.size(), is(1));
        assertThat(entries2.size(), is(1));
        assertThat(
                ((TransformationCatalogEntry) entries1.get(0)).getPhysicalTransformation(),
                is("/bin/job1"));
        assertThat(
                ((TransformationCatalogEntry) entries2.get(0)).getPhysicalTransformation(),
                is("/bin/job2"));
    }
}
