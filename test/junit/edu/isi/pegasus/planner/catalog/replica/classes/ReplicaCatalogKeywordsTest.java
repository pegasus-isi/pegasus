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
package edu.isi.pegasus.planner.catalog.replica.classes;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

/** @author Rajiv Mayani */
public class ReplicaCatalogKeywordsTest {

    @Test
    public void testGetReservedNamePegasus() {
        assertThat(ReplicaCatalogKeywords.PEGASUS.getReservedName(), is("pegasus"));
    }

    @Test
    public void testGetReservedNameReplicas() {
        assertThat(ReplicaCatalogKeywords.REPLICAS.getReservedName(), is("replicas"));
    }

    @Test
    public void testGetReservedNameLfn() {
        assertThat(ReplicaCatalogKeywords.LFN.getReservedName(), is("lfn"));
    }

    @Test
    public void testGetReservedNamePfns() {
        assertThat(ReplicaCatalogKeywords.PFNS.getReservedName(), is("pfns"));
    }

    @Test
    public void testGetReservedNamePfn() {
        assertThat(ReplicaCatalogKeywords.PFN.getReservedName(), is("pfn"));
    }

    @Test
    public void testGetReservedNameSite() {
        assertThat(ReplicaCatalogKeywords.SITE.getReservedName(), is("site"));
    }

    @Test
    public void testGetReservedNameRegex() {
        assertThat(ReplicaCatalogKeywords.REGEX.getReservedName(), is("regex"));
    }

    @Test
    public void testGetReservedNameMetadata() {
        assertThat(ReplicaCatalogKeywords.METADATA.getReservedName(), is("metadata"));
    }

    @Test
    public void testGetReservedNameChecksum() {
        assertThat(ReplicaCatalogKeywords.CHECKSUM.getReservedName(), is("checksum"));
    }

    @Test
    public void testGetReservedNameSha256() {
        assertThat(ReplicaCatalogKeywords.SHA256.getReservedName(), is("sha256"));
    }

    @Test
    public void testGetReservedKeyLookupPegasus() {
        ReplicaCatalogKeywords kw = ReplicaCatalogKeywords.getReservedKey("pegasus");
        assertThat(kw, is(ReplicaCatalogKeywords.PEGASUS));
    }

    @Test
    public void testGetReservedKeyLookupReplicas() {
        ReplicaCatalogKeywords kw = ReplicaCatalogKeywords.getReservedKey("replicas");
        assertThat(kw, is(ReplicaCatalogKeywords.REPLICAS));
    }

    @Test
    public void testGetReservedKeyLookupLfn() {
        ReplicaCatalogKeywords kw = ReplicaCatalogKeywords.getReservedKey("lfn");
        assertThat(kw, is(ReplicaCatalogKeywords.LFN));
    }

    @Test
    public void testGetReservedKeyLookupUnknownReturnsNull() {
        ReplicaCatalogKeywords kw = ReplicaCatalogKeywords.getReservedKey("unknown_key");
        assertThat(kw, is(nullValue()));
    }

    @Test
    public void testGetReservedKeyLookupNullReturnsNull() {
        ReplicaCatalogKeywords kw = ReplicaCatalogKeywords.getReservedKey(null);
        assertThat(kw, is(nullValue()));
    }

    @Test
    public void testAllKeywordsHaveDistinctReservedNames() {
        ReplicaCatalogKeywords[] values = ReplicaCatalogKeywords.values();
        long distinctCount =
                java.util.Arrays.stream(values)
                        .map(ReplicaCatalogKeywords::getReservedName)
                        .distinct()
                        .count();
        assertThat(distinctCount, is((long) values.length));
    }

    @Test
    public void testEnumValuesCount() {
        assertThat(ReplicaCatalogKeywords.values().length, is(10));
    }

    @Test
    public void testEveryKeywordRoundTripsThroughReservedLookup() {
        for (ReplicaCatalogKeywords keyword : ReplicaCatalogKeywords.values()) {
            assertThat(
                    ReplicaCatalogKeywords.getReservedKey(keyword.getReservedName()), is(keyword));
        }
    }

    @Test
    public void testReservedLookupIsCaseSensitive() {
        assertThat(ReplicaCatalogKeywords.getReservedKey("PEGASUS"), is(nullValue()));
        assertThat(ReplicaCatalogKeywords.getReservedKey("Replicas"), is(nullValue()));
        assertThat(ReplicaCatalogKeywords.getReservedKey("Sha256"), is(nullValue()));
    }

    @Test
    public void testValueOfStillResolvesEnumConstantNames() {
        assertThat(ReplicaCatalogKeywords.valueOf("PEGASUS"), is(ReplicaCatalogKeywords.PEGASUS));
        assertThat(ReplicaCatalogKeywords.valueOf("REPLICAS"), is(ReplicaCatalogKeywords.REPLICAS));
        assertThat(ReplicaCatalogKeywords.valueOf("SHA256"), is(ReplicaCatalogKeywords.SHA256));
    }
}
