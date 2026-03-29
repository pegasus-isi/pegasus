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

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** @author Rajiv Mayani */
public class ReplicaCatalogKeywordsTest {

    @BeforeAll
    public static void setUpClass() {}

    @AfterAll
    public static void tearDownClass() {}

    @BeforeEach
    public void setUp() {}

    @AfterEach
    public void tearDown() {}

    @Test
    public void testGetReservedNamePegasus() {
        assertEquals("pegasus", ReplicaCatalogKeywords.PEGASUS.getReservedName());
    }

    @Test
    public void testGetReservedNameReplicas() {
        assertEquals("replicas", ReplicaCatalogKeywords.REPLICAS.getReservedName());
    }

    @Test
    public void testGetReservedNameLfn() {
        assertEquals("lfn", ReplicaCatalogKeywords.LFN.getReservedName());
    }

    @Test
    public void testGetReservedNamePfns() {
        assertEquals("pfns", ReplicaCatalogKeywords.PFNS.getReservedName());
    }

    @Test
    public void testGetReservedNamePfn() {
        assertEquals("pfn", ReplicaCatalogKeywords.PFN.getReservedName());
    }

    @Test
    public void testGetReservedNameSite() {
        assertEquals("site", ReplicaCatalogKeywords.SITE.getReservedName());
    }

    @Test
    public void testGetReservedNameRegex() {
        assertEquals("regex", ReplicaCatalogKeywords.REGEX.getReservedName());
    }

    @Test
    public void testGetReservedNameMetadata() {
        assertEquals("metadata", ReplicaCatalogKeywords.METADATA.getReservedName());
    }

    @Test
    public void testGetReservedNameChecksum() {
        assertEquals("checksum", ReplicaCatalogKeywords.CHECKSUM.getReservedName());
    }

    @Test
    public void testGetReservedNameSha256() {
        assertEquals("sha256", ReplicaCatalogKeywords.SHA256.getReservedName());
    }

    @Test
    public void testGetReservedKeyLookupPegasus() {
        ReplicaCatalogKeywords kw = ReplicaCatalogKeywords.getReservedKey("pegasus");
        assertEquals(ReplicaCatalogKeywords.PEGASUS, kw);
    }

    @Test
    public void testGetReservedKeyLookupReplicas() {
        ReplicaCatalogKeywords kw = ReplicaCatalogKeywords.getReservedKey("replicas");
        assertEquals(ReplicaCatalogKeywords.REPLICAS, kw);
    }

    @Test
    public void testGetReservedKeyLookupLfn() {
        ReplicaCatalogKeywords kw = ReplicaCatalogKeywords.getReservedKey("lfn");
        assertEquals(ReplicaCatalogKeywords.LFN, kw);
    }

    @Test
    public void testGetReservedKeyLookupUnknownReturnsNull() {
        ReplicaCatalogKeywords kw = ReplicaCatalogKeywords.getReservedKey("unknown_key");
        assertNull(kw, "Unknown key lookup should return null");
    }

    @Test
    public void testGetReservedKeyLookupNullReturnsNull() {
        ReplicaCatalogKeywords kw = ReplicaCatalogKeywords.getReservedKey(null);
        assertNull(kw, "Null key lookup should return null");
    }

    @Test
    public void testAllKeywordsHaveDistinctReservedNames() {
        ReplicaCatalogKeywords[] values = ReplicaCatalogKeywords.values();
        long distinctCount =
                java.util.Arrays.stream(values)
                        .map(ReplicaCatalogKeywords::getReservedName)
                        .distinct()
                        .count();
        assertEquals(
                values.length, distinctCount, "All keywords should have unique reserved names");
    }

    @Test
    public void testEnumValuesCount() {
        assertEquals(10, ReplicaCatalogKeywords.values().length, "Should have 10 keyword entries");
    }
}
