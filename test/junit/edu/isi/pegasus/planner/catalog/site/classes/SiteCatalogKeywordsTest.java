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
package edu.isi.pegasus.planner.catalog.site.classes;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** @author Rajiv Mayani */
public class SiteCatalogKeywordsTest {

    @BeforeAll
    public static void setUpClass() {}

    @AfterAll
    public static void tearDownClass() {}

    @BeforeEach
    public void setUp() {}

    @AfterEach
    public void tearDown() {}

    @Test
    public void testProfilesKeyReservedName() {
        assertEquals("profiles", SiteCatalogKeywords.PROFILES.getReservedName());
    }

    @Test
    public void testSitesKeyReservedName() {
        assertEquals("sites", SiteCatalogKeywords.SITES.getReservedName());
    }

    @Test
    public void testNameKeyReservedName() {
        assertEquals("name", SiteCatalogKeywords.NAME.getReservedName());
    }

    @Test
    public void testPegasusKeyReservedName() {
        assertEquals("pegasus", SiteCatalogKeywords.PEGASUS.getReservedName());
    }

    @Test
    public void testGetReservedKeyReturnsSitesForSitesString() {
        SiteCatalogKeywords kw = SiteCatalogKeywords.getReservedKey("sites");
        assertEquals(SiteCatalogKeywords.SITES, kw);
    }

    @Test
    public void testGetReservedKeyReturnsNullForUnknownKey() {
        assertNull(SiteCatalogKeywords.getReservedKey("unknownKey"));
    }

    @Test
    public void testValueOfSites() {
        assertEquals(SiteCatalogKeywords.SITES, SiteCatalogKeywords.valueOf("SITES"));
    }

    @Test
    public void testAllKeywordsHaveNonNullReservedNames() {
        for (SiteCatalogKeywords kw : SiteCatalogKeywords.values()) {
            assertNotNull(kw.getReservedName(), "Reserved name should not be null for " + kw);
        }
    }

    @Test
    public void testGetReservedKeyForOperation() {
        assertEquals(
                SiteCatalogKeywords.OPERATION, SiteCatalogKeywords.getReservedKey("operation"));
    }

    @Test
    public void testGetReservedKeyForFileServers() {
        assertEquals(
                SiteCatalogKeywords.FILESERVERS, SiteCatalogKeywords.getReservedKey("fileServers"));
    }
}
