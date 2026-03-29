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

import java.util.Set;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** @author Rajiv Mayani */
public class SiteStoreTest {

    private SiteStore store;

    @BeforeAll
    public static void setUpClass() {}

    @AfterAll
    public static void tearDownClass() {}

    @BeforeEach
    public void setUp() {
        store = new SiteStore();
    }

    @AfterEach
    public void tearDown() {}

    @Test
    public void testDefaultConstructorIsEmpty() {
        assertTrue(store.isEmpty());
    }

    @Test
    public void testDefaultVersionIsSet() {
        assertEquals(SiteStore.DEFAULT_SITE_CATALOG_VERSION, store.getVersion());
    }

    @Test
    public void testAddEntryAndContains() {
        SiteCatalogEntry entry = new SiteCatalogEntry("local");
        store.addEntry(entry);
        assertTrue(store.contains("local"));
    }

    @Test
    public void testLookupReturnsAddedEntry() {
        SiteCatalogEntry entry = new SiteCatalogEntry("condor_pool");
        store.addEntry(entry);
        SiteCatalogEntry found = store.lookup("condor_pool");
        assertNotNull(found);
        assertEquals("condor_pool", found.getSiteHandle());
    }

    @Test
    public void testLookupReturnsNullForMissingSite() {
        assertNull(store.lookup("nonexistent"));
    }

    @Test
    public void testListReturnsSiteHandles() {
        store.addEntry(new SiteCatalogEntry("local"));
        store.addEntry(new SiteCatalogEntry("remote"));
        Set<String> sites = store.list();
        assertTrue(sites.contains("local"));
        assertTrue(sites.contains("remote"));
    }

    @Test
    public void testSetAndGetVersion() {
        store.setVersion("5.0");
        assertEquals("5.0", store.getVersion());
    }

    @Test
    public void testCloneProducesDistinctInstance() {
        store.addEntry(new SiteCatalogEntry("local"));
        SiteStore cloned = (SiteStore) store.clone();
        assertNotSame(store, cloned);
        assertTrue(cloned.contains("local"));
    }

    @Test
    public void testContainsReturnsFalseForAbsentSite() {
        assertFalse(store.contains("absent_site"));
    }

    @Test
    public void testIsEmptyFalseAfterAddingEntry() {
        store.addEntry(new SiteCatalogEntry("local"));
        assertFalse(store.isEmpty());
    }
}
