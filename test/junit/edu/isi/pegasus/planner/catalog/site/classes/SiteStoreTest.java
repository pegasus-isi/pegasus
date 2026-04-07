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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.jupiter.api.Assertions.*;

import edu.isi.pegasus.planner.catalog.classes.SysInfo;
import java.io.File;
import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** @author Rajiv Mayani */
public class SiteStoreTest {

    private SiteStore store;

    @BeforeEach
    public void setUp() {
        store = new SiteStore();
    }

    @Test
    public void testDefaultConstructorIsEmpty() {
        assertThat(store.isEmpty(), is(true));
    }

    @Test
    public void testDefaultVersionIsSet() {
        assertThat(store.getVersion(), is(SiteStore.DEFAULT_SITE_CATALOG_VERSION));
    }

    @Test
    public void testAddEntryAndContains() {
        SiteCatalogEntry entry = new SiteCatalogEntry("local");
        store.addEntry(entry);
        assertThat(store.contains("local"), is(true));
    }

    @Test
    public void testLookupReturnsAddedEntry() {
        SiteCatalogEntry entry = new SiteCatalogEntry("condor_pool");
        store.addEntry(entry);
        SiteCatalogEntry found = store.lookup("condor_pool");
        assertThat(found, is(notNullValue()));
        assertThat(found.getSiteHandle(), is("condor_pool"));
    }

    @Test
    public void testLookupReturnsNullForMissingSite() {
        assertThat(store.lookup("nonexistent"), is(nullValue()));
    }

    @Test
    public void testListReturnsSiteHandles() {
        store.addEntry(new SiteCatalogEntry("local"));
        store.addEntry(new SiteCatalogEntry("remote"));
        Set<String> sites = store.list();
        assertThat(sites, hasItem("local"));
        assertThat(sites, hasItem("remote"));
    }

    @Test
    public void testSetAndGetVersion() {
        store.setVersion("5.0");
        assertThat(store.getVersion(), is("5.0"));
    }

    @Test
    public void testCloneProducesDistinctInstance() {
        store.addEntry(new SiteCatalogEntry("local"));
        SiteStore cloned = (SiteStore) store.clone();
        assertNotSame(store, cloned);
        assertThat(cloned.contains("local"), is(true));
    }

    @Test
    public void testContainsReturnsFalseForAbsentSite() {
        assertThat(store.contains("absent_site"), is(false));
    }

    @Test
    public void testIsEmptyFalseAfterAddingEntry() {
        store.addEntry(new SiteCatalogEntry("local"));
        assertThat(store.isEmpty(), is(false));
    }

    @Test
    public void testEntryIteratorReturnsAddedEntries() {
        store.addEntry(new SiteCatalogEntry("local"));
        store.addEntry(new SiteCatalogEntry("remote"));

        List<String> handles = new ArrayList<String>();
        for (Iterator<SiteCatalogEntry> it = store.entryIterator(); it.hasNext(); ) {
            handles.add(it.next().getSiteHandle());
        }

        assertThat(handles, hasSize(2));
        assertThat(handles, hasItem("local"));
        assertThat(handles, hasItem("remote"));
    }

    @Test
    public void testGetSysInfoReturnsNullForMissingSite() {
        assertThat(store.getSysInfo("missing"), is(nullValue()));
    }

    @Test
    public void testGetSysInfoReturnsValueForAddedSite() {
        SiteCatalogEntry entry = new SiteCatalogEntry("local");
        SysInfo sysInfo = new SysInfo();
        entry.setSysInfo(sysInfo);
        store.addEntry(entry);

        assertThat(store.getSysInfo("local"), is(sameInstance(sysInfo)));
    }

    @Test
    public void testSetAndGetFileSource() {
        File source = new File("/tmp/sites.yml");

        store.setFileSource(source);

        assertThat(store.getFileSource(), is(source));
    }

    @Test
    public void testToXMLWritesEntriesAndFooter() throws IOException {
        store.addEntry(new SiteCatalogEntry("local"));
        StringWriter sw = new StringWriter();

        store.toXML(sw, "");

        assertThat(sw.toString(), containsString("handle=\"local\""));
        assertThat(sw.toString(), containsString("</sitecatalog>"));
    }

    @Test
    public void testCloneIsIndependentOfOriginalMutations() {
        store.addEntry(new SiteCatalogEntry("local"));

        SiteStore cloned = (SiteStore) store.clone();
        store.addEntry(new SiteCatalogEntry("remote"));

        assertThat(cloned.contains("local"), is(true));
        assertThat(cloned.contains("remote"), is(false));
        assertThat(store.contains("remote"), is(true));
    }

    @Test
    public void testAcceptVisitsSiteStoreAndEntries() throws IOException {
        store.addEntry(new SiteCatalogEntry("local"));
        RecordingVisitor visitor = new RecordingVisitor();

        store.accept(visitor);

        assertThat(visitor.events.get(0), is("visit:SiteStore"));
        assertThat(visitor.events, hasItem("visit:SiteCatalogEntry"));
        assertThat(visitor.events, hasItem("depart:SiteCatalogEntry"));
        assertThat(visitor.events.get(visitor.events.size() - 1), is("depart:SiteStore"));
    }

    private static class RecordingVisitor implements SiteDataVisitor {
        private final List<String> events = new ArrayList<String>();

        @Override
        public void initialize(java.io.Writer writer) {}

        @Override
        public void visit(SiteStore entry) {
            events.add("visit:SiteStore");
        }

        @Override
        public void depart(SiteStore entry) {
            events.add("depart:SiteStore");
        }

        @Override
        public void visit(SiteCatalogEntry entry) {
            events.add("visit:SiteCatalogEntry");
        }

        @Override
        public void depart(SiteCatalogEntry entry) {
            events.add("depart:SiteCatalogEntry");
        }

        @Override
        public void visit(GridGateway entry) {}

        @Override
        public void depart(GridGateway entry) {}

        @Override
        public void visit(Directory directory) {}

        @Override
        public void depart(Directory directory) {}

        @Override
        public void visit(FileServer server) {}

        @Override
        public void depart(FileServer server) {}

        @Override
        public void visit(ReplicaCatalog catalog) {}

        @Override
        public void depart(ReplicaCatalog catalog) {}

        @Override
        public void visit(Connection c) {}

        @Override
        public void depart(Connection c) {}

        @Override
        public void visit(SiteData data) {}

        @Override
        public void depart(SiteData data) {}
    }
}
