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
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Tests for SiteDataVisitor interface via a recording implementation.
 *
 * @author Rajiv Mayani
 */
public class SiteDataVisitorTest {

    private static class MinimalSiteData extends SiteData {}

    /** A visitor that records which objects were visited. */
    private static class RecordingVisitor implements SiteDataVisitor {
        final List<String> visited = new ArrayList<>();

        @Override
        public void initialize(java.io.Writer writer) {}

        @Override
        public void visit(SiteStore entry) {
            visited.add("visit:SiteStore");
        }

        @Override
        public void depart(SiteStore entry) {
            visited.add("depart:SiteStore");
        }

        @Override
        public void visit(SiteCatalogEntry entry) {
            visited.add("visit:SiteCatalogEntry");
        }

        @Override
        public void depart(SiteCatalogEntry entry) {
            visited.add("depart:SiteCatalogEntry");
        }

        @Override
        public void visit(GridGateway entry) {
            visited.add("visit:GridGateway");
        }

        @Override
        public void depart(GridGateway entry) {
            visited.add("depart:GridGateway");
        }

        @Override
        public void visit(Directory directory) {
            visited.add("visit:Directory");
        }

        @Override
        public void depart(Directory directory) {
            visited.add("depart:Directory");
        }

        @Override
        public void visit(FileServer server) {
            visited.add("visit:FileServer");
        }

        @Override
        public void depart(FileServer server) {
            visited.add("depart:FileServer");
        }

        @Override
        public void visit(ReplicaCatalog catalog) {
            visited.add("visit:ReplicaCatalog");
        }

        @Override
        public void depart(ReplicaCatalog catalog) {
            visited.add("depart:ReplicaCatalog");
        }

        @Override
        public void visit(Connection c) {
            visited.add("visit:Connection");
        }

        @Override
        public void depart(Connection c) {
            visited.add("depart:Connection");
        }

        @Override
        public void visit(SiteData data) {
            visited.add("visit:SiteData");
        }

        @Override
        public void depart(SiteData data) {
            visited.add("depart:SiteData");
        }
    }

    @Test
    public void testRecordingVisitorVisitsSiteStore() throws IOException {
        RecordingVisitor rv = new RecordingVisitor();
        SiteStore store = new SiteStore();
        store.accept(rv);
        assertThat(rv.visited, hasItem("visit:SiteStore"));
        assertThat(rv.visited, hasItem("depart:SiteStore"));
    }

    @Test
    public void testRecordingVisitorVisitsFileServer() throws IOException {
        RecordingVisitor rv = new RecordingVisitor();
        FileServer fs = new FileServer("file", "file://", "/tmp");
        fs.accept(rv);
        assertThat(rv.visited, hasItem("visit:FileServer"));
        assertThat(rv.visited, hasItem("depart:FileServer"));
    }

    @Test
    public void testVisitorReceivesVisitBeforeDepart() throws IOException {
        RecordingVisitor rv = new RecordingVisitor();
        SiteStore store = new SiteStore();
        store.accept(rv);
        int visitIdx = rv.visited.indexOf("visit:SiteStore");
        int departIdx = rv.visited.indexOf("depart:SiteStore");
        assertThat(visitIdx < departIdx, is(true));
    }

    @Test
    public void testInitializeAcceptsWriter() {
        RecordingVisitor rv = new RecordingVisitor();
        // should not throw
        rv.initialize(new StringWriter());
    }

    @Test
    public void testRecordingVisitorVisitsReplicaCatalogDirectly() throws IOException {
        RecordingVisitor rv = new RecordingVisitor();

        rv.visit(new ReplicaCatalog());
        rv.depart(new ReplicaCatalog());

        assertThat(rv.visited, hasItem("visit:ReplicaCatalog"));
        assertThat(rv.visited, hasItem("depart:ReplicaCatalog"));
    }

    @Test
    public void testRecordingVisitorVisitsConnectionDirectly() throws IOException {
        RecordingVisitor rv = new RecordingVisitor();

        rv.visit(new Connection("user", "alice"));
        rv.depart(new Connection("user", "alice"));

        assertThat(rv.visited, hasItem("visit:Connection"));
        assertThat(rv.visited, hasItem("depart:Connection"));
    }

    @Test
    public void testRecordingVisitorVisitsGenericSiteDataDirectly() throws IOException {
        RecordingVisitor rv = new RecordingVisitor();
        MinimalSiteData data = new MinimalSiteData();

        rv.visit(data);
        rv.depart(data);

        assertThat(rv.visited.get(0), is("visit:SiteData"));
        assertThat(rv.visited.get(1), is("depart:SiteData"));
    }

    @Test
    public void testSiteDataVisitorIsInterface() {
        assertThat(SiteDataVisitor.class.isInterface(), is(true));
    }
}
