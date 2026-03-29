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

import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for SiteDataVisitor interface via a recording implementation.
 *
 * @author Rajiv Mayani
 */
public class SiteDataVisitorTest {

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

    @BeforeAll
    public static void setUpClass() {}

    @AfterAll
    public static void tearDownClass() {}

    @BeforeEach
    public void setUp() {}

    @AfterEach
    public void tearDown() {}

    @Test
    public void testSiteDataVisitorIsInterface() {
        assertTrue(SiteDataVisitor.class.isInterface());
    }

    @Test
    public void testRecordingVisitorVisitsSiteStore() throws IOException {
        RecordingVisitor rv = new RecordingVisitor();
        SiteStore store = new SiteStore();
        store.accept(rv);
        assertTrue(rv.visited.contains("visit:SiteStore"));
        assertTrue(rv.visited.contains("depart:SiteStore"));
    }

    @Test
    public void testRecordingVisitorVisitsFileServer() throws IOException {
        RecordingVisitor rv = new RecordingVisitor();
        FileServer fs = new FileServer("file", "file://", "/tmp");
        fs.accept(rv);
        assertTrue(rv.visited.contains("visit:FileServer"));
        assertTrue(rv.visited.contains("depart:FileServer"));
    }

    @Test
    public void testVisitorReceivesVisitBeforeDepart() throws IOException {
        RecordingVisitor rv = new RecordingVisitor();
        SiteStore store = new SiteStore();
        store.accept(rv);
        int visitIdx = rv.visited.indexOf("visit:SiteStore");
        int departIdx = rv.visited.indexOf("depart:SiteStore");
        assertTrue(visitIdx < departIdx, "visit should precede depart");
    }

    @Test
    public void testInitializeAcceptsWriter() {
        RecordingVisitor rv = new RecordingVisitor();
        // should not throw
        rv.initialize(new StringWriter());
    }
}
