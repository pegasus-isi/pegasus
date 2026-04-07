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
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/** @author Rajiv Mayani */
public class LocalDirectoryTest {

    @Test
    public void testDefaultConstructorIsEmpty() {
        LocalDirectory dir = new LocalDirectory();
        assertThat(dir.isEmpty(), is(true));
    }

    @Test
    public void testSetAndGetInternalMountPoint() {
        LocalDirectory dir = new LocalDirectory();
        InternalMountPoint mp = new InternalMountPoint("/tmp/local");
        dir.setInternalMountPoint(mp);
        assertThat(dir.getInternalMountPoint().getMountPoint(), is("/tmp/local"));
    }

    @Test
    public void testAddFileServerMakesNotEmpty() {
        LocalDirectory dir = new LocalDirectory();
        FileServer fs = new FileServer("file", "file://", "/tmp");
        fs.setSupportedOperation(FileServer.OPERATION.all);
        dir.addFileServer(fs);
        assertThat(dir.isEmpty(), is(false));
    }

    @Test
    public void testToXMLIsEmptyWhenNoContent() throws IOException {
        LocalDirectory dir = new LocalDirectory();
        StringWriter sw = new StringWriter();
        dir.toXML(sw, "");
        assertThat(sw.toString(), is(""));
    }

    @Test
    public void testToXMLContainsLocalElementWhenNotEmpty() throws IOException {
        LocalDirectory dir = new LocalDirectory();
        InternalMountPoint mp = new InternalMountPoint("/local/scratch");
        dir.setInternalMountPoint(mp);
        FileServer fs = new FileServer("file", "file://", "/local/scratch");
        fs.setSupportedOperation(FileServer.OPERATION.all);
        dir.addFileServer(fs);
        StringWriter sw = new StringWriter();
        dir.toXML(sw, "");
        String xml = sw.toString();
        assertThat(xml, containsString("<local>"));
        assertThat(xml, containsString("</local>"));
    }

    @Test
    public void testToXMLIncludesFileServerAndInternalMountPoint() throws IOException {
        LocalDirectory dir = new LocalDirectory();
        dir.setInternalMountPoint(new InternalMountPoint("/local/scratch"));
        FileServer fs = new FileServer("file", "file://", "/local/scratch");
        fs.setSupportedOperation(FileServer.OPERATION.get);
        dir.addFileServer(fs);
        StringWriter sw = new StringWriter();

        dir.toXML(sw, "");

        assertThat(sw.toString(), containsString("<file-server"));
        assertThat(sw.toString(), containsString("mount-point=\"/local/scratch\""));
        assertThat(sw.toString(), containsString("<internal-mount-point"));
    }

    @Test
    public void testCloneProducesDistinctInstance() {
        LocalDirectory dir = new LocalDirectory();
        InternalMountPoint mp = new InternalMountPoint("/local");
        dir.setInternalMountPoint(mp);
        LocalDirectory cloned = (LocalDirectory) dir.clone();
        assertNotSame(dir, cloned);
        assertThat(
                cloned.getInternalMountPoint().getMountPoint(),
                is(dir.getInternalMountPoint().getMountPoint()));
    }

    @Test
    public void testCloneIsIndependentOfOriginalMutations() {
        LocalDirectory dir = new LocalDirectory();
        dir.setInternalMountPoint(new InternalMountPoint("/local"));
        FileServer fs = new FileServer("file", "file://", "/local");
        fs.setSupportedOperation(FileServer.OPERATION.all);
        dir.addFileServer(fs);

        LocalDirectory cloned = (LocalDirectory) dir.clone();

        dir.getInternalMountPoint().setMountPoint("/changed");
        dir.getFileServers(FileServer.OPERATION.all).get(0).setMountPoint("/changed");

        assertThat(cloned.getInternalMountPoint().getMountPoint(), is("/local"));
        assertThat(
                cloned.getFileServers(FileServer.OPERATION.all).get(0).getMountPoint(),
                is("/local"));
    }

    @Test
    public void testHasFileServerForGETWithAllOperation() {
        LocalDirectory dir = new LocalDirectory();
        FileServer fs = new FileServer("gsiftp", "gsiftp://site.edu", "/data");
        fs.setSupportedOperation(FileServer.OPERATION.all);
        dir.addFileServer(fs);
        assertThat(dir.hasFileServerForGETOperations(), is(true));
    }

    @Test
    public void testAcceptVisitsContainedFileServers() throws IOException {
        LocalDirectory dir = new LocalDirectory();
        FileServer fs = new FileServer("file", "file://", "/local");
        fs.setSupportedOperation(FileServer.OPERATION.all);
        dir.addFileServer(fs);
        RecordingVisitor visitor = new RecordingVisitor();

        dir.accept(visitor);

        assertThat(visitor.events.contains("visit:Directory"), is(false));
        assertThat(visitor.events.contains("depart:Directory"), is(false));
        assertThat(visitor.events, hasItem("visit:FileServer"));
        assertThat(visitor.events, hasItem("depart:FileServer"));
        assertThat(visitor.events.size(), is(2));
    }

    private static class RecordingVisitor implements SiteDataVisitor {
        private final List<String> events = new ArrayList<String>();

        @Override
        public void initialize(java.io.Writer writer) {}

        @Override
        public void visit(SiteStore entry) {}

        @Override
        public void depart(SiteStore entry) {}

        @Override
        public void visit(SiteCatalogEntry entry) {}

        @Override
        public void depart(SiteCatalogEntry entry) {}

        @Override
        public void visit(GridGateway entry) {}

        @Override
        public void depart(GridGateway entry) {}

        @Override
        public void visit(Directory directory) {
            events.add("visit:Directory");
        }

        @Override
        public void depart(Directory directory) {
            events.add("depart:Directory");
        }

        @Override
        public void visit(FileServer server) {
            events.add("visit:FileServer");
        }

        @Override
        public void depart(FileServer server) {
            events.add("depart:FileServer");
        }

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
