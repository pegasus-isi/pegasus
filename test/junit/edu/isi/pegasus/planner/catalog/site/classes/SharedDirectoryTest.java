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
public class SharedDirectoryTest {

    @Test
    public void testDefaultConstructorIsEmpty() {
        SharedDirectory dir = new SharedDirectory();
        assertThat(dir.isEmpty(), is(true));
    }

    @Test
    public void testSetAndGetInternalMountPoint() {
        SharedDirectory dir = new SharedDirectory();
        InternalMountPoint mp = new InternalMountPoint("/lustre");
        dir.setInternalMountPoint(mp);
        assertThat(dir.getInternalMountPoint().getMountPoint(), is("/lustre"));
    }

    @Test
    public void testAddFileServerMakesNotEmpty() {
        SharedDirectory dir = new SharedDirectory();
        FileServer fs = new FileServer("gsiftp", "gsiftp://site.edu", "/lustre");
        fs.setSupportedOperation(FileServer.OPERATION.all);
        dir.addFileServer(fs);
        assertThat(dir.isEmpty(), is(false));
    }

    @Test
    public void testToXMLIsEmptyWhenNoContent() throws IOException {
        SharedDirectory dir = new SharedDirectory();
        StringWriter sw = new StringWriter();
        dir.toXML(sw, "");
        assertThat(sw.toString(), is(""));
    }

    @Test
    public void testToXMLContainsSharedElementWhenNotEmpty() throws IOException {
        SharedDirectory dir = new SharedDirectory();
        InternalMountPoint mp = new InternalMountPoint("/shared/scratch");
        dir.setInternalMountPoint(mp);
        FileServer fs = new FileServer("gsiftp", "gsiftp://site.edu", "/shared/scratch");
        fs.setSupportedOperation(FileServer.OPERATION.all);
        dir.addFileServer(fs);
        StringWriter sw = new StringWriter();
        dir.toXML(sw, "");
        String xml = sw.toString();
        assertThat(xml, containsString("<shared>"));
        assertThat(xml, containsString("</shared>"));
    }

    @Test
    public void testToXMLIncludesFileServerAndInternalMountPoint() throws IOException {
        SharedDirectory dir = new SharedDirectory();
        dir.setInternalMountPoint(new InternalMountPoint("/shared/scratch"));
        FileServer fs = new FileServer("gsiftp", "gsiftp://site.edu", "/shared/scratch");
        fs.setSupportedOperation(FileServer.OPERATION.put);
        dir.addFileServer(fs);
        StringWriter sw = new StringWriter();

        dir.toXML(sw, "");

        assertThat(sw.toString(), containsString("<file-server"));
        assertThat(sw.toString(), containsString("mount-point=\"/shared/scratch\""));
        assertThat(sw.toString(), containsString("<internal-mount-point"));
    }

    @Test
    public void testHasFileServerForPUTWithPutOperation() {
        SharedDirectory dir = new SharedDirectory();
        FileServer fs = new FileServer("gsiftp", "gsiftp://site.edu", "/data");
        fs.setSupportedOperation(FileServer.OPERATION.put);
        dir.addFileServer(fs);
        assertThat(dir.hasFileServerForPUTOperations(), is(true));
    }

    @Test
    public void testCloneProducesDistinctInstance() {
        SharedDirectory dir = new SharedDirectory();
        InternalMountPoint mp = new InternalMountPoint("/shared");
        dir.setInternalMountPoint(mp);
        SharedDirectory cloned = (SharedDirectory) dir.clone();
        assertNotSame(dir, cloned);
        assertThat(
                cloned.getInternalMountPoint().getMountPoint(),
                is(dir.getInternalMountPoint().getMountPoint()));
    }

    @Test
    public void testCloneIsIndependentOfOriginalMutations() {
        SharedDirectory dir = new SharedDirectory();
        dir.setInternalMountPoint(new InternalMountPoint("/shared"));
        FileServer fs = new FileServer("gsiftp", "gsiftp://site.edu", "/shared");
        fs.setSupportedOperation(FileServer.OPERATION.all);
        dir.addFileServer(fs);

        SharedDirectory cloned = (SharedDirectory) dir.clone();

        dir.getInternalMountPoint().setMountPoint("/changed");
        dir.getFileServers(FileServer.OPERATION.all).get(0).setMountPoint("/changed");

        assertThat(cloned.getInternalMountPoint().getMountPoint(), is("/shared"));
        assertThat(
                cloned.getFileServers(FileServer.OPERATION.all).get(0).getMountPoint(),
                is("/shared"));
    }

    @Test
    public void testAcceptVisitsContainedFileServers() throws IOException {
        SharedDirectory dir = new SharedDirectory();
        FileServer fs = new FileServer("gsiftp", "gsiftp://site.edu", "/shared");
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
