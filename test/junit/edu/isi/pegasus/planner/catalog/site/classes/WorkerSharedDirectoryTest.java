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
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.io.StringWriter;
import java.util.Iterator;
import org.junit.jupiter.api.Test;

/** Tests for the WorkerSharedDirectory class. */
public class WorkerSharedDirectoryTest {

    @Test
    public void testWorkerSharedDirectoryExtendsDirectoryLayout() {
        assertThat(DirectoryLayout.class.isAssignableFrom(WorkerSharedDirectory.class), is(true));
    }

    @Test
    public void testDefaultConstructor() {
        WorkerSharedDirectory dir = new WorkerSharedDirectory();
        assertThat(dir, is(notNullValue()));
    }

    @Test
    public void testDefaultConstructorCreatesInternalMountPoint() {
        WorkerSharedDirectory dir = new WorkerSharedDirectory();
        assertThat(dir.getInternalMountPoint(), is(notNullValue()));
    }

    @Test
    public void testToXMLIncludesFileServerAndMountPoint() throws IOException {
        WorkerSharedDirectory dir = new WorkerSharedDirectory();
        dir.setInternalMountPoint(new InternalMountPoint("/worker/shared"));
        FileServer server = new FileServer("file", "file://", "/worker/shared");
        dir.addFileServer(server);
        StringWriter writer = new StringWriter();

        dir.toXML(writer, "");

        assertThat(writer.toString(), containsString("<wshared>"));
        assertThat(writer.toString(), containsString("<file-server"));
        assertThat(writer.toString(), containsString("/worker/shared"));
        assertThat(writer.toString(), containsString("</wshared>"));
    }

    @Test
    public void testCloneDeepCopiesMountPointAndFileServers() {
        WorkerSharedDirectory dir = new WorkerSharedDirectory();
        dir.setInternalMountPoint(new InternalMountPoint("/worker/shared"));
        dir.addFileServer(new FileServer("file", "file://", "/worker/shared"));

        WorkerSharedDirectory cloned = (WorkerSharedDirectory) dir.clone();

        assertNotSame(dir, cloned);
        assertNotSame(dir.getInternalMountPoint(), cloned.getInternalMountPoint());
        Iterator<FileServer> original = dir.getFileServersIterator(FileServer.OPERATION.all);
        Iterator<FileServer> copy = cloned.getFileServersIterator(FileServer.OPERATION.all);
        assertThat(original.hasNext(), is(true));
        assertThat(copy.hasNext(), is(true));
        assertNotSame(original.next(), copy.next());
    }

    @Test
    public void testCloneIsIndependentOfOriginalMutations() {
        WorkerSharedDirectory dir = new WorkerSharedDirectory();
        dir.setInternalMountPoint(new InternalMountPoint("/worker/shared"));

        WorkerSharedDirectory cloned = (WorkerSharedDirectory) dir.clone();

        dir.getInternalMountPoint().setMountPoint("/changed");

        assertThat(cloned.getInternalMountPoint().getMountPoint(), is("/worker/shared"));
    }

    @Test
    public void testAcceptThrowsUnsupportedOperationException() {
        WorkerSharedDirectory dir = new WorkerSharedDirectory();

        UnsupportedOperationException exception =
                assertThrows(
                        UnsupportedOperationException.class, () -> dir.accept(new NoOpVisitor()));

        assertThat(exception.getMessage(), is("Not supported yet."));
    }

    private static class NoOpVisitor implements SiteDataVisitor {
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
