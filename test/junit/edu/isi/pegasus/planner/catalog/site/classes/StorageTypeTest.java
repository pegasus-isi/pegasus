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
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.jupiter.api.Assertions.*;

import java.io.StringWriter;
import java.util.Iterator;
import org.junit.jupiter.api.Test;

/**
 * Tests for StorageType via HeadNodeScratch (concrete subclass).
 *
 * @author Rajiv Mayani
 */
public class StorageTypeTest {

    @Test
    public void testDefaultConstructorCreatesNonNullLocalDirectory() {
        HeadNodeScratch scratch = new HeadNodeScratch();
        assertThat(scratch.getLocalDirectory(), is(org.hamcrest.Matchers.notNullValue()));
    }

    @Test
    public void testDefaultConstructorCreatesNonNullSharedDirectory() {
        HeadNodeScratch scratch = new HeadNodeScratch();
        assertThat(scratch.getSharedDirectory(), is(org.hamcrest.Matchers.notNullValue()));
    }

    @Test
    public void testSetAndGetLocalDirectory() {
        HeadNodeScratch scratch = new HeadNodeScratch();
        LocalDirectory local = new LocalDirectory();
        scratch.setLocalDirectory(local);
        assertThat(scratch.getLocalDirectory(), is(sameInstance(local)));
    }

    @Test
    public void testSetAndGetSharedDirectory() {
        HeadNodeScratch scratch = new HeadNodeScratch();
        SharedDirectory shared = new SharedDirectory();
        scratch.setSharedDirectory(shared);
        assertThat(scratch.getSharedDirectory(), is(sameInstance(shared)));
    }

    @Test
    public void testOverloadedConstructorSetsLocalAndShared() {
        LocalDirectory local = new LocalDirectory();
        SharedDirectory shared = new SharedDirectory();
        HeadNodeScratch scratch = new HeadNodeScratch(local, shared);
        assertThat(scratch.getLocalDirectory(), is(sameInstance(local)));
        assertThat(scratch.getSharedDirectory(), is(sameInstance(shared)));
    }

    @Test
    public void testCloneProducesDistinctDirectories() {
        HeadNodeScratch scratch = new HeadNodeScratch();
        HeadNodeScratch cloned = (HeadNodeScratch) scratch.clone();
        assertNotSame(scratch, cloned);
        assertNotSame(scratch.getLocalDirectory(), cloned.getLocalDirectory());
        assertNotSame(scratch.getSharedDirectory(), cloned.getSharedDirectory());
    }

    @Test
    public void testSetLocalDirectoryFromDirectoryCopiesMountPointAndFileServers() {
        HeadNodeScratch scratch = new HeadNodeScratch();
        Directory local = new Directory();
        local.setType(Directory.TYPE.local_scratch);
        local.setInternalMountPoint(new InternalMountPoint("/scratch/local"));
        FileServer getServer = new FileServer("file", "file://", "/scratch/local");
        getServer.setSupportedOperation(FileServer.OPERATION.get);
        local.addFileServer(getServer);

        scratch.setLocalDirectory(local);

        assertNotSame(local, scratch.getLocalDirectory());
        assertThat(
                scratch.getLocalDirectory().getInternalMountPoint().getMountPoint(),
                is("/scratch/local"));
        Iterator<FileServer> servers =
                scratch.getLocalDirectory().getFileServersIterator(FileServer.OPERATION.get);
        assertThat(servers.hasNext(), is(true));
        assertThat(servers.next(), is(sameInstance(getServer)));
        assertThat(servers.hasNext(), is(false));
    }

    @Test
    public void testSetSharedDirectoryFromDirectoryCopiesMountPointAndFileServers() {
        HeadNodeScratch scratch = new HeadNodeScratch();
        Directory shared = new Directory();
        shared.setType(Directory.TYPE.shared_storage);
        shared.setInternalMountPoint(new InternalMountPoint("/storage/shared"));
        FileServer putServer = new FileServer("file", "file://", "/storage/shared");
        putServer.setSupportedOperation(FileServer.OPERATION.put);
        shared.addFileServer(putServer);

        scratch.setSharedDirectory(shared);

        assertNotSame(shared, scratch.getSharedDirectory());
        assertThat(
                scratch.getSharedDirectory().getInternalMountPoint().getMountPoint(),
                is("/storage/shared"));
        Iterator<FileServer> servers =
                scratch.getSharedDirectory().getFileServersIterator(FileServer.OPERATION.put);
        assertThat(servers.hasNext(), is(true));
        assertThat(servers.next(), is(sameInstance(putServer)));
        assertThat(servers.hasNext(), is(false));
    }

    @Test
    public void testSetLocalDirectoryFromSharedTypeThrowsRuntimeException() {
        HeadNodeScratch scratch = new HeadNodeScratch();
        Directory shared = new Directory();
        shared.setType(Directory.TYPE.shared_scratch);

        RuntimeException exception =
                assertThrows(RuntimeException.class, () -> scratch.setLocalDirectory(shared));

        assertThat(
                exception.getMessage(),
                containsString("Invalid directory type associated with storage type"));
        assertThat(exception.getMessage(), containsString("shared-scratch"));
    }

    @Test
    public void testSetSharedDirectoryFromLocalTypeThrowsRuntimeException() {
        HeadNodeScratch scratch = new HeadNodeScratch();
        Directory local = new Directory();
        local.setType(Directory.TYPE.local_storage);

        RuntimeException exception =
                assertThrows(RuntimeException.class, () -> scratch.setSharedDirectory(local));

        assertThat(
                exception.getMessage(),
                containsString("Invalid directory type associated with storage type"));
        assertThat(exception.getMessage(), containsString("local-storage"));
    }

    @Test
    public void testCloneIsIndependentOfOriginalMutations() {
        HeadNodeScratch scratch = new HeadNodeScratch();
        LocalDirectory local = new LocalDirectory();
        local.setInternalMountPoint(new InternalMountPoint("/scratch/local"));
        local.addFileServer(new FileServer("file", "file://", "/scratch/local"));
        SharedDirectory shared = new SharedDirectory();
        shared.setInternalMountPoint(new InternalMountPoint("/storage/shared"));
        shared.addFileServer(new FileServer("http", "http://example.com", "/storage/shared"));
        scratch.setLocalDirectory(local);
        scratch.setSharedDirectory(shared);

        HeadNodeScratch cloned = (HeadNodeScratch) scratch.clone();

        scratch.getLocalDirectory().getInternalMountPoint().setMountPoint("/changed/local");
        scratch.getSharedDirectory().getInternalMountPoint().setMountPoint("/changed/shared");
        scratch.getLocalDirectory()
                .addFileServer(new FileServer("scp", "scp://example.com", "/later"));

        assertThat(
                cloned.getLocalDirectory().getInternalMountPoint().getMountPoint(),
                is("/scratch/local"));
        assertThat(
                cloned.getSharedDirectory().getInternalMountPoint().getMountPoint(),
                is("/storage/shared"));
        assertThat(
                count(cloned.getLocalDirectory().getFileServersIterator(FileServer.OPERATION.all)),
                is(1));
    }

    @Test
    public void testToXMLThrowsUnsupportedOperationException() {
        StorageType storage = new StorageType();

        UnsupportedOperationException exception =
                assertThrows(
                        UnsupportedOperationException.class,
                        () -> storage.toXML(new StringWriter(), ""));

        assertThat(exception.getMessage(), is("Not supported yet."));
    }

    @Test
    public void testAcceptThrowsUnsupportedOperationException() {
        StorageType storage = new StorageType();

        UnsupportedOperationException exception =
                assertThrows(
                        UnsupportedOperationException.class,
                        () -> storage.accept(new NoOpVisitor()));

        assertThat(exception.getMessage(), is("Not supported yet."));
    }

    private static int count(Iterator<FileServer> iterator) {
        int count = 0;
        while (iterator.hasNext()) {
            iterator.next();
            count++;
        }
        return count;
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
