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
import org.junit.jupiter.api.Test;

/** @author Rajiv Mayani */
public class HeadNodeStorageTest {

    @Test
    public void testDefaultConstructorCreatesNonNullDirectories() {
        HeadNodeStorage storage = new HeadNodeStorage();
        assertThat(storage.getLocalDirectory(), is(notNullValue()));
        assertThat(storage.getSharedDirectory(), is(notNullValue()));
    }

    @Test
    public void testOverloadedConstructorWithLocalAndShared() {
        LocalDirectory local = new LocalDirectory();
        SharedDirectory shared = new SharedDirectory();
        HeadNodeStorage storage = new HeadNodeStorage(local, shared);
        assertThat(storage.getLocalDirectory(), is(local));
        assertThat(storage.getSharedDirectory(), is(shared));
    }

    @Test
    public void testOverloadedConstructorWithStorageTypeUsesSameDirectories() {
        LocalDirectory local = new LocalDirectory();
        SharedDirectory shared = new SharedDirectory();
        StorageType type = new StorageType(local, shared);

        HeadNodeStorage storage = new HeadNodeStorage(type);

        assertThat(storage.getLocalDirectory(), is(local));
        assertThat(storage.getSharedDirectory(), is(shared));
    }

    @Test
    public void testSetLocalDirectory() {
        HeadNodeStorage storage = new HeadNodeStorage();
        LocalDirectory local = new LocalDirectory();
        InternalMountPoint mp = new InternalMountPoint("/storage/local");
        local.setInternalMountPoint(mp);
        storage.setLocalDirectory(local);
        assertThat(
                storage.getLocalDirectory().getInternalMountPoint().getMountPoint(),
                is("/storage/local"));
    }

    @Test
    public void testSetSharedDirectory() {
        HeadNodeStorage storage = new HeadNodeStorage();
        SharedDirectory shared = new SharedDirectory();
        InternalMountPoint mp = new InternalMountPoint("/storage/shared");
        shared.setInternalMountPoint(mp);
        storage.setSharedDirectory(shared);
        assertThat(
                storage.getSharedDirectory().getInternalMountPoint().getMountPoint(),
                is("/storage/shared"));
    }

    @Test
    public void testToXMLContainsStorageElement() throws IOException {
        HeadNodeStorage storage = new HeadNodeStorage();
        StringWriter sw = new StringWriter();
        storage.toXML(sw, "");
        String xml = sw.toString();
        assertThat(xml, containsString("<storage>"));
        assertThat(xml, containsString("</storage>"));
    }

    @Test
    public void testToXMLIncludesLocalAndSharedSections() throws IOException {
        HeadNodeStorage storage = new HeadNodeStorage();
        LocalDirectory local = new LocalDirectory();
        local.setInternalMountPoint(new InternalMountPoint("/storage/local"));
        SharedDirectory shared = new SharedDirectory();
        shared.setInternalMountPoint(new InternalMountPoint("/storage/shared"));
        storage.setLocalDirectory(local);
        storage.setSharedDirectory(shared);
        StringWriter sw = new StringWriter();

        storage.toXML(sw, "");

        assertThat(sw.toString(), containsString("<local"));
        assertThat(sw.toString(), containsString("<shared"));
    }

    @Test
    public void testCloneProducesDistinctInstance() {
        HeadNodeStorage storage = new HeadNodeStorage();
        HeadNodeStorage cloned = (HeadNodeStorage) storage.clone();
        assertThat(cloned, is(not(storage)));
        assertThat(cloned.getLocalDirectory(), is(not(storage.getLocalDirectory())));
        assertThat(cloned.getSharedDirectory(), is(not(storage.getSharedDirectory())));
    }

    @Test
    public void testCloneIsIndependentOfOriginalDirectoryMutations() {
        HeadNodeStorage storage = new HeadNodeStorage();
        LocalDirectory local = new LocalDirectory();
        local.setInternalMountPoint(new InternalMountPoint("/storage/local"));
        SharedDirectory shared = new SharedDirectory();
        shared.setInternalMountPoint(new InternalMountPoint("/storage/shared"));
        storage.setLocalDirectory(local);
        storage.setSharedDirectory(shared);

        HeadNodeStorage cloned = (HeadNodeStorage) storage.clone();

        storage.getLocalDirectory().getInternalMountPoint().setMountPoint("/changed/local");
        storage.getSharedDirectory().getInternalMountPoint().setMountPoint("/changed/shared");

        assertThat(
                cloned.getLocalDirectory().getInternalMountPoint().getMountPoint(),
                is("/storage/local"));
        assertThat(
                cloned.getSharedDirectory().getInternalMountPoint().getMountPoint(),
                is("/storage/shared"));
    }

    @Test
    public void testAcceptThrowsUnsupportedOperationException() {
        HeadNodeStorage storage = new HeadNodeStorage();

        UnsupportedOperationException exception =
                assertThrows(
                        UnsupportedOperationException.class,
                        () -> storage.accept(new NoOpVisitor()));

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
