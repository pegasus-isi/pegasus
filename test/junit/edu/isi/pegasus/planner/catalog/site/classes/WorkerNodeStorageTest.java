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
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.io.StringWriter;
import org.junit.jupiter.api.Test;

/** Tests for the WorkerNodeStorage class. */
public class WorkerNodeStorageTest {

    @Test
    public void testWorkerNodeStorageExtendsStorageType() {
        assertThat(StorageType.class.isAssignableFrom(WorkerNodeStorage.class), is(true));
    }

    @Test
    public void testDefaultConstructor() {
        WorkerNodeStorage storage = new WorkerNodeStorage();
        assertThat(storage, is(notNullValue()));
    }

    @Test
    public void testDefaultConstructorCreatesNonNullLocalAndSharedDirectories() {
        WorkerNodeStorage storage = new WorkerNodeStorage();
        assertThat(storage.getLocalDirectory(), is(notNullValue()));
        assertThat(storage.getSharedDirectory(), is(notNullValue()));
    }

    @Test
    public void testDefaultConstructorHasNullWorkerSharedDirectory() {
        WorkerNodeStorage storage = new WorkerNodeStorage();
        assertThat(storage.getWorkerSharedDirectory(), is(nullValue()));
    }

    @Test
    public void testSetAndGetWorkerSharedDirectory() {
        WorkerNodeStorage storage = new WorkerNodeStorage();
        WorkerSharedDirectory shared = new WorkerSharedDirectory();

        storage.setWorkerSharedDirectory(shared);

        assertThat(storage.getWorkerSharedDirectory(), is(sameInstance(shared)));
    }

    @Test
    public void testOverloadedConstructorSetsDirectories() {
        LocalDirectory local = new LocalDirectory();
        SharedDirectory shared = new SharedDirectory();

        WorkerNodeStorage storage = new WorkerNodeStorage(local, shared);

        assertThat(storage.getLocalDirectory(), is(sameInstance(local)));
        assertThat(storage.getSharedDirectory(), is(sameInstance(shared)));
    }

    @Test
    public void testStorageTypeConstructorUsesSameDirectories() {
        LocalDirectory local = new LocalDirectory();
        SharedDirectory shared = new SharedDirectory();
        StorageType type = new StorageType(local, shared);

        WorkerNodeStorage storage = new WorkerNodeStorage(type);

        assertThat(storage.getLocalDirectory(), is(sameInstance(local)));
        assertThat(storage.getSharedDirectory(), is(sameInstance(shared)));
    }

    @Test
    public void testToXMLContainsStorageElement() throws IOException {
        WorkerNodeStorage storage = new WorkerNodeStorage();
        StringWriter writer = new StringWriter();

        storage.toXML(writer, "");

        assertThat(writer.toString(), containsString("<storage>"));
        assertThat(writer.toString(), containsString("</storage>"));
    }

    @Test
    public void testToXMLIncludesWorkerSharedDirectoryWhenPresent() throws IOException {
        WorkerNodeStorage storage = new WorkerNodeStorage();
        WorkerSharedDirectory workerShared = new WorkerSharedDirectory();
        workerShared.setInternalMountPoint(new InternalMountPoint("/worker/storage"));
        storage.setWorkerSharedDirectory(workerShared);
        StringWriter writer = new StringWriter();

        storage.toXML(writer, "");

        assertThat(writer.toString(), containsString("<wshared>"));
        assertThat(writer.toString(), containsString("/worker/storage"));
    }

    @Test
    public void testCloneProducesDistinctLocalAndSharedDirectories() {
        WorkerNodeStorage storage = new WorkerNodeStorage();

        WorkerNodeStorage cloned = (WorkerNodeStorage) storage.clone();

        assertNotSame(storage, cloned);
        assertNotSame(storage.getLocalDirectory(), cloned.getLocalDirectory());
        assertNotSame(storage.getSharedDirectory(), cloned.getSharedDirectory());
    }

    @Test
    public void testAcceptThrowsUnsupportedOperationException() {
        WorkerNodeStorage storage = new WorkerNodeStorage();

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
