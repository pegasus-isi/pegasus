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
public class WorkerNodeScratchTest {

    @Test
    public void testDefaultConstructorCreatesNonNullLocalAndSharedDirectories() {
        WorkerNodeScratch scratch = new WorkerNodeScratch();
        assertThat(scratch.getLocalDirectory(), is(notNullValue()));
        assertThat(scratch.getSharedDirectory(), is(notNullValue()));
    }

    @Test
    public void testDefaultConstructorHasNullWorkerSharedDirectory() {
        WorkerNodeScratch scratch = new WorkerNodeScratch();
        assertThat(scratch.getWorkerSharedDirectory(), is(nullValue()));
    }

    @Test
    public void testSetAndGetWorkerSharedDirectory() {
        WorkerNodeScratch scratch = new WorkerNodeScratch();
        WorkerSharedDirectory wsd = new WorkerSharedDirectory();
        scratch.setWorkerSharedDirectory(wsd);
        assertThat(scratch.getWorkerSharedDirectory(), is(sameInstance(wsd)));
    }

    @Test
    public void testOverloadedConstructorSetsDirectories() {
        LocalDirectory local = new LocalDirectory();
        SharedDirectory shared = new SharedDirectory();
        WorkerNodeScratch scratch = new WorkerNodeScratch(local, shared);
        assertThat(scratch.getLocalDirectory(), is(sameInstance(local)));
        assertThat(scratch.getSharedDirectory(), is(sameInstance(shared)));
    }

    @Test
    public void testStorageTypeConstructorUsesSameDirectories() {
        LocalDirectory local = new LocalDirectory();
        SharedDirectory shared = new SharedDirectory();
        StorageType storage = new StorageType(local, shared);

        WorkerNodeScratch scratch = new WorkerNodeScratch(storage);

        assertThat(scratch.getLocalDirectory(), is(sameInstance(local)));
        assertThat(scratch.getSharedDirectory(), is(sameInstance(shared)));
    }

    @Test
    public void testToXMLContainsScratchElement() throws IOException {
        WorkerNodeScratch scratch = new WorkerNodeScratch();
        StringWriter sw = new StringWriter();
        scratch.toXML(sw, "");
        String xml = sw.toString();
        assertThat(xml, containsString("<scratch>"));
        assertThat(xml, containsString("</scratch>"));
    }

    @Test
    public void testToXMLIncludesWorkerSharedDirectoryWhenPresent() throws IOException {
        WorkerNodeScratch scratch = new WorkerNodeScratch();
        WorkerSharedDirectory workerShared = new WorkerSharedDirectory();
        workerShared.setInternalMountPoint(new InternalMountPoint("/worker/shared"));
        scratch.setWorkerSharedDirectory(workerShared);
        StringWriter sw = new StringWriter();

        scratch.toXML(sw, "");

        assertThat(sw.toString(), containsString("<wshared>"));
        assertThat(sw.toString(), containsString("/worker/shared"));
    }

    @Test
    public void testCloneWithNullWorkerSharedDirectory() {
        WorkerNodeScratch scratch = new WorkerNodeScratch();
        WorkerNodeScratch cloned = (WorkerNodeScratch) scratch.clone();
        assertNotSame(scratch, cloned);
        assertThat(cloned.getWorkerSharedDirectory(), is(nullValue()));
    }

    @Test
    public void testCloneWithWorkerSharedDirectoryIsDistinct() {
        WorkerNodeScratch scratch = new WorkerNodeScratch();
        WorkerSharedDirectory wsd = new WorkerSharedDirectory();
        scratch.setWorkerSharedDirectory(wsd);
        WorkerNodeScratch cloned = (WorkerNodeScratch) scratch.clone();
        assertNotSame(scratch.getWorkerSharedDirectory(), cloned.getWorkerSharedDirectory());
    }

    @Test
    public void testCloneIsIndependentOfOriginalMutations() {
        WorkerNodeScratch scratch = new WorkerNodeScratch();
        scratch.getLocalDirectory().setInternalMountPoint(new InternalMountPoint("/scratch/local"));
        scratch.getSharedDirectory()
                .setInternalMountPoint(new InternalMountPoint("/scratch/shared"));
        WorkerSharedDirectory workerShared = new WorkerSharedDirectory();
        workerShared.setInternalMountPoint(new InternalMountPoint("/worker/shared"));
        scratch.setWorkerSharedDirectory(workerShared);

        WorkerNodeScratch cloned = (WorkerNodeScratch) scratch.clone();

        scratch.getLocalDirectory().getInternalMountPoint().setMountPoint("/changed/local");
        scratch.getSharedDirectory().getInternalMountPoint().setMountPoint("/changed/shared");
        scratch.getWorkerSharedDirectory().getInternalMountPoint().setMountPoint("/changed/worker");

        assertThat(
                cloned.getLocalDirectory().getInternalMountPoint().getMountPoint(),
                is("/scratch/local"));
        assertThat(
                cloned.getSharedDirectory().getInternalMountPoint().getMountPoint(),
                is("/scratch/shared"));
        assertThat(
                cloned.getWorkerSharedDirectory().getInternalMountPoint().getMountPoint(),
                is("/worker/shared"));
    }

    @Test
    public void testAcceptThrowsUnsupportedOperationException() {
        WorkerNodeScratch scratch = new WorkerNodeScratch();

        UnsupportedOperationException exception =
                assertThrows(
                        UnsupportedOperationException.class,
                        () -> scratch.accept(new NoOpVisitor()));

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
