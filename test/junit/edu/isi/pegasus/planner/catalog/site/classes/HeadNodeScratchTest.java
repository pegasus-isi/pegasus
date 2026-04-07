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
public class HeadNodeScratchTest {

    @Test
    public void testDefaultConstructorCreatesNonNullLocalAndSharedDirectories() {
        HeadNodeScratch scratch = new HeadNodeScratch();
        assertThat(scratch.getLocalDirectory(), is(notNullValue()));
        assertThat(scratch.getSharedDirectory(), is(notNullValue()));
    }

    @Test
    public void testOverloadedConstructorWithLocalAndShared() {
        LocalDirectory local = new LocalDirectory();
        SharedDirectory shared = new SharedDirectory();
        HeadNodeScratch scratch = new HeadNodeScratch(local, shared);
        assertThat(scratch.getLocalDirectory(), is(local));
        assertThat(scratch.getSharedDirectory(), is(shared));
    }

    @Test
    public void testOverloadedConstructorWithStorageTypeUsesSameDirectories() {
        LocalDirectory local = new LocalDirectory();
        SharedDirectory shared = new SharedDirectory();
        StorageType storage = new StorageType(local, shared);

        HeadNodeScratch scratch = new HeadNodeScratch(storage);

        assertThat(scratch.getLocalDirectory(), is(local));
        assertThat(scratch.getSharedDirectory(), is(shared));
    }

    @Test
    public void testSetLocalDirectory() {
        HeadNodeScratch scratch = new HeadNodeScratch();
        LocalDirectory local = new LocalDirectory();
        InternalMountPoint mp = new InternalMountPoint("/scratch/local");
        local.setInternalMountPoint(mp);
        scratch.setLocalDirectory(local);
        assertThat(
                scratch.getLocalDirectory().getInternalMountPoint().getMountPoint(),
                is("/scratch/local"));
    }

    @Test
    public void testSetSharedDirectory() {
        HeadNodeScratch scratch = new HeadNodeScratch();
        SharedDirectory shared = new SharedDirectory();
        InternalMountPoint mp = new InternalMountPoint("/scratch/shared");
        shared.setInternalMountPoint(mp);
        scratch.setSharedDirectory(shared);
        assertThat(
                scratch.getSharedDirectory().getInternalMountPoint().getMountPoint(),
                is("/scratch/shared"));
    }

    @Test
    public void testToXMLContainsScratchElement() throws IOException {
        HeadNodeScratch scratch = new HeadNodeScratch();
        StringWriter sw = new StringWriter();
        scratch.toXML(sw, "");
        String xml = sw.toString();
        assertThat(xml, containsString("<scratch>"));
        assertThat(xml, containsString("</scratch>"));
    }

    @Test
    public void testToXMLIncludesLocalAndSharedSections() throws IOException {
        HeadNodeScratch scratch = new HeadNodeScratch();
        LocalDirectory local = new LocalDirectory();
        local.setInternalMountPoint(new InternalMountPoint("/scratch/local"));
        SharedDirectory shared = new SharedDirectory();
        shared.setInternalMountPoint(new InternalMountPoint("/scratch/shared"));
        scratch.setLocalDirectory(local);
        scratch.setSharedDirectory(shared);
        StringWriter sw = new StringWriter();

        scratch.toXML(sw, "");

        assertThat(sw.toString(), containsString("<local"));
        assertThat(sw.toString(), containsString("<shared"));
    }

    @Test
    public void testCloneProducesDistinctInstance() {
        HeadNodeScratch scratch = new HeadNodeScratch();
        HeadNodeScratch cloned = (HeadNodeScratch) scratch.clone();
        assertThat(cloned, is(not(scratch)));
        assertThat(cloned.getLocalDirectory(), is(not(scratch.getLocalDirectory())));
        assertThat(cloned.getSharedDirectory(), is(not(scratch.getSharedDirectory())));
    }

    @Test
    public void testCloneIsIndependentOfOriginalDirectoryMutations() {
        HeadNodeScratch scratch = new HeadNodeScratch();
        LocalDirectory local = new LocalDirectory();
        local.setInternalMountPoint(new InternalMountPoint("/scratch/local"));
        SharedDirectory shared = new SharedDirectory();
        shared.setInternalMountPoint(new InternalMountPoint("/scratch/shared"));
        scratch.setLocalDirectory(local);
        scratch.setSharedDirectory(shared);

        HeadNodeScratch cloned = (HeadNodeScratch) scratch.clone();

        scratch.getLocalDirectory().getInternalMountPoint().setMountPoint("/changed/local");
        scratch.getSharedDirectory().getInternalMountPoint().setMountPoint("/changed/shared");

        assertThat(
                cloned.getLocalDirectory().getInternalMountPoint().getMountPoint(),
                is("/scratch/local"));
        assertThat(
                cloned.getSharedDirectory().getInternalMountPoint().getMountPoint(),
                is("/scratch/shared"));
    }

    @Test
    public void testAcceptThrowsUnsupportedOperationException() {
        HeadNodeScratch scratch = new HeadNodeScratch();

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
