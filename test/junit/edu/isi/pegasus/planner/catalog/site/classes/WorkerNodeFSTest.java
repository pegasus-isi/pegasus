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

import edu.isi.pegasus.planner.catalog.classes.Profiles;
import edu.isi.pegasus.planner.classes.Profile;
import java.io.IOException;
import java.io.StringWriter;
import org.junit.jupiter.api.Test;

/** @author Rajiv Mayani */
public class WorkerNodeFSTest {

    @Test
    public void testDefaultConstructorCreatesNonNullScratchAndStorage() {
        WorkerNodeFS wfs = new WorkerNodeFS();
        assertThat(wfs.getScratch(), is(notNullValue()));
        assertThat(wfs.getStorage(), is(notNullValue()));
    }

    @Test
    public void testOverloadedConstructorSetsScratchAndStorage() {
        WorkerNodeScratch scratch = new WorkerNodeScratch();
        WorkerNodeStorage storage = new WorkerNodeStorage();
        WorkerNodeFS wfs = new WorkerNodeFS(scratch, storage);
        assertThat(wfs.getScratch(), is(sameInstance(scratch)));
        assertThat(wfs.getStorage(), is(sameInstance(storage)));
    }

    @Test
    public void testSetAndGetScratch() {
        WorkerNodeFS wfs = new WorkerNodeFS();
        WorkerNodeScratch scratch = new WorkerNodeScratch();
        wfs.setScratch(scratch);
        assertThat(wfs.getScratch(), is(sameInstance(scratch)));
    }

    @Test
    public void testSetAndGetStorage() {
        WorkerNodeFS wfs = new WorkerNodeFS();
        WorkerNodeStorage storage = new WorkerNodeStorage();
        wfs.setStorage(storage);
        assertThat(wfs.getStorage(), is(sameInstance(storage)));
    }

    @Test
    public void testDefaultConstructorCreatesNonNullProfiles() {
        WorkerNodeFS wfs = new WorkerNodeFS();
        assertThat(wfs.getProfiles(), is(notNullValue()));
    }

    @Test
    public void testAddProfileStoresProfile() {
        WorkerNodeFS wfs = new WorkerNodeFS();

        wfs.addProfile(new Profile(Profile.ENV, "PATH", "/bin"));

        assertThat(wfs.getProfiles().getProfiles(Profile.ENV).size(), is(1));
        assertThat(wfs.getProfiles().getProfiles(Profile.ENV).get(0).getProfileKey(), is("PATH"));
    }

    @Test
    public void testSetProfilesReplacesExistingProfiles() {
        WorkerNodeFS wfs = new WorkerNodeFS();
        wfs.addProfile(new Profile(Profile.ENV, "PATH", "/bin"));

        Profiles replacement = new Profiles();
        replacement.addProfileDirectly(Profiles.NAMESPACES.globus, "maxwalltime", "60");
        wfs.setProfiles(replacement);

        assertThat(wfs.getProfiles().getProfiles(Profile.ENV).isEmpty(), is(true));
        assertThat(wfs.getProfiles().getProfiles(Profile.GLOBUS).size(), is(1));
        assertThat(
                wfs.getProfiles().getProfiles(Profile.GLOBUS).get(0).getProfileKey(),
                is("maxwalltime"));
    }

    @Test
    public void testToXMLContainsWorkerFsElement() throws IOException {
        WorkerNodeFS wfs = new WorkerNodeFS();
        StringWriter sw = new StringWriter();
        wfs.toXML(sw, "");
        String xml = sw.toString();
        assertThat(xml, containsString("<worker-fs>"));
        assertThat(xml, containsString("</worker-fs>"));
    }

    @Test
    public void testToXMLIncludesProfiles() throws IOException {
        WorkerNodeFS wfs = new WorkerNodeFS();
        wfs.addProfile(new Profile(Profile.ENV, "PATH", "/bin"));
        StringWriter sw = new StringWriter();

        wfs.toXML(sw, "");

        assertThat(sw.toString(), containsString("<profile"));
        assertThat(sw.toString(), containsString("namespace=\"env\""));
        assertThat(sw.toString(), containsString("key=\"PATH\""));
    }

    @Test
    public void testCloneProducesDistinctInstance() {
        WorkerNodeFS wfs = new WorkerNodeFS();
        WorkerNodeFS cloned = (WorkerNodeFS) wfs.clone();
        assertNotSame(wfs, cloned);
        assertNotSame(wfs.getScratch(), cloned.getScratch());
        assertNotSame(wfs.getStorage(), cloned.getStorage());
    }

    @Test
    public void testCloneDeepCopiesProfiles() {
        WorkerNodeFS wfs = new WorkerNodeFS();
        wfs.addProfile(new Profile(Profile.ENV, "PATH", "/bin"));

        WorkerNodeFS cloned = (WorkerNodeFS) wfs.clone();

        assertNotSame(wfs.getProfiles(), cloned.getProfiles());
        wfs.addProfile(new Profile(Profile.ENV, "HOME", "/home/user"));

        assertThat(wfs.getProfiles().getProfiles(Profile.ENV).size(), is(2));
        assertThat(cloned.getProfiles().getProfiles(Profile.ENV).size(), is(1));
        assertThat(
                cloned.getProfiles().getProfiles(Profile.ENV).get(0).getProfileKey(), is("PATH"));
    }

    @Test
    public void testAcceptThrowsUnsupportedOperationExceptionFromChildStorageTypes() {
        WorkerNodeFS wfs = new WorkerNodeFS();

        UnsupportedOperationException exception =
                assertThrows(
                        UnsupportedOperationException.class, () -> wfs.accept(new NoOpVisitor()));

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
