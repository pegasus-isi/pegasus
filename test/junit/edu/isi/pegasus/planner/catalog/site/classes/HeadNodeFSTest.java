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
public class HeadNodeFSTest {

    @Test
    public void testDefaultConstructorCreatesNonNullScratchAndStorage() {
        HeadNodeFS hfs = new HeadNodeFS();
        assertThat(hfs.getScratch(), is(notNullValue()));
        assertThat(hfs.getStorage(), is(notNullValue()));
    }

    @Test
    public void testOverloadedConstructorSetsScratchAndStorage() {
        HeadNodeScratch scratch = new HeadNodeScratch();
        HeadNodeStorage storage = new HeadNodeStorage();
        HeadNodeFS hfs = new HeadNodeFS(scratch, storage);
        assertThat(hfs.getScratch(), is(scratch));
        assertThat(hfs.getStorage(), is(storage));
    }

    @Test
    public void testSetAndGetScratch() {
        HeadNodeFS hfs = new HeadNodeFS();
        HeadNodeScratch scratch = new HeadNodeScratch();
        hfs.setScratch(scratch);
        assertThat(hfs.getScratch(), is(scratch));
    }

    @Test
    public void testSetAndGetStorage() {
        HeadNodeFS hfs = new HeadNodeFS();
        HeadNodeStorage storage = new HeadNodeStorage();
        hfs.setStorage(storage);
        assertThat(hfs.getStorage(), is(storage));
    }

    @Test
    public void testDefaultConstructorCreatesNonNullProfiles() {
        HeadNodeFS hfs = new HeadNodeFS();
        assertThat(hfs.getProfiles(), is(notNullValue()));
    }

    @Test
    public void testAddProfileStoresProfile() {
        HeadNodeFS hfs = new HeadNodeFS();

        hfs.addProfile(new Profile(Profile.ENV, "PATH", "/bin"));

        assertThat(hfs.getProfiles().getProfiles(Profile.ENV).size(), is(1));
        assertThat(hfs.getProfiles().getProfiles(Profile.ENV).get(0).getProfileKey(), is("PATH"));
    }

    @Test
    public void testSetProfilesReplacesExistingProfiles() {
        HeadNodeFS hfs = new HeadNodeFS();
        hfs.addProfile(new Profile(Profile.ENV, "PATH", "/bin"));

        Profiles replacement = new Profiles();
        replacement.addProfileDirectly(Profiles.NAMESPACES.globus, "maxwalltime", "60");
        hfs.setProfiles(replacement);

        assertThat(hfs.getProfiles().getProfiles(Profile.ENV).isEmpty(), is(true));
        assertThat(hfs.getProfiles().getProfiles(Profile.GLOBUS).size(), is(1));
        assertThat(
                hfs.getProfiles().getProfiles(Profile.GLOBUS).get(0).getProfileKey(),
                is("maxwalltime"));
    }

    @Test
    public void testToXMLContainsHeadFsElement() throws IOException {
        HeadNodeFS hfs = new HeadNodeFS();
        StringWriter sw = new StringWriter();
        hfs.toXML(sw, "");
        String xml = sw.toString();
        assertThat(xml, containsString("<head-fs>"));
        assertThat(xml, containsString("</head-fs>"));
    }

    @Test
    public void testToXMLIncludesProfiles() throws IOException {
        HeadNodeFS hfs = new HeadNodeFS();
        hfs.addProfile(new Profile(Profile.ENV, "PATH", "/bin"));
        StringWriter sw = new StringWriter();

        hfs.toXML(sw, "");

        assertThat(sw.toString(), containsString("<profile"));
        assertThat(sw.toString(), containsString("namespace=\"env\""));
        assertThat(sw.toString(), containsString("key=\"PATH\""));
    }

    @Test
    public void testCloneProducesDistinctInstance() {
        HeadNodeFS hfs = new HeadNodeFS();
        HeadNodeFS cloned = (HeadNodeFS) hfs.clone();
        assertThat(cloned, is(not(hfs)));
        assertThat(cloned.getScratch(), is(not(hfs.getScratch())));
        assertThat(cloned.getStorage(), is(not(hfs.getStorage())));
    }

    @Test
    public void testCloneDeepCopiesProfiles() {
        HeadNodeFS hfs = new HeadNodeFS();
        hfs.addProfile(new Profile(Profile.ENV, "PATH", "/bin"));

        HeadNodeFS cloned = (HeadNodeFS) hfs.clone();

        assertThat(cloned.getProfiles(), is(not(hfs.getProfiles())));
        hfs.addProfile(new Profile(Profile.ENV, "HOME", "/home/user"));

        assertThat(hfs.getProfiles().getProfiles(Profile.ENV).size(), is(2));
        assertThat(cloned.getProfiles().getProfiles(Profile.ENV).size(), is(1));
        assertThat(
                cloned.getProfiles().getProfiles(Profile.ENV).get(0).getProfileKey(), is("PATH"));
    }

    @Test
    public void testAcceptThrowsUnsupportedOperationExceptionFromChildStorageTypes() {
        HeadNodeFS hfs = new HeadNodeFS();

        UnsupportedOperationException exception =
                assertThrows(
                        UnsupportedOperationException.class, () -> hfs.accept(new NoOpVisitor()));

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
