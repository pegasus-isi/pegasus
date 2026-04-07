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
package edu.isi.pegasus.planner.mapper.staging;

import static org.junit.jupiter.api.Assertions.*;

import edu.isi.pegasus.planner.catalog.site.classes.Directory;
import edu.isi.pegasus.planner.catalog.site.classes.FileServer;
import edu.isi.pegasus.planner.catalog.site.classes.SiteCatalogEntry;
import edu.isi.pegasus.planner.catalog.site.classes.SiteStore;
import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.classes.PlannerOptions;
import edu.isi.pegasus.planner.common.PegasusProperties;
import edu.isi.pegasus.planner.mapper.StagingMapper;
import java.io.File;
import java.lang.reflect.Modifier;
import java.util.Properties;
import org.junit.jupiter.api.Test;

/** Tests for the Abstract staging mapper class structure. */
public class AbstractTest {

    @Test
    public void testAbstractImplementsStagingMapper() {
        org.hamcrest.MatcherAssert.assertThat(
                StagingMapper.class.isAssignableFrom(Abstract.class),
                org.hamcrest.Matchers.is(true));
    }

    @Test
    public void testFlatExtendsAbstract() {
        org.hamcrest.MatcherAssert.assertThat(
                Abstract.class.isAssignableFrom(Flat.class), org.hamcrest.Matchers.is(true));
    }

    @Test
    public void testHashedExtendsAbstract() {
        org.hamcrest.MatcherAssert.assertThat(
                Abstract.class.isAssignableFrom(Hashed.class), org.hamcrest.Matchers.is(true));
    }

    @Test
    public void testFlatInstantiation() {
        Flat flat = new Flat();
        org.hamcrest.MatcherAssert.assertThat(flat, org.hamcrest.Matchers.notNullValue());
    }

    @Test
    public void testHashedInstantiation() {
        Hashed hashed = new Hashed();
        org.hamcrest.MatcherAssert.assertThat(hashed, org.hamcrest.Matchers.notNullValue());
    }

    @Test
    public void testAbstractClassIsAbstract() {
        org.hamcrest.MatcherAssert.assertThat(
                Modifier.isAbstract(Abstract.class.getModifiers()), org.hamcrest.Matchers.is(true));
    }

    @Test
    public void testInitializeStoresSiteStoreFromBag() {
        TestAbstract mapper = new TestAbstract();
        SiteStore store = createSiteStoreWithScratchServer("local", "file://", "/work");
        PegasusBag bag = new PegasusBag();
        bag.add(PegasusBag.SITE_STORE, store);

        mapper.initialize(bag, new Properties());

        org.hamcrest.MatcherAssert.assertThat(
                mapper.siteStore(), org.hamcrest.Matchers.sameInstance(store));
        org.hamcrest.MatcherAssert.assertThat(mapper.logger(), org.hamcrest.Matchers.nullValue());
    }

    @Test
    public void testMapBuildsUrlFromScratchFileServerAndLfn() throws Exception {
        TestAbstract mapper = new TestAbstract();
        mapper.mSiteStore = createSiteStoreWithScratchServer("local", "file://", "/work");
        SiteCatalogEntry site = mapper.mSiteStore.lookup("local");
        Job job = new Job();
        job.setName("stage-job");

        String result =
                mapper.map(
                        job,
                        new File("staging/leaf"),
                        site,
                        FileServer.OPERATION.put,
                        "output.txt");

        org.hamcrest.MatcherAssert.assertThat(
                result, org.hamcrest.Matchers.is("file:///work/staging/leaf/output.txt"));
    }

    @Test
    public void testMapWithoutLfnOmitsTrailingLeaf() throws Exception {
        TestAbstract mapper = new TestAbstract();
        mapper.mSiteStore = createSiteStoreWithScratchServer("local", "file://", "/work");
        SiteCatalogEntry site = mapper.mSiteStore.lookup("local");

        String result =
                mapper.map(
                        new Job(), new File("staging/leaf"), site, FileServer.OPERATION.put, null);

        org.hamcrest.MatcherAssert.assertThat(
                result, org.hamcrest.Matchers.is("file:///work/staging/leaf"));
    }

    @Test
    public void testComplainForScratchFileServerIncludesJobAndDescription() {
        TestAbstract mapper = new TestAbstract();

        RuntimeException e =
                assertThrows(
                        RuntimeException.class,
                        () -> mapper.complain("jobA", FileServer.OPERATION.get, "missing-site"));

        org.hamcrest.MatcherAssert.assertThat(
                e.getMessage().contains("[Test Staging Mapper]"), org.hamcrest.Matchers.is(true));
        org.hamcrest.MatcherAssert.assertThat(
                e.getMessage().contains("For job (jobA)."), org.hamcrest.Matchers.is(true));
        org.hamcrest.MatcherAssert.assertThat(
                e.getMessage()
                        .contains(
                                "File Server not specified for shared-scratch filesystem for site: missing-site"),
                org.hamcrest.Matchers.is(true));
    }

    private SiteStore createSiteStoreWithScratchServer(
            String siteHandle, String urlPrefix, String mountPoint) {
        SiteCatalogEntry site = new SiteCatalogEntry(siteHandle);
        Directory directory = new Directory();
        directory.setType(Directory.TYPE.shared_scratch);
        directory.addFileServer(new FileServer("file", urlPrefix, mountPoint));
        site.addDirectory(directory);

        SiteStore store = new SiteStore();
        PegasusProperties properties = PegasusProperties.nonSingletonInstance();
        PlannerOptions options = new PlannerOptions();
        store.setForPlannerUse(properties, options);
        store.addEntry(site);
        return store;
    }

    private static final class TestAbstract extends Abstract {

        @Override
        public String description() {
            return "Test Staging Mapper";
        }

        @Override
        public File mapToRelativeDirectory(Job job, SiteCatalogEntry site, String lfn) {
            return new File("relative");
        }

        @Override
        public File getRelativeDirectory(String site, String lfn) {
            return new File("relative");
        }

        SiteStore siteStore() {
            return this.mSiteStore;
        }

        Object logger() {
            return this.mLogger;
        }

        void complain(String jobName, FileServer.OPERATION operation, String site) {
            this.complainForScratchFileServer(jobName, operation, site);
        }
    }
}
