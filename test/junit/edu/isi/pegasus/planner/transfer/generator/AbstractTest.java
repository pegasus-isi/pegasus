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
package edu.isi.pegasus.planner.transfer.generator;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.*;

import edu.isi.pegasus.planner.catalog.site.classes.FileServer;
import edu.isi.pegasus.planner.catalog.site.classes.SiteCatalogEntry;
import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.mapper.StagingMapper;
import java.io.File;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Properties;
import org.junit.jupiter.api.Test;

/** @author Rajiv Mayani */
public class AbstractTest {

    @Test
    public void testAbstractClassAndDeclaredFieldTypes() throws Exception {
        assertThat(Modifier.isAbstract(Abstract.class.getModifiers()), is(true));

        assertThat(Abstract.class.getDeclaredField("mLogMsg").getType(), is(String.class));
        assertThat(
                Abstract.class.getDeclaredField("mProps").getType().getName(),
                is("edu.isi.pegasus.planner.common.PegasusProperties"));
        assertThat(
                Abstract.class.getDeclaredField("mSiteStore").getType().getName(),
                is("edu.isi.pegasus.planner.catalog.site.classes.SiteStore"));
        assertThat(
                Abstract.class.getDeclaredField("mLogger").getType().getName(),
                is("edu.isi.pegasus.common.logging.LogManager"));
        assertThat(
                Abstract.class.getDeclaredField("mStagingMapper").getType().getName(),
                is("edu.isi.pegasus.planner.mapper.StagingMapper"));
        assertThat(
                Abstract.class.getDeclaredField("mTXRefiner").getType().getName(),
                is("edu.isi.pegasus.planner.transfer.Refiner"));
        assertThat(
                Abstract.class.getDeclaredField("mTransferJobPlacer").getType().getName(),
                is("edu.isi.pegasus.planner.transfer.JobPlacer"));

        Method initializeMethod =
                Abstract.class.getDeclaredMethod(
                        "initalize",
                        edu.isi.pegasus.planner.classes.ADag.class,
                        edu.isi.pegasus.planner.classes.PegasusBag.class,
                        edu.isi.pegasus.planner.transfer.Refiner.class);
        assertThat(initializeMethod.getReturnType(), is(void.class));
        assertThat(Modifier.isProtected(initializeMethod.getModifiers()), is(true));
    }

    @Test
    public void testSiteNotFoundMsgFormat() {
        TestGenerator generator = new TestGenerator();

        String message = generator.callSiteNotFoundMsg("condorpool", "vanilla");

        assertThat(message, containsString("site = condorpool"));
        assertThat(message, containsString("universe = vanilla"));
        assertThat(message, containsString("Site Catalog"));
    }

    @Test
    public void testComplainForHeadNodeURLPrefixMessages() {
        TestGenerator generator = new TestGenerator();

        RuntimeException withoutJob =
                assertThrows(
                        RuntimeException.class,
                        () ->
                                generator.callComplainForHeadNodeURLPrefix(
                                        "generator", "siteA", FileServer.OPERATION.get, null));
        assertThat(withoutJob.getMessage(), containsString("[generator]"));
        assertThat(withoutJob.getMessage(), containsString("operation get"));
        assertThat(withoutJob.getMessage(), containsString("site: siteA"));
        assertThat(withoutJob.getMessage().contains("For job ("), is(false));

        Job job = new Job();
        job.setName("jobA");
        RuntimeException withJob =
                assertThrows(
                        RuntimeException.class,
                        () ->
                                generator.callComplainForHeadNodeURLPrefix(
                                        "generator", "siteB", FileServer.OPERATION.put, job));
        assertThat(withJob.getMessage(), containsString("For job (jobA)."));
        assertThat(withJob.getMessage(), containsString("operation put"));
        assertThat(withJob.getMessage(), containsString("site: siteB"));
    }

    @Test
    public void testGetURLOnSharedScratchDelegatesToStagingMapper() {
        TestGenerator generator = new TestGenerator();
        generator.setStagingMapper(new StubStagingMapper());

        Job job = new Job();
        job.setName("jobA");
        SiteCatalogEntry entry = new SiteCatalogEntry("siteA");
        File addOn = new File("subdir");

        String url =
                generator.callGetURLOnSharedScratch(
                        entry, job, FileServer.OPERATION.get, addOn, "input.dat");

        assertThat(url, is("mapped://siteA/jobA/subdir/input.dat/get"));
    }

    private static class TestGenerator extends Abstract {

        String callSiteNotFoundMsg(String siteName, String universe) {
            return this.siteNotFoundMsg(siteName, universe);
        }

        void callComplainForHeadNodeURLPrefix(
                String refiner, String site, FileServer.OPERATION operation, Job job) {
            this.complainForHeadNodeURLPrefix(refiner, site, operation, job);
        }

        String callGetURLOnSharedScratch(
                SiteCatalogEntry entry,
                Job job,
                FileServer.OPERATION operation,
                File addOn,
                String lfn) {
            return this.getURLOnSharedScratch(entry, job, operation, addOn, lfn);
        }

        void setStagingMapper(StagingMapper mapper) {
            this.mStagingMapper = mapper;
        }
    }

    private static class StubStagingMapper implements StagingMapper {

        @Override
        public void initialize(
                edu.isi.pegasus.planner.classes.PegasusBag bag, Properties properties) {}

        @Override
        public File mapToRelativeDirectory(Job job, SiteCatalogEntry site, String lfn) {
            return new File(lfn == null ? "" : lfn);
        }

        @Override
        public File getRelativeDirectory(String site, String lfn) {
            return new File(lfn == null ? "" : lfn);
        }

        @Override
        public String map(
                Job job,
                File addOn,
                SiteCatalogEntry site,
                FileServer.OPERATION operation,
                String lfn) {
            return "mapped://"
                    + site.getSiteHandle()
                    + "/"
                    + job.getName()
                    + "/"
                    + addOn.getPath()
                    + "/"
                    + lfn
                    + "/"
                    + operation;
        }

        @Override
        public String description() {
            return "stub";
        }
    }
}
