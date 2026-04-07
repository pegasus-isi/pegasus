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
package edu.isi.pegasus.planner.code.generator.condor.style;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;

import edu.isi.pegasus.common.credential.CredentialHandler.TYPE;
import edu.isi.pegasus.common.logging.LogFormatter;
import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.planner.catalog.site.classes.GridGateway;
import edu.isi.pegasus.planner.catalog.site.classes.SiteCatalogEntry;
import edu.isi.pegasus.planner.catalog.site.classes.SiteStore;
import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.code.generator.condor.CondorStyle;
import edu.isi.pegasus.planner.code.generator.condor.CondorStyleException;
import edu.isi.pegasus.planner.namespace.Condor;
import edu.isi.pegasus.planner.namespace.ENV;
import java.io.PrintStream;
import java.util.Properties;
import org.junit.jupiter.api.Test;

/** Tests for the CreamCE style class. */
public class CreamCETest {

    private static final class NoOpLogManager extends LogManager {
        private int mLevel;

        @Override
        public void initialize(LogFormatter formatter, Properties properties) {}

        @Override
        public void configure(boolean prefixTimestamp) {}

        @Override
        protected void setLevel(int level, boolean info) {
            mLevel = level;
        }

        @Override
        public int getLevel() {
            return mLevel;
        }

        @Override
        public void setWriters(String out) {}

        @Override
        public void setWriter(STREAM_TYPE type, PrintStream ps) {}

        @Override
        public PrintStream getWriter(STREAM_TYPE type) {
            return null;
        }

        @Override
        public void log(String message, Exception e, int level) {}

        @Override
        public void log(String message, int level) {}

        @Override
        protected void logAlreadyFormattedMessage(String message, int level) {}

        @Override
        public void logEventCompletion(int level) {}
    }

    private static final class TestCreamCE extends CreamCE {
        private boolean mCredentialsApplied;
        private boolean mHandledRequirements;

        @Override
        protected void applyCredentialsForRemoteExec(Job job) throws CondorStyleException {
            mCredentialsApplied = true;
        }

        @Override
        protected void handleResourceRequirements(Job job) throws CondorStyleException {
            mHandledRequirements = true;
        }

        String gridResource(Job job) throws Exception {
            return constructGridResource(job);
        }

        void setSiteStore(SiteStore store) {
            mSiteStore = store;
        }

        void setLogger(LogManager logger) {
            mLogger = logger;
        }

        boolean credentialsApplied() {
            return mCredentialsApplied;
        }

        boolean handledRequirements() {
            return mHandledRequirements;
        }
    }

    private TestCreamCE newStyleWithSite(
            String siteHandle, String contact, GridGateway.SCHEDULER_TYPE scheduler) {
        GridGateway gateway = new GridGateway(GridGateway.TYPE.cream, contact, scheduler);
        gateway.setJobType(GridGateway.JOB_TYPE.compute);

        SiteCatalogEntry site = new SiteCatalogEntry();
        site.setSiteHandle(siteHandle);
        site.addGridGateway(gateway);

        SiteStore store = new SiteStore();
        store.addEntry(site);

        TestCreamCE style = new TestCreamCE();
        style.setSiteStore(store);
        style.setLogger(new NoOpLogManager());
        return style;
    }

    @Test
    public void testCreamCEExtendsAbstract() {
        assertThat(Abstract.class.isAssignableFrom(CreamCE.class), is(true));
    }

    @Test
    public void testCreamCEImplementsCondorStyle() {
        assertThat(CondorStyle.class.isAssignableFrom(CreamCE.class), is(true));
    }

    @Test
    public void testStyleNameConstant() {
        assertThat(CreamCE.STYLE_NAME, is("CreamCE"));
    }

    @Test
    public void testGridResourceKeyNotNull() {
        assertThat(CreamCE.GRID_RESOURCE_KEY, notNullValue());
    }

    @Test
    public void testInstantiation() {
        CreamCE style = new CreamCE();
        assertThat(style, notNullValue());
    }

    @Test
    public void testApplySetsGridUniverseSubmissionCredentialAndScratchDirectory()
            throws Exception {
        TestCreamCE style =
                newStyleWithSite(
                        "cream",
                        "https://ce.example:8443/ce-cream/services",
                        GridGateway.SCHEDULER_TYPE.pbs);
        Job job = new Job();
        job.setSiteHandle("cream");
        job.setDirectory("/scratch/work");
        job.globusRSL.construct("queue", "batch");

        style.apply(job);

        assertThat(job.condorVariables.get(Condor.UNIVERSE_KEY), is("grid"));
        assertThat(
                job.condorVariables.get(CreamCE.GRID_RESOURCE_KEY),
                is("cream https://ce.example:8443/ce-cream/services pbs batch"));
        assertThat(job.getSubmissionCredential(), is(TYPE.x509));
        assertThat(job.condorVariables.get("remote_initialdir"), is("/scratch/work"));
        assertThat(job.envVariables.get(ENV.PEGASUS_SCRATCH_DIR_KEY), is("/scratch/work"));
        assertThat(style.credentialsApplied(), is(true));
        assertThat(style.handledRequirements(), is(true));
    }

    @Test
    public void testConstructGridResourceAllowsMissingQueueAndAppendsNull() throws Exception {
        TestCreamCE style =
                newStyleWithSite(
                        "cream",
                        "https://ce.example:8443/ce-cream/services",
                        GridGateway.SCHEDULER_TYPE.condor);
        Job job = new Job();
        job.setSiteHandle("cream");

        assertThat(
                style.gridResource(job),
                is("cream https://ce.example:8443/ce-cream/services condor null"));
        assertThat(style.handledRequirements(), is(true));
    }

    @Test
    public void testConstructGridResourceThrowsWhenGridGatewayMissing() {
        SiteCatalogEntry site = new SiteCatalogEntry();
        site.setSiteHandle("missing");
        SiteStore store = new SiteStore();
        store.addEntry(site);

        TestCreamCE style = new TestCreamCE();
        style.setSiteStore(store);
        style.setLogger(new NoOpLogManager());

        Job job = new Job();
        job.setSiteHandle("missing");

        CondorStyleException e =
                assertThrows(CondorStyleException.class, () -> style.gridResource(job));
        assertThat(e.getMessage(), containsString("Grid Gateway not specified"));
    }

    @Test
    public void testConstructGridResourceRejectsForkScheduler() {
        TestCreamCE style =
                newStyleWithSite(
                        "cream",
                        "https://ce.example:8443/ce-cream/services",
                        GridGateway.SCHEDULER_TYPE.fork);
        Job job = new Job();
        job.setSiteHandle("cream");
        job.globusRSL.construct("queue", "batch");

        RuntimeException e = assertThrows(RuntimeException.class, () -> style.gridResource(job));
        assertThat(e.getMessage(), containsString("Please specify a valid scheduler"));
    }
}
