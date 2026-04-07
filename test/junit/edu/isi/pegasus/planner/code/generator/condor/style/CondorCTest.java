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

import edu.isi.pegasus.common.logging.LogFormatter;
import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.planner.catalog.site.classes.GridGateway;
import edu.isi.pegasus.planner.catalog.site.classes.SiteCatalogEntry;
import edu.isi.pegasus.planner.catalog.site.classes.SiteStore;
import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.code.generator.condor.CondorStyle;
import java.io.PrintStream;
import java.util.Properties;
import org.junit.jupiter.api.Test;

/** Tests for the CondorC style class. */
public class CondorCTest {

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

    private static final class TestCondorC extends CondorC {
        String gridResource(Job job) throws Exception {
            return constructGridResource(job);
        }

        void setSiteStore(SiteStore store) {
            mSiteStore = store;
        }

        void setLogger(LogManager logger) {
            mLogger = logger;
        }
    }

    private TestCondorC newStyleWithSite(String siteHandle, String contact) {
        GridGateway gateway =
                new GridGateway(
                        GridGateway.TYPE.condor, contact, GridGateway.SCHEDULER_TYPE.condor);
        gateway.setJobType(GridGateway.JOB_TYPE.compute);

        SiteCatalogEntry site = new SiteCatalogEntry();
        site.setSiteHandle(siteHandle);
        site.addGridGateway(gateway);

        SiteStore store = new SiteStore();
        store.addEntry(site);

        TestCondorC style = new TestCondorC();
        style.setSiteStore(store);
        style.setLogger(new NoOpLogManager());
        return style;
    }

    @Test
    public void testCondorCExtendsCondor() {
        assertThat(Condor.class.isAssignableFrom(CondorC.class), is(true));
    }

    @Test
    public void testCondorCImplementsCondorStyle() {
        assertThat(CondorStyle.class.isAssignableFrom(CondorC.class), is(true));
    }

    @Test
    public void testRemoteUniverseKeyNotNull() {
        assertThat(CondorC.REMOTE_UNIVERSE_KEY, notNullValue());
    }

    @Test
    public void testShouldTransferFilesKeyNotNull() {
        assertThat(CondorC.SHOULD_TRANSFER_FILES_KEY, notNullValue());
    }

    @Test
    public void testWhenToTransferOutputKeyNotNull() {
        assertThat(CondorC.WHEN_TO_TRANSFER_OUTPUT_KEY, notNullValue());
    }

    @Test
    public void testStyleNameConstant() {
        assertThat(CondorC.STYLE_NAME, is("CondorC"));
    }

    @Test
    public void testConstructGridResourceUsesExplicitCollector() throws Exception {
        TestCondorC style = newStyleWithSite("condorc", "schedd.example");
        Job job = new Job();
        job.setSiteHandle("condorc");
        job.condorVariables.construct(CondorC.COLLECTOR_KEY, "collector.example");

        assertThat(style.gridResource(job), is("condor schedd.example collector.example"));
        assertThat(job.condorVariables.containsKey(CondorC.COLLECTOR_KEY), is(false));
    }

    @Test
    public void testConstructGridResourceFallsBackToContactForCollector() throws Exception {
        TestCondorC style = newStyleWithSite("condorc", "schedd.example");
        Job job = new Job();
        job.setSiteHandle("condorc");

        assertThat(style.gridResource(job), is("condor schedd.example schedd.example"));
    }

    @Test
    public void testConstructGridResourceThrowsWhenGridGatewayMissing() {
        SiteCatalogEntry site = new SiteCatalogEntry();
        site.setSiteHandle("missing");
        SiteStore store = new SiteStore();
        store.addEntry(site);

        TestCondorC style = new TestCondorC();
        style.setSiteStore(store);
        style.setLogger(new NoOpLogManager());

        Job job = new Job();
        job.setSiteHandle("missing");

        Exception e = assertThrows(Exception.class, () -> style.gridResource(job));
        assertThat(e.getMessage(), containsString("Grid Gateway not specified"));
    }
}
