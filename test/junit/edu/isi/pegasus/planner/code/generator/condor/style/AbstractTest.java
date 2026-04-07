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
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import edu.isi.pegasus.common.credential.CredentialHandlerFactory;
import edu.isi.pegasus.common.logging.LogFormatter;
import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.planner.catalog.site.classes.SiteCatalogEntry;
import edu.isi.pegasus.planner.catalog.site.classes.SiteStore;
import edu.isi.pegasus.planner.classes.AggregatedJob;
import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.code.generator.condor.CondorStyle;
import edu.isi.pegasus.planner.code.generator.condor.CondorStyleException;
import edu.isi.pegasus.planner.common.PegasusProperties;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.junit.jupiter.api.Test;

/** Tests for the Abstract condor style class. */
public class AbstractTest {

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

    private static final class TestStyle extends Abstract {
        private final List<Job> mAppliedJobs = new ArrayList<Job>();

        @Override
        public void apply(Job job) throws CondorStyleException {
            mAppliedJobs.add(job);
        }

        List<Job> appliedJobs() {
            return mAppliedJobs;
        }

        boolean encryptCredentialForFileTransfer() {
            return mEncryptCredentialForFileTX;
        }

        SiteStore siteStore() {
            return mSiteStore;
        }

        LogManager logger() {
            return mLogger;
        }

        CredentialHandlerFactory credentialFactory() {
            return mCredentialFactory;
        }

        PegasusProperties properties() {
            return mProps;
        }

        List<String> mountUnderScratchDirs() {
            return mMountUnderScratchDirs;
        }
    }

    @Test
    public void testAbstractImplementsCondorStyle() {
        assertThat(CondorStyle.class.isAssignableFrom(Abstract.class), is(true));
    }

    @Test
    public void testCondorExtendsAbstract() {
        assertThat(Abstract.class.isAssignableFrom(Condor.class), is(true));
    }

    @Test
    public void testCondorGlideINExtendsAbstract() {
        assertThat(Abstract.class.isAssignableFrom(CondorGlideIN.class), is(true));
    }

    @Test
    public void testCreamCEExtendsAbstract() {
        assertThat(Abstract.class.isAssignableFrom(CreamCE.class), is(true));
    }

    @Test
    public void testAbstractClassIsAbstract() {
        assertThat(java.lang.reflect.Modifier.isAbstract(Abstract.class.getModifiers()), is(true));
    }

    @Test
    public void testInitializePopulatesCoreFieldsAndEncryptFlag() throws Exception {
        PegasusProperties props = PegasusProperties.nonSingletonInstance();
        props.setProperty("pegasus.credential.encrypt", "false");

        SiteStore store = new SiteStore();
        NoOpLogManager logger = new NoOpLogManager();
        CredentialHandlerFactory factory = new CredentialHandlerFactory();
        PegasusBag bag = new PegasusBag();
        bag.add(PegasusBag.PEGASUS_PROPERTIES, props);
        bag.add(PegasusBag.PEGASUS_LOGMANAGER, logger);
        bag.add(PegasusBag.SITE_STORE, store);

        TestStyle style = new TestStyle();
        style.initialize(bag, factory);

        assertThat(style.properties(), sameInstance(props));
        assertThat(style.siteStore(), sameInstance(store));
        assertThat(style.logger(), sameInstance(logger));
        assertThat(style.credentialFactory(), sameInstance(factory));
        assertThat(style.mountUnderScratchDirs(), notNullValue());
        assertThat(style.encryptCredentialForFileTransfer(), is(false));
    }

    @Test
    public void testApplyAggregatedJobDelegatesToChildrenThenAggregate() throws Exception {
        TestStyle style = new TestStyle();
        AggregatedJob aggregated = new AggregatedJob();
        Job child1 = new Job();
        child1.setName("child1");
        child1.setLogicalID("child1");
        aggregated.setName("cluster");
        aggregated.add(child1);

        style.apply(aggregated);

        assertThat(style.appliedJobs().size(), is(2));
        assertThat(style.appliedJobs().get(0), sameInstance(child1));
        assertThat(style.appliedJobs().get(1), sameInstance(aggregated));
    }

    @Test
    public void testApplySiteCatalogEntryIsNoOp() throws Exception {
        TestStyle style = new TestStyle();

        assertDoesNotThrow(() -> style.apply(new SiteCatalogEntry()));
        assertThat(style.appliedJobs().isEmpty(), is(true));
    }
}
