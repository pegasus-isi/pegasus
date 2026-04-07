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
package edu.isi.pegasus.planner.transfer;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

import edu.isi.pegasus.common.logging.LogFormatter;
import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.planner.classes.ADag;
import edu.isi.pegasus.planner.classes.FileTransfer;
import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.classes.PlannerOptions;
import edu.isi.pegasus.planner.classes.TransferJob;
import edu.isi.pegasus.planner.common.PegasusProperties;
import edu.isi.pegasus.planner.refiner.ReplicaCatalogBridge;
import edu.isi.pegasus.planner.transfer.implementation.TransferImplementationFactoryException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import org.junit.jupiter.api.Test;

/** @author Rajiv Mayani */
public class AbstractRefinerTest {

    @Test
    public void testConstructorInitializesFieldsAndGetWorkflow() {
        PegasusBag bag = createBag();
        ADag dag = new ADag();

        TestRefiner refiner = new TestRefiner(dag, bag);

        assertThat(refiner.getWorkflow(), sameInstance(dag));
        assertThat(refiner.mDAG, sameInstance(dag));
        assertThat(refiner.mProps, sameInstance(bag.getPegasusProperties()));
        assertThat(refiner.mPOptions, sameInstance(bag.getPlannerOptions()));
        assertThat(refiner.mLogger, sameInstance(bag.getLogger()));
        assertThat(refiner.mTPT, notNullValue());
        assertThat(refiner.mRemoteTransfers, notNullValue());
    }

    @Test
    public void testAddStageInXFERNodesThreeArgMergesAndDelegates() {
        TestRefiner refiner = new TestRefiner(new ADag(), createBag());
        Job job = new Job();
        List<FileTransfer> localTransfers = new ArrayList<FileTransfer>();
        List<FileTransfer> remoteTransfers = new ArrayList<FileTransfer>();

        FileTransfer local = new FileTransfer();
        FileTransfer remote = new FileTransfer();
        localTransfers.add(local);
        remoteTransfers.add(remote);

        refiner.addStageInXFERNodes(job, localTransfers, remoteTransfers);

        assertThat(refiner.lastStageInJob, sameInstance(job));
        assertThat(refiner.lastStageInFiles, sameInstance(localTransfers));
        assertThat(localTransfers.size(), equalTo(2));
        assertThat(localTransfers.contains(local), is(true));
        assertThat(localTransfers.contains(remote), is(true));
    }

    @Test
    public void testDefaultTwoArgAddStageInThrowsUnsupportedOperationException() {
        UnsupportedRefiner refiner = new UnsupportedRefiner(new ADag(), createBag());

        UnsupportedOperationException exception =
                assertThrows(
                        UnsupportedOperationException.class,
                        () ->
                                refiner.addStageInXFERNodes(
                                        new Job(), new ArrayList<FileTransfer>()));

        assertThat(exception.getMessage(), containsString("addStageInXFERNodes"));
    }

    @Test
    public void testPreferenceMethodsDefaultBehavior() {
        TestRefiner refiner = new TestRefiner(new ADag(), createBag());

        assertThat(refiner.refinerPreferenceForTransferJobLocation(), is(false));

        UnsupportedOperationException exception =
                assertThrows(
                        UnsupportedOperationException.class,
                        () -> refiner.refinerPreferenceForLocalTransferJobs(Job.STAGE_IN_JOB));

        assertThat(exception.getMessage(), containsString("local transfer jobs"));
    }

    @Test
    public void testTransferLocationHelpersAndInvalidTypeHandling() {
        TestRefiner refiner = new TestRefiner(new ADag(), createBag());
        refiner.mTXStageInImplementation = new StubImplementation(true, "stagein");
        refiner.mTXInterImplementation = new StubImplementation(false, "inter");
        refiner.mTXStageOutImplementation = new StubImplementation(false, "stageout");
        refiner.mTXSymbolicLinkImplementation = new StubImplementation(false, "symlink");

        assertThat(refiner.runTransferRemotely("local", Job.STAGE_IN_JOB), is(false));
        assertThat(refiner.isSiteThirdParty("local", Job.STAGE_IN_JOB), is(true));
        assertThat(refiner.runTPTOnRemoteSite("local", Job.STAGE_IN_JOB), is(false));

        assertThrows(
                IllegalArgumentException.class, () -> refiner.runTransferRemotely("local", -1));
        assertThrows(IllegalArgumentException.class, () -> refiner.isSiteThirdParty("local", -1));
        assertThrows(IllegalArgumentException.class, () -> refiner.runTPTOnRemoteSite("local", -1));
    }

    @Test
    public void testLogConfigMessagesLogsAllLoadedImplementations() {
        CapturingLogManager logger = new CapturingLogManager();
        PegasusBag bag = createBag(logger);
        TestRefiner refiner = new TestRefiner(new ADag(), bag);
        refiner.mTXStageInImplementation = new StubImplementation(false, "stagein impl");
        refiner.mTXSymbolicLinkImplementation = new StubImplementation(false, "symlink impl");
        refiner.mTXInterImplementation = new StubImplementation(false, "inter impl");
        refiner.mTXStageOutImplementation = new StubImplementation(false, "stageout impl");

        refiner.logConfigMessages();

        assertThat(logger.messages.size(), equalTo(4));
        assertThat(logger.messages.get(0), containsString("stagein impl"));
        assertThat(logger.messages.get(1), containsString("symlink impl"));
        assertThat(logger.messages.get(2), containsString("inter impl"));
        assertThat(logger.messages.get(3), containsString("stageout impl"));
    }

    private PegasusBag createBag() {
        return createBag(new CapturingLogManager());
    }

    private PegasusBag createBag(LogManager logger) {
        PegasusBag bag = new PegasusBag();
        bag.add(PegasusBag.PEGASUS_PROPERTIES, PegasusProperties.nonSingletonInstance());
        bag.add(PegasusBag.PLANNER_OPTIONS, new PlannerOptions());
        bag.add(PegasusBag.PEGASUS_LOGMANAGER, logger);
        return bag;
    }

    private static class TestRefiner extends AbstractRefiner {

        Job lastStageInJob;
        Collection<FileTransfer> lastStageInFiles;

        TestRefiner(ADag dag, PegasusBag bag) {
            super(dag, bag);
        }

        @Override
        public void loadImplementations(PegasusBag bag)
                throws TransferImplementationFactoryException {}

        @Override
        public void addInterSiteTXNodes(Job job, Collection files, boolean localTransfer) {}

        @Override
        public void addStageOutXFERNodes(
                Job job,
                Collection<FileTransfer> localFileTransfers,
                Collection<FileTransfer> remoteFileTransfers,
                ReplicaCatalogBridge rcb) {}

        @Override
        public void addStageOutXFERNodes(
                Job job,
                Collection<FileTransfer> files,
                ReplicaCatalogBridge rcb,
                boolean localTransfer,
                boolean deletedLeaf) {}

        @Override
        public void addStageInXFERNodes(Job job, Collection<FileTransfer> files) {
            lastStageInJob = job;
            lastStageInFiles = files;
        }

        @Override
        public void done() {}

        @Override
        public void addJob(Job job) {}

        @Override
        public void addRelation(String parent, String child) {}

        @Override
        public void addRelation(String parent, String child, String pool, boolean parentNew) {}

        @Override
        public String getDescription() {
            return "test";
        }
    }

    private static class UnsupportedRefiner extends AbstractRefiner {

        UnsupportedRefiner(ADag dag, PegasusBag bag) {
            super(dag, bag);
        }

        @Override
        public void loadImplementations(PegasusBag bag)
                throws TransferImplementationFactoryException {}

        @Override
        public void addInterSiteTXNodes(Job job, Collection files, boolean localTransfer) {}

        @Override
        public void addStageOutXFERNodes(
                Job job,
                Collection<FileTransfer> localFileTransfers,
                Collection<FileTransfer> remoteFileTransfers,
                ReplicaCatalogBridge rcb) {}

        @Override
        public void addStageOutXFERNodes(
                Job job,
                Collection<FileTransfer> files,
                ReplicaCatalogBridge rcb,
                boolean localTransfer,
                boolean deletedLeaf) {}

        @Override
        public void done() {}

        @Override
        public void addJob(Job job) {}

        @Override
        public void addRelation(String parent, String child) {}

        @Override
        public void addRelation(String parent, String child, String pool, boolean parentNew) {}

        @Override
        public String getDescription() {
            return "unsupported";
        }
    }

    private static class StubImplementation implements Implementation {

        private final boolean mUseThirdPartyAlways;
        private final String mDescription;

        StubImplementation(boolean useThirdPartyAlways, String description) {
            mUseThirdPartyAlways = useThirdPartyAlways;
            mDescription = description;
        }

        @Override
        public void setRefiner(Refiner refiner) {}

        @Override
        public TransferJob createTransferJob(
                Job job,
                String site,
                Collection files,
                Collection execFiles,
                String txJobName,
                int jobClass) {
            return null;
        }

        @Override
        public boolean doesPreserveXBit() {
            return false;
        }

        @Override
        public boolean addSetXBitJobs(
                Job computeJob,
                String txJobName,
                Collection execFiles,
                int transferClass,
                int xbitIndex) {
            return false;
        }

        @Override
        public Job createSetXBitJob(
                Job computeJob,
                Collection<FileTransfer> execFiles,
                int transferClass,
                int xbitIndex) {
            return null;
        }

        @Override
        public String getSetXBitJobName(String name, int counter) {
            return null;
        }

        @Override
        public edu.isi.pegasus.planner.catalog.transformation.TransformationCatalogEntry
                getTransformationCatalogEntry(String siteHandle, int jobClass) {
            return null;
        }

        @Override
        public boolean useThirdPartyTransferAlways() {
            return mUseThirdPartyAlways;
        }

        @Override
        public void applyPriority(TransferJob job) {}

        @Override
        public String getDescription() {
            return mDescription;
        }
    }

    private static class CapturingLogManager extends LogManager {

        final List<String> messages = new ArrayList<String>();

        @Override
        public void initialize(LogFormatter formatter, Properties properties) {
            this.mLogFormatter = formatter;
        }

        @Override
        public void configure(boolean prefixTimestamp) {}

        @Override
        protected void setLevel(int level, boolean info) {}

        @Override
        public int getLevel() {
            return LogManager.DEBUG_MESSAGE_LEVEL;
        }

        @Override
        public void setWriters(String out) {}

        @Override
        public void setWriter(STREAM_TYPE type, PrintStream ps) {}

        @Override
        public PrintStream getWriter(STREAM_TYPE type) {
            return System.out;
        }

        @Override
        public void log(String message, Exception e, int level) {
            messages.add(message);
        }

        @Override
        public void log(String message, int level) {
            messages.add(message);
        }

        @Override
        protected void logAlreadyFormattedMessage(String message, int level) {
            messages.add(message);
        }

        @Override
        public void logEventCompletion(int level) {}
    }
}
