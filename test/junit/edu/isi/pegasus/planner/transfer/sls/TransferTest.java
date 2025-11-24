/**
 * Copyright 2007-2021 University Of Southern California
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
package edu.isi.pegasus.planner.transfer.sls;

import static org.junit.jupiter.api.Assertions.*;

import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.common.util.PegasusURL;
import edu.isi.pegasus.planner.catalog.classes.SysInfo;
import edu.isi.pegasus.planner.catalog.replica.ReplicaCatalogEntry;
import edu.isi.pegasus.planner.catalog.site.classes.Directory;
import edu.isi.pegasus.planner.catalog.site.classes.FileServer;
import edu.isi.pegasus.planner.catalog.site.classes.FileServerType;
import edu.isi.pegasus.planner.catalog.site.classes.InternalMountPoint;
import edu.isi.pegasus.planner.catalog.site.classes.SiteCatalogEntry;
import edu.isi.pegasus.planner.catalog.site.classes.SiteStore;
import edu.isi.pegasus.planner.catalog.transformation.classes.Container;
import edu.isi.pegasus.planner.classes.ADag;
import edu.isi.pegasus.planner.classes.FileTransfer;
import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.classes.PegasusFile;
import edu.isi.pegasus.planner.classes.PlannerOptions;
import edu.isi.pegasus.planner.common.PegasusProperties;
import edu.isi.pegasus.planner.mapper.StagingMapperFactory;
import edu.isi.pegasus.planner.test.DefaultTestSetup;
import edu.isi.pegasus.planner.test.TestSetup;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;

/**
 * Unit tests for nonshared fs mode, PegasusLite transfers
 *
 * @author Karan Vahi
 */
public abstract class TransferTest {
    protected PegasusBag mBag;

    protected PegasusProperties mProps;

    protected LogManager mLogger;

    protected TestSetup mTestSetup;

    protected ADag mDAG;

    protected static int mTestNumber = 1;

    public TransferTest() {}

    @BeforeAll
    public static void setUpClass() {}

    @AfterAll
    public static void tearDownClass() {}

    /** Setup the logger and properties that all test functions require */
    @BeforeEach
    public void setUp() {
        mTestSetup = new DefaultTestSetup();
        mBag = new PegasusBag();
        mTestSetup.setInputDirectory(this.getClass());
        System.out.println("Input Test Dir is " + mTestSetup.getInputDirectory());

        mProps = PegasusProperties.nonSingletonInstance();
        mBag.add(PegasusBag.PEGASUS_PROPERTIES, mProps);

        PlannerOptions options = new PlannerOptions();
        options.setExecutionSites("compute");
        mBag.add(PegasusBag.PLANNER_OPTIONS, options);

        mLogger = mTestSetup.loadLogger(mProps);
        mLogger.setLevel(LogManager.DEBUG_MESSAGE_LEVEL);
        mLogger.logEventStart("test.transfer.sls.transfer", "setup", "0");
        mBag.add(PegasusBag.PEGASUS_LOGMANAGER, mLogger);

        SiteStore store = this.constructTestSiteStore();
        store.setForPlannerUse(mProps, mBag.getPlannerOptions());
        mBag.add(PegasusBag.SITE_STORE, store);

        // we don't care for type of staging mapper
        mBag.add(PegasusBag.PEGASUS_STAGING_MAPPER, StagingMapperFactory.loadInstance(mBag));

        // we only need a workflow with one job
        mDAG = constructTestWorkflow();

        mLogger.logEventCompletion();
    }

    @AfterEach
    public void tearDown() {
        mLogger = null;
        mProps = null;
        mBag = null;
        mTestSetup = null;
    }

    protected void testUseFileURLAsSource(
            String computeSite,
            String stagingSite,
            boolean sharedFileSystem,
            boolean expectedValue) {
        mLogger.logEventStart("test.transfer.sls.transfer", "set", Integer.toString(mTestNumber++));
        Transfer t = new Transfer();
        t.initialize(mBag);
        SiteCatalogEntry compute = new SiteCatalogEntry(computeSite);
        Directory dir = new Directory();
        dir.setType(Directory.TYPE.shared_scratch);
        dir.setSharedFileSystemAccess(sharedFileSystem);
        compute.addDirectory(dir);
        assertEquals(
                expectedValue,
                t.useFileURLAsSource(compute, stagingSite),
                "use file URL as source:");
        mLogger.logEventCompletion();
    }

    protected void testSymlinkingEnabledForJob(
            Job job, boolean workflowSymlinking, boolean expectedValue) {
        mLogger.logEventStart("test.transfer.sls.transfer", "set", Integer.toString(mTestNumber++));
        Transfer t = new Transfer();
        t.initialize(mBag);
        assertEquals(
                expectedValue,
                t.symlinkingEnabled(job, workflowSymlinking),
                "Symlinking enabled for job:");
        mLogger.logEventCompletion();
    }

    protected void testSymlinkingEnabledForFile(
            PegasusFile pf,
            boolean symlinkingEnabledForJob,
            boolean useFileURLAsSource,
            boolean expectedValue) {
        mLogger.logEventStart("test.transfer.sls.transfer", "set", Integer.toString(mTestNumber++));
        Transfer t = new Transfer();
        t.initialize(mBag);
        assertEquals(
                expectedValue,
                t.symlinkingEnabled(pf, symlinkingEnabledForJob, useFileURLAsSource),
                "Symlinking enabled for file:");
        mLogger.logEventCompletion();
    }

    protected void testSourceFileURLForContainerizedJob(
            String sourceURL,
            Container.MountPoint mp,
            String expectedReplacedURL,
            boolean transferOnHostOS) {
        mLogger.logEventStart("test.transfer.sls.transfer", "set", Integer.toString(mTestNumber++));
        Transfer t = new Transfer();
        mBag.getPegasusProperties()
                .setProperty(
                        PegasusProperties.PEGASUS_TRANSFER_CONTAINER_ON_HOST,
                        Boolean.toString(transferOnHostOS));
        t.initialize(mBag);
        ReplicaCatalogEntry source = new ReplicaCatalogEntry(sourceURL);
        Container c = new Container("centos-9");
        if (mp != null) {
            c.addMountPoint(mp);
        }
        t.updateSourceFileURLForContainerizedJob(c, new PegasusFile("f.in"), source, "ID1");
        assertEquals(expectedReplacedURL, source.getPFN(), "source file url in containerized jobs");
        mLogger.logEventCompletion();
    }

    protected void testStageOut(String stagingSite, FileTransfer expected) {

        mLogger.logEventStart("test.transfer.sls.transfer", "set", Integer.toString(mTestNumber++));
        Transfer t = new Transfer();
        t.initialize(mBag);

        Job job = (Job) mDAG.getNode("preprocess_ID1").getContent();
        job.setStagingSiteHandle(stagingSite);
        PegasusFile inputFile = (PegasusFile) job.getInputFiles().toArray()[0];
        FileServer stagingSiteServer = null;
        String stagingSiteDirectory = null;

        SiteStore store = this.mBag.getHandleToSiteStore();
        SiteCatalogEntry stagingSiteEntry = store.lookup(job.getStagingSiteHandle());
        stagingSiteDirectory = stagingSiteEntry.getInternalMountPointOfWorkDirectory();
        stagingSiteServer =
                stagingSiteEntry.selectHeadNodeScratchSharedFileServer(FileServer.OPERATION.get);

        Collection<FileTransfer> result =
                t.determineSLSOutputTransfers(
                        job, inputFile.getLFN(), stagingSiteServer, stagingSiteDirectory, "$PWD");
        // System.err.println(result);
        assertNotNull(result);
        assertEquals(1, result.size());
        testFileTransfer(expected, (FileTransfer) result.toArray()[0]);
        mLogger.logEventCompletion();
    }

    protected void testStageIn(String stagingSite, FileTransfer expected) {
        this.testStageIn(stagingSite, expected, new HashMap<String, String>());
    }

    protected void testStageIn(
            String stagingSite, FileTransfer expected, Map<String, String> pegasusProfiles) {

        mLogger.logEventStart("test.transfer.sls.transfer", "set", Integer.toString(mTestNumber++));
        Transfer t = new Transfer();
        t.initialize(mBag);

        Job job = (Job) mDAG.getNode("preprocess_ID1").getContent();
        job.setStagingSiteHandle(stagingSite);

        for (String key : pegasusProfiles.keySet()) {
            job.vdsNS.construct(key, pegasusProfiles.get(key));
        }

        PegasusFile inputFile = (PegasusFile) job.getInputFiles().toArray()[0];
        FileServer stagingSiteServer = null;
        String stagingSiteDirectory = null;

        SiteStore store = this.mBag.getHandleToSiteStore();
        SiteCatalogEntry stagingSiteEntry = store.lookup(job.getStagingSiteHandle());
        stagingSiteDirectory = stagingSiteEntry.getInternalMountPointOfWorkDirectory();
        stagingSiteServer =
                stagingSiteEntry.selectHeadNodeScratchSharedFileServer(FileServer.OPERATION.get);

        Collection<FileTransfer> result =
                t.determineSLSInputTransfers(
                        job,
                        inputFile.getLFN(),
                        stagingSiteServer,
                        stagingSiteDirectory,
                        "$PWD",
                        false);
        // System.err.println(result);
        assertNotNull(result);
        assertEquals(1, result.size());
        testFileTransfer(expected, (FileTransfer) result.toArray()[0]);
        mLogger.logEventCompletion();
    }

    protected void testFileTransfer(FileTransfer expected, FileTransfer actual) {
        assertNotNull(actual);
        assertEquals(expected.getLFN(), actual.getLFN());
        assertEquals(expected.getLinkage(), actual.getLinkage());
        assertEquals(expected.getTransferFlag(), actual.getTransferFlag());
        assertTrue(expected.getRegisterFlag() == actual.getRegisterFlag());
        assertEquals(expected.getSourceURLCount(), actual.getSourceURLCount());
        assertEquals(expected.getDestURLCount(), actual.getDestURLCount());

        // we only expect 1 source and destination URL for these unit tests
        assertEquals(expected.getSourceURLCount(), 1);
        assertEquals(actual.getDestURLCount(), 1);
        assertEquals(expected.getSourceURL(), actual.getSourceURL());
        assertEquals(expected.getDestURL(), actual.getDestURL());
    }

    protected ADag constructTestWorkflow() {

        ADag dag = new ADag();
        dag.setLabel("test");

        Job j = new Job();
        j.setTXName("preprocess");
        j.setLogicalID("ID1");
        j.setName("preprocess_ID1");
        j.setRemoteExecutable("/usr/bin/pegasus-keg");
        j.setSiteHandle("compute");
        j.setJobType(Job.COMPUTE_JOB);
        j.addInputFile(new PegasusFile("f.in"));
        PegasusFile output = new PegasusFile("f.out");
        output.setLinkage(PegasusFile.LINKAGE.output);
        j.addOutputFile(output);

        dag.add(j);
        return dag;
    }

    protected SiteStore constructTestSiteStore() {
        SiteStore store = new SiteStore();

        SiteCatalogEntry computeSite = new SiteCatalogEntry("compute");
        computeSite.setArchitecture(SysInfo.Architecture.x86_64);
        computeSite.setOS(SysInfo.OS.linux);
        Directory dir = new Directory();
        dir.setType(Directory.TYPE.shared_scratch);
        dir.setInternalMountPoint(
                new InternalMountPoint("/internal/workflows/compute/shared-scratch"));
        FileServer fs = new FileServer();
        fs.setSupportedOperation(FileServerType.OPERATION.get);
        PegasusURL url =
                new PegasusURL("gsiftp://compute.isi.edu/workflows/compute/shared-scratch");
        fs.setURLPrefix(url.getURLPrefix());
        fs.setProtocol(url.getProtocol());
        fs.setMountPoint(url.getPath());
        dir.addFileServer(fs);
        computeSite.addDirectory(dir);
        store.addEntry(computeSite);

        SiteCatalogEntry outputSite = new SiteCatalogEntry("output");
        outputSite.setArchitecture(SysInfo.Architecture.x86_64);
        outputSite.setOS(SysInfo.OS.linux);
        dir = new Directory();
        dir.setType(Directory.TYPE.shared_storage);
        dir.setInternalMountPoint(new InternalMountPoint("/workflows/output"));
        fs = new FileServer();
        fs.setSupportedOperation(FileServerType.OPERATION.all);
        url = new PegasusURL("gsiftp://output.isi.edu/workflows/output");
        fs.setURLPrefix(url.getURLPrefix());
        fs.setProtocol(url.getProtocol());
        fs.setMountPoint(url.getPath());
        dir.addFileServer(fs);
        outputSite.addDirectory(dir);
        store.addEntry(outputSite);

        SiteCatalogEntry stagingSite = new SiteCatalogEntry("staging");
        stagingSite.setArchitecture(SysInfo.Architecture.x86_64);
        stagingSite.setOS(SysInfo.OS.linux);
        dir = new Directory();
        dir.setType(Directory.TYPE.shared_scratch);
        dir.setInternalMountPoint(
                new InternalMountPoint("/internal/workflows/staging/shared-scratch"));
        fs = new FileServer();
        fs.setSupportedOperation(FileServerType.OPERATION.all);
        url = new PegasusURL("gsiftp://staging.isi.edu/workflows/staging/shared-scratch");
        fs.setURLPrefix(url.getURLPrefix());
        fs.setProtocol(url.getProtocol());
        fs.setMountPoint(url.getPath());
        dir.addFileServer(fs);
        stagingSite.addDirectory(dir);
        store.addEntry(stagingSite);

        // add a default local site
        SiteCatalogEntry localSite = new SiteCatalogEntry("local");
        localSite.setArchitecture(SysInfo.Architecture.x86_64);
        localSite.setOS(SysInfo.OS.linux);
        dir = new Directory();
        dir.setType(Directory.TYPE.shared_scratch);
        dir.setInternalMountPoint(
                new InternalMountPoint("/internal/workflows/local/shared-scratch"));
        fs = new FileServer();
        fs.setSupportedOperation(FileServerType.OPERATION.all);
        url = new PegasusURL("gsiftp://local.isi.edu/workflows/local/shared-scratch");
        fs.setURLPrefix(url.getURLPrefix());
        fs.setProtocol(url.getProtocol());
        fs.setMountPoint(url.getPath());
        dir.addFileServer(fs);
        localSite.addDirectory(dir);
        store.addEntry(localSite);

        return store;
    }
}
