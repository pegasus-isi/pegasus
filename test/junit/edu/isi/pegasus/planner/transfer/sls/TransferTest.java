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

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

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
import edu.isi.pegasus.planner.catalog.transformation.classes.Container.MountPoint;
import edu.isi.pegasus.planner.classes.ADag;
import edu.isi.pegasus.planner.classes.FileTransfer;
import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.classes.PegasusFile;
import edu.isi.pegasus.planner.classes.PlannerCache;
import edu.isi.pegasus.planner.classes.PlannerOptions;
import edu.isi.pegasus.planner.classes.Profile;
import edu.isi.pegasus.planner.common.PegasusProperties;
import edu.isi.pegasus.planner.mapper.StagingMapperFactory;
import edu.isi.pegasus.planner.namespace.Pegasus;
import edu.isi.pegasus.planner.test.DefaultTestSetup;
import edu.isi.pegasus.planner.test.TestSetup;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Unit tests for nonshared fs mode, PegasusLite transfers
 *
 * @author Karan Vahi
 */
public class TransferTest {
    private PegasusBag mBag;

    private PegasusProperties mProps;

    private LogManager mLogger;

    private TestSetup mTestSetup;

    private ADag mDAG;

    private static int mTestNumber = 1;

    public TransferTest() {}

    @BeforeClass
    public static void setUpClass() {}

    @AfterClass
    public static void tearDownClass() {}

    /** Setup the logger and properties that all test functions require */
    @Before
    public final void setUp() {
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

    @After
    public void tearDown() {
        mLogger = null;
        mProps = null;
        mBag = null;
        mTestSetup = null;
    }

    @Test
    public void testSymlinkingEnabledForJobWithWFSymlinkOff() {
        Job j = new Job();
        this.testSymlinkingEnabledForJob(j, false, false);
    }

    @Test
    public void testSymlinkingEnabledForJobWithWFSymlinkOn() {
        Job j = new Job();
        this.testSymlinkingEnabledForJob(j, true, true);
    }

    @Test
    public void testSymlinkingEnabledForJobWithWFSymlinkOnAndNoSymlinkProfileOn() {
        Job j = new Job();
        j.vdsNS.construct(Pegasus.NO_SYMLINK_KEY, "True");
        this.testSymlinkingEnabledForJob(j, true, false);
    }

    @Test
    public void testSymlinkingEnabledForJobWithWFSymlinkOnAndNoSymlinkProfileOFF() {
        Job j = new Job();
        j.vdsNS.construct(Pegasus.NO_SYMLINK_KEY, "False");
        this.testSymlinkingEnabledForJob(j, true, true);
    }

    @Test
    public void testUseFileURLAsSourceWithComputeSiteDifferentStagingSite() {
        this.testUseFileURLAsSource("compute", "staging", false, false);
    }

    @Test
    public void testUseFileURLAsSourceWithComputeSiteSameStagingSite() {
        // cannot use file url as source since no shared filesystem on
        // compute
        this.testUseFileURLAsSource("compute", "compute", false, false);
    }

    @Test
    public void testUseFileURLAsSourceWithComputeSiteSameStagingSiteSharedFS() {
        // cannot use file url as source since no shared filesystem on
        // compute
        this.testUseFileURLAsSource("compute", "compute", true, true);
    }

    @Test
    public void testUseFileURLAsSourceWithComputeStagingLocal() {
        // use file url only if compute site is visible to local site
        this.testUseFileURLAsSource("compute", "local", false, false);
    }

    @Test
    public void testUseFileURLAsSourceWithComputeStagingLocalAndVisible() {
        // use file url only if compute site is visible to local site
        mLogger.logEventStart("test.transfer.sls.transfer", "set", Integer.toString(mTestNumber++));
        Transfer t = new Transfer();
        t.initialize(mBag);
        SiteCatalogEntry compute = mBag.getHandleToSiteStore().lookup("compute");
        compute.addProfile(new Profile("pegasus", Pegasus.LOCAL_VISIBLE_KEY, "True"));
        assertEquals("use file URL as source:", t.useFileURLAsSource(compute, "local"), true);
        mLogger.logEventCompletion();
    }

    @Test
    public void testDefaultStageIn() {
        FileTransfer expectedOutput = new FileTransfer();
        expectedOutput.setLFN("f.in");
        expectedOutput.setRegisterFlag(true);
        expectedOutput.setTransferFlag(true);
        expectedOutput.addSource(
                "staging", "gsiftp://staging.isi.edu/workflows/staging/shared-scratch/./f.in");
        expectedOutput.addDestination("compute", "file://$PWD/f.in");

        this.testStageIn("staging", expectedOutput);
    }

    @Test
    public void testComputeStagingSiteSameStageIn() {
        FileTransfer expectedOutput = new FileTransfer();
        expectedOutput.setLFN("f.in");
        expectedOutput.setRegisterFlag(true);
        expectedOutput.setTransferFlag(true);
        //  compute and staging site are the same, but shared fs attribute is not
        // set on compute directory . so no file url substitution
        expectedOutput.addSource(
                "compute", "gsiftp://compute.isi.edu/workflows/compute/shared-scratch/./f.in");
        expectedOutput.addDestination("compute", "file://$PWD/f.in");

        this.testStageIn("compute", expectedOutput);
    }

    @Test
    public void testSymlinkingForFileWithFileURLAndJobSymlinkOn() {
        PegasusFile pf = new PegasusFile("f.in");
        this.testSymlinkingEnabledForFile(pf, true, true, true);
    }

    @Test
    public void testSymlinkingForFileWithFileURLAndJobSymlinkOFF() {
        PegasusFile pf = new PegasusFile("f.in");
        this.testSymlinkingEnabledForFile(pf, false, true, false);
    }

    @Test
    public void testSymlinkingForFileWithHTTPURLAndJobSymlinkOn() {
        PegasusFile pf = new PegasusFile("f.in");
        this.testSymlinkingEnabledForFile(pf, true, false, false);
    }

    @Test
    public void testSymlinkingForFileWithHTTPURLAndJobSymlinkOff() {
        PegasusFile pf = new PegasusFile("f.in");
        this.testSymlinkingEnabledForFile(pf, false, false, false);
    }

    @Test
    public void testSymlinkingForCheckpointFile() {
        PegasusFile pf = new PegasusFile("f.checkpoint");
        pf.setType(PegasusFile.CHECKPOINT_TYPE);
        // even though job symlinking on and file urls. no symlink for checkpoint
        this.testSymlinkingEnabledForFile(pf, true, true, false);
    }

    @Test
    public void testSymlinkingForExecutableFile() {
        PegasusFile pf = new PegasusFile("pegasus-ket");
        pf.setType(PegasusFile.EXECUTABLE_TYPE);
        // even though job symlinking on and file urls. no symlink for executable files
        this.testSymlinkingEnabledForFile(pf, true, true, false);
    }

    @Test(expected = RuntimeException.class)
    public void testUpdateSourceFileURLForContainerizedJobWithNoMount() {
        this.testSourceFileURLForContainerizedJob(
                "file:///shared/scratch/f.in", null, "file:///shared/scratch/f.in");
    }

    @Test(expected = RuntimeException.class)
    public void testUpdateSourceFileURLForContainerizedJobWithWrongMount() {
        this.testSourceFileURLForContainerizedJob(
                "file:///shared/scratch/f.in",
                new Container.MountPoint("/scratch/:/scratch"),
                "file:///shared/scratch/f.in");
    }

    @Test
    public void testUpdateSourceFileURLForContainerizedJobWithCorrectSameMount() {
        this.testSourceFileURLForContainerizedJob(
                "file:///shared/scratch/f.in",
                new Container.MountPoint("/shared/scratch:/shared/scratch"),
                "file:///shared/scratch/f.in");
    }

    @Test
    public void testUpdateSourceFileURLForContainerizedJobWithCorrectDiffMount() {
        // valid mount. but mount to different dest dir
        this.testSourceFileURLForContainerizedJob(
                "file:///shared/scratch/f.in",
                new Container.MountPoint("/shared/scratch:/incontainer"),
                "file:///incontainer/f.in");
    }

    @Test
    public void testUpdateSourceHTTPURLForContainerizedJobWithCorrectMount() {
        // valid mount. but mount to different dest dir
        this.testSourceFileURLForContainerizedJob(
                "http://test.example.com/shared/scratch/f.in",
                new Container.MountPoint("/shared/scratch:/shared/scratch"),
                "http://test.example.com/shared/scratch/f.in");
    }

    /**
     * PM-1789 file url substitution should be triggered only if compute site has a sharedFileSystem
     * attribute set to true on the shared scratch directory
     */
    @Test
    public void testComputeStagingSiteSameStageInWithSharedFSAttributeSpecified() {
        FileTransfer expectedOutput = new FileTransfer();
        expectedOutput.setLFN("f.in");
        expectedOutput.setRegisterFlag(true);
        expectedOutput.setTransferFlag(true);

        SiteCatalogEntry computeSiteEntry = this.mBag.getHandleToSiteStore().lookup("compute");
        Directory sharedScratch = computeSiteEntry.getDirectory(Directory.TYPE.shared_scratch);
        sharedScratch.setSharedFileSystemAccess(true);

        // since compute and staging site are the same, source is a file url not a gsiftp
        expectedOutput.addSource(
                "compute", "file:///internal/workflows/compute/shared-scratch/./f.in");
        expectedOutput.addDestination("compute", "file://$PWD/f.in");

        this.testStageIn("compute", expectedOutput);
    }

    @Test
    // PM-1893
    public void testForStageInWithSharedFSSemanticsAndContainer() {
        FileTransfer expectedOutput = new FileTransfer();
        expectedOutput.setLFN("f.in");
        expectedOutput.setRegisterFlag(true);
        expectedOutput.setTransferFlag(true);

        SiteCatalogEntry computeSiteEntry = this.mBag.getHandleToSiteStore().lookup("compute");
        Directory sharedScratch = computeSiteEntry.getDirectory(Directory.TYPE.shared_scratch);
        sharedScratch.setSharedFileSystemAccess(true);

        // staging site and compute site are the same
        expectedOutput.addSource(
                "compute", "file:///internal/workflows/compute/shared-scratch/./f.in");
        expectedOutput.addDestination("compute", "file://$PWD/f.in");

        // associate container with job
        Job j = (Job) mDAG.getNode("preprocess_ID1").getContent();
        j.setContainer(new Container("centos8"));

        this.testStageIn("compute", expectedOutput);
        // the container with the job should have a mount point associated
        Container c = j.getContainer();
        assertNotNull(c.getMountPoints());
        assertEquals(1, c.getMountPoints().size());
        MountPoint expectedMP = new MountPoint();
        expectedMP.setSourceDirectory("/internal/workflows/compute/shared-scratch");
        expectedMP.setDestinationDirectory("/internal/workflows/compute/shared-scratch");
        assertEquals(expectedMP, c.getMountPoints().toArray()[0]);
    }

    @Test
    // PM-1893
    public void testForStageInWithSharedFSSemanticsAndContainerWithBypasson() {
        mLogger.logEventStart(
                "test.transfer.sls.transfer", "sharedfs", Integer.toString(mTestNumber));
        FileTransfer expectedOutput = new FileTransfer();
        expectedOutput.setLFN("f.in");
        expectedOutput.setRegisterFlag(true);
        expectedOutput.setTransferFlag(true);

        SiteCatalogEntry computeSiteEntry = this.mBag.getHandleToSiteStore().lookup("compute");
        Directory sharedScratch = computeSiteEntry.getDirectory(Directory.TYPE.shared_scratch);
        sharedScratch.setSharedFileSystemAccess(true);

        // staging site and compute site are the same
        String sourceURL = "file:///path/on/shared/scratch/host/os/f.in";
        expectedOutput.addSource("compute", sourceURL);
        expectedOutput.addDestination("compute", "file://$PWD/f.in");

        // associate container with job and sourceDir has to be mounted
        Job j = (Job) mDAG.getNode("preprocess_ID1").getContent();
        Container c = new Container("centos8");
        MountPoint mp = new MountPoint();
        c.addMountPoint(
                new MountPoint("/path/on/shared/scratch/host/os:/path/on/shared/scratch/host/os"));
        j.setContainer(c);

        // set bypass on for the input files
        for (PegasusFile pf : j.getInputFiles()) {
            pf.setForBypassStaging();
        }
        // the bypass location is retrieved from the planner cache. set in there
        PlannerCache cache = new PlannerCache();
        cache.initialize(mBag, mDAG);
        cache.insert("f.in", sourceURL, "compute", FileServerType.OPERATION.get);
        mBag.add(PegasusBag.PLANNER_CACHE, cache);

        this.testStageIn("compute", expectedOutput);

        mLogger.logEventCompletion();
    }

    /**
     * PM-1789 symlink should be triggered only if compute site has a sharedFileSystem attribute set
     * to true on the shared scratch directory
     */
    @Test
    public void testSymlinkForStageIn() {
        FileTransfer expectedOutput = new FileTransfer();
        expectedOutput.setLFN("f.in");
        expectedOutput.setRegisterFlag(true);
        expectedOutput.setTransferFlag(true);

        this.mProps.setProperty(PegasusProperties.PEGASUS_TRANSFER_LINKS_PROPERTY_KEY, "true");

        // shared fs attribute not set on the directory. so no file url as source and symlink as
        // dest
        expectedOutput.addSource(
                "compute", "gsiftp://compute.isi.edu/workflows/compute/shared-scratch/./f.in");
        expectedOutput.addDestination("compute", "file://$PWD/f.in");

        this.testStageIn("compute", expectedOutput);
    }

    /**
     * PM-1789 symlink should be triggered only if compute site has a sharedFileSystem attribute set
     * to true on the shared scratch directory
     */
    @Test
    public void testSymlinkForStageInWithSharedFSAttributeSpecified() {
        FileTransfer expectedOutput = new FileTransfer();
        expectedOutput.setLFN("f.in");
        expectedOutput.setRegisterFlag(true);
        expectedOutput.setTransferFlag(true);

        this.mProps.setProperty(PegasusProperties.PEGASUS_TRANSFER_LINKS_PROPERTY_KEY, "true");

        SiteCatalogEntry computeSiteEntry = this.mBag.getHandleToSiteStore().lookup("compute");
        Directory sharedScratch = computeSiteEntry.getDirectory(Directory.TYPE.shared_scratch);
        sharedScratch.setSharedFileSystemAccess(true);

        // since compute and staging site are the same, source is a file url not a gsiftp
        expectedOutput.addSource(
                "compute", "file:///internal/workflows/compute/shared-scratch/./f.in");
        expectedOutput.addDestination("compute", "symlink://$PWD/f.in");

        this.testStageIn("compute", expectedOutput);
    }

    /** PM-1879 turn off symlink via a job profile */
    @Test
    public void testSymlinkTurnOffInProfileForStageIn() {
        FileTransfer expectedOutput = new FileTransfer();
        expectedOutput.setLFN("f.in");
        expectedOutput.setRegisterFlag(true);
        expectedOutput.setTransferFlag(true);

        this.mProps.setProperty(PegasusProperties.PEGASUS_TRANSFER_LINKS_PROPERTY_KEY, "true");

        SiteCatalogEntry computeSiteEntry = this.mBag.getHandleToSiteStore().lookup("compute");
        Directory sharedScratch = computeSiteEntry.getDirectory(Directory.TYPE.shared_scratch);
        sharedScratch.setSharedFileSystemAccess(true);

        // since compute and staging site are the same, source is a file url not a gsiftp
        expectedOutput.addSource(
                "compute", "file:///internal/workflows/compute/shared-scratch/./f.in");
        expectedOutput.addDestination("compute", "file://$PWD/f.in");

        // add a profile to turn off symlink
        Map<String, String> profiles = new HashMap();
        profiles.put(Pegasus.NO_SYMLINK_KEY, "true");

        this.testStageIn("compute", expectedOutput, profiles);
    }

    /** PM-1787 */
    @Test
    public void testSymlinkForStageInFromLocalToCompute() {
        FileTransfer expectedOutput = new FileTransfer();
        expectedOutput.setLFN("f.in");
        expectedOutput.setRegisterFlag(true);
        expectedOutput.setTransferFlag(true);

        this.mProps.setProperty(PegasusProperties.PEGASUS_TRANSFER_LINKS_PROPERTY_KEY, "true");

        // symlink is on but source and destination site are different. so symlink should not happen
        expectedOutput.addSource(
                "local", "gsiftp://local.isi.edu/workflows/local/shared-scratch/./f.in");
        expectedOutput.addDestination("compute", "file://$PWD/f.in");

        this.testStageIn("local", expectedOutput);
    }

    /** PM-1787 */
    @Test
    public void testSymlinkForStageInFromLocalToComputeWithAuxillaryLocal() {
        FileTransfer expectedOutput = new FileTransfer();
        expectedOutput.setLFN("f.in");
        expectedOutput.setRegisterFlag(true);
        expectedOutput.setTransferFlag(true);

        this.mProps.setProperty(PegasusProperties.PEGASUS_TRANSFER_LINKS_PROPERTY_KEY, "true");
        SiteCatalogEntry computeSiteEntry = this.mBag.getHandleToSiteStore().lookup("compute");
        computeSiteEntry.addProfile(new Profile("pegasus", Pegasus.LOCAL_VISIBLE_KEY, "true"));

        // source and destination site are different. symlink is triggered
        // as symlink is on auxillary local is set to true
        expectedOutput.addSource("local", "file:///internal/workflows/local/shared-scratch/./f.in");
        expectedOutput.addDestination("compute", "symlink://$PWD/f.in");

        this.testStageIn("local", expectedOutput);
    }

    /** PM-1787 */
    @Test
    public void testForStageInFromLocalToComputeWithAuxillaryLocal() {
        FileTransfer expectedOutput = new FileTransfer();
        expectedOutput.setLFN("f.in");
        expectedOutput.setRegisterFlag(true);
        expectedOutput.setTransferFlag(true);

        SiteCatalogEntry computeSiteEntry = this.mBag.getHandleToSiteStore().lookup("compute");
        computeSiteEntry.addProfile(new Profile("pegasus", Pegasus.LOCAL_VISIBLE_KEY, "true"));

        // source and destination site are different.
        // auxillary.local is true . so source url is file instead of gsiftp
        expectedOutput.addSource("local", "file:///internal/workflows/local/shared-scratch/./f.in");
        expectedOutput.addDestination("compute", "file://$PWD/f.in");

        this.testStageIn("local", expectedOutput);
    }

    @Test
    public void testDefaultStageOut() {
        FileTransfer expectedOutput = new FileTransfer();
        expectedOutput.setLFN("f.out");
        expectedOutput.setRegisterFlag(true);
        expectedOutput.setTransferFlag(true);
        expectedOutput.addSource("compute", "file://$PWD/f.out");
        expectedOutput.addDestination(
                "staging", "gsiftp://staging.isi.edu/workflows/staging/shared-scratch/./f.out");

        this.testStageOut("staging", expectedOutput);
    }

    @Test
    public void testDefaultStageOutToLocal() {
        FileTransfer expectedOutput = new FileTransfer();
        expectedOutput.setLFN("f.out");
        expectedOutput.setRegisterFlag(true);
        expectedOutput.setTransferFlag(true);
        expectedOutput.addSource("compute", "file://$PWD/f.out");
        // destination is a gsiftp url
        expectedOutput.addDestination(
                "local", "gsiftp://local.isi.edu/workflows/local/shared-scratch/./f.out");

        this.testStageOut("local", expectedOutput);
    }

    @Test
    public void testDefaultStageOutToLocalWithAuxillaryLocal() {
        SiteCatalogEntry computeSiteEntry = this.mBag.getHandleToSiteStore().lookup("compute");
        computeSiteEntry.addProfile(new Profile("pegasus", Pegasus.LOCAL_VISIBLE_KEY, "true"));

        FileTransfer expectedOutput = new FileTransfer();
        expectedOutput.setLFN("f.out");
        expectedOutput.setRegisterFlag(true);
        expectedOutput.setTransferFlag(true);
        expectedOutput.addSource("compute", "file://$PWD/f.out");
        // destination is a file url instead of gsiftp
        expectedOutput.addDestination(
                "local", "file:///internal/workflows/local/shared-scratch/./f.out");

        this.testStageOut("local", expectedOutput);
    }

    /**
     * PM-1789 file url substitution does not happen if sharedfs attribute is not set Even though
     * staging and compute site are same
     */
    @Test
    public void testDefaultStageOutToCompute() {
        FileTransfer expectedOutput = new FileTransfer();
        expectedOutput.setLFN("f.out");
        expectedOutput.setRegisterFlag(true);
        expectedOutput.setTransferFlag(true);
        expectedOutput.addSource("compute", "file://$PWD/f.out");
        expectedOutput.addDestination(
                "compute", "gsiftp://compute.isi.edu/workflows/compute/shared-scratch/./f.out");

        this.testStageOut("compute", expectedOutput);
    }

    /**
     * PM-1789 file url substitution only happens if sharedfs attribute is set and staging and
     * compute site are same
     */
    @Test
    public void testDefaultStageOutToComputeWithSharedFSAttributeSpecified() {
        SiteCatalogEntry computeSiteEntry = this.mBag.getHandleToSiteStore().lookup("compute");
        Directory sharedScratch = computeSiteEntry.getDirectory(Directory.TYPE.shared_scratch);
        sharedScratch.setSharedFileSystemAccess(true);

        FileTransfer expectedOutput = new FileTransfer();
        expectedOutput.setLFN("f.out");
        expectedOutput.setRegisterFlag(true);
        expectedOutput.setTransferFlag(true);
        expectedOutput.addSource("compute", "file://$PWD/f.out");
        // destination is a file url instead of gsiftp
        expectedOutput.addDestination(
                "compute", "file:///internal/workflows/compute/shared-scratch/./f.out");

        this.testStageOut("compute", expectedOutput);
    }

    public void testUseFileURLAsSource(
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
                "use file URL as source:",
                expectedValue,
                t.useFileURLAsSource(compute, stagingSite));
        mLogger.logEventCompletion();
    }

    public void testSymlinkingEnabledForJob(
            Job job, boolean workflowSymlinking, boolean expectedValue) {
        mLogger.logEventStart("test.transfer.sls.transfer", "set", Integer.toString(mTestNumber++));
        Transfer t = new Transfer();
        t.initialize(mBag);
        assertEquals(
                "Symlinking enabled for job:",
                expectedValue,
                t.symlinkingEnabled(job, workflowSymlinking));
        mLogger.logEventCompletion();
    }

    public void testSymlinkingEnabledForFile(
            PegasusFile pf,
            boolean symlinkingEnabledForJob,
            boolean useFileURLAsSource,
            boolean expectedValue) {
        mLogger.logEventStart("test.transfer.sls.transfer", "set", Integer.toString(mTestNumber++));
        Transfer t = new Transfer();
        t.initialize(mBag);
        assertEquals(
                "Symlinking enabled for file:",
                expectedValue,
                t.symlinkingEnabled(pf, symlinkingEnabledForJob, useFileURLAsSource));
        mLogger.logEventCompletion();
    }

    private void testSourceFileURLForContainerizedJob(
            String sourceURL, Container.MountPoint mp, String expectedReplacedURL) {
        mLogger.logEventStart("test.transfer.sls.transfer", "set", Integer.toString(mTestNumber++));
        Transfer t = new Transfer();
        t.initialize(mBag);
        ReplicaCatalogEntry source = new ReplicaCatalogEntry(sourceURL);
        Container c = new Container("centos-9");
        if (mp != null) {
            c.addMountPoint(mp);
        }
        t.updateSourceFileURLForContainerizedJob(c, new PegasusFile("f.in"), source, "ID1");
        assertEquals("source file url in containerized jobs", expectedReplacedURL, source.getPFN());
        mLogger.logEventCompletion();
    }

    private void testStageOut(String stagingSite, FileTransfer expected) {

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

    private void testStageIn(String stagingSite, FileTransfer expected) {
        this.testStageIn(stagingSite, expected, new HashMap<String, String>());
    }

    private void testStageIn(
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
                        job, inputFile.getLFN(), stagingSiteServer, stagingSiteDirectory, "$PWD");
        // System.err.println(result);
        assertNotNull(result);
        assertEquals(1, result.size());
        testFileTransfer(expected, (FileTransfer) result.toArray()[0]);
        mLogger.logEventCompletion();
    }

    private void testFileTransfer(FileTransfer expected, FileTransfer actual) {
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

    private ADag constructTestWorkflow() {

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

    private SiteStore constructTestSiteStore() {
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
