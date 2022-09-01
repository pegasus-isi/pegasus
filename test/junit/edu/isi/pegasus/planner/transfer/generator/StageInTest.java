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
package edu.isi.pegasus.planner.transfer.generator;

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
import edu.isi.pegasus.planner.classes.ADag;
import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.classes.PegasusFile;
import edu.isi.pegasus.planner.classes.PlannerOptions;
import edu.isi.pegasus.planner.classes.Profile;
import edu.isi.pegasus.planner.common.PegasusConfiguration;
import edu.isi.pegasus.planner.common.PegasusProperties;
import edu.isi.pegasus.planner.mapper.StagingMapperFactory;
import edu.isi.pegasus.planner.namespace.Pegasus;
import edu.isi.pegasus.planner.test.DefaultTestSetup;
import edu.isi.pegasus.planner.test.TestSetup;
import edu.isi.pegasus.planner.transfer.refiner.RefinerFactory;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/** @author Karan Vahi */
public class StageInTest {

    private PegasusBag mBag;

    private PegasusProperties mProps;

    private LogManager mLogger;

    private TestSetup mTestSetup;

    private ADag mDAG;

    private static int mTestNumber = 1;

    @BeforeClass
    public static void setUpClass() {}

    @AfterClass
    public static void tearDownClass() {}

    public StageInTest() {}

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
        mLogger.logEventStart("test.transfer.generator.stagein", "setup", "0");
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

    /** Bypass staging does not work for shared fs, as there is no PegasusLite involved. */
    @Test
    public void testBypassForSharedFS() {

        testBypass(
                new ReplicaCatalogEntry("http://example.isi.edu/input/f.a", "nonlocal"),
                PegasusConfiguration.SHARED_FS_CONFIGURATION_VALUE,
                false);
    }

    @Test
    /**
     * After PM-1885 this should be true, as in CondorIO mode we let pegasus-transfer handle bypass
     * files in the PegasusLite script
     */
    public void testBypassForCondorIOWithNonFileURL() {
        testBypass(
                new ReplicaCatalogEntry("http://example.isi.edu/input/f.in", "compute"),
                PegasusConfiguration.CONDOR_CONFIGURATION_VALUE,
                true);
    }

    /**
     * Test to ensure there is bypass in condor io mode , if the replica catalog location is a file
     * url on compute site. After PM-1885 this should be true, as in CondorIO mode we let
     * pegasus-transfer handle bypass files in the PegasusLite script
     */
    @Test
    public void testBypassForCondorIOWithFileURLOnComputeSite() {
        testBypass(
                new ReplicaCatalogEntry("file:///input/f.a", "compute"),
                PegasusConfiguration.CONDOR_CONFIGURATION_VALUE,
                true);
    }

    /**
     * Test to ensure there is bypass in condor io mode , if the replica catalog location is a file
     * URL on the local site. After PM-1885 this should be false, as in CondorIO mode we let
     * pegasus-transfer handle bypass files in the PegasusLite script
     */
    @Test
    public void testBypassForCondorIOWithFileURLOnLocalSite() {
        testBypass(
                new ReplicaCatalogEntry("file:///input/f.in", "local"),
                PegasusConfiguration.CONDOR_CONFIGURATION_VALUE,
                false);
    }

    /**
     * PM-1875, PM-1885 Test to ensure there is bypass in condor io mode , if the replica catalog
     * location is a file URL on the local site, but the PFN does not end in the basename of the LFN
     * After PM-1885 this should be false, as in CondorIO mode we let pegasus-transfer handle bypass
     * files in the PegasusLite script
     */
    @Test
    public void testBypassForCondorIOWithFileURLOnLocalSiteWithRandomBasename() {
        testBypass(
                new ReplicaCatalogEntry("file:///input/f.random", "local"),
                PegasusConfiguration.CONDOR_CONFIGURATION_VALUE,
                false);
    }

    /**
     * PM-1875, PM-1885 Test to ensure there is bypass in condor io mode , if the replica catalog
     * location is a file URL on the compute site, but the PFN does not end in the basename of the
     * LFN
     */
    @Test
    public void testBypassForCondorIOWithFileURLOnComputeSiteWithDeepLFN() {
        // update the test workflow for this first
        Job job = (Job) mDAG.getNode("preprocess_ID1").getContent();
        PegasusFile inputFile = (PegasusFile) job.getInputFiles().toArray()[0];
        inputFile.setLFN("deep/f.in");
        testBypass(
                new ReplicaCatalogEntry("file:///input/deep/f.in", "compute"),
                PegasusConfiguration.CONDOR_CONFIGURATION_VALUE,
                true);
    }

    /**
     * Test to ensure there is bypass in condorio mode for file URL's on compute site, when compute
     * site has auxillary.local set to true PM-1783
     */
    @Test
    public void testBypassForCondorIOWithFileURLOnLocalWithAuxillaryLocal() {
        SiteStore s = mBag.getHandleToSiteStore();
        SiteCatalogEntry computeSite = s.lookup("compute");

        // add the profile
        computeSite.addProfile(new Profile("pegasus", Pegasus.LOCAL_VISIBLE_KEY, "true"));

        testBypass(
                new ReplicaCatalogEntry("file:///input/f.in", "compute"),
                PegasusConfiguration.CONDOR_CONFIGURATION_VALUE,
                true);
    }

    /** Test to ensure there is bypass in nonsharedfs mode for file URL's on compute site */
    @Test
    public void testBypassForNonSharedFSWithFileURLOnCompute() {

        testBypass(
                new ReplicaCatalogEntry("file:///input/f.in", "compute"),
                PegasusConfiguration.NON_SHARED_FS_CONFIGURATION_VALUE,
                true);
    }

    /**
     * Test to ensure there is bypass in nonsharedfs mode we dont care about the basename of the
     * PFN. The rename happens when pegasus-transfer is invoked
     */
    @Test
    public void testBypassForNonSharedFSWithRandomeBasenameOnCompute() {

        testBypass(
                new ReplicaCatalogEntry("file:///input/f.random", "compute"),
                PegasusConfiguration.NON_SHARED_FS_CONFIGURATION_VALUE,
                true);
    }

    /** Test to ensure there is bypass in nonsharedfs mode for non file URL's */
    @Test
    public void testBypassForNonSharedFSWithNonFileURLOnCompute() {

        testBypass(
                new ReplicaCatalogEntry("http://example.isi.edu/input/f.in", "compute"),
                PegasusConfiguration.NON_SHARED_FS_CONFIGURATION_VALUE,
                true);
    }

    /** Test to ensure there is bypass in nonsharedfs mode for file URL's on local site */
    @Test
    public void testBypassForNonSharedFSWithFileURLOnLocal() {

        testBypass(
                new ReplicaCatalogEntry("file:///input/f.in", "local"),
                PegasusConfiguration.NON_SHARED_FS_CONFIGURATION_VALUE,
                false);
    }

    /**
     * Test to ensure there is bypass in nonsharedfs mode for file URL's on local site, when compute
     * site has auxillary.local set to true PM-1783
     */
    @Test
    public void testBypassForNonSharedFSWithFileURLOnLocalWithAuxillaryLocal() {
        SiteStore s = mBag.getHandleToSiteStore();
        SiteCatalogEntry computeSite = s.lookup("compute");

        // add the profile
        computeSite.addProfile(new Profile("pegasus", Pegasus.LOCAL_VISIBLE_KEY, "true"));

        testBypass(
                new ReplicaCatalogEntry("file:///input/f.in", "local"),
                PegasusConfiguration.NON_SHARED_FS_CONFIGURATION_VALUE,
                true);
    }

    /** Test to ensure there is bypass in nonsharedfs mode for non file URL's */
    @Test
    public void testBypassForNonSharedFSWithNonFileURLOnLocal() {

        testBypass(
                new ReplicaCatalogEntry("http://example.isi.edu/input/f.in", "local"),
                PegasusConfiguration.NON_SHARED_FS_CONFIGURATION_VALUE,
                true);
    }

    /**
     * Test to test when nothing is set in properties for workflow symlinking or profiles for job
     */
    @Test
    public void testSymlinkingEnabledWithNothingSet() {
        this.testSymlinkingEnabled(false, false, null);
    }

    /** Test to test for default symlinking, when symlinking turned on for workflow */
    @Test
    public void testDefaultSymlinkingEnabled() {
        this.testSymlinkingEnabled(true, true, null);
    }

    /** Test to test for symlinking when symlinking turned on for workflow but off for job */
    @Test
    public void testSymlinkingEnabledWhenProfileSetTrue() {
        this.testSymlinkingEnabled(false, true, true);
    }

    /** Test to test for symlinking when symlinking turned on for workflow but also for job */
    @Test
    public void testSymlinkingEnabledWhenProfileSetFalse() {
        this.testSymlinkingEnabled(true, true, false);
    }

    /** Test to test for symlinking when symlinking turned off for workflow and also for job */
    @Test
    public void testSymlinkingEnabledWhenWorkflowOffAndProfileSetFalse() {
        this.testSymlinkingEnabled(false, false, false);
    }

    /** Test to test for symlinking when symlinking turned off for workflow and also for job */
    @Test
    public void testSymlinkingEnabledWhenWorkflowOffAndProfileSetTrue() {
        this.testSymlinkingEnabled(false, false, true);
    }

    private void testSymlinkingEnabled(
            boolean expected, boolean workflowSymlinking, Boolean noSymlinkProfileValue) {
        mLogger.logEventStart(
                "test.transfer.generator.stagein", "set", Integer.toString(mTestNumber++));
        StageIn si = new StageIn();
        Job j = new Job();
        if (noSymlinkProfileValue != null) {
            j.vdsNS.construct(Pegasus.NO_SYMLINK_KEY, noSymlinkProfileValue.toString());
        }
        assertEquals(
                "Symlinking for job was ", expected, si.symlinkingEnabled(j, workflowSymlinking));
        mLogger.logEventCompletion();
    }

    /**
     * Convenience method
     *
     * @param rce
     * @param dataConfig
     */
    private void testBypass(ReplicaCatalogEntry rce, String dataConfig, boolean expected) {
        mLogger.logEventStart(
                "test.transfer.generator.stagein", "set", Integer.toString(mTestNumber++));

        String computeSite = "compute";
        StageIn si = new StageIn();
        SiteStore s = mBag.getHandleToSiteStore();

        // for sharedfs setting bypass staging should have no effect
        PegasusProperties props = mBag.getPegasusProperties();
        props.setProperty(
                PegasusProperties.PEGASUS_TRANSFER_BYPASS_INPUT_STAGING_PROPERTY_KEY, "true");

        Job job = (Job) mDAG.getNode("preprocess_ID1").getContent();
        PegasusFile inputFile = (PegasusFile) job.getInputFiles().toArray()[0];
        si.initalize(mDAG, mBag, RefinerFactory.loadInstance(mDAG, mBag));

        // In TransferEngine the data configuration is already associated at per job level
        // so set profile instead of properties
        job.setDataConfiguration(dataConfig);

        assertEquals(expected, si.bypassStaging(rce, inputFile, job, s.lookup(computeSite)));
        mLogger.logEventCompletion();
    }

    @After
    public void tearDown() {
        mLogger = null;
        mProps = null;
        mBag = null;
        mTestSetup = null;
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
        dir.setInternalMountPoint(new InternalMountPoint("/workflows/compute/shared-scratch"));
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
        dir.setInternalMountPoint(new InternalMountPoint("/workflows/staging/shared-scratch"));
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
        dir.setInternalMountPoint(new InternalMountPoint("/workflows/local/shared-scratch"));
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
