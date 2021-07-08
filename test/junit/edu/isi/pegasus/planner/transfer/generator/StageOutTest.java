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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.common.util.PegasusURL;
import edu.isi.pegasus.planner.catalog.classes.SysInfo;
import edu.isi.pegasus.planner.catalog.site.classes.Directory;
import edu.isi.pegasus.planner.catalog.site.classes.FileServer;
import edu.isi.pegasus.planner.catalog.site.classes.FileServerType;
import edu.isi.pegasus.planner.catalog.site.classes.InternalMountPoint;
import edu.isi.pegasus.planner.catalog.site.classes.SiteCatalogEntry;
import edu.isi.pegasus.planner.catalog.site.classes.SiteStore;
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
import edu.isi.pegasus.planner.transfer.refiner.RefinerFactory;
import java.util.Collection;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/** @author Karan Vahi */
public class StageOutTest {

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

    public StageOutTest() {}

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
        mLogger.logEventStart("test.transfer.generator.stageout", "setup", "0");
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

    /**
     * Tests file transfer for a file for which the output file transfer flag is set to false, but
     * the registration flag is true. In that case the FileTransfer object generated should have the
     * destination URL same as the source URL.
     */
    @Test
    public void testStageOutToOutputSiteDisabled() {
        mLogger.logEventStart(
                "test.transfer.generator.stageout", "set", Integer.toString(mTestNumber++));

        String outputSite = "output";
        // make sure planner options are set also to the output site
        mBag.getPlannerOptions().addOutputSite(outputSite);

        StageOut so = new StageOut();
        so.initalize(mDAG, mBag, RefinerFactory.loadInstance(mDAG, mBag));
        Job job = (Job) mDAG.getNode("preprocess_ID1").getContent();
        PegasusFile outputFile = (PegasusFile) job.getOutputFiles().toArray()[0];
        // force output file to have transfer flag set to false
        outputFile.setTransferFlag(false);

        // staging site in this case is same as compute site
        job.setStagingSiteHandle(job.getSiteHandle());

        Collection<FileTransfer>[] soFTX = so.constructFileTX(job, outputSite);
        assertEquals(soFTX.length, 2);

        // in this case only a local file transfer object is created
        assertTrue(soFTX[1].isEmpty());
        assertNotNull(soFTX[0]);
        Collection<FileTransfer> remoteStageOutFtxs = soFTX[0];
        assertEquals(1, remoteStageOutFtxs.size());

        FileTransfer actualOutput = (FileTransfer) remoteStageOutFtxs.toArray()[0];
        FileTransfer expectedOutput = new FileTransfer();
        expectedOutput.setLFN("f.out");
        expectedOutput.setRegisterFlag(true);
        expectedOutput.setTransferFlag(false);
        expectedOutput.addSource(
                "compute", "gsiftp://compute.isi.edu/workflows/compute/shared-scratch/./f.out");
        expectedOutput.addDestination(
                "compute", "gsiftp://compute.isi.edu/workflows/compute/shared-scratch/./f.out");

        testFileTransfer(expectedOutput, actualOutput);
        mLogger.logEventCompletion();
    }

    @Test
    public void testFileSharedFSStageOutToOutputSite() {
        mLogger.logEventStart(
                "test.transfer.generator.stageout", "set", Integer.toString(mTestNumber++));

        String outputSite = "output";
        // make sure planner options are set also to the output site
        mBag.getPlannerOptions().addOutputSite(outputSite);

        SiteStore s = mBag.getHandleToSiteStore();
        SiteCatalogEntry computeSite = s.lookup("compute");

        // remove the existing shared scratch dir and instead setup a new one
        // with file based server
        computeSite.remove(Directory.TYPE.shared_scratch);
        Directory dir = new Directory();
        dir.setType(Directory.TYPE.shared_scratch);
        dir.setInternalMountPoint(new InternalMountPoint("/workflows/compute/shared-scratch"));
        FileServer fs = new FileServer();
        fs.setSupportedOperation(FileServerType.OPERATION.get);
        PegasusURL url = new PegasusURL("/workflows/compute/shared-scratch");
        fs.setURLPrefix(url.getURLPrefix());
        fs.setProtocol(url.getProtocol());
        fs.setMountPoint(url.getPath());
        dir.addFileServer(fs);
        computeSite.addDirectory(dir);

        StageOut so = new StageOut();
        so.initalize(mDAG, mBag, RefinerFactory.loadInstance(mDAG, mBag));
        Job job = (Job) mDAG.getNode("preprocess_ID1").getContent();

        // staging site in this case is same as compute site
        job.setStagingSiteHandle(job.getSiteHandle());

        Collection<FileTransfer>[] soFTX = so.constructFileTX(job, outputSite);
        assertEquals(soFTX.length, 2);

        // compute site has a file server and output site a gsiftp
        // no stageout transfers locally
        assertTrue(soFTX[0].isEmpty());
        assertNotNull(soFTX[1]);
        Collection<FileTransfer> remoteStageOutFtxs = soFTX[1];
        assertEquals(1, remoteStageOutFtxs.size());

        FileTransfer actualOutput = (FileTransfer) remoteStageOutFtxs.toArray()[0];
        FileTransfer expectedOutput = new FileTransfer();
        expectedOutput.setLFN("f.out");
        expectedOutput.setRegisterFlag(true);
        expectedOutput.setTransferFlag(true);
        expectedOutput.addSource("compute", "file:///workflows/compute/shared-scratch/./f.out");
        expectedOutput.addDestination("output", "gsiftp://output.isi.edu/workflows/output/f.out");

        testFileTransfer(expectedOutput, actualOutput);
        mLogger.logEventCompletion();
    }

    /** The default case where the compute site has a remotely accessible server such as Gridftp. */
    @Test
    public void testSharedFSStageOutToOutputSite() {
        mLogger.logEventStart(
                "test.transfer.generator.stageout", "set", Integer.toString(mTestNumber++));

        String outputSite = "output";
        // make sure planner options are set also to the output site
        mBag.getPlannerOptions().addOutputSite(outputSite);

        StageOut so = new StageOut();
        so.initalize(mDAG, mBag, RefinerFactory.loadInstance(mDAG, mBag));
        Job job = (Job) mDAG.getNode("preprocess_ID1").getContent();

        // staging site in this case is same as compute site
        job.setStagingSiteHandle(job.getSiteHandle());

        Collection<FileTransfer>[] soFTX = so.constructFileTX(job, outputSite);
        assertEquals(soFTX.length, 2);

        // compute site has a gridftp (non file) server and output site a gsiftp
        // So stageout transfers will happen locally
        assertTrue(soFTX[1].isEmpty());
        assertNotNull(soFTX[0]);
        Collection<FileTransfer> localStageOutFtxs = soFTX[0];
        assertEquals(1, localStageOutFtxs.size());

        FileTransfer actualOutput = (FileTransfer) localStageOutFtxs.toArray()[0];
        FileTransfer expectedOutput = new FileTransfer();
        expectedOutput.setLFN("f.out");
        expectedOutput.setRegisterFlag(true);
        expectedOutput.setTransferFlag(true);
        expectedOutput.addSource(
                "compute", "gsiftp://compute.isi.edu/workflows/compute/shared-scratch/./f.out");
        expectedOutput.addDestination("output", "gsiftp://output.isi.edu/workflows/output/f.out");

        testFileTransfer(expectedOutput, actualOutput);
        mLogger.logEventCompletion();
    }

    /** For PM-1779 */
    @Test
    public void testFileSharedFSStageOutToComputeSite() {
        mLogger.logEventStart(
                "test.transfer.generator.stageout", "set", Integer.toString(mTestNumber++));

        String outputSite = "compute";
        // make sure planner options are set also to the output site
        mBag.getPlannerOptions().addOutputSite(outputSite);

        // for this test, we need to first add a shared-storage to the compute site since
        // that is where we are transferring outputs to
        SiteStore s = mBag.getHandleToSiteStore();
        // compute and output site are same
        SiteCatalogEntry computeSite = s.lookup(outputSite);
        Directory dir = new Directory();
        dir.setType(Directory.TYPE.shared_storage);
        dir.setInternalMountPoint(new InternalMountPoint("/workflows/compute/shared-storage"));
        FileServer fs = new FileServer();
        fs.setSupportedOperation(FileServerType.OPERATION.all);
        PegasusURL url = new PegasusURL("/workflows/compute/shared-storage");
        fs.setURLPrefix(url.getURLPrefix());
        fs.setProtocol(url.getProtocol());
        fs.setMountPoint(url.getPath());
        dir.addFileServer(fs);
        computeSite.addDirectory(dir);

        // remove the existing shared scratch dir and instead setup a new one
        // with file based server
        computeSite.remove(Directory.TYPE.shared_scratch);
        dir = new Directory();
        dir.setType(Directory.TYPE.shared_scratch);
        dir.setInternalMountPoint(new InternalMountPoint("/workflows/compute/shared-scratch"));
        fs = new FileServer();
        fs.setSupportedOperation(FileServerType.OPERATION.get);
        url = new PegasusURL("/workflows/compute/shared-scratch");
        fs.setURLPrefix(url.getURLPrefix());
        fs.setProtocol(url.getProtocol());
        fs.setMountPoint(url.getPath());
        dir.addFileServer(fs);
        computeSite.addDirectory(dir);

        StageOut so = new StageOut();
        so.initalize(mDAG, mBag, RefinerFactory.loadInstance(mDAG, mBag));
        Job job = (Job) mDAG.getNode("preprocess_ID1").getContent();

        // staging site in this case is same as compute site
        job.setStagingSiteHandle(job.getSiteHandle());

        Collection<FileTransfer>[] soFTX = so.constructFileTX(job, outputSite);
        assertEquals(soFTX.length, 2);

        // compute site has a file server and output site a gsiftp
        // no stageout transfers locally
        assertTrue(soFTX[0].isEmpty());
        assertNotNull(soFTX[1]);
        Collection<FileTransfer> remoteStageOutFtxs = soFTX[1];
        assertEquals(1, remoteStageOutFtxs.size());

        FileTransfer actualOutput = (FileTransfer) remoteStageOutFtxs.toArray()[0];
        FileTransfer expectedOutput = new FileTransfer();
        expectedOutput.setLFN("f.out");
        expectedOutput.setRegisterFlag(true);
        expectedOutput.setTransferFlag(true);
        expectedOutput.addSource("compute", "file:///workflows/compute/shared-scratch/./f.out");
        expectedOutput.addDestination("compute", "file:///workflows/compute/shared-storage/f.out");

        testFileTransfer(expectedOutput, actualOutput);
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

        return store;
    }
}