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
import edu.isi.pegasus.planner.catalog.ReplicaCatalog;
import edu.isi.pegasus.planner.catalog.classes.SysInfo;
import edu.isi.pegasus.planner.catalog.replica.ReplicaFactory;
import edu.isi.pegasus.planner.catalog.site.classes.Directory;
import edu.isi.pegasus.planner.catalog.site.classes.FileServer;
import edu.isi.pegasus.planner.catalog.site.classes.FileServerType;
import edu.isi.pegasus.planner.catalog.site.classes.InternalMountPoint;
import edu.isi.pegasus.planner.catalog.site.classes.SiteCatalogEntry;
import edu.isi.pegasus.planner.catalog.site.classes.SiteStore;
import edu.isi.pegasus.planner.classes.ADag;
import edu.isi.pegasus.planner.classes.DAXJob;
import edu.isi.pegasus.planner.classes.FileTransfer;
import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.classes.PegasusFile;
import edu.isi.pegasus.planner.classes.PlannerOptions;
import edu.isi.pegasus.planner.common.PegasusProperties;
import edu.isi.pegasus.planner.mapper.OutputMapperFactory;
import edu.isi.pegasus.planner.mapper.StagingMapperFactory;
import edu.isi.pegasus.planner.test.DefaultTestSetup;
import edu.isi.pegasus.planner.test.TestSetup;
import edu.isi.pegasus.planner.transfer.refiner.RefinerFactory;
import java.io.File;
import java.io.IOException;
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

    /**
     * For PM-1608 For a pegasusWorkflow/DAXJob A there is an output file specified with transfer
     * set to true. The output file is generated by some job X in the sub workflow referred to by
     * the DAXJob A . So in addition to a File Transfer being generated, the output map file created
     * for the sub workflow should log the source URL location in the FileTransfer that is
     * generated.
     */
    @Test
    public void testPegasusWorkflowJobStageOutToOutputSite() {
        mLogger.logEventStart(
                "test.transfer.generator.stageout", "set", Integer.toString(mTestNumber++));

        String outputSite = "output";
        String computeSite = "local";
        // make sure planner options are set also to the output site
        mBag.getPlannerOptions().addOutputSite(outputSite);

        StageOut so = new StageOut();
        so.initalize(mDAG, mBag, RefinerFactory.loadInstance(mDAG, mBag));
        DAXJob job = (DAXJob) mDAG.getNode("pegasus-plan_ID2").getContent();

        // staging site in this case is same as compute site
        job.setStagingSiteHandle(job.getSiteHandle());

        Collection<FileTransfer>[] soFTX = so.constructFileTX(job, outputSite);
        job.closeOutputMapper();
        assertEquals(soFTX.length, 2);

        // compute site has a gridftp (non file) server and output site a gsiftp
        // So stageout transfers will happen locally
        assertTrue(soFTX[1].isEmpty());
        assertNotNull(soFTX[0]);
        Collection<FileTransfer> localStageOutFtxs = soFTX[0];
        assertEquals(1, localStageOutFtxs.size());

        FileTransfer actualOutput = (FileTransfer) localStageOutFtxs.toArray()[0];
        FileTransfer expectedOutput = new FileTransfer();
        String expectedSource = "gsiftp://local.isi.edu/workflows/local/shared-scratch/./f.d";
        expectedOutput.setLFN("f.d");
        expectedOutput.setRegisterFlag(true);
        expectedOutput.setTransferFlag(true);
        expectedOutput.addSource(computeSite, expectedSource);
        expectedOutput.addDestination("output", "gsiftp://output.isi.edu/workflows/output/f.d");

        testFileTransfer(expectedOutput, actualOutput);

        // additional check for the output map file
        String mapperPath = job.getOutputMapperBackendPath();
        assertNotNull(mapperPath);
        ReplicaCatalog rc = this.loadMapperBackend(mapperPath);
        assertNotNull(rc);
        assertEquals(expectedSource, rc.lookup("f.d", computeSite));

        // make sure the mapper file is deleted
        File mapperFile = new File(mapperPath);
        mapperFile.delete();

        mLogger.logEventCompletion();
    }

    /**
     * For PM-1795 a test where a system property is set indicating a path to a non existent mapper
     * replica file. Test ensures that the correct mapper file specified in the planner options is
     * loaded.
     */
    @Test
    public void testStageOutToOutputMapperLocationWithSystemPropertySet() throws IOException {
        mLogger.logEventStart(
                "test.transfer.generator.stageout", "set", Integer.toString(mTestNumber++));

        String outputSite = "output";
        // make sure planner options are set also to the output site
        mBag.getPlannerOptions().addOutputSite(outputSite);

        String expectedMapSite = "random";
        String expectedMapPFN = "gsiftp://random.isi.edu/f.out";
        // create a temporary map file for this test and insert an entry in there
        File mapFile = File.createTempFile("pegasus", ".output.map", new File("."));
        String key =
                OutputMapperFactory.PROPERTY_KEY
                        + "."
                        + "replica.file"; // pegasus.dir.storage.mapper.replica.file
        try {
            ReplicaCatalog rc = this.loadMapperBackend(mapFile.getAbsolutePath());
            rc.insert("f.out", expectedMapPFN, expectedMapSite);
            rc.close();
            mBag.getPlannerOptions().setOutputMap(mapFile.getAbsolutePath());

            // PM-1795 set a system property to override the output mapper replica back
            System.setProperty(key, "/does/not/exist/file");
            mLogger.log(
                    "Set incorrect System property " + key + "->" + System.getProperty(key),
                    LogManager.INFO_MESSAGE_LEVEL);

            this.testStageOutToOutputMapperLocation(outputSite, expectedMapSite, expectedMapPFN);
        } finally {
            // delete the map file generated in the test
            mapFile.delete();
            // remove the set properties
            System.getProperties().remove(key);
        }

        mLogger.logEventCompletion();
    }

    /**
     * For PM-1608 this test simulates the case when a sub workflow is planned, and is given an
     * output map file that contains locations of where the output files of a compute job should be
     * placed.
     */
    @Test
    public void testStageOutToOutputMapperLocation() throws IOException {
        mLogger.logEventStart(
                "test.transfer.generator.stageout", "set", Integer.toString(mTestNumber++));

        String outputSite = "output";
        // make sure planner options are set also to the output site
        mBag.getPlannerOptions().addOutputSite(outputSite);

        String expectedMapSite = "random";
        String expectedMapPFN = "gsiftp://random.isi.edu/f.out";
        // create a temporary map file for this test and insert an entry in there
        File mapFile = File.createTempFile("pegasus", "output.map", new File("."));
        try {
            ReplicaCatalog rc = this.loadMapperBackend(mapFile.getAbsolutePath());
            rc.insert("f.out", expectedMapPFN, expectedMapSite);
            rc.close();
            mBag.getPlannerOptions().setOutputMap(mapFile.getAbsolutePath());
            this.testStageOutToOutputMapperLocation(outputSite, expectedMapSite, expectedMapPFN);
        } finally {

            // delete the map file generated in the test
            mapFile.delete();
        }

        mLogger.logEventCompletion();
    }

    /**
     * A general unit test to test output mapper specified on the command line
     *
     * @param outputSite the output site
     * @param expectedMapSite the map site in the map file
     * @param expectedMapPFN the expected pfn
     * @throws IOException
     */
    private void testStageOutToOutputMapperLocation(
            String outputSite, String expectedMapSite, String expectedMapPFN) throws IOException {

        StageOut so = new StageOut();
        so.initalize(mDAG, mBag, RefinerFactory.loadInstance(mDAG, mBag));
        Job job = (Job) mDAG.getNode("preprocess_ID1").getContent();

        // staging site in this case is same as compute site
        job.setStagingSiteHandle(job.getSiteHandle());

        // test for location of f.out that is staged to output site
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

        // test for location of f.out that is staged to output map location
        // achieved by setting output site to null
        soFTX = so.constructFileTX(job, null);
        assertEquals(soFTX.length, 2);

        // compute site has a gridftp (non file) server and output site a gsiftp
        // So stageout transfers will happen locally
        assertTrue(soFTX[1].isEmpty());
        assertNotNull(soFTX[0]);
        localStageOutFtxs = soFTX[0];
        assertEquals(1, localStageOutFtxs.size());

        actualOutput = (FileTransfer) localStageOutFtxs.toArray()[0];
        expectedOutput = new FileTransfer();
        expectedOutput.setLFN("f.out");
        expectedOutput.setRegisterFlag(false);
        expectedOutput.setTransferFlag(true);
        expectedOutput.addSource(
                "compute", "gsiftp://compute.isi.edu/workflows/compute/shared-scratch/./f.out");
        expectedOutput.addDestination(expectedMapSite, expectedMapPFN);

        testFileTransfer(expectedOutput, actualOutput);
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

        DAXJob dJob = new DAXJob();
        dJob.setDAXFile("blackdiamond.yml");
        dJob.setTXName("pegasus-plan");
        dJob.setSiteHandle("local");
        dJob.setLogicalID("ID2");
        dJob.setName("pegasus-plan_ID2");
        output = new PegasusFile("f.d");
        output.setLinkage(PegasusFile.LINKAGE.output);
        dJob.addOutputFile(output);

        dag.add(j);
        dag.add(dJob);
        dag.addEdge(j.getID(), dJob.getID());
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

    /** */
    private ReplicaCatalog loadMapperBackend(String mapperPath) {
        PegasusBag b = new PegasusBag();
        b.add(PegasusBag.PEGASUS_LOGMANAGER, this.mLogger);

        // set the properties for initialization
        PegasusProperties p = PegasusProperties.nonSingletonInstance();
        // set the appropriate property to designate path to file
        p.setProperty(ReplicaCatalog.c_prefix, ReplicaFactory.FILE_CATALOG_IMPLEMENTOR);
        p.setProperty(ReplicaCatalog.c_prefix + "." + ReplicaCatalog.FILE_KEY, mapperPath);

        b.add(PegasusBag.PEGASUS_PROPERTIES, p);
        ReplicaCatalog rc = null;
        try {
            rc = ReplicaFactory.loadInstance(b);
        } catch (Exception e) {
            throw new RuntimeException(
                    "Unable to initialize job output mapper in the test Directory  " + mapperPath,
                    e);
        }
        return rc;
    }
}
