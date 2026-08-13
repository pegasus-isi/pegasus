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
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.*;

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
import edu.isi.pegasus.planner.classes.PlannerCache;
import edu.isi.pegasus.planner.classes.PlannerOptions;
import edu.isi.pegasus.planner.common.PegasusProperties;
import edu.isi.pegasus.planner.mapper.StagingMapperFactory;
import edu.isi.pegasus.planner.namespace.Pegasus;
import edu.isi.pegasus.planner.test.DefaultTestSetup;
import edu.isi.pegasus.planner.test.TestSetup;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.Collection;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.Set;

/**
 * Unit tests for CondorIO mode, PegasusLite transfers
 *
 * @author Karan Vahi
 */
public class CondorTest {
    private PegasusBag mBag;

    private PegasusProperties mProps;

    private LogManager mLogger;

    private TestSetup mTestSetup;

    private ADag mDAG;

    private static int mTestNumber = 1;

    public CondorTest() {}

    /** Setup the logger and properties that all test functions require */
    @BeforeEach
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

        // we don'cTX care for type of staging mapper
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

    @Test
    public void testDefaultStageIn() {
        // unless there is bypass staging. everything happens w.r.t to the intialdir
        // so files are relative paths
        this.testStageIn("local", "f.in");
    }

    /**
     * PM-1875 For deep LFN's we generate FileTransfer pair to be handled by pegasus-transfer. Both
     * source and destination are compute site, since condor file io has transferred the file from
     * submit host to compute site before this kicks in.
     */
    @Test
    public void testStageInForDeepLFN() {
        String lfn = "deep/f.in";
        Job job = (Job) mDAG.getNode("preprocess_ID1").getContent();
        PegasusFile inputFile = (PegasusFile) job.getInputFiles().toArray()[0];
        inputFile.setLFN(lfn);

        FileTransfer expectedOutput = new FileTransfer();
        expectedOutput.setLFN(lfn);
        expectedOutput.setRegisterFlag(true);
        expectedOutput.setTransferFlag(true);
        expectedOutput.addSource(
                "compute", "file://$pegasus_lite_work_dir/" + new File(lfn).getName());
        expectedOutput.addDestination("compute", "moveto://$PWD/" + lfn);

        this.testStageIn("local", expectedOutput);
    }

    /**
     * GH-2106 For deep LFN's with the same basename, in case of HTCondor/CEDAR overwriting of files
     * happen
     */
    @Test
    public void testStageInForDeepLFNWAWConflict() {
        String[] lfns = new String[2];
        lfns[0] = "saved_models/timevae2/TimeVAE_encoder_wts.h5";
        lfns[1] = "saved_models/timevae_sampled/TimeVAE_encoder_wts.h5";

        Job job = (Job) mDAG.getNode("preprocess_ID1").getContent();

        PegasusFile inputFile = (PegasusFile) job.getInputFiles().toArray()[0];
        // reset the input files
        job.setInputFiles(new LinkedHashSet());

        LinkedList<FileTransfer> expectedOutputs = new LinkedList();
        for (String lfn : lfns) {
            PegasusFile pf = (PegasusFile) inputFile.clone();
            pf.setLFN(lfn);
            job.addInputFile(pf);

            FileTransfer expectedOutput = new FileTransfer();
            expectedOutput.setLFN(lfn);
            expectedOutput.setRegisterFlag(true);
            expectedOutput.setTransferFlag(true);
            expectedOutput.addSource(
                    "compute", "file://$pegasus_lite_work_dir/" + new File(lfn).getName());
            expectedOutput.addDestination("compute", "moveto://$PWD/" + lfn);

            expectedOutputs.add(expectedOutput);
        }

        RuntimeException thrown =
                Assertions.assertThrows(
                        RuntimeException.class, () -> this.testStageIn("local", expectedOutputs));
        assertThat(
                thrown.getMessage(), containsString("Conflict for job preprocess_ID1 detected."));
    }

    /**
     * GH-233 a directory input arrives as a tarball via HTCondor file transfer; it must be untarred
     * locally before the job runs, symmetric to a directory output being tarred locally before
     * HTCondor ships it back (see fileTransferForTarOfDirectoryOutput).
     */
    @Test
    public void testStageInForDirectory() {
        String lfn = "out";
        Job job = (Job) mDAG.getNode("preprocess_ID1").getContent();
        PegasusFile inputFile = (PegasusFile) job.getInputFiles().toArray()[0];
        inputFile.setLFN(lfn);
        inputFile.setForDirectory();

        FileTransfer expectedOutput = new FileTransfer();
        expectedOutput.setLFN(lfn);
        expectedOutput.setRegisterFlag(true);
        expectedOutput.setTransferFlag(true);
        expectedOutput.addSource("compute", "file://$pegasus_lite_work_dir/" + lfn + ".tar.gz");
        expectedOutput.addDestination("compute", "file://$PWD/" + lfn);

        this.testStageIn("local", expectedOutput);
    }

    /**
     * GH-233 HTCondor file transfer always flattens a deep LFN down to its basename on delivery - a
     * directory input's tarball must be looked up under that basename, not the full deep LFN,
     * mirroring what testStageInForDeepLFN already verifies for a plain (non-directory) file.
     */
    @Test
    public void testStageInForDeepLFNDirectory() {
        String lfn = "data/out";
        Job job = (Job) mDAG.getNode("preprocess_ID1").getContent();
        PegasusFile inputFile = (PegasusFile) job.getInputFiles().toArray()[0];
        inputFile.setLFN(lfn);
        inputFile.setForDirectory();

        FileTransfer expectedOutput = new FileTransfer();
        expectedOutput.setLFN(lfn);
        expectedOutput.setRegisterFlag(true);
        expectedOutput.setTransferFlag(true);
        expectedOutput.addSource(
                "compute", "file://$pegasus_lite_work_dir/" + new File(lfn).getName() + ".tar.gz");
        expectedOutput.addDestination("compute", "file://$PWD/" + lfn);

        this.testStageIn("local", expectedOutput);
    }

    /**
     * GH-233 a directory input with a deep LFN flattens to &lt;basename&gt;.tar.gz on delivery,
     * exactly like an existing flat input already named &lt;basename&gt;.tar.gz would land - both
     * silently clobbering the other in the job's own sandbox unless caught, mirroring what
     * testStageInForDeepLFNWAWConflict already verifies for two plain deep LFNs.
     */
    @Test
    public void testStageInForDirectoryWAWConflictWithShallowFile() {
        Job job = (Job) mDAG.getNode("preprocess_ID1").getContent();
        PegasusFile inputFile = (PegasusFile) job.getInputFiles().toArray()[0];
        job.setInputFiles(new LinkedHashSet());

        PegasusFile shallowFile = (PegasusFile) inputFile.clone();
        shallowFile.setLFN("model.tar.gz");
        job.addInputFile(shallowFile);

        PegasusFile directoryFile = (PegasusFile) inputFile.clone();
        directoryFile.setLFN("a/model");
        directoryFile.setForDirectory();
        job.addInputFile(directoryFile);

        RuntimeException thrown =
                Assertions.assertThrows(
                        RuntimeException.class,
                        () -> this.testStageIn("local", new LinkedList<FileTransfer>()));
        assertThat(
                thrown.getMessage(), containsString("Conflict for job preprocess_ID1 detected."));
    }

    /**
     * GH-233 a clustered job can legitimately require the very same input (e.g. a shared
     * executable) via two distinct PegasusFile objects that both report the same lfn - this is not
     * two different files landing at the same basename, just the same file listed twice, and must
     * not be flagged as a WAW conflict. Reproduces the failure seen for a horizontally clustered
     * job, where two constituent jobs both add the same transformation as an input.
     */
    @Test
    public void testStageInDuplicateInputLFNIsNotAWAWConflict() {
        Job job = (Job) mDAG.getNode("preprocess_ID1").getContent();
        PegasusFile inputFile = (PegasusFile) job.getInputFiles().toArray()[0];

        Set<PegasusFile> files = Collections.newSetFromMap(new IdentityHashMap());
        PegasusFile first = (PegasusFile) inputFile.clone();
        first.setLFN("pegasus-findrange-4.0");
        PegasusFile second = (PegasusFile) inputFile.clone();
        second.setLFN("pegasus-findrange-4.0");
        files.add(first);
        files.add(second);
        job.setInputFiles(files);

        Assertions.assertDoesNotThrow(
                () -> this.testStageIn("local", new LinkedList<FileTransfer>()));
    }

    /** PM-1885 */
    @Test
    public void testStageInForBypassLFN() {
        this.testStageInForBypassLFN(
                "f.in", "http://remote.isi.edu/data/f.in", "compute", "file://$PWD/f.in");
    }

    /** PM-1885 */
    @Test
    public void testStageInSymlinkForBypassLFN() {
        this.mProps.setProperty(PegasusProperties.PEGASUS_TRANSFER_LINKS_PROPERTY_KEY, "true");
        this.testStageInForBypassLFN(
                "V-V1Online-1239948000-2000.gwf",
                "file:///cvmfs/osg/oasis.opensciencegrid.org/ligo/frames/O3/V1Online/V-V1Online-12399/V-V1Online-1239948000-2000.gwf",
                "compute",
                "symlink://$PWD/V-V1Online-1239948000-2000.gwf");
    }

    @Test
    public void testStageInSymlinkForBypassDeepLFN() {
        this.mProps.setProperty(PegasusProperties.PEGASUS_TRANSFER_LINKS_PROPERTY_KEY, "true");
        this.testStageInForBypassLFN(
                "123994/V-V1Online-1239948000-2000.gwf",
                "file:///cvmfs/osg/oasis.opensciencegrid.org/ligo/frames/O3/V1Online/V-V1Online-12399/V-V1Online-1239948000-2000.gwf",
                "compute",
                "symlink://$PWD/123994/V-V1Online-1239948000-2000.gwf");
    }

    public void testStageInForBypassLFN(
            String lfn, String sourcePFN, String sourcePFNSite, String expectedDestPFN) {
        Job job = (Job) mDAG.getNode("preprocess_ID1").getContent();
        PegasusFile inputFile = (PegasusFile) job.getInputFiles().toArray()[0];
        inputFile.setLFN(lfn);
        inputFile.setForBypassStaging();

        // the bypass location is retrieved from the planner cache. set in there
        PlannerCache cache = new PlannerCache();
        cache.initialize(mBag, mDAG);
        cache.insert(lfn, sourcePFN, sourcePFNSite, FileServerType.OPERATION.get);
        mBag.add(PegasusBag.PLANNER_CACHE, cache);

        FileTransfer expectedInput = new FileTransfer();
        expectedInput.setLFN(lfn);
        expectedInput.setRegisterFlag(true);
        expectedInput.setTransferFlag(true);
        expectedInput.addSource("compute", sourcePFN);
        expectedInput.addDestination("compute", expectedDestPFN);

        this.testStageIn("local", expectedInput);
    }

    private void testStageIn(String stagingSite, String expectedSource) {

        FileTransfer ft = new FileTransfer();
        ft.addSource(stagingSite, expectedSource);
        this.testStageIn(stagingSite, ft);
    }

    private void testStageIn(String stagingSite, FileTransfer expected) {

        mLogger.logEventStart("test.transfer.sls.condor", "set", Integer.toString(mTestNumber++));
        Condor cTX = new Condor();
        cTX.initialize(mBag);

        Job job = (Job) mDAG.getNode("preprocess_ID1").getContent();
        job.setStagingSiteHandle(stagingSite);
        job.vdsNS.checkKeyInNS(Pegasus.STYLE_KEY, Pegasus.CONDOR_STYLE);

        PegasusFile inputFile = (PegasusFile) job.getInputFiles().toArray()[0];
        FileServer stagingSiteServer = null;
        String stagingSiteDirectory = null;

        SiteStore store = this.mBag.getHandleToSiteStore();
        SiteCatalogEntry stagingSiteEntry = store.lookup(job.getStagingSiteHandle());
        stagingSiteDirectory = stagingSiteEntry.getInternalMountPointOfWorkDirectory();
        stagingSiteServer =
                stagingSiteEntry.selectHeadNodeScratchSharedFileServer(FileServer.OPERATION.get);

        Collection<FileTransfer> result =
                cTX.determineSLSInputTransfers(
                        job, null, stagingSiteServer, stagingSiteDirectory, "$PWD", false);

        if (expected.getDestURL() == null && result.isEmpty()) {
            // indicates we peek into transfer_input_files
            // that is constructed when the job is modified for worker node
            // execution
            cTX.modifyJobForWorkerNodeExecution(
                    job, stagingSiteServer.getURLPrefix(), stagingSiteDirectory, "$PWD");
            assertThat(
                    job.condorVariables.getIPFilesForTransfer(),
                    is(expected.getSourceURL().getValue()));
            return;
        }

        assertThat(result, notNullValue());
        assertThat(result.size(), is(1));
        testFileTransfer(expected, (FileTransfer) result.toArray()[0]);
        mLogger.logEventCompletion();
    }

    private void testStageIn(String stagingSite, LinkedList<FileTransfer> expectedFTs) {

        mLogger.logEventStart("test.transfer.sls.condor", "set", Integer.toString(mTestNumber++));
        Condor cTX = new Condor();
        cTX.initialize(mBag);

        Job job = (Job) mDAG.getNode("preprocess_ID1").getContent();
        job.setStagingSiteHandle(stagingSite);
        job.vdsNS.checkKeyInNS(Pegasus.STYLE_KEY, Pegasus.CONDOR_STYLE);

        PegasusFile inputFile = (PegasusFile) job.getInputFiles().toArray()[0];
        FileServer stagingSiteServer = null;
        String stagingSiteDirectory = null;

        SiteStore store = this.mBag.getHandleToSiteStore();
        SiteCatalogEntry stagingSiteEntry = store.lookup(job.getStagingSiteHandle());
        stagingSiteDirectory = stagingSiteEntry.getInternalMountPointOfWorkDirectory();
        stagingSiteServer =
                stagingSiteEntry.selectHeadNodeScratchSharedFileServer(FileServer.OPERATION.get);

        Collection<FileTransfer> result =
                cTX.determineSLSInputTransfers(
                        job, null, stagingSiteServer, stagingSiteDirectory, "$PWD", false);

        /**
         * fixme: not doing it for the mix case?? if (expected.getDestURL() == null &&
         * result.isEmpty()) { // indicates we peek into transfer_input_files // that is constructed
         * when the job is modified for worker node // execution
         * cTX.modifyJobForWorkerNodeExecution( job, stagingSiteServer.getURLPrefix(),
         * stagingSiteDirectory, "$PWD"); assertThat( expected.getSourceURL().getValue(),
         * job.condorVariables.getIPFilesForTransfer()); return; }
         */
        assertThat(result, notNullValue());
        assertThat(result.size(), is(expectedFTs.size()));

        Object[] resultArray = result.toArray();
        Object[] expectedArray = expectedFTs.toArray();
        for (int i = 0; i < expectedArray.length; i++) {
            // System.err.println("**DEBUG**" + resultArray[i]);
            testFileTransfer((FileTransfer) expectedArray[i], (FileTransfer) resultArray[i]);
        }
        mLogger.logEventCompletion();
    }

    private void testFileTransfer(FileTransfer expected, FileTransfer actual) {
        assertThat(actual, notNullValue());
        assertThat(actual.getLFN(), is(expected.getLFN()));
        assertThat(actual.getLinkage(), is(expected.getLinkage()));
        assertThat(actual.getTransferFlag(), is(expected.getTransferFlag()));
        assertThat(actual.getRegisterFlag(), is(expected.getRegisterFlag()));
        assertThat(actual.getSourceURLCount(), is(expected.getSourceURLCount()));
        assertThat(actual.getDestURLCount(), is(expected.getDestURLCount()));

        // we only expect 1 source and destination URL for these unit tests
        assertThat(actual.getSourceURLCount(), is(1));
        assertThat(actual.getDestURLCount(), is(1));
        assertThat(actual.getSourceURL(), is(expected.getSourceURL()));
        assertThat(actual.getDestURL(), is(expected.getDestURL()));
    }

    /**
     * GH-233 for a directory output with a deep LFN, both the requested transfer_output_files entry
     * and the transfer_output_remaps target need the .tar.gz suffix, since HTCondor delivers
     * &lt;basename&gt;.tar.gz (not &lt;basename&gt;) once the local tar step (see
     * determineSLSOutputTransfers) has run.
     */
    @Test
    public void testStageOutRemapForDeepLFNDirectory() {
        String lfn = "data/out";
        Job job = (Job) mDAG.getNode("preprocess_ID1").getContent();
        job.setStagingSiteHandle("local");
        job.vdsNS.checkKeyInNS(Pegasus.STYLE_KEY, Pegasus.CONDOR_STYLE);

        PegasusFile outputFile = (PegasusFile) job.getOutputFiles().toArray()[0];
        outputFile.setLFN(lfn);
        outputFile.setForDirectory();

        modifyJobForWorkerNodeExecution(job);

        assertThat(
                (String)
                        job.condorVariables.get(
                                edu.isi.pegasus.planner.namespace.Condor.TRANSFER_OP_FILES_KEY),
                is(lfn + ".tar.gz"));
        assertThat(
                (String)
                        job.condorVariables.get(
                                edu.isi.pegasus.planner.namespace.Condor.TRANSFER_OP_REMAPS_KEY),
                is("out.tar.gz=" + lfn + ".tar.gz"));
    }

    /**
     * GH-233 a directory output with a deep LFN flattens to &lt;basename&gt;.tar.gz on delivery,
     * exactly like an existing flat output already named &lt;basename&gt;.tar.gz would land - both
     * silently clobbering the other at the staging site unless caught, mirroring the input-side
     * check verified above.
     */
    @Test
    public void testStageOutWAWConflictForDirectory() {
        Job job = (Job) mDAG.getNode("preprocess_ID1").getContent();
        job.setStagingSiteHandle("local");
        job.vdsNS.checkKeyInNS(Pegasus.STYLE_KEY, Pegasus.CONDOR_STYLE);

        PegasusFile outputFile = (PegasusFile) job.getOutputFiles().toArray()[0];
        job.setOutputFiles(new LinkedHashSet());

        PegasusFile shallowFile = (PegasusFile) outputFile.clone();
        shallowFile.setLFN("model.tar.gz");
        job.addOutputFile(shallowFile);

        PegasusFile directoryFile = (PegasusFile) outputFile.clone();
        directoryFile.setLFN("a/model");
        directoryFile.setForDirectory();
        job.addOutputFile(directoryFile);

        RuntimeException thrown =
                Assertions.assertThrows(
                        RuntimeException.class, () -> modifyJobForWorkerNodeExecution(job));
        assertThat(
                thrown.getMessage(), containsString("Conflict for job preprocess_ID1 detected."));
    }

    /**
     * GH-233 mirrors testStageInDuplicateInputLFNIsNotAWAWConflict for the output side - the same
     * output lfn showing up via two distinct PegasusFile objects is not a basename collision
     * between two different files and must not be flagged as a WAW conflict.
     */
    @Test
    public void testStageOutDuplicateOutputLFNIsNotAWAWConflict() {
        Job job = (Job) mDAG.getNode("preprocess_ID1").getContent();
        job.setStagingSiteHandle("local");
        job.vdsNS.checkKeyInNS(Pegasus.STYLE_KEY, Pegasus.CONDOR_STYLE);

        PegasusFile outputFile = (PegasusFile) job.getOutputFiles().toArray()[0];

        Set<PegasusFile> files = Collections.newSetFromMap(new IdentityHashMap());
        PegasusFile first = (PegasusFile) outputFile.clone();
        first.setLFN("pegasus-findrange-4.0");
        PegasusFile second = (PegasusFile) outputFile.clone();
        second.setLFN("pegasus-findrange-4.0");
        files.add(first);
        files.add(second);
        job.setOutputFiles(files);

        Assertions.assertDoesNotThrow(() -> modifyJobForWorkerNodeExecution(job));
    }

    private void modifyJobForWorkerNodeExecution(Job job) {
        Condor cTX = new Condor();
        cTX.initialize(mBag);

        SiteStore store = this.mBag.getHandleToSiteStore();
        SiteCatalogEntry stagingSiteEntry = store.lookup(job.getStagingSiteHandle());
        String stagingSiteDirectory = stagingSiteEntry.getInternalMountPointOfWorkDirectory();
        FileServer stagingSiteServer =
                stagingSiteEntry.selectHeadNodeScratchSharedFileServer(FileServer.OPERATION.get);

        cTX.modifyJobForWorkerNodeExecution(
                job, stagingSiteServer.getURLPrefix(), stagingSiteDirectory, "$PWD");
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
