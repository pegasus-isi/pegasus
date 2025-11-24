/**
 * Copyright 2007-2025 University Of Southern California
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
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for nonshared fs mode, PegasusLite transfers in reference to OSDF.
 *
 * @author Karan Vahi
 */
public class TransferTestOSDF extends TransferTest {

    private String OSDF_ENDPOINT_URL = "osdf:///ospool/ospool-ap2140/pegasuswfs";

    public TransferTestOSDF() {}

    @BeforeAll
    public static void setUpClass() {}

    @AfterAll
    public static void tearDownClass() {}

    @BeforeEach
    @Override
    public void setUp() {
        super.setUp();
    }

    @AfterEach
    @Override
    public void tearDown() {}

    @Test
    public void testOSDFStageinFromStagingSite() {
        this.testOSDFStagein(
                "f.in",
                OSDF_ENDPOINT_URL + "/./" + "f.in",
                "file://$pegasus_lite_start_dir/f.in",
                "moveto://$PWD/f.in");
    }

    @Test
    public void testOSDFStageinWithByPass() {
        mLogger.logEventStart(
                "test.transfer.sls.transfer", "sharedfs", Integer.toString(mTestNumber));

        Job job = (Job) mDAG.getNode("preprocess_ID1").getContent();

        // set bypass on for the input files
        for (PegasusFile pf : job.getInputFiles()) {
            pf.setForBypassStaging();
        }
        // the bypass location is retrieved from the planner cache. set in there
        PlannerCache cache = new PlannerCache();
        cache.initialize(mBag, mDAG);
        cache.insert(
                "f.in",
                "osdf:///pelicanplatform/test/hello-world.txt",
                "compute",
                FileServerType.OPERATION.get);
        mBag.add(PegasusBag.PLANNER_CACHE, cache);

        this.testOSDFStagein(
                "f.in",
                "osdf:///pelicanplatform/test/hello-world.txt",
                "file://$pegasus_lite_start_dir/hello-world.txt",
                "moveto://$PWD/f.in");
        mLogger.logEventCompletion();
    }

    public void testOSDFStagein(
            String lfn, String sourceOSDFURL, String expectedSource, String expectedDestination) {
        FileTransfer expectedOutput = new FileTransfer();
        expectedOutput.setLFN(lfn);
        expectedOutput.addSource("compute", expectedSource);
        expectedOutput.addDestination("compute", expectedDestination);
        this.testStageIn("osdf", expectedOutput);

        // also check for transfer_input_files

        Job job = (Job) mDAG.getNode("preprocess_ID1").getContent();
        assertEquals(
                sourceOSDFURL,
                job.condorVariables.getIPFilesForTransfer(),
                "job transfer_input_files");
        System.err.println(job.condorVariables.getIPFilesForTransfer());
    }

    @Override
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

    @Override
    protected SiteStore constructTestSiteStore() {
        SiteStore store = super.constructTestSiteStore();

        SiteCatalogEntry stagingSite = new SiteCatalogEntry("osdf");
        stagingSite.setArchitecture(SysInfo.Architecture.x86_64);
        stagingSite.setOS(SysInfo.OS.linux);

        FileServer fs = new FileServer();
        fs.setSupportedOperation(FileServerType.OPERATION.all);
        PegasusURL url = new PegasusURL(OSDF_ENDPOINT_URL);
        fs.setURLPrefix(url.getURLPrefix());
        fs.setProtocol(url.getProtocol());
        fs.setMountPoint(url.getPath());

        Directory dir = new Directory();
        dir.setType(Directory.TYPE.shared_scratch);
        dir.setInternalMountPoint(new InternalMountPoint(fs.getMountPoint()));
        dir.setSharedFileSystemAccess(false);

        dir.addFileServer(fs);
        stagingSite.addDirectory(dir);
        store.addEntry(stagingSite);

        return store;
    }
}
