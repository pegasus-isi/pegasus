/**
 * Copyright 2007-2024 University Of Southern California
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

import static edu.isi.pegasus.planner.transfer.sls.TransferTest.mTestNumber;
import static org.junit.jupiter.api.Assertions.*;

import edu.isi.pegasus.planner.catalog.site.classes.Directory;
import edu.isi.pegasus.planner.catalog.site.classes.FileServerType;
import edu.isi.pegasus.planner.catalog.site.classes.SiteCatalogEntry;
import edu.isi.pegasus.planner.catalog.transformation.classes.Container;
import edu.isi.pegasus.planner.classes.FileTransfer;
import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.classes.PegasusFile;
import edu.isi.pegasus.planner.classes.PlannerCache;
import edu.isi.pegasus.planner.common.PegasusProperties;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** @author vahi */
public class TransferTestContainerOnHost extends TransferTest {

    public TransferTestContainerOnHost() {}

    @BeforeAll
    public static void setUpClass() {}

    @AfterAll
    public static void tearDownClass() {}

    @BeforeEach
    public void setUp() {
        super.setUp();
        // ensure explicitly that transfers are set up to be
        // within the container
        mProps.setProperty(PegasusProperties.PEGASUS_TRANSFER_CONTAINER_ON_HOST, "true");
    }

    @AfterEach
    public void tearDown() {}

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
        Container.MountPoint expectedMP = new Container.MountPoint();
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
        Container.MountPoint mp = new Container.MountPoint();
        c.addMountPoint(
                new Container.MountPoint(
                        "/path/on/shared/scratch/host/os:/path/on/shared/scratch/host/os"));
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

    @Test
    public void testUpdateSourceFileURLForContainerizedJobWithNoMount() {
        assertThrows(
                RuntimeException.class,
                () ->
                        this.testSourceFileURLForContainerizedJob(
                                "file:///shared/scratch/f.in",
                                null,
                                "file:///shared/scratch/f.in",
                                false));
    }

    @Test
    public void testUpdateSourceFileURLForContainerizedJobWithWrongMount() {
        assertThrows(
                RuntimeException.class,
                () ->
                        this.testSourceFileURLForContainerizedJob(
                                "file:///shared/scratch/f.in",
                                new Container.MountPoint("/scratch/:/scratch"),
                                "file:///shared/scratch/f.in",
                                false));
    }

    @Test
    public void testUpdateSourceFileURLForContainerizedJobWithCorrectSameMount() {
        this.testSourceFileURLForContainerizedJob(
                "file:///shared/scratch/f.in",
                new Container.MountPoint("/shared/scratch:/shared/scratch"),
                "file:///shared/scratch/f.in",
                false);
    }

    @Test
    public void testUpdateSourceFileURLForContainerizedJobWithCorrectDiffMount() {
        // valid mount. but mount to different dest dir
        this.testSourceFileURLForContainerizedJob(
                "file:///shared/scratch/f.in",
                new Container.MountPoint("/shared/scratch:/incontainer"),
                "file:///incontainer/f.in",
                false);
    }

    @Test
    public void testUpdateSourceHTTPURLForContainerizedJobWithCorrectMount() {
        // valid mount. but mount to different dest dir
        this.testSourceFileURLForContainerizedJob(
                "http://test.example.com/shared/scratch/f.in",
                new Container.MountPoint("/shared/scratch:/shared/scratch"),
                "http://test.example.com/shared/scratch/f.in",
                false);
    }

    @Test
    public void testUpdateSourceFileURLForHostOSContainerizedJobWithCorrectDiffMount() {
        // valid mount. but mount to different dest dir
        // no effect since transfers are on the HOST OS
        this.testSourceFileURLForContainerizedJob(
                "file:///shared/scratch/f.in",
                new Container.MountPoint("/shared/scratch:/incontainer"),
                "file:///shared/scratch/f.in",
                true);
    }
}
