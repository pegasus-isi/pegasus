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
import static org.junit.Assert.assertEquals;

import edu.isi.pegasus.planner.catalog.site.classes.Directory;
import edu.isi.pegasus.planner.catalog.site.classes.SiteCatalogEntry;
import edu.isi.pegasus.planner.classes.FileTransfer;
import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.classes.PegasusFile;
import edu.isi.pegasus.planner.classes.Profile;
import edu.isi.pegasus.planner.common.PegasusProperties;
import edu.isi.pegasus.planner.namespace.Pegasus;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** @author vahi */
public class TransferTestNoContainer extends TransferTest {

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
}
