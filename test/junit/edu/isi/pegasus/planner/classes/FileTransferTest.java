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
package edu.isi.pegasus.planner.classes;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

import edu.isi.pegasus.planner.catalog.replica.ReplicaCatalogEntry;
import java.util.Collection;
import java.util.List;
import org.junit.jupiter.api.Test;

/** Tests for the FileTransfer class. */
public class FileTransferTest {

    // -----------------------------------------------------------------------
    // Construction — default constructor
    // -----------------------------------------------------------------------

    @Test
    public void testDefaultConstructorJobNameIsEmpty() {
        FileTransfer ft = new FileTransfer();
        assertThat(ft.getJobName(), is(""));
    }

    @Test
    public void testDefaultConstructorPriorityIsZero() {
        FileTransfer ft = new FileTransfer();
        assertThat(ft.getPriority(), is(0));
    }

    @Test
    public void testDefaultConstructorVerifySymlinkSourceIsTrue() {
        FileTransfer ft = new FileTransfer();
        assertThat(ft.verifySymlinkSource(), is(true));
    }

    @Test
    public void testDefaultConstructorRegistrationURLIsNull() {
        FileTransfer ft = new FileTransfer();
        assertThat(ft.getURLForRegistrationOnDestination(), is(nullValue()));
    }

    @Test
    public void testDefaultConstructorIsNotValid() {
        FileTransfer ft = new FileTransfer();
        assertThat(ft.isValid(), is(false));
    }

    // -----------------------------------------------------------------------
    // Construction — lfn + job constructor
    // -----------------------------------------------------------------------

    @Test
    public void testLfnJobConstructorSetsLFN() {
        FileTransfer ft = new FileTransfer("f.txt", "job1");
        assertThat(ft.getLFN(), is("f.txt"));
    }

    @Test
    public void testLfnJobConstructorSetsJobName() {
        FileTransfer ft = new FileTransfer("f.txt", "job1");
        assertThat(ft.getJobName(), is("job1"));
    }

    // -----------------------------------------------------------------------
    // Construction — from PegasusFile
    // -----------------------------------------------------------------------

    @Test
    public void testPegasusFileConstructorCopiesLFN() {
        PegasusFile pf = new PegasusFile("input.dat");
        FileTransfer ft = new FileTransfer(pf);
        assertThat(ft.getLFN(), is("input.dat"));
    }

    @Test
    public void testPegasusFileConstructorJobNameIsEmpty() {
        PegasusFile pf = new PegasusFile("input.dat");
        FileTransfer ft = new FileTransfer(pf);
        assertThat(ft.getJobName(), is(""));
    }

    // -----------------------------------------------------------------------
    // Source URL management
    // -----------------------------------------------------------------------

    @Test
    public void testAddSourceURLBySiteAndURL() {
        FileTransfer ft = new FileTransfer("f.txt", "j1");
        ft.addSource("siteA", "gsiftp://siteA/f.txt");
        Collection<String> sites = ft.getSourceSites();
        assertThat(sites, hasItem("siteA"));
    }

    @Test
    public void testAddSourceURLCountIncrements() {
        FileTransfer ft = new FileTransfer("f.txt", "j1");
        ft.addSource("siteA", "gsiftp://siteA/f.txt");
        assertThat(ft.getSourceURLCount(), is(1));
    }

    @Test
    public void testAddMultipleSourceURLsSameSite() {
        FileTransfer ft = new FileTransfer("f.txt", "j1");
        ft.addSource("siteA", "gsiftp://siteA/f.txt");
        ft.addSource("siteA", "gsiftp://siteA/mirror/f.txt");
        assertThat(ft.getSourceURLCount(), is(2));
        assertThat(ft.getSourceURLs("siteA"), hasSize(2));
    }

    @Test
    public void testAddSourceURLDifferentSites() {
        FileTransfer ft = new FileTransfer("f.txt", "j1");
        ft.addSource("siteA", "gsiftp://siteA/f.txt");
        ft.addSource("siteB", "gsiftp://siteB/f.txt");
        assertThat(ft.getSourceURLCount(), is(2));
        assertThat(ft.getSourceSites(), hasItems("siteA", "siteB"));
    }

    @Test
    public void testGetSourceURLReturnsFirstEntry() {
        FileTransfer ft = new FileTransfer("f.txt", "j1");
        ft.addSource("siteA", "gsiftp://siteA/f.txt");
        NameValue<String, String> nv = ft.getSourceURL();
        assertThat(nv, is(notNullValue()));
        assertThat(nv.getKey(), is("siteA"));
        assertThat(nv.getValue(), is("gsiftp://siteA/f.txt"));
    }

    @Test
    public void testGetSourceURLWithNoURLsReturnsNull() {
        FileTransfer ft = new FileTransfer("f.txt", "j1");
        assertThat(ft.getSourceURL(), is(nullValue()));
    }

    @Test
    public void testAddSourceViaNameValue() {
        FileTransfer ft = new FileTransfer("f.txt", "j1");
        ft.addSource(new NameValue<String, String>("siteX", "file:///data/f.txt"));
        assertThat(ft.getSourceURLCount(), is(1));
        assertThat(ft.getSourceSites(), hasItem("siteX"));
    }

    @Test
    public void testAddSourceViaReplicaCatalogEntry() {
        FileTransfer ft = new FileTransfer("f.txt", "j1");
        ReplicaCatalogEntry rce = new ReplicaCatalogEntry("gsiftp://host/f.txt", "rceSite");
        ft.addSource(rce);
        assertThat(ft.getSourceURLCount(), is(1));
        assertThat(ft.getSourceSites(), hasItem("rceSite"));
    }

    @Test
    public void testGetSourceURLsForMissingSiteReturnsEmptyList() {
        FileTransfer ft = new FileTransfer("f.txt", "j1");

        assertThat(ft.getSourceURLs("missing"), empty());
    }

    // -----------------------------------------------------------------------
    // Destination URL management
    // -----------------------------------------------------------------------

    @Test
    public void testAddDestinationURLBySiteAndURL() {
        FileTransfer ft = new FileTransfer("f.txt", "j1");
        ft.addDestination("destSite", "gsiftp://dest/f.txt");
        Collection<String> sites = ft.getDestSites();
        assertThat(sites, hasItem("destSite"));
    }

    @Test
    public void testAddDestinationURLCountIncrements() {
        FileTransfer ft = new FileTransfer("f.txt", "j1");
        ft.addDestination("destSite", "gsiftp://dest/f.txt");
        assertThat(ft.getDestURLCount(), is(1));
    }

    @Test
    public void testGetDestURLReturnsFirstEntry() {
        FileTransfer ft = new FileTransfer("f.txt", "j1");
        ft.addDestination("destSite", "gsiftp://dest/f.txt");
        NameValue<String, String> nv = ft.getDestURL();
        assertThat(nv, is(notNullValue()));
        assertThat(nv.getKey(), is("destSite"));
        assertThat(nv.getValue(), is("gsiftp://dest/f.txt"));
    }

    @Test
    public void testGetDestURLWithNoURLsReturnsNull() {
        FileTransfer ft = new FileTransfer("f.txt", "j1");
        assertThat(ft.getDestURL(), is(nullValue()));
    }

    @Test
    public void testAddDestinationViaNameValue() {
        FileTransfer ft = new FileTransfer("f.txt", "j1");
        ft.addDestination(new NameValue<String, String>("dstSite", "file:///out/f.txt"));
        assertThat(ft.getDestURLCount(), is(1));
    }

    @Test
    public void testGetDestURLsForSite() {
        FileTransfer ft = new FileTransfer("f.txt", "j1");
        ft.addDestination("dSite", "gsiftp://d/f.txt");
        List<ReplicaCatalogEntry> urls = ft.getDestURLs("dSite");
        assertThat(urls, hasSize(1));
        assertThat(urls.get(0).getPFN(), is("gsiftp://d/f.txt"));
    }

    @Test
    public void testGetDestURLsForMissingSiteReturnsEmptyList() {
        FileTransfer ft = new FileTransfer("f.txt", "j1");

        assertThat(ft.getDestURLs("missing"), empty());
    }

    // -----------------------------------------------------------------------
    // isValid
    // -----------------------------------------------------------------------

    @Test
    public void testIsValidWithBothSourceAndDestination() {
        FileTransfer ft = new FileTransfer("f.txt", "j1");
        ft.addSource("src", "gsiftp://src/f.txt");
        ft.addDestination("dst", "gsiftp://dst/f.txt");
        assertThat(ft.isValid(), is(true));
    }

    @Test
    public void testIsValidWithOnlySourceIsFalse() {
        FileTransfer ft = new FileTransfer("f.txt", "j1");
        ft.addSource("src", "gsiftp://src/f.txt");
        assertThat(ft.isValid(), is(false));
    }

    @Test
    public void testIsValidWithOnlyDestinationIsFalse() {
        FileTransfer ft = new FileTransfer("f.txt", "j1");
        ft.addDestination("dst", "gsiftp://dst/f.txt");
        assertThat(ft.isValid(), is(false));
    }

    // -----------------------------------------------------------------------
    // Priority
    // -----------------------------------------------------------------------

    @Test
    public void testSetAndGetPriority() {
        FileTransfer ft = new FileTransfer("f.txt", "j1");
        ft.setPriority(42);
        assertThat(ft.getPriority(), is(42));
    }

    // -----------------------------------------------------------------------
    // VerifySymlinkSource
    // -----------------------------------------------------------------------

    @Test
    public void testSetVerifySymlinkSourceFalse() {
        FileTransfer ft = new FileTransfer("f.txt", "j1");
        ft.setVerifySymlinkSource(false);
        assertThat(ft.verifySymlinkSource(), is(false));
    }

    @Test
    public void testSetVerifySymlinkSourceTrue() {
        FileTransfer ft = new FileTransfer("f.txt", "j1");
        ft.setVerifySymlinkSource(false);
        ft.setVerifySymlinkSource(true);
        assertThat(ft.verifySymlinkSource(), is(true));
    }

    @Test
    public void testIsTransferringExecutableFileReflectsType() {
        FileTransfer ft = new FileTransfer("tool.sh", "j1");
        ft.setType(PegasusFile.EXECUTABLE_FILE);

        assertThat(ft.isTransferringExecutableFile(), is(true));
    }

    @Test
    public void testIsTransferringContainerReflectsType() {
        FileTransfer ft = new FileTransfer("image.sif", "j1");
        ft.setType(PegasusFile.SINGULARITY_CONTAINER_FILE);

        assertThat(ft.isTransferringContainer(), is(true));
    }

    // -----------------------------------------------------------------------
    // Registration URL
    // -----------------------------------------------------------------------

    @Test
    public void testSetAndGetURLForRegistrationOnDestination() {
        FileTransfer ft = new FileTransfer("f.txt", "j1");
        ft.setURLForRegistrationOnDestination("rls://catalog/f.txt");
        assertThat(ft.getURLForRegistrationOnDestination(), is("rls://catalog/f.txt"));
    }

    // -----------------------------------------------------------------------
    // removeSourceURL / removeDestURL
    // -----------------------------------------------------------------------

    @Test
    public void testRemoveSourceURLDecreasesCount() {
        FileTransfer ft = new FileTransfer("f.txt", "j1");
        ft.addSource("src", "gsiftp://src/f.txt");
        NameValue<String, String> removed = ft.removeSourceURL();
        assertThat(removed, is(notNullValue()));
        assertThat(ft.getSourceURLCount(), is(0));
    }

    @Test
    public void testRemoveSourceURLRemovesWholeFirstSiteBucket() {
        FileTransfer ft = new FileTransfer("f.txt", "j1");
        ft.addSource("src", "gsiftp://src/f1.txt");
        ft.addSource("src", "gsiftp://src/f2.txt");

        NameValue<String, String> removed = ft.removeSourceURL();

        assertThat(removed, is(notNullValue()));
        assertThat(removed.getKey(), is("src"));
        assertThat(removed.getValue(), is("gsiftp://src/f1.txt"));
        assertThat(ft.getSourceURLCount(), is(0));
    }

    @Test
    public void testRemoveSourceURLOnEmptyReturnsNull() {
        FileTransfer ft = new FileTransfer("f.txt", "j1");
        assertThat(ft.removeSourceURL(), is(nullValue()));
    }

    @Test
    public void testRemoveDestURLDecreasesCount() {
        FileTransfer ft = new FileTransfer("f.txt", "j1");
        ft.addDestination("dst", "gsiftp://dst/f.txt");
        NameValue<String, String> removed = ft.removeDestURL();
        assertThat(removed, is(notNullValue()));
        assertThat(ft.getDestURLCount(), is(0));
    }

    @Test
    public void testRemoveDestURLRemovesWholeFirstSiteBucket() {
        FileTransfer ft = new FileTransfer("f.txt", "j1");
        ft.addDestination("dst", "gsiftp://dst/f1.txt");
        ft.addDestination("dst", "gsiftp://dst/f2.txt");

        NameValue<String, String> removed = ft.removeDestURL();

        assertThat(removed, is(notNullValue()));
        assertThat(removed.getKey(), is("dst"));
        assertThat(removed.getValue(), is("gsiftp://dst/f1.txt"));
        assertThat(ft.getDestURLCount(), is(0));
    }

    @Test
    public void testRemoveDestURLOnEmptyReturnsNull() {
        FileTransfer ft = new FileTransfer("f.txt", "j1");
        assertThat(ft.removeDestURL(), is(nullValue()));
    }

    // -----------------------------------------------------------------------
    // equals
    // -----------------------------------------------------------------------

    @Test
    public void testEqualsForIdenticalTransfers() {
        FileTransfer ft1 = new FileTransfer("same.txt", "j1");
        ft1.addSource("src", "gsiftp://src/same.txt");
        ft1.addDestination("dst", "gsiftp://dst/same.txt");

        FileTransfer ft2 = new FileTransfer("same.txt", "j1");
        ft2.addSource("src", "gsiftp://src/same.txt");
        ft2.addDestination("dst", "gsiftp://dst/same.txt");

        assertThat(ft1, is(ft2));
    }

    @Test
    public void testNotEqualsForDifferentLFN() {
        FileTransfer ft1 = new FileTransfer("a.txt", "j1");
        FileTransfer ft2 = new FileTransfer("b.txt", "j1");
        assertThat(ft1, is(not(ft2)));
    }

    @Test
    public void testNotEqualsWithNull() {
        FileTransfer ft = new FileTransfer("f.txt", "j1");
        assertThat(ft, is(notNullValue()));
    }

    @Test
    public void testHashCodeChangesWhenPriorityChanges() {
        FileTransfer ft = new FileTransfer("same.txt", "j1");
        int before = ft.hashCode();

        ft.setPriority(10);

        assertThat(ft.hashCode(), is(not(before)));
    }

    // -----------------------------------------------------------------------
    // clone
    // -----------------------------------------------------------------------

    @Test
    public void testClonePreservesLFN() {
        FileTransfer ft = new FileTransfer("clone.txt", "j1");
        FileTransfer clone = (FileTransfer) ft.clone();
        assertThat(clone.getLFN(), is("clone.txt"));
    }

    @Test
    public void testClonePreservesJobName() {
        FileTransfer ft = new FileTransfer("clone.txt", "jobClone");
        FileTransfer clone = (FileTransfer) ft.clone();
        assertThat(clone.getJobName(), is("jobClone"));
    }

    @Test
    public void testClonePreservesPriority() {
        FileTransfer ft = new FileTransfer("clone.txt", "j1");
        ft.setPriority(7);
        FileTransfer clone = (FileTransfer) ft.clone();
        assertThat(clone.getPriority(), is(7));
    }

    @Test
    public void testCloneDoesNotCarryOverSourceOrDestinationMaps() {
        FileTransfer ft = new FileTransfer("clone.txt", "j1");
        ft.addSource("src", "gsiftp://src/clone.txt");
        ft.addDestination("dst", "gsiftp://dst/clone.txt");

        FileTransfer clone = (FileTransfer) ft.clone();

        assertThat(clone.getSourceURLCount(), is(0));
        assertThat(clone.getDestURLCount(), is(0));
    }

    @Test
    public void testCloneIsIndependentObject() {
        FileTransfer ft = new FileTransfer("clone.txt", "j1");
        FileTransfer clone = (FileTransfer) ft.clone();
        assertThat(clone, is(not(sameInstance(ft))));
    }

    // -----------------------------------------------------------------------
    // toString
    // -----------------------------------------------------------------------

    @Test
    public void testToStringContainsLFN() {
        FileTransfer ft = new FileTransfer("myfile.txt", "j1");
        assertThat(ft.toString(), containsString("myfile.txt"));
    }

    @Test
    public void testToStringContainsSourceURL() {
        FileTransfer ft = new FileTransfer("myfile.txt", "j1");
        ft.addSource("src", "gsiftp://src/myfile.txt");
        assertThat(ft.toString(), containsString("gsiftp://src/myfile.txt"));
    }

    @Test
    public void testToStringContainsDestURL() {
        FileTransfer ft = new FileTransfer("myfile.txt", "j1");
        ft.addDestination("dst", "gsiftp://dst/myfile.txt");
        assertThat(ft.toString(), containsString("gsiftp://dst/myfile.txt"));
    }
}
