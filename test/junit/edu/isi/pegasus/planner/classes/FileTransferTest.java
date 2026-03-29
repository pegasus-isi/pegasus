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
import static org.junit.jupiter.api.Assertions.*;

import edu.isi.pegasus.planner.catalog.replica.ReplicaCatalogEntry;
import java.util.Collection;
import java.util.List;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Tests for the FileTransfer class. */
public class FileTransferTest {

    @BeforeAll
    public static void setUpClass() {}

    @AfterAll
    public static void tearDownClass() {}

    @BeforeEach
    public void setUp() {}

    @AfterEach
    public void tearDown() {}

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
        assertTrue(ft.verifySymlinkSource());
    }

    @Test
    public void testDefaultConstructorRegistrationURLIsNull() {
        FileTransfer ft = new FileTransfer();
        assertNull(ft.getURLForRegistrationOnDestination());
    }

    @Test
    public void testDefaultConstructorIsNotValid() {
        FileTransfer ft = new FileTransfer();
        assertFalse(ft.isValid());
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
        assertNotNull(nv);
        assertThat(nv.getKey(), is("siteA"));
        assertThat(nv.getValue(), is("gsiftp://siteA/f.txt"));
    }

    @Test
    public void testGetSourceURLWithNoURLsReturnsNull() {
        FileTransfer ft = new FileTransfer("f.txt", "j1");
        assertNull(ft.getSourceURL());
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
        assertNotNull(nv);
        assertThat(nv.getKey(), is("destSite"));
        assertThat(nv.getValue(), is("gsiftp://dest/f.txt"));
    }

    @Test
    public void testGetDestURLWithNoURLsReturnsNull() {
        FileTransfer ft = new FileTransfer("f.txt", "j1");
        assertNull(ft.getDestURL());
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

    // -----------------------------------------------------------------------
    // isValid
    // -----------------------------------------------------------------------

    @Test
    public void testIsValidWithBothSourceAndDestination() {
        FileTransfer ft = new FileTransfer("f.txt", "j1");
        ft.addSource("src", "gsiftp://src/f.txt");
        ft.addDestination("dst", "gsiftp://dst/f.txt");
        assertTrue(ft.isValid());
    }

    @Test
    public void testIsValidWithOnlySourceIsFalse() {
        FileTransfer ft = new FileTransfer("f.txt", "j1");
        ft.addSource("src", "gsiftp://src/f.txt");
        assertFalse(ft.isValid());
    }

    @Test
    public void testIsValidWithOnlyDestinationIsFalse() {
        FileTransfer ft = new FileTransfer("f.txt", "j1");
        ft.addDestination("dst", "gsiftp://dst/f.txt");
        assertFalse(ft.isValid());
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
        assertFalse(ft.verifySymlinkSource());
    }

    @Test
    public void testSetVerifySymlinkSourceTrue() {
        FileTransfer ft = new FileTransfer("f.txt", "j1");
        ft.setVerifySymlinkSource(false);
        ft.setVerifySymlinkSource(true);
        assertTrue(ft.verifySymlinkSource());
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
        assertNotNull(removed);
        assertThat(ft.getSourceURLCount(), is(0));
    }

    @Test
    public void testRemoveSourceURLOnEmptyReturnsNull() {
        FileTransfer ft = new FileTransfer("f.txt", "j1");
        assertNull(ft.removeSourceURL());
    }

    @Test
    public void testRemoveDestURLDecreasesCount() {
        FileTransfer ft = new FileTransfer("f.txt", "j1");
        ft.addDestination("dst", "gsiftp://dst/f.txt");
        NameValue<String, String> removed = ft.removeDestURL();
        assertNotNull(removed);
        assertThat(ft.getDestURLCount(), is(0));
    }

    @Test
    public void testRemoveDestURLOnEmptyReturnsNull() {
        FileTransfer ft = new FileTransfer("f.txt", "j1");
        assertNull(ft.removeDestURL());
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

        assertEquals(ft1, ft2);
    }

    @Test
    public void testNotEqualsForDifferentLFN() {
        FileTransfer ft1 = new FileTransfer("a.txt", "j1");
        FileTransfer ft2 = new FileTransfer("b.txt", "j1");
        assertNotEquals(ft1, ft2);
    }

    @Test
    public void testNotEqualsWithNull() {
        FileTransfer ft = new FileTransfer("f.txt", "j1");
        assertNotEquals(ft, null);
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
    public void testCloneIsIndependentObject() {
        FileTransfer ft = new FileTransfer("clone.txt", "j1");
        FileTransfer clone = (FileTransfer) ft.clone();
        assertNotSame(ft, clone);
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
