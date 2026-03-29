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
package edu.isi.pegasus.planner.dax;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Tests for the CatalogType abstract class, exercised through the concrete File and Executable
 * subclasses.
 */
public class CatalogTypeTest {

    @Test
    public void testFileExtendsCatalogType() {
        File f = new File("test.txt");
        assertInstanceOf(CatalogType.class, f, "File should extend CatalogType");
    }

    @Test
    public void testExecutableExtendsCatalogType() {
        Executable e = new Executable("test-exec");
        assertInstanceOf(CatalogType.class, e, "Executable should extend CatalogType");
    }

    @Test
    public void testAddPhysicalFileUrl() {
        File f = new File("test.txt");
        f.addPhysicalFile("gsiftp://site1/test.txt");
        List<PFN> pfns = f.getPhysicalFiles();
        assertEquals(1, pfns.size(), "Should have 1 PFN");
        assertEquals("gsiftp://site1/test.txt", pfns.get(0).getURL(), "PFN URL should match");
    }

    @Test
    public void testAddPhysicalFileWithSite() {
        File f = new File("test.txt");
        f.addPhysicalFile("gsiftp://site1/test.txt", "site1");
        List<PFN> pfns = f.getPhysicalFiles();
        assertEquals(1, pfns.size(), "Should have 1 PFN");
        assertEquals("site1", pfns.get(0).getSite(), "PFN site should match");
    }

    @Test
    public void testAddPhysicalFilePFNObject() {
        File f = new File("test.txt");
        PFN pfn = new PFN("gsiftp://site2/test.txt", "site2");
        f.addPhysicalFile(pfn);
        List<PFN> pfns = f.getPhysicalFiles();
        assertEquals(1, pfns.size(), "Should have 1 PFN");
        assertEquals(pfn, pfns.get(0), "Should be same PFN object");
    }

    @Test
    public void testAddProfile() {
        File f = new File("test.txt");
        f.addProfile("pegasus", "runtime", "100");
        List<Profile> profiles = f.getProfiles();
        assertFalse(profiles.isEmpty(), "Profiles should not be empty");
        assertEquals("pegasus", profiles.get(0).getNameSpace(), "Namespace should match");
    }

    @Test
    public void testAddMetaData() {
        File f = new File("test.txt");
        f.addMetaData("checksum", "abc123");
        // Metadata is in the internal set
        assertNotNull(f.getMetaData(), "Metadata should not be null");
    }

    @Test
    public void testInitiallyEmptyPFNs() {
        File f = new File("test.txt");
        List<PFN> pfns = f.getPhysicalFiles();
        assertTrue(pfns.isEmpty(), "PFNs should be empty initially");
    }

    @Test
    public void testMultiplePFNs() {
        File f = new File("test.txt");
        f.addPhysicalFile("gsiftp://site1/test.txt", "site1");
        f.addPhysicalFile("gsiftp://site2/test.txt", "site2");
        assertEquals(2, f.getPhysicalFiles().size(), "Should have 2 PFNs");
    }
}
