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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.*;

import java.util.Arrays;
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
        assertThat(f, instanceOf(CatalogType.class));
    }

    @Test
    public void testExecutableExtendsCatalogType() {
        Executable e = new Executable("test-exec");
        assertThat(e, instanceOf(CatalogType.class));
    }

    @Test
    public void testAddPhysicalFileUrl() {
        File f = new File("test.txt");
        f.addPhysicalFile("gsiftp://site1/test.txt");
        List<PFN> pfns = f.getPhysicalFiles();
        assertThat(pfns, hasSize(1));
        assertThat(pfns.get(0).getURL(), is("gsiftp://site1/test.txt"));
    }

    @Test
    public void testAddPhysicalFileWithSite() {
        File f = new File("test.txt");
        f.addPhysicalFile("gsiftp://site1/test.txt", "site1");
        List<PFN> pfns = f.getPhysicalFiles();
        assertThat(pfns, hasSize(1));
        assertThat(pfns.get(0).getSite(), is("site1"));
    }

    @Test
    public void testAddPhysicalFilePFNObject() {
        File f = new File("test.txt");
        PFN pfn = new PFN("gsiftp://site2/test.txt", "site2");
        f.addPhysicalFile(pfn);
        List<PFN> pfns = f.getPhysicalFiles();
        assertThat(pfns, hasSize(1));
        assertThat(pfns.get(0), is(pfn));
    }

    @Test
    public void testAddProfile() {
        File f = new File("test.txt");
        f.addProfile("pegasus", "runtime", "100");
        List<Profile> profiles = f.getProfiles();
        assertThat(profiles.isEmpty(), is(false));
        assertThat(profiles.get(0).getNameSpace(), is("pegasus"));
    }

    @Test
    public void testAddMetaData() {
        File f = new File("test.txt");
        f.addMetaData("checksum", "abc123");
        // Metadata is in the internal set
        assertThat(f.getMetaData(), notNullValue());
    }

    @Test
    public void testInitiallyEmptyPFNs() {
        File f = new File("test.txt");
        List<PFN> pfns = f.getPhysicalFiles();
        assertThat(pfns.isEmpty(), is(true));
    }

    @Test
    public void testMultiplePFNs() {
        File f = new File("test.txt");
        f.addPhysicalFile("gsiftp://site1/test.txt", "site1");
        f.addPhysicalFile("gsiftp://site2/test.txt", "site2");
        assertThat(f.getPhysicalFiles().size(), is(2));
    }

    @Test
    public void testAddPhysicalFilesPreservesInsertionOrder() {
        File f = new File("test.txt");
        PFN first = new PFN("gsiftp://site1/test.txt", "site1");
        PFN second = new PFN("gsiftp://site2/test.txt", "site2");

        f.addPhysicalFiles(Arrays.asList(first, second));

        assertThat(f.getPhysicalFiles(), is(Arrays.asList(first, second)));
    }

    @Test
    public void testAddProfilesWithListAddsAllProfiles() {
        File f = new File("test.txt");
        Profile first = new Profile("pegasus", "runtime", "10");
        Profile second = new Profile("env", "JAVA_HOME", "/opt/java");

        f.addProfiles(Arrays.asList(first, second));

        assertThat(f.getProfiles(), hasSize(2));
        assertThat(f.getProfiles().get(0), is(first));
        assertThat(f.getProfiles().get(1), is(second));
    }

    @Test
    public void testAddProfilesWithSingleProfileAddsProfile() {
        File f = new File("test.txt");
        Profile profile = new Profile("pegasus", "clusters.size", "3");

        f.addProfiles(profile);

        assertThat(f.getProfiles(), hasSize(1));
        assertThat(f.getProfiles().get(0), is(profile));
    }

    @Test
    public void testAddMetaDataListAddsDistinctMetadataEntries() {
        File f = new File("test.txt");
        MetaData first = new MetaData("checksum", "abc123");
        MetaData second = new MetaData("owner", "user");

        f.addMetaData(Arrays.asList(first, second));

        assertThat(f.getMetaData().size(), is(2));
        assertThat(f.getMetaData(), hasItem(first));
        assertThat(f.getMetaData(), hasItem(second));
    }

    @Test
    public void testBaseCatalogTypeDefaultsToNeitherFileNorExecutable() {
        CatalogType type = new CatalogType();

        assertThat(type.isFile(), is(false));
        assertThat(type.isExecutable(), is(false));
    }
}
