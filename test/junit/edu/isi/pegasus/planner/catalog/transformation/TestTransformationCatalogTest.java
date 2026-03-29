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
package edu.isi.pegasus.planner.catalog.transformation;

import static org.junit.jupiter.api.Assertions.*;

import edu.isi.pegasus.planner.catalog.transformation.classes.Arch;
import edu.isi.pegasus.planner.catalog.transformation.classes.Os;
import edu.isi.pegasus.planner.catalog.transformation.classes.TCType;
import edu.isi.pegasus.planner.catalog.transformation.classes.VDSSysInfo;
import edu.isi.pegasus.planner.classes.Profile;
import java.util.List;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests covering the pattern exercised by TestTransformationCatalog#main — creating, configuring
 * and inspecting TransformationCatalogEntry objects without a live catalog backend.
 */
public class TestTransformationCatalogTest {

    @BeforeAll
    public static void setUpClass() {}

    @AfterAll
    public static void tearDownClass() {}

    @BeforeEach
    public void setUp() {}

    @AfterEach
    public void tearDown() {}

    @Test
    public void testCreateEntryWithNamespaceNameVersion() {
        TransformationCatalogEntry entry =
                new TransformationCatalogEntry("pegasus", "preprocess", null);
        assertEquals("pegasus", entry.getLogicalNamespace());
        assertEquals("preprocess", entry.getLogicalName());
        assertNull(entry.getLogicalVersion());
    }

    @Test
    public void testSetPhysicalTransformationAndResourceId() {
        TransformationCatalogEntry entry =
                new TransformationCatalogEntry("pegasus", "preprocess", null);
        entry.setPhysicalTransformation("/usr/pegasus/bin/keg");
        entry.setResourceId("isi");
        assertEquals("/usr/pegasus/bin/keg", entry.getPhysicalTransformation());
        assertEquals("isi", entry.getResourceId());
    }

    @Test
    public void testSetTypeInstalled() {
        TransformationCatalogEntry entry =
                new TransformationCatalogEntry("pegasus", "preprocess", null);
        entry.setType(TCType.INSTALLED);
        assertEquals(TCType.INSTALLED, entry.getType());
    }

    @Test
    public void testSetVDSSysInfo() {
        TransformationCatalogEntry entry =
                new TransformationCatalogEntry("pegasus", "preprocess", null);
        entry.setVDSSysInfo(new VDSSysInfo(Arch.INTEL32, Os.LINUX, null, null));
        assertNotNull(entry.getSysInfo());
    }

    @Test
    public void testAddEnvProfile() {
        TransformationCatalogEntry entry =
                new TransformationCatalogEntry("pegasus", "preprocess", null);
        entry.addProfile(new Profile(Profile.ENV, "PEGASUS_HOME", "/usr/pegasus/bin"));
        List profiles = entry.getProfiles(Profile.ENV);
        assertNotNull(profiles);
        assertFalse(profiles.isEmpty());
    }

    @Test
    public void testToStringContainsPhysicalName() {
        TransformationCatalogEntry entry =
                new TransformationCatalogEntry("pegasus", "preprocess", null);
        entry.setPhysicalTransformation("/usr/pegasus/bin/keg");
        String s = entry.toString();
        assertTrue(s.contains("/usr/pegasus/bin/keg"));
    }

    @Test
    public void testVDSSysInfoIntel32Linux() {
        VDSSysInfo sysInfo = new VDSSysInfo(Arch.INTEL32, Os.LINUX, null, null);
        assertSame(Arch.INTEL32, sysInfo.getArch());
        assertSame(Os.LINUX, sysInfo.getOs());
    }

    @Test
    public void testClonedEntryIsEquivalent() {
        TransformationCatalogEntry entry =
                new TransformationCatalogEntry("pegasus", "preprocess", null);
        entry.setPhysicalTransformation("/bin/keg");
        entry.setResourceId("isi");
        entry.setType(TCType.INSTALLED);
        TransformationCatalogEntry clone = (TransformationCatalogEntry) entry.clone();
        assertEquals(entry.getLogicalName(), clone.getLogicalName());
        assertEquals(entry.getResourceId(), clone.getResourceId());
        assertEquals(entry.getType(), clone.getType());
    }
}
