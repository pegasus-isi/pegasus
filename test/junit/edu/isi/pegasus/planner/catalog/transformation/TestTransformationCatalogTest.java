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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.jupiter.api.Assertions.*;

import edu.isi.pegasus.planner.catalog.transformation.classes.Arch;
import edu.isi.pegasus.planner.catalog.transformation.classes.Os;
import edu.isi.pegasus.planner.catalog.transformation.classes.TCType;
import edu.isi.pegasus.planner.catalog.transformation.classes.VDSSysInfo;
import edu.isi.pegasus.planner.classes.Profile;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Tests covering the pattern exercised by TestTransformationCatalog#main — creating, configuring
 * and inspecting TransformationCatalogEntry objects without a live catalog backend.
 */
public class TestTransformationCatalogTest {

    @Test
    public void testCreateEntryWithNamespaceNameVersion() {
        TransformationCatalogEntry entry =
                new TransformationCatalogEntry("pegasus", "preprocess", null);
        assertThat(entry.getLogicalNamespace(), is("pegasus"));
        assertThat(entry.getLogicalName(), is("preprocess"));
        assertThat(entry.getLogicalVersion(), is(nullValue()));
    }

    @Test
    public void testSetPhysicalTransformationAndResourceId() {
        TransformationCatalogEntry entry =
                new TransformationCatalogEntry("pegasus", "preprocess", null);
        entry.setPhysicalTransformation("/usr/pegasus/bin/keg");
        entry.setResourceId("isi");
        assertThat(entry.getPhysicalTransformation(), is("/usr/pegasus/bin/keg"));
        assertThat(entry.getResourceId(), is("isi"));
    }

    @Test
    public void testSetTypeInstalled() {
        TransformationCatalogEntry entry =
                new TransformationCatalogEntry("pegasus", "preprocess", null);
        entry.setType(TCType.INSTALLED);
        assertThat(entry.getType(), is(TCType.INSTALLED));
    }

    @Test
    public void testSetVDSSysInfo() {
        TransformationCatalogEntry entry =
                new TransformationCatalogEntry("pegasus", "preprocess", null);
        entry.setVDSSysInfo(new VDSSysInfo(Arch.INTEL32, Os.LINUX, null, null));
        assertThat(entry.getSysInfo(), is(notNullValue()));
    }

    @Test
    public void testAddEnvProfile() {
        TransformationCatalogEntry entry =
                new TransformationCatalogEntry("pegasus", "preprocess", null);
        entry.addProfile(new Profile(Profile.ENV, "PEGASUS_HOME", "/usr/pegasus/bin"));
        List profiles = entry.getProfiles(Profile.ENV);
        assertThat(profiles, is(notNullValue()));
        assertThat(profiles.isEmpty(), is(false));
    }

    @Test
    public void testToStringContainsPhysicalName() {
        TransformationCatalogEntry entry =
                new TransformationCatalogEntry("pegasus", "preprocess", null);
        entry.setPhysicalTransformation("/usr/pegasus/bin/keg");
        String s = entry.toString();
        assertThat(s, containsString("/usr/pegasus/bin/keg"));
    }

    @Test
    public void testVDSSysInfoIntel32Linux() {
        VDSSysInfo sysInfo = new VDSSysInfo(Arch.INTEL32, Os.LINUX, null, null);
        assertThat(sysInfo.getArch(), is(sameInstance(Arch.INTEL32)));
        assertThat(sysInfo.getOs(), is(sameInstance(Os.LINUX)));
    }

    @Test
    public void testClonedEntryIsEquivalent() {
        TransformationCatalogEntry entry =
                new TransformationCatalogEntry("pegasus", "preprocess", null);
        entry.setPhysicalTransformation("/bin/keg");
        entry.setResourceId("isi");
        entry.setType(TCType.INSTALLED);
        TransformationCatalogEntry clone = (TransformationCatalogEntry) entry.clone();
        assertThat(clone.getLogicalName(), is(entry.getLogicalName()));
        assertThat(clone.getResourceId(), is(entry.getResourceId()));
        assertThat(clone.getType(), is(entry.getType()));
    }
}
