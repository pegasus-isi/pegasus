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
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.*;

import edu.isi.pegasus.planner.catalog.transformation.classes.TCType;
import edu.isi.pegasus.planner.classes.Profile;
import edu.isi.pegasus.planner.dax.Executable;
import edu.isi.pegasus.planner.dax.Invoke;
import org.junit.jupiter.api.Test;

/**
 * Tests for TransformationCatalogEntry from the transformation package perspective. (Complements
 * the more detailed tests in classes.TransformationCatalogEntryTest.)
 */
public class TransformationCatalogEntryTest {

    @Test
    public void testDefaultConstructorCreatesEntry() {
        TransformationCatalogEntry entry = new TransformationCatalogEntry();
        assertThat(entry.getLogicalNamespace(), is(nullValue()));
        assertThat(entry.getLogicalName(), is(nullValue()));
        assertThat(entry.getLogicalVersion(), is(nullValue()));
    }

    @Test
    public void testThreeArgConstructorSetsNamespaceNameVersion() {
        TransformationCatalogEntry entry = new TransformationCatalogEntry("pegasus", "keg", "1.0");
        assertThat(entry.getLogicalNamespace(), equalTo("pegasus"));
        assertThat(entry.getLogicalName(), equalTo("keg"));
        assertThat(entry.getLogicalVersion(), equalTo("1.0"));
    }

    @Test
    public void testDefaultTypeIsInstalled() {
        TransformationCatalogEntry entry = new TransformationCatalogEntry("ns", "name", "1.0");
        assertThat(entry.getType(), is(TCType.INSTALLED));
    }

    @Test
    public void testSetTypeToStageable() {
        TransformationCatalogEntry entry = new TransformationCatalogEntry("ns", "name", "1.0");
        entry.setType(TCType.STAGEABLE);
        assertThat(entry.getType(), is(TCType.STAGEABLE));
    }

    @Test
    public void testSetTypeNullFallsBackToInstalled() {
        TransformationCatalogEntry entry = new TransformationCatalogEntry("ns", "name", "1.0");
        entry.setType(null);
        assertThat(entry.getType(), is(TCType.INSTALLED));
    }

    @Test
    public void testSetPhysicalTransformation() {
        TransformationCatalogEntry entry = new TransformationCatalogEntry("ns", "name", "1.0");
        entry.setPhysicalTransformation("/usr/bin/keg");
        assertThat(entry.getPhysicalTransformation(), equalTo("/usr/bin/keg"));
    }

    @Test
    public void testSetResourceId() {
        TransformationCatalogEntry entry = new TransformationCatalogEntry("ns", "name", "1.0");
        entry.setResourceId("isi");
        assertThat(entry.getResourceId(), equalTo("isi"));
    }

    @Test
    public void testBypassStagingDefaultIsFalse() {
        TransformationCatalogEntry entry = new TransformationCatalogEntry("ns", "name", "1.0");
        assertThat(entry.bypassStaging(), is(false));
    }

    @Test
    public void testSetBypassStaging() {
        TransformationCatalogEntry entry = new TransformationCatalogEntry("ns", "name", "1.0");
        entry.setForBypassStaging(true);
        assertThat(entry.bypassStaging(), is(true));
    }

    @Test
    public void testSetForBypassStagingNoArgSetsFlagToTrue() {
        TransformationCatalogEntry entry = new TransformationCatalogEntry("ns", "name", "1.0");
        entry.setForBypassStaging();
        assertThat(entry.bypassStaging(), is(true));
    }

    @Test
    public void testAddProfileIncreasesProfileCount() {
        TransformationCatalogEntry entry = new TransformationCatalogEntry("ns", "name", "1.0");
        entry.addProfile(new Profile("env", "JAVA_HOME", "/opt/java"));
        assertThat(entry.getProfiles(), is(notNullValue()));
        assertThat(entry.getProfiles().size(), is(1));
    }

    @Test
    public void testGetLogicalTransformationCombinesNamespaceNameVersion() {
        TransformationCatalogEntry entry = new TransformationCatalogEntry("pegasus", "keg", "1.0");
        String lt = entry.getLogicalTransformation();
        assertThat(lt, is(notNullValue()));
        assertThat(lt, containsString("keg"));
    }

    @Test
    public void testSetLogicalTransformationParsesFullyQualifiedName() {
        TransformationCatalogEntry entry = new TransformationCatalogEntry();

        entry.setLogicalTransformation("pegasus::keg:1.0");

        assertThat(entry.getLogicalNamespace(), equalTo("pegasus"));
        assertThat(entry.getLogicalName(), equalTo("keg"));
        assertThat(entry.getLogicalVersion(), equalTo("1.0"));
    }

    @Test
    public void testToTCStringUsesNullWhenProfilesAreAbsent() {
        TransformationCatalogEntry entry = new TransformationCatalogEntry("pegasus", "keg", "1.0");
        entry.setResourceId("local");
        entry.setPhysicalTransformation("/usr/bin/keg");

        String tcString = entry.toTCString();

        assertThat(tcString, containsString("local"));
        assertThat(tcString, containsString("/usr/bin/keg"));
        assertThat(tcString.endsWith("NULL"), is(true));
    }

    @Test
    public void testToXMLSelfClosesWhenProfilesAreAbsent() {
        TransformationCatalogEntry entry = new TransformationCatalogEntry("pegasus", "keg", "1.0");
        entry.setResourceId("local");
        entry.setPhysicalTransformation("/usr/bin/keg");

        String xml = entry.toXML();

        assertThat(xml, containsString("physicalName=\"/usr/bin/keg\""));
        assertThat(xml, containsString("siteid=\"local\""));
        assertThat(xml, containsString("/>"));
    }

    @Test
    public void testAddDependantTransformationStringCreatesExecutableRequirement() {
        TransformationCatalogEntry entry = new TransformationCatalogEntry("pegasus", "keg", "1.0");

        entry.addDependantTransformation("ns::helper:2.0");

        assertThat(entry.getDependantFiles(), hasSize(1));
        assertThat(entry.getDependantFiles().get(0).getLFN(), equalTo("ns::helper:2.0"));
    }

    @Test
    public void testAddRequirementFormatsNamespaceNameAndVersion() {
        TransformationCatalogEntry entry = new TransformationCatalogEntry("pegasus", "keg", "1.0");
        Executable executable = new Executable("helper", "ns", "2.0");

        entry.addRequirement(executable);

        assertThat(entry.getRequirements(), hasSize(1));
        assertThat(entry.getRequirements().get(0), equalTo("helper::ns:2.0"));
    }

    @Test
    public void testCloneProducesEquivalentEntry() {
        TransformationCatalogEntry original =
                new TransformationCatalogEntry("pegasus", "keg", "1.0");
        original.setPhysicalTransformation("/usr/bin/keg");
        original.setResourceId("isi");
        original.setType(TCType.STAGEABLE);

        TransformationCatalogEntry clone = (TransformationCatalogEntry) original.clone();
        assertThat(clone.getLogicalNamespace(), equalTo(original.getLogicalNamespace()));
        assertThat(clone.getLogicalName(), equalTo(original.getLogicalName()));
        assertThat(clone.getLogicalVersion(), equalTo(original.getLogicalVersion()));
        assertThat(
                clone.getPhysicalTransformation(), equalTo(original.getPhysicalTransformation()));
        assertThat(clone.getResourceId(), equalTo(original.getResourceId()));
        assertThat(clone.getType(), equalTo(original.getType()));
    }

    @Test
    public void testCloneCarriesOverNotificationsAndDependantFiles() {
        TransformationCatalogEntry original =
                new TransformationCatalogEntry("pegasus", "keg", "1.0");
        original.addNotification(new Invoke(Invoke.WHEN.start, "/bin/date"));
        original.addDependantTransformation("ns::helper:2.0");

        TransformationCatalogEntry clone = (TransformationCatalogEntry) original.clone();

        assertThat(clone.getNotifications(Invoke.WHEN.start), hasSize(1));
        assertThat(clone.getDependantFiles(), hasSize(1));
        assertThat(clone.getDependantFiles().get(0).getLFN(), equalTo("ns::helper:2.0"));
    }

    @Test
    public void testToStringContainsLogicalName() {
        TransformationCatalogEntry entry = new TransformationCatalogEntry("pegasus", "keg", "1.0");
        String s = entry.toString();
        assertThat(s, containsString("keg"));
    }

    @Test
    public void testNullNamespaceAllowed() {
        TransformationCatalogEntry entry = new TransformationCatalogEntry(null, "keg", null);
        assertThat(entry.getLogicalNamespace(), is(nullValue()));
        assertThat(entry.getLogicalName(), equalTo("keg"));
        assertThat(entry.getLogicalVersion(), is(nullValue()));
    }
}
