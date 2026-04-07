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
package edu.isi.pegasus.planner.catalog.transformation.client;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.*;

import edu.isi.pegasus.planner.catalog.classes.SysInfo;
import edu.isi.pegasus.planner.catalog.transformation.TransformationCatalogEntry;
import edu.isi.pegasus.planner.catalog.transformation.classes.TCType;
import edu.isi.pegasus.planner.catalog.transformation.classes.TransformationStore;
import edu.isi.pegasus.planner.classes.Profile;
import org.junit.jupiter.api.Test;

/** Tests for TCFormatUtility. */
public class TCFormatUtilityTest {

    private TransformationStore buildStoreWithSingleEntry() {
        TransformationCatalogEntry entry = new TransformationCatalogEntry("pegasus", "keg", "1.0");
        entry.setPhysicalTransformation("/usr/bin/keg");
        entry.setResourceId("isi");
        entry.setType(TCType.INSTALLED);
        TransformationStore store = new TransformationStore();
        store.addEntry(entry);
        return store;
    }

    @Test
    public void testToTextFormatReturnsNonNullString() {
        TransformationStore store = buildStoreWithSingleEntry();
        String result = TCFormatUtility.toTextFormat(store);
        assertThat(result, is(notNullValue()));
    }

    @Test
    public void testToTextFormatContainsTransformationName() {
        TransformationStore store = buildStoreWithSingleEntry();
        String result = TCFormatUtility.toTextFormat(store);
        assertThat(result, containsString("keg"));
    }

    @Test
    public void testToTextFormatContainsSiteBlock() {
        TransformationStore store = buildStoreWithSingleEntry();
        String result = TCFormatUtility.toTextFormat(store);
        assertThat(result, containsString("site"));
        assertThat(result, containsString("isi"));
    }

    @Test
    public void testToTextFormatContainsPfn() {
        TransformationStore store = buildStoreWithSingleEntry();
        String result = TCFormatUtility.toTextFormat(store);
        assertThat(result, containsString("/usr/bin/keg"));
    }

    @Test
    public void testToTextFormatContainsTrKeyword() {
        TransformationStore store = buildStoreWithSingleEntry();
        String result = TCFormatUtility.toTextFormat(store);
        assertThat(result, containsString("tr "));
    }

    @Test
    public void testToTextFormatWithProfile() {
        TransformationCatalogEntry entry = new TransformationCatalogEntry("test", "app", null);
        entry.setPhysicalTransformation("/bin/app");
        entry.setResourceId("local");
        entry.setType(TCType.STAGEABLE);
        entry.addProfile(new Profile("env", "HOME", "/home/user"));

        TransformationStore store = new TransformationStore();
        store.addEntry(entry);

        String result = TCFormatUtility.toTextFormat(store);
        assertThat(result, is(notNullValue()));
        assertThat(result, containsString("app"));
        assertThat(result, containsString("profile"));
        assertThat(result, containsString("HOME"));
    }

    @Test
    public void testToTextFormatEmptyStoreReturnsHeaderOnly() {
        TransformationStore store = new TransformationStore();
        String result = TCFormatUtility.toTextFormat(store);
        assertThat(result, is(notNullValue()));
        assertThat(result, not(is("")));
    }

    @Test
    public void testToTextFormatTypeAppears() {
        TransformationStore store = buildStoreWithSingleEntry();
        String result = TCFormatUtility.toTextFormat(store);
        assertThat(result, either(containsString("INSTALLED")).or(containsString("installed")));
    }

    @Test
    public void testToTextFormatWithMultipleEntries() {
        TransformationCatalogEntry entry1 = new TransformationCatalogEntry("ns", "app1", "1.0");
        entry1.setPhysicalTransformation("/bin/app1");
        entry1.setResourceId("site1");
        entry1.setType(TCType.INSTALLED);

        TransformationCatalogEntry entry2 = new TransformationCatalogEntry("ns", "app2", "1.0");
        entry2.setPhysicalTransformation("/bin/app2");
        entry2.setResourceId("site2");
        entry2.setType(TCType.STAGEABLE);

        TransformationStore store = new TransformationStore();
        store.addEntry(entry1);
        store.addEntry(entry2);

        String result = TCFormatUtility.toTextFormat(store);
        assertThat(result, is(notNullValue()));
        assertThat(result, either(containsString("app1")).or(containsString("app2")));
    }

    @Test
    public void testToTextFormatGroupsMultipleSitesUnderSingleTransformationBlock() {
        TransformationCatalogEntry entry1 = new TransformationCatalogEntry("ns", "app", "1.0");
        entry1.setPhysicalTransformation("/bin/app-site1");
        entry1.setResourceId("site1");
        entry1.setType(TCType.INSTALLED);

        TransformationCatalogEntry entry2 = new TransformationCatalogEntry("ns", "app", "1.0");
        entry2.setPhysicalTransformation("/bin/app-site2");
        entry2.setResourceId("site2");
        entry2.setType(TCType.STAGEABLE);

        TransformationStore store = new TransformationStore();
        store.addEntry(entry1);
        store.addEntry(entry2);

        String result = TCFormatUtility.toTextFormat(store);

        assertThat(countOccurrences(result, "tr ns::app:1.0 {"), is(1));
        assertThat(result, containsString("site site1 {"));
        assertThat(result, containsString("site site2 {"));
    }

    @Test
    public void testToTextFormatIncludesSysInfoFieldsWhenPresent() {
        TransformationCatalogEntry entry = new TransformationCatalogEntry("ns", "app", "1.0");
        entry.setPhysicalTransformation("/bin/app");
        entry.setResourceId("local");
        entry.setType(TCType.STAGEABLE);

        SysInfo sysInfo = new SysInfo();
        sysInfo.setArchitecture(SysInfo.Architecture.amd64);
        sysInfo.setOS(SysInfo.OS.windows);
        sysInfo.setOSRelease("rhel");
        sysInfo.setOSVersion("8");
        sysInfo.setGlibc("2.28");
        entry.setSysInfo(sysInfo);

        TransformationStore store = new TransformationStore();
        store.addEntry(entry);

        String result = TCFormatUtility.toTextFormat(store);

        assertThat(result, containsString("arch \"amd64\""));
        assertThat(result, containsString("os \"windows\""));
        assertThat(result, containsString("osrelease \"rhel\""));
        assertThat(result, containsString("osversion \"8\""));
        assertThat(result, containsString("glibc \"2.28\""));
    }

    @Test
    public void testToTextFormatQuotesProfileValuesAndPfn() {
        TransformationCatalogEntry entry = new TransformationCatalogEntry("ns", "quoted", "1.0");
        entry.setPhysicalTransformation("/path with spaces/tool");
        entry.setResourceId("local");
        entry.setType(TCType.INSTALLED);
        entry.addProfile(new Profile("env", "DATA_DIR", "/tmp/with spaces"));

        TransformationStore store = new TransformationStore();
        store.addEntry(entry);

        String result = TCFormatUtility.toTextFormat(store);

        assertThat(result, containsString("profile env \"DATA_DIR\" \"/tmp/with spaces\""));
        assertThat(result, containsString("pfn \"/path with spaces/tool\""));
    }

    @Test
    public void testToTextFormatOmitsEmptySysInfoFields() {
        TransformationCatalogEntry entry = new TransformationCatalogEntry("ns", "app", "1.0");
        entry.setPhysicalTransformation("/bin/app");
        entry.setResourceId("local");
        entry.setType(TCType.INSTALLED);

        SysInfo sysInfo = new SysInfo();
        sysInfo.setOSRelease("");
        sysInfo.setOSVersion("");
        sysInfo.setGlibc("");
        entry.setSysInfo(sysInfo);

        TransformationStore store = new TransformationStore();
        store.addEntry(entry);

        String result = TCFormatUtility.toTextFormat(store);

        assertThat(result, not(containsString("osrelease ")));
        assertThat(result, not(containsString("osversion ")));
        assertThat(result, not(containsString("glibc ")));
    }

    private int countOccurrences(String value, String token) {
        int count = 0;
        int index = 0;

        while ((index = value.indexOf(token, index)) != -1) {
            count++;
            index += token.length();
        }

        return count;
    }
}
