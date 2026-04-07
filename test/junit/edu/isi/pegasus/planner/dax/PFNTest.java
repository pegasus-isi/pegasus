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

import edu.isi.pegasus.common.util.XMLWriter;
import java.io.StringWriter;
import java.util.Arrays;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Tests for the PFN class. */
public class PFNTest {

    private PFN mPFN;

    @BeforeEach
    public void setUp() {
        mPFN = new PFN("gsiftp://server/path/to/file", "local");
    }

    @Test
    public void testInstantiation() {
        assertThat(mPFN, notNullValue());
    }

    @Test
    public void testGetURL() {
        assertThat(mPFN.getURL(), is("gsiftp://server/path/to/file"));
    }

    @Test
    public void testGetSite() {
        assertThat(mPFN.getSite(), is("local"));
    }

    @Test
    public void testConstructorUrlOnly() {
        PFN pfn = new PFN("file:///tmp/data.txt");
        assertThat(pfn.getURL(), is("file:///tmp/data.txt"));
        assertThat(pfn.getSite(), is(""));
    }

    @Test
    public void testDefaultConstructor() {
        PFN pfn = new PFN();
        assertThat(pfn.getURL(), nullValue());
        assertThat(pfn.getSite(), is(""));
    }

    @Test
    public void testSetURL() {
        mPFN.setURL("gsiftp://other/path");
        assertThat(mPFN.getURL(), is("gsiftp://other/path"));
    }

    @Test
    public void testSetSite() {
        mPFN.setSite("remote");
        assertThat(mPFN.getSite(), is("remote"));
    }

    @Test
    public void testSetURLReturnsSelf() {
        PFN result = mPFN.setURL("new-url");
        assertThat(result, sameInstance(mPFN));
    }

    @Test
    public void testSetSiteReturnsSelf() {
        PFN result = mPFN.setSite("remote");
        assertThat(result, sameInstance(mPFN));
    }

    @Test
    public void testAddProfile() {
        mPFN.addProfile("pegasus", "transfer.threads", "4");
        assertThat(mPFN.getProfiles().isEmpty(), is(false));
        assertThat(mPFN.getProfiles().size(), is(1));
    }

    @Test
    public void testAddProfileWithNamespaceEnum() {
        mPFN.addProfile(Profile.NAMESPACE.pegasus, "maxwalltime", "3600");
        assertThat(mPFN.getProfiles().isEmpty(), is(false));
    }

    @Test
    public void testAddProfilesObject() {
        Profile p = new Profile("condor", "universe", "vanilla");
        mPFN.addProfiles(p);
        assertThat(mPFN.getProfiles().size(), is(1));
    }

    @Test
    public void testAddProfilesListAppendsInOrder() {
        Profile first = new Profile("env", "A", "1");
        Profile second = new Profile("env", "B", "2");

        mPFN.addProfiles(Arrays.asList(first, second));

        assertThat(mPFN.getProfiles().size(), is(2));
        assertThat(mPFN.getProfiles().get(0), sameInstance(first));
        assertThat(mPFN.getProfiles().get(1), sameInstance(second));
    }

    @Test
    public void testAddProfilesListReturnsSelf() {
        PFN result = mPFN.addProfiles(Arrays.asList(new Profile("env", "K", "V")));

        assertThat(result, sameInstance(mPFN));
    }

    @Test
    public void testInitialProfilesEmpty() {
        PFN pfn = new PFN("file:///tmp/test.txt");
        assertThat(pfn.getProfiles().isEmpty(), is(true));
    }

    @Test
    public void testXMLSerialization() {
        StringWriter sw = new StringWriter();
        XMLWriter writer = new XMLWriter(sw);
        mPFN.toXML(writer);
        String result = sw.toString();
        assertThat(
                result,
                allOf(
                        containsString("pfn"),
                        containsString("gsiftp://server/path/to/file"),
                        containsString("local")));
    }

    @Test
    public void testXMLSerializationNoSite() {
        PFN pfn = new PFN("file:///tmp/data.txt");
        StringWriter sw = new StringWriter();
        XMLWriter writer = new XMLWriter(sw);
        pfn.toXML(writer, 0);
        String result = sw.toString();
        assertThat(result, allOf(containsString("pfn"), containsString("file:///tmp/data.txt")));
        assertThat(result.contains("site="), is(false));
    }

    @Test
    public void testSetSiteNullReturnsEmptyStringFromGetter() {
        mPFN.setSite(null);

        assertThat(mPFN.getSite(), is(""));
    }

    @Test
    public void testXMLSerializationIncludesNestedProfiles() {
        mPFN.addProfile("env", "JAVA_HOME", "/usr/java");
        StringWriter sw = new StringWriter();
        XMLWriter writer = new XMLWriter(sw);

        mPFN.toXML(writer, 1);

        String result = sw.toString();
        assertThat(
                result,
                allOf(
                        containsString("<profile"),
                        containsString("namespace=\"env\""),
                        containsString("key=\"JAVA_HOME\""),
                        containsString("/usr/java")));
    }
}
