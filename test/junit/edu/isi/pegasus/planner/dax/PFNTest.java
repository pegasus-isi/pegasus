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

import edu.isi.pegasus.common.util.XMLWriter;
import java.io.StringWriter;
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
        assertNotNull(mPFN, "PFN should be instantiatable");
    }

    @Test
    public void testGetURL() {
        assertEquals(
                "gsiftp://server/path/to/file", mPFN.getURL(), "getURL() should return the URL");
    }

    @Test
    public void testGetSite() {
        assertEquals("local", mPFN.getSite(), "getSite() should return the site");
    }

    @Test
    public void testConstructorUrlOnly() {
        PFN pfn = new PFN("file:///tmp/data.txt");
        assertEquals("file:///tmp/data.txt", pfn.getURL(), "URL-only constructor should set URL");
        assertEquals("", pfn.getSite(), "Site should be empty string when not specified");
    }

    @Test
    public void testDefaultConstructor() {
        PFN pfn = new PFN();
        assertNull(pfn.getURL(), "Default constructor should have null URL");
        assertEquals("", pfn.getSite(), "Default constructor should have empty site");
    }

    @Test
    public void testSetURL() {
        mPFN.setURL("gsiftp://other/path");
        assertEquals("gsiftp://other/path", mPFN.getURL(), "setURL should update the URL");
    }

    @Test
    public void testSetSite() {
        mPFN.setSite("remote");
        assertEquals("remote", mPFN.getSite(), "setSite should update the site");
    }

    @Test
    public void testSetURLReturnsSelf() {
        PFN result = mPFN.setURL("new-url");
        assertSame(mPFN, result, "setURL should return this for chaining");
    }

    @Test
    public void testSetSiteReturnsSelf() {
        PFN result = mPFN.setSite("remote");
        assertSame(mPFN, result, "setSite should return this for chaining");
    }

    @Test
    public void testAddProfile() {
        mPFN.addProfile("pegasus", "transfer.threads", "4");
        assertFalse(mPFN.getProfiles().isEmpty(), "Profiles should not be empty after addProfile");
        assertEquals(1, mPFN.getProfiles().size(), "Should have one profile");
    }

    @Test
    public void testAddProfileWithNamespaceEnum() {
        mPFN.addProfile(Profile.NAMESPACE.pegasus, "maxwalltime", "3600");
        assertFalse(
                mPFN.getProfiles().isEmpty(),
                "Profiles should not be empty after addProfile with enum");
    }

    @Test
    public void testAddProfilesObject() {
        Profile p = new Profile("condor", "universe", "vanilla");
        mPFN.addProfiles(p);
        assertEquals(
                1, mPFN.getProfiles().size(), "Should have one profile after addProfiles(Profile)");
    }

    @Test
    public void testInitialProfilesEmpty() {
        PFN pfn = new PFN("file:///tmp/test.txt");
        assertTrue(pfn.getProfiles().isEmpty(), "New PFN should have empty profiles");
    }

    @Test
    public void testXMLSerialization() {
        StringWriter sw = new StringWriter();
        XMLWriter writer = new XMLWriter(sw);
        mPFN.toXML(writer);
        String result = sw.toString();
        assertTrue(result.contains("pfn"), "XML should contain 'pfn' element");
        assertTrue(result.contains("gsiftp://server/path/to/file"), "XML should contain the URL");
        assertTrue(result.contains("local"), "XML should contain the site");
    }

    @Test
    public void testXMLSerializationNoSite() {
        PFN pfn = new PFN("file:///tmp/data.txt");
        StringWriter sw = new StringWriter();
        XMLWriter writer = new XMLWriter(sw);
        pfn.toXML(writer, 0);
        String result = sw.toString();
        assertTrue(result.contains("pfn"), "XML should contain 'pfn' element");
        assertTrue(result.contains("file:///tmp/data.txt"), "XML should contain the URL");
        assertFalse(
                result.contains("site="),
                "XML should not contain site attribute when site is null");
    }
}
