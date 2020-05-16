/*
 * Copyright 2007-2014 University Of Southern California
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.isi.pegasus.planner.catalog.replica.impl;

import static org.junit.Assert.*;

import edu.isi.pegasus.planner.catalog.replica.ReplicaCatalogEntry;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import org.junit.*;

/**
 * Test class to test YAML based RC with Regex Support
 *
 * @author Karan Vahi
 * @author Rajiv Mayani
 */
public class MetaRCTest {

    private Meta mMetaRC = null;
    private File mRCFile = null;

    public MetaRCTest() {}

    @Before
    public void setUp() throws IOException {
        mMetaRC = new Meta();
        mRCFile = File.createTempFile("replica", ".meta");
        BufferedWriter writer = new BufferedWriter(new FileWriter(mRCFile));
        writer.write(
                "[\n"
                        + "  {\n"
                        + "    \"_id\": \"f.b2\",\n"
                        + "    \"_type\": \"file\",\n"
                        + "    \"_attributes\": {\n"
                        + "      \"user\": \"bamboo\",\n"
                        + "      \"size\": \"56\",\n"
                        + "      \"ctime\": \"2020-05-15T10:05:04-07:00\",\n"
                        + "      \"checksum.type\": \"sha256\",\n"
                        + "      \"checksum.value\": \"a69fef1a4b597ea5e61ce403b6ef8bb5b4cd3aba19e734bf340ea00f5095c894\",\n"
                        + "      \"checksum.timing\": \"0.0\"\n"
                        + "    }\n"
                        + "  },\n"
                        + "  {\n"
                        + "    \"_id\": \"f.b1\",\n"
                        + "    \"_type\": \"file\",\n"
                        + "    \"_attributes\": {\n"
                        + "      \"user\": \"bamboo\",\n"
                        + "      \"size\": \"56\",\n"
                        + "      \"ctime\": \"2020-05-15T10:05:04-07:00\",\n"
                        + "      \"checksum.type\": \"sha256\",\n"
                        + "      \"checksum.value\": \"a69fef1a4b597ea5e61ce403b6ef8bb5b4cd3aba19e734bf340ea00f5095c894\",\n"
                        + "      \"checksum.timing\": \"0.0\"\n"
                        + "    }\n"
                        + "  }\n"
                        + "]");
        writer.close();
        mMetaRC.connect(mRCFile.getAbsolutePath());
    }

    @Test
    public void simpleChecksOnMetaFile() {
        assertEquals("Number of Entries in RC ", 2, mMetaRC.list().size());
        assertFalse(mMetaRC.list().contains("f.a"));
        assertTrue(mMetaRC.list().contains("f.b1"));
        assertTrue(mMetaRC.list().contains("f.b2"));
    }

    @Test
    public void checkParsedRCE() {
        String lfn = "f.b1";
        assertEquals("Number of Entries in RC ", 2, mMetaRC.list().size());
        assertTrue("Replica Catalog should contain lfn", mMetaRC.list().contains(lfn));
        Collection<ReplicaCatalogEntry> rces = mMetaRC.lookup(lfn);
        assertEquals("There should be only one rce for lfn " + lfn, 1, rces.size());
        for (ReplicaCatalogEntry rce : rces) {
            assertNull("PFN for RCE should be null", rce.getPFN());
            assertEquals("Number of attributes found", 6, rce.getAttributeCount());
            assertAttribute("user", "bamboo", rce);

            assertAttribute("size", "56", rce);
            assertAttribute("ctime", "2020-05-15T10:05:04-07:00", rce);
            assertAttribute("checksum.type", "sha256", rce);
            assertAttribute(
                    "checksum.value",
                    "a69fef1a4b597ea5e61ce403b6ef8bb5b4cd3aba19e734bf340ea00f5095c894",
                    rce);
            assertAttribute("checksum.timing", "0.0", rce);
        }
    }

    @Test
    public void lookupWholeCatalogWithConstraints(){
        Map<String,Collection<ReplicaCatalogEntry>> m = mMetaRC.lookup(new HashMap());
        String lfn = "f.b1";
        assertEquals("Number of Entries in map ", 2, m.entrySet().size());
        assertTrue("MAP should contain lfn", m.containsKey(lfn));
        Collection<ReplicaCatalogEntry> rces = m.get(lfn);
        assertEquals("There should be only one rce for lfn " + lfn, 1, rces.size());
        for (ReplicaCatalogEntry rce : rces) {
            assertNull("PFN for RCE should be null", rce.getPFN());
            assertEquals("Number of attributes found", 6, rce.getAttributeCount());
            assertAttribute("user", "bamboo", rce);

            assertAttribute("size", "56", rce);
            assertAttribute("ctime", "2020-05-15T10:05:04-07:00", rce);
            assertAttribute("checksum.type", "sha256", rce);
            assertAttribute(
                    "checksum.value",
                    "a69fef1a4b597ea5e61ce403b6ef8bb5b4cd3aba19e734bf340ea00f5095c894",
                    rce);
            assertAttribute("checksum.timing", "0.0", rce);
        }
    }
    
    
    @Test
    public void simpleInsert() {
        mMetaRC.insert("a", new ReplicaCatalogEntry("b"));
        Collection<ReplicaCatalogEntry> c = mMetaRC.lookup("a");
        assertTrue(c.contains(new ReplicaCatalogEntry("b")));
    }

    @Test
    public void simpleInsertWithMetadata() {
        ReplicaCatalogEntry rc = new ReplicaCatalogEntry("b");
        rc.addAttribute("user", "bamboo");
        rc.addAttribute("size", "100GB");
        // because of references create a new one
        mMetaRC.insert("a", (ReplicaCatalogEntry) rc.clone());
        rc.addAttribute("user", "bamboo");
        rc.addAttribute("size", "100GB");
        Collection<ReplicaCatalogEntry> c = mMetaRC.lookup("a");
        // System.err.println(c);
        assertTrue(c.contains(rc));
    }

    @Test
    public void multipleSimpleInsert() {
        mMetaRC.insert("a", new ReplicaCatalogEntry("b"));
        mMetaRC.insert("a", new ReplicaCatalogEntry("b", "handle"));
        mMetaRC.insert("a", new ReplicaCatalogEntry("c"));
        mMetaRC.insert("a", new ReplicaCatalogEntry("c", "handle"));

        Collection<ReplicaCatalogEntry> c = mMetaRC.lookup("a");
        assertTrue(c.contains(new ReplicaCatalogEntry("b")));
        assertTrue(c.contains(new ReplicaCatalogEntry("b", "handle")));
        assertTrue(c.contains(new ReplicaCatalogEntry("c")));
        assertTrue(c.contains(new ReplicaCatalogEntry("c", "handle")));
    }

    @After
    public void tearDown() {
        mRCFile.delete();
    }

    private void assertAttribute(
            String expectedKey, String expectedValue, ReplicaCatalogEntry rce) {
        assertEquals(rce.getAttribute(expectedKey), expectedValue);
    }
}
