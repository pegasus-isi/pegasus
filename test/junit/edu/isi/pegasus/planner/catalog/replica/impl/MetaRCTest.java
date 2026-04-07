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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.*;

import edu.isi.pegasus.planner.catalog.replica.ReplicaCatalogEntry;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.*;

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

    @BeforeEach
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
        assertThat(mMetaRC.list().size(), is(2));
        assertThat(mMetaRC.list().contains("f.a"), is(false));
        assertThat(mMetaRC.list().contains("f.b1"), is(true));
        assertThat(mMetaRC.list().contains("f.b2"), is(true));
    }

    @Test
    public void checkParsedRCE() {
        String lfn = "f.b1";
        assertThat(mMetaRC.list().size(), is(2));
        assertThat(mMetaRC.list().contains(lfn), is(true));
        Collection<ReplicaCatalogEntry> rces = mMetaRC.lookup(lfn);
        assertThat(rces.size(), is(1));
        for (ReplicaCatalogEntry rce : rces) {
            assertThat(rce.getPFN(), is(nullValue()));
            assertThat(rce.getAttributeCount(), is(6));
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
    public void lookupWholeCatalogWithConstraints() {
        Map<String, Collection<ReplicaCatalogEntry>> m = mMetaRC.lookup(new HashMap());
        String lfn = "f.b1";
        assertThat(m.entrySet().size(), is(2));
        assertThat(m.containsKey(lfn), is(true));
        Collection<ReplicaCatalogEntry> rces = m.get(lfn);
        assertThat(rces.size(), is(1));
        for (ReplicaCatalogEntry rce : rces) {
            assertThat(rce.getPFN(), is(nullValue()));
            assertThat(rce.getAttributeCount(), is(6));
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
        assertThat(c.contains(new ReplicaCatalogEntry("b")), is(true));
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
        assertThat(c.contains(rc), is(true));
    }

    @Test
    public void multipleSimpleInsert() {
        mMetaRC.insert("a", new ReplicaCatalogEntry("b"));
        mMetaRC.insert("a", new ReplicaCatalogEntry("b", "handle"));
        mMetaRC.insert("a", new ReplicaCatalogEntry("c"));
        mMetaRC.insert("a", new ReplicaCatalogEntry("c", "handle"));

        Collection<ReplicaCatalogEntry> c = mMetaRC.lookup("a");
        assertThat(c.contains(new ReplicaCatalogEntry("b")), is(true));
        assertThat(c.contains(new ReplicaCatalogEntry("b", "handle")), is(true));
        assertThat(c.contains(new ReplicaCatalogEntry("c")), is(true));
        assertThat(c.contains(new ReplicaCatalogEntry("c", "handle")), is(true));
    }

    @AfterEach
    public void tearDown() {
        mRCFile.delete();
    }

    private void assertAttribute(
            String expectedKey, String expectedValue, ReplicaCatalogEntry rce) {
        assertThat(rce.getAttribute(expectedKey), is(expectedValue));
    }
}
