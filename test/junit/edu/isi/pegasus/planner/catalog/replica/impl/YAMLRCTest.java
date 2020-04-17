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
import org.junit.*;

/**
 * Test class to test YAML based RC with Regex Support
 *
 * @author Karan Vahi
 * @author Rajiv Mayani
 */
public class YAMLRCTest {

    private YAML mYAMLRC = null;
    private File mRCFile = null;

    public YAMLRCTest() {}

    @Before
    public void setUp() throws IOException {
        mYAMLRC = new YAML();
        mRCFile = File.createTempFile("replica", ".yml");
        BufferedWriter writer = new BufferedWriter(new FileWriter(mRCFile));
        writer.write("pegasus: \"5.0\"\n");
        writer.write("replicas: \n");
        writer.write(
                "# matches \"f.a\"\n"
                        + "  - lfn: \"f.a\"\n"
                        + "    pfn: \"file:///Volumes/data/input/f.a\"\n"
                        + "    site: \"local\"");
        writer.close();
        mYAMLRC.connect(mRCFile.getAbsolutePath());
    }

    @Test
    public void simpleInsert() {
        mYAMLRC.insert("a", new ReplicaCatalogEntry("b"));
        Collection<ReplicaCatalogEntry> c = mYAMLRC.lookup("a");
        assertTrue(c.contains(new ReplicaCatalogEntry("b")));
    }

    @Test
    public void simpleInsertWithMetadata() {
        ReplicaCatalogEntry rc = new ReplicaCatalogEntry("b");
        rc.addAttribute("user", "bamboo");
        rc.addAttribute("size", "100GB");
        // because of references create a new one
        mYAMLRC.insert("a", (ReplicaCatalogEntry) rc.clone());
        rc.addAttribute("user", "bamboo");
        rc.addAttribute("size", "100GB");
        Collection<ReplicaCatalogEntry> c = mYAMLRC.lookup("a");
        // System.err.println(c);
        assertTrue(c.contains(rc));
    }

    @Test
    public void lookupWithSubstitutionsTest() {
        HashMap attr = new HashMap();
        attr.put("regex", "true");
        mYAMLRC.insert(
                "(\\w+)_f[xyz]_(\\d+)\\.sgt.*",
                new ReplicaCatalogEntry("file://test.isi.edu/scratch/[2]/[1]/[0]", attr));
        Collection<ReplicaCatalogEntry> c = mYAMLRC.lookup("TEST_fy_3810.sgt.md5");

        for (ReplicaCatalogEntry x : c) {
            assertEquals("file://test.isi.edu/scratch/3810/TEST/TEST_fy_3810.sgt.md5", x.getPFN());
        }

        c = mYAMLRC.lookup("TEST_fz_33810.sgt.md5");

        for (ReplicaCatalogEntry x : c) {
            assertEquals(
                    "file://test.isi.edu/scratch/33810/TEST/TEST_fz_33810.sgt.md5", x.getPFN());
        }

        c = mYAMLRC.lookup("TEST_fa_33810.sgt.md5");
        assertEquals(0, c.size());
    }

    @Test
    public void lookupWithSubstitutionsTestSummit() {
        HashMap attr = new HashMap();
        attr.put("regex", "true");
        attr.put("pool", "summit");
        mYAMLRC.insert(
                "(\\w+)_f[xyz]_(\\d+)\\.sgt.*",
                new ReplicaCatalogEntry(
                        "gsiftp://gridftp.ccs.ornl.gov/gpfs/alpine/scratch/callag/geo112/SGT_Storage/[1]/[0]",
                        attr));
        Collection<ReplicaCatalogEntry> c = mYAMLRC.lookup("USC_fx_7056.sgt");

        for (ReplicaCatalogEntry x : c) {
            assertEquals(
                    "gsiftp://gridftp.ccs.ornl.gov/gpfs/alpine/scratch/callag/geo112/SGT_Storage/USC/USC_fx_7056.sgt",
                    x.getPFN());
        }
    }

    @Test
    public void multipleSimpleInsert() {
        mYAMLRC.insert("a", new ReplicaCatalogEntry("b"));
        mYAMLRC.insert("a", new ReplicaCatalogEntry("b", "handle"));
        mYAMLRC.insert("a", new ReplicaCatalogEntry("c"));
        mYAMLRC.insert("a", new ReplicaCatalogEntry("c", "handle"));

        Collection<ReplicaCatalogEntry> c = mYAMLRC.lookup("a");
        assertTrue(c.contains(new ReplicaCatalogEntry("b")));
        assertTrue(c.contains(new ReplicaCatalogEntry("b", "handle")));
        assertTrue(c.contains(new ReplicaCatalogEntry("c")));
        assertTrue(c.contains(new ReplicaCatalogEntry("c", "handle")));
    }

    @Test
    public void simpleToRegexConversion() {
        HashMap attr = new HashMap();
        attr.put("regex", "true");

        mYAMLRC.insert("a", new ReplicaCatalogEntry("b"));
        mYAMLRC.insert("a", new ReplicaCatalogEntry("b", attr));

        Collection<ReplicaCatalogEntry> c = mYAMLRC.lookup("a");

        assertFalse(c.contains(new ReplicaCatalogEntry("b")));
        for (ReplicaCatalogEntry x : c) {
            assertEquals("b", x.getPFN());
            assertEquals("true", ((String) x.getAttribute("regex")));
        }
    }

    @Test
    public void regexToSimpleConversion() {
        HashMap attr = new HashMap();
        attr.put("regex", "true");

        mYAMLRC.insert("a", new ReplicaCatalogEntry("b", attr));
        mYAMLRC.insert("a", new ReplicaCatalogEntry("b"));

        Collection<ReplicaCatalogEntry> c = mYAMLRC.lookup("a");

        for (ReplicaCatalogEntry x : c) {
            assertEquals("b", x.getPFN());
            assertNull(x.getAttribute("regex"));
        }

        assertTrue(c.contains(new ReplicaCatalogEntry("b")));
    }
    
    public void serialization() {
        HashMap attr = new HashMap();
        attr.put("regex", "true");

        mYAMLRC.insert("a", new ReplicaCatalogEntry("file://tmp/file", attr));
        mYAMLRC.insert("a", new ReplicaCatalogEntry("file://tmp/file.a", attr));
        mYAMLRC.insert("b", new ReplicaCatalogEntry("file://tmp/b"));
        System.err.println(mYAMLRC.mFilename);
        mYAMLRC.close(); 
    }

    
    @After
    public void tearDown() {
        mRCFile.delete();
    }
}
