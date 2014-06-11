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

import edu.isi.pegasus.planner.catalog.replica.ReplicaCatalogEntry;
import edu.isi.pegasus.planner.catalog.replica.impl.Regex;
import org.junit.*;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;

import static org.junit.Assert.*;

/**
 * Test class to test Regex RC
 *
 * @author Rajiv Mayani
 */
public class RegexRCTest {

    private Regex regex = null;
    private File rcFile = null;

    public RegexRCTest() {
    }

    @Before
    public void setUp() throws IOException {
        regex = new Regex();
        rcFile = File.createTempFile("replica", ".txt");
        regex.connect(rcFile.getName());
    }

    @Test
    public void simpleInsert() {
        regex.insert("a", new ReplicaCatalogEntry("b"));
        Collection<ReplicaCatalogEntry> c = regex.lookup("a");
        assertTrue(c.contains(new ReplicaCatalogEntry("b")));
    }

    @Test
    public void lookupWithSubstitutionsTest() {
        HashMap attr = new HashMap();
        attr.put("regex", "true");
        regex.insert("(\\w+)_f[xyz]_(\\d+)\\.sgt.*", new ReplicaCatalogEntry("file://test.isi.edu/scratch/[2]/[1]/[0]", "handle", attr));
        Collection<ReplicaCatalogEntry> c = regex.lookup("TEST_fy_3810.sgt.md5");

        for (ReplicaCatalogEntry x : c) {
            assertEquals("file://test.isi.edu/scratch/3810/TEST/TEST_fy_3810.sgt.md5", x.getPFN());
        }

        c = regex.lookup("TEST_fz_33810.sgt.md5");

        for (ReplicaCatalogEntry x : c) {
            assertEquals("file://test.isi.edu/scratch/33810/TEST/TEST_fz_33810.sgt.md5", x.getPFN());
        }

        c = regex.lookup("TEST_fa_33810.sgt.md5");
        assertEquals(0, c.size());
    }

    @Test
    public void multipleSimpleInsert() {
        regex.insert("a", new ReplicaCatalogEntry("b"));
        regex.insert("a", new ReplicaCatalogEntry("b", "handle"));
        regex.insert("a", new ReplicaCatalogEntry("c"));
        regex.insert("a", new ReplicaCatalogEntry("c", "handle"));

        Collection<ReplicaCatalogEntry> c = regex.lookup("a");
        assertTrue(c.contains(new ReplicaCatalogEntry("b")));
        assertTrue(c.contains(new ReplicaCatalogEntry("b", "handle")));
        assertTrue(c.contains(new ReplicaCatalogEntry("c")));
        assertTrue(c.contains(new ReplicaCatalogEntry("c", "handle")));
    }

    @Test
    public void simpleToRegexConversion() {
        HashMap attr = new HashMap();
        attr.put("regex", "true");

        regex.insert("a", new ReplicaCatalogEntry("b", "handle"));
        regex.insert("a", new ReplicaCatalogEntry("b", "handle", attr));

        Collection<ReplicaCatalogEntry> c = regex.lookup("a");

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

        regex.insert("a", new ReplicaCatalogEntry("b", "handle", attr));
        regex.insert("a", new ReplicaCatalogEntry("b", "handle"));

        Collection<ReplicaCatalogEntry> c = regex.lookup("a");

        for (ReplicaCatalogEntry x : c) {
            assertEquals("b", x.getPFN());
            assertNull(x.getAttribute("regex"));
        }

        assertTrue(c.contains(new ReplicaCatalogEntry("b")));
    }

    @After
    public void tearDown() {
        rcFile.delete();
    }
}
