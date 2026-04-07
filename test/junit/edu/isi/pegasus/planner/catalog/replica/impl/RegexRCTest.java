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
import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import org.junit.jupiter.api.*;

/**
 * Test class to test Regex RC
 *
 * @author Rajiv Mayani
 */
public class RegexRCTest {

    private Regex mRegex = null;
    private File mRCFile = null;

    public RegexRCTest() {}

    @BeforeEach
    public void setUp() throws IOException {
        mRegex = new Regex();
        mRCFile = File.createTempFile("replica", ".txt");
        mRegex.connect(mRCFile.getName());
        mRCFile.delete();
    }

    @Test
    public void simpleInsert() {
        mRegex.insert("a", new ReplicaCatalogEntry("b"));
        Collection<ReplicaCatalogEntry> c = mRegex.lookup("a");
        assertThat(c.contains(new ReplicaCatalogEntry("b")), is(true));
    }

    @Test
    public void lookupWithSubstitutionsTest() {
        HashMap attr = new HashMap();
        attr.put("regex", "true");
        mRegex.insert(
                "(\\w+)_f[xyz]_(\\d+)\\.sgt.*",
                new ReplicaCatalogEntry("file://test.isi.edu/scratch/[2]/[1]/[0]", attr));
        Collection<ReplicaCatalogEntry> c = mRegex.lookup("TEST_fy_3810.sgt.md5");

        for (ReplicaCatalogEntry x : c) {
            assertThat(
                    x.getPFN(), is("file://test.isi.edu/scratch/3810/TEST/TEST_fy_3810.sgt.md5"));
        }

        c = mRegex.lookup("TEST_fz_33810.sgt.md5");

        for (ReplicaCatalogEntry x : c) {
            assertThat(
                    x.getPFN(), is("file://test.isi.edu/scratch/33810/TEST/TEST_fz_33810.sgt.md5"));
        }

        c = mRegex.lookup("TEST_fa_33810.sgt.md5");
        assertThat(c.size(), is(0));
    }

    @Test
    public void lookupWithSubstitutionsTestSummit() {
        HashMap attr = new HashMap();
        attr.put("regex", "true");
        attr.put("pool", "summit");
        mRegex.insert(
                "(\\w+)_f[xyz]_(\\d+)\\.sgt.*",
                new ReplicaCatalogEntry(
                        "gsiftp://gridftp.ccs.ornl.gov/gpfs/alpine/scratch/callag/geo112/SGT_Storage/[1]/[0]",
                        attr));
        Collection<ReplicaCatalogEntry> c = mRegex.lookup("USC_fx_7056.sgt");

        for (ReplicaCatalogEntry x : c) {
            assertThat(
                    x.getPFN(),
                    is(
                            "gsiftp://gridftp.ccs.ornl.gov/gpfs/alpine/scratch/callag/geo112/SGT_Storage/USC/USC_fx_7056.sgt"));
        }
    }

    @Test
    public void multipleSimpleInsert() {
        mRegex.insert("a", new ReplicaCatalogEntry("b"));
        mRegex.insert("a", new ReplicaCatalogEntry("b", "handle"));
        mRegex.insert("a", new ReplicaCatalogEntry("c"));
        mRegex.insert("a", new ReplicaCatalogEntry("c", "handle"));

        Collection<ReplicaCatalogEntry> c = mRegex.lookup("a");
        assertThat(c.contains(new ReplicaCatalogEntry("b")), is(true));
        assertThat(c.contains(new ReplicaCatalogEntry("b", "handle")), is(true));
        assertThat(c.contains(new ReplicaCatalogEntry("c")), is(true));
        assertThat(c.contains(new ReplicaCatalogEntry("c", "handle")), is(true));
    }

    @Test
    public void simpleToRegexConversion() {
        HashMap attr = new HashMap();
        attr.put("regex", "true");

        mRegex.insert("a", new ReplicaCatalogEntry("b"));
        mRegex.insert("a", new ReplicaCatalogEntry("b", attr));

        Collection<ReplicaCatalogEntry> c = mRegex.lookup("a");

        assertThat(c.contains(new ReplicaCatalogEntry("b")), is(false));
        for (ReplicaCatalogEntry x : c) {
            assertThat(x.getPFN(), is("b"));
            assertThat(((String) x.getAttribute("regex")), is("true"));
        }
    }

    @Test
    public void regexToSimpleConversion() {
        HashMap attr = new HashMap();
        attr.put("regex", "true");

        mRegex.insert("a", new ReplicaCatalogEntry("b", attr));
        mRegex.insert("a", new ReplicaCatalogEntry("b"));

        Collection<ReplicaCatalogEntry> c = mRegex.lookup("a");

        for (ReplicaCatalogEntry x : c) {
            assertThat(x.getPFN(), is("b"));
            assertThat(x.getAttribute("regex"), is(nullValue()));
        }

        assertThat(c.contains(new ReplicaCatalogEntry("b")), is(true));
    }

    @AfterEach
    public void tearDown() {
        mRCFile.delete();
    }
}
