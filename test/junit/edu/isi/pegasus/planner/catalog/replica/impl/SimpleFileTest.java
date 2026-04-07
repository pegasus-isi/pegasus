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
import static org.junit.jupiter.api.Assertions.*;

import edu.isi.pegasus.planner.catalog.replica.ReplicaCatalogEntry;
import edu.isi.pegasus.planner.test.DefaultTestSetup;
import edu.isi.pegasus.planner.test.TestSetup;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Collection;
import java.util.Properties;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.TestMethodOrder;

/**
 * Test class to test File based replica catalog.
 *
 * <p>Test are run in lexographic order of method name, to ensure deletes happen after insertion.
 *
 * @author Karan Vahi
 */
@TestMethodOrder(MethodOrderer.Alphanumeric.class)
public class SimpleFileTest {

    /** * track across insert and delete methods */
    private static File mTempRC;

    private SimpleFile mCatalog = null;

    private TestSetup mTestSetup;

    private File mExpandedSubstitutionInput;

    protected static final String EXPANDED_USER = "bamboo";

    public SimpleFileTest() {}

    @BeforeAll
    public static void setUpClass() throws IOException {
        // create a temp file for some tests
        // that is not tracked in git
        mTempRC = File.createTempFile("replica", ".txt");
    }

    @BeforeEach
    public void setUp() throws IOException {
        mTestSetup = new DefaultTestSetup();
        mTestSetup.setInputDirectory(this.getClass());
        mExpandedSubstitutionInput = File.createTempFile("simple-file-substitute", ".in");
        String raw =
                Files.readString(
                        new File(mTestSetup.getInputDirectory(), "simple-file-substitute.in")
                                .toPath(),
                        StandardCharsets.UTF_8);
        Files.writeString(
                mExpandedSubstitutionInput.toPath(),
                raw.replace("${USER}", EXPANDED_USER),
                StandardCharsets.UTF_8);
    }

    @Test
    public void insertSingle() {
        System.out.println("insertSingle");
        setupCatalog(mTempRC.getAbsolutePath(), false);
        mCatalog.insert("a", new ReplicaCatalogEntry("b"));
        Collection<ReplicaCatalogEntry> c = mCatalog.lookup("a");
        assertThat(c.contains(new ReplicaCatalogEntry("b")), is(true));
    }

    @Test
    public void insertMultiple() {
        System.out.println("insertMultiple");
        setupCatalog(mTempRC.getAbsolutePath(), false);
        mCatalog.insert("a", new ReplicaCatalogEntry("b"));
        mCatalog.insert("a", new ReplicaCatalogEntry("b", "handle"));
        mCatalog.insert("a", new ReplicaCatalogEntry("c"));
        mCatalog.insert("a", new ReplicaCatalogEntry("c", "handle"));

        Collection<ReplicaCatalogEntry> c = mCatalog.lookup("a");
        assertThat(c.contains(new ReplicaCatalogEntry("b")), is(true));
        assertThat(c.contains(new ReplicaCatalogEntry("b", "handle")), is(true));
        assertThat(c.contains(new ReplicaCatalogEntry("c")), is(true));
        assertThat(c.contains(new ReplicaCatalogEntry("c", "handle")), is(true));
    }

    @Test
    public void lookupWithSubstitutionsTest() {
        System.out.println("lookupWithSubstitutionsTest");
        setupCatalog(mExpandedSubstitutionInput.getAbsolutePath(), true);

        Collection<ReplicaCatalogEntry> c = mCatalog.lookup("f.b");

        assertThat(c.size(), is(1));
        for (ReplicaCatalogEntry x : c) {
            assertThat(x.getPFN(), is("file:///tmp/" + EXPANDED_USER + "/f.b"));
            assertThat(x.getResourceHandle(), is("isi"));
        }
    }

    @Test
    public void remove() {
        System.out.println("removeSingle");
        setupCatalog(mTempRC.getAbsolutePath(), false);
        int count = mCatalog.remove("a");
        assertThat(count, is(4));
    }

    @AfterEach
    public void tearDown() {
        if (mCatalog != null) {
            mCatalog.close();
        }
        if (mExpandedSubstitutionInput != null) {
            mExpandedSubstitutionInput.delete();
        }
    }

    @AfterAll
    public static void tearDownClass() {
        if (mTempRC != null) {
            mTempRC.delete();
        }
    }

    private void setupCatalog(String file, boolean readOnly) {
        mCatalog = new SimpleFile();
        // mRCFile  = new File( mTestSetup.getInputDirectory(), file );
        System.out.println("Input Test File is " + file);
        Properties props = new Properties();
        props.setProperty(SimpleFile.READ_ONLY_KEY, Boolean.toString(readOnly));
        props.setProperty("file", file);
        mCatalog.connect(props);
    }
}
