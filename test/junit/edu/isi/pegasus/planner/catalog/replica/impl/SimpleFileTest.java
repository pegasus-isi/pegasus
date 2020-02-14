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
import edu.isi.pegasus.planner.test.DefaultTestSetup;
import edu.isi.pegasus.planner.test.EnvSetup;
import edu.isi.pegasus.planner.test.TestSetup;
import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.junit.*;
import org.junit.runners.MethodSorters;

/**
 * Test class to test File based replica catalog.
 *
 * <p>Test are run in lexographic order of method name, to ensure deletes happen after insertion.
 *
 * @author Karan Vahi
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class SimpleFileTest {

    /** * track across insert and delete methods */
    private static File mTempRC;

    private SimpleFile mCatalog = null;

    private TestSetup mTestSetup;

    private static final String EXPANDED_USER = "bamboo";

    public SimpleFileTest() {}

    @BeforeClass
    public static void setUpClass() throws IOException {
        Map<String, String> testEnvVariables = new HashMap();
        testEnvVariables.put("USER", "bamboo");
        EnvSetup.setEnvironmentVariables(testEnvVariables);

        // create a temp file for some tests
        // that is not tracked in git
        mTempRC = File.createTempFile("replica", ".txt");
    }

    @Before
    public void setUp() throws IOException {
        Map<String, String> testEnvVariables = new HashMap();
        testEnvVariables.put("USER", EXPANDED_USER);
        EnvSetup.setEnvironmentVariables(testEnvVariables);

        mTestSetup = new DefaultTestSetup();
        mTestSetup.setInputDirectory(this.getClass());
    }

    @Test
    public void insertSingle() {
        System.out.println("insertSingle");
        setupCatalog(mTempRC.getAbsolutePath(), false);
        mCatalog.insert("a", new ReplicaCatalogEntry("b"));
        Collection<ReplicaCatalogEntry> c = mCatalog.lookup("a");
        assertTrue(c.contains(new ReplicaCatalogEntry("b")));
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
        assertTrue(c.contains(new ReplicaCatalogEntry("b")));
        assertTrue(c.contains(new ReplicaCatalogEntry("b", "handle")));
        assertTrue(c.contains(new ReplicaCatalogEntry("c")));
        assertTrue(c.contains(new ReplicaCatalogEntry("c", "handle")));
    }

    @Test
    public void lookupWithSubstitutionsTest() {
        System.out.println("lookupWithSubstitutionsTest");
        File f = new File(mTestSetup.getInputDirectory(), "simple-file-substitute.in");
        setupCatalog(f.getAbsolutePath(), true);

        Collection<ReplicaCatalogEntry> c = mCatalog.lookup("f.b");

        assertEquals(1, c.size());
        for (ReplicaCatalogEntry x : c) {
            assertEquals("file:///tmp/" + EXPANDED_USER + "/f.b", x.getPFN());
            assertEquals("isi", x.getResourceHandle());
        }
    }

    @Test
    public void remove() {
        System.out.println("removeSingle");
        setupCatalog(mTempRC.getAbsolutePath(), false);
        int count = mCatalog.remove("a");
        assertEquals(4, count);
    }

    @After
    public void tearDown() {
        mCatalog.close();
    }

    @AfterClass
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
