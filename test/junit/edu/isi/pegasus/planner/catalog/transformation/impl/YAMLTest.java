/*
 * Copyright 2007-2015 University Of Southern California
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
package edu.isi.pegasus.planner.catalog.transformation.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.planner.catalog.classes.SysInfo;
import edu.isi.pegasus.planner.catalog.classes.SysInfo.Architecture;
import edu.isi.pegasus.planner.catalog.classes.SysInfo.OS;
import edu.isi.pegasus.planner.catalog.transformation.TransformationCatalogEntry;
import edu.isi.pegasus.planner.catalog.transformation.TransformationFactory;
import edu.isi.pegasus.planner.catalog.transformation.classes.Container;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.classes.Profile;
import edu.isi.pegasus.planner.common.PegasusProperties;
import edu.isi.pegasus.planner.test.DefaultTestSetup;
import edu.isi.pegasus.planner.test.EnvSetup;
import edu.isi.pegasus.planner.test.TestSetup;
import java.io.File;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** A Test class to test the YAML format of the Transformation Catalog */
public class YAMLTest {

    private static final String CORRECT_FILE = "tc_test.yml";

    private static final String ERROR_FILE = "tc_test_error.yml";

    private static final String INVALID_YAML_FILE = "tc_test_invalid.yml";

    private static final String EMPTY_FILE = "tc_test_empty.yml";

    private PegasusBag mBag;

    private PegasusProperties mProps;

    private LogManager mLogger;

    private TestSetup mTestSetup;

    private static int mTestNumber = 1;

    private YAML mCatalog;

    private static final String PROPERTIES_BASENAME = "properties";

    private static final String EXPANDED_SITE = "bamboo";
    private static final String EXPANDED_NAMESPACE = "pegasus";
    private static final String EXPANDED_NAME = "keg";
    private static final String EXPANDED_VERSION =
            "\"1.0\""; // need to specify like this to ensure YAML parser see's quoted value
    private static final String EXPECTED_VERSION = "1.0";
    private static final String EXPANDED_ARCH = "x86_64";
    private static final String EXPANDED_OS = "linux";
    private static final String EXPANDED_KEG_PATH = "file:///usr/bin/pegasus-keg";

    @BeforeAll
    public static void setUpClass() {
        Map<String, String> testEnvVariables = new HashMap();
        testEnvVariables.put("SITE", EXPANDED_SITE);
        testEnvVariables.put("NAMESPACE", EXPANDED_NAMESPACE);
        testEnvVariables.put("NAME", EXPANDED_NAME);
        testEnvVariables.put("VERSION", EXPANDED_VERSION);
        testEnvVariables.put("ARCH", EXPANDED_ARCH);
        testEnvVariables.put("OS", EXPANDED_OS);
        testEnvVariables.put("KEGPATH", EXPANDED_KEG_PATH);
        EnvSetup.setEnvironmentVariables(testEnvVariables);
    }

    @AfterAll
    public static void tearDownClass() {}

    public YAMLTest() {}

    /** Setup the logger and properties that all test functions require */
    @BeforeEach
    public final void setUp() {
        mTestSetup = new DefaultTestSetup();
        mBag = new PegasusBag();
        mTestSetup.setInputDirectory(this.getClass());
        System.out.println("Input Test Dir is " + mTestSetup.getInputDirectory());

        mProps = mTestSetup.loadPropertiesFromFile(PROPERTIES_BASENAME, new LinkedList());

        mLogger = mTestSetup.loadLogger(mProps);
        mLogger.setLevel(LogManager.INFO_MESSAGE_LEVEL);
        mLogger.logEventStart("test.catalog.transformation.impl.YAML", "setup", "0");
        mBag.add(PegasusBag.PEGASUS_LOGMANAGER, mLogger);
        mBag.add(PegasusBag.PEGASUS_PROPERTIES, mProps);
        // mBag.add( PegasusBag.PLANNER_OPTIONS, mTestSetup.loadPlannerOptions() );

        mProps.setProperty(
                PegasusProperties.PEGASUS_TRANSFORMATION_CATALOG_FILE_PROPERTY,
                new File(mTestSetup.getInputDirectory(), CORRECT_FILE).getAbsolutePath());
        mProps.setProperty(
                PegasusProperties.PEGASUS_TRANSFORMATION_CATALOG_PROPERTY,
                TransformationFactory.YAML_CATALOG_IMPLEMENTOR);
        mCatalog = (YAML) TransformationFactory.loadInstance(mBag);

        mLogger.logEventCompletion();
    }

    @Test
    public void testWholeCount() throws Exception {
        mLogger.logEventStart(
                "test.catalog.transformation.impl.YAML",
                "whole-count-test",
                Integer.toString(mTestNumber++));
        List<TransformationCatalogEntry> entries = mCatalog.getContents();
        assertEquals(4, entries.size(), "Expected total number of entries");
        List<TransformationCatalogEntry> kegEntries =
                mCatalog.lookup("example", "keg", "1.0", (String) null, null);
        assertEquals(2, kegEntries.size(), "Expected total number of keg entries");
        mLogger.logEventCompletion();
    }

    @Test
    public void testKegCount() throws Exception {
        mLogger.logEventStart(
                "test.catalog.transformation.impl.YAML",
                "keg-count-test",
                Integer.toString(mTestNumber++));
        List<TransformationCatalogEntry> kegEntries =
                mCatalog.lookup("example", "keg", "1.0", (String) null, null);
        assertEquals(2, kegEntries.size(), "Expected total number of keg entries");
        mLogger.logEventCompletion();
    }

    @Test
    public void testContainer() throws Exception {
        mLogger.logEventStart(
                "test.catalog.transformation.impl.YAML",
                "keg-site-test",
                Integer.toString(mTestNumber++));
        List<TransformationCatalogEntry> kegEntries =
                mCatalog.lookup(null, "myxform", null, "condorpool", null);
        TransformationCatalogEntry entry = kegEntries.get(0);
        Container containerInfo = entry.getContainer();
        assertEquals("centos-pegasus", containerInfo.getName());
        assertEquals("optional site", containerInfo.getImageSite());
        assertEquals("docker:///rynge/montage:latest", containerInfo.getImageURL().getURL());
        testProfile(containerInfo, Profile.ENV, "JAVA_HOME", "/opt/java/1.6");
        mLogger.logEventCompletion();
    }

    private void testProfile(Container containerInfo, String env, String key, String value) {
        Profile p = new Profile(env, key, value);
        List profiles = containerInfo.getProfiles();
        assertTrue(profiles.contains(p), "Entry " + containerInfo);
    }

    @Test
    public void testMetadataKeyword() throws Exception {
        mLogger.logEventStart(
                "test.catalog.transformation.impl.YAML",
                "metadata-keyword",
                Integer.toString(mTestNumber++));
        List<TransformationCatalogEntry> entries =
                mCatalog.lookup(null, "myxform", null, "condorpool", null);
        TransformationCatalogEntry entry = entries.get(0);
        SysInfo info = entry.getSysInfo();
        assertEquals("INSTALLED", entry.getType().name(), "Expected attribute ");
        assertEquals(null, entry.getLogicalNamespace(), "Expected attribute ");
        assertEquals("myxform", entry.getLogicalName(), "Expected attribute ");
        assertEquals(null, entry.getLogicalVersion(), "Expected attribute ");
        assertEquals("condorpool", entry.getResourceId(), "Expected attribute ");
        assertEquals("/usr/bin/true", entry.getPhysicalTransformation(), "Expected attribute ");
        assertEquals(
                Architecture.x86_64.toString(),
                info.getArchitecture().name(),
                "Expected attribute ");
        assertEquals(OS.linux.toString(), info.getOS().name(), "Expected attribute ");
        testProfile(entry, Profile.METADATA, "key", "value");
        testProfile(entry, Profile.METADATA, "appmodel", "myxform.aspen");
        testProfile(entry, Profile.METADATA, "version", "2.0");
        mLogger.logEventCompletion();
    }

    @Test
    public void testErrorYAMLFile() {
        PegasusBag mBag = new PegasusBag();
        PegasusProperties mErrorProps =
                mTestSetup.loadPropertiesFromFile(PROPERTIES_BASENAME, new LinkedList());
        mBag.add(PegasusBag.PEGASUS_PROPERTIES, mErrorProps);
        mBag.add(PegasusBag.PEGASUS_LOGMANAGER, mLogger);
        mBag.add(PegasusBag.PEGASUS_PROPERTIES, mErrorProps);
        mLogger.logEventStart("test.catalog.transformation.impl.YAML", "setup", "0");

        YAML catalog = new YAML();
        mErrorProps.setProperty(
                PegasusProperties.PEGASUS_TRANSFORMATION_CATALOG_FILE_PROPERTY,
                new File(mTestSetup.getInputDirectory(), ERROR_FILE).getAbsolutePath());
        try {
            catalog.initialize(mBag);
        } catch (RuntimeException e) {
            assertTrue(e.getCause().getMessage().contains("unknown: is not defined in the schema"));
        }
    }

    @Test
    public void testEmptyYAMLFile() {
        PegasusBag mBag = new PegasusBag();
        PegasusProperties mErrorProps =
                mTestSetup.loadPropertiesFromFile(PROPERTIES_BASENAME, new LinkedList());
        mBag.add(PegasusBag.PEGASUS_PROPERTIES, mErrorProps);
        mBag.add(PegasusBag.PEGASUS_LOGMANAGER, mLogger);
        mBag.add(PegasusBag.PEGASUS_PROPERTIES, mErrorProps);
        mLogger.logEventStart("test.catalog.transformation.impl.YAML", "setup", "0");

        YAML mCorrectCatalog = new YAML();
        mErrorProps.setProperty(
                PegasusProperties.PEGASUS_TRANSFORMATION_CATALOG_FILE_PROPERTY,
                new File(mTestSetup.getInputDirectory(), EMPTY_FILE).getAbsolutePath());
        try {
            mCorrectCatalog.initialize(mBag);
        } catch (RuntimeException e) {
            assertTrue(false);
        }
        assertTrue(true);
    }

    @Test
    public void testParameterExpansionContents() throws Exception {
        mLogger.logEventStart(
                "test.catalog.transformation.impl.Text",
                "parameter-expansion-contents",
                Integer.toString(mTestNumber++));
        List<TransformationCatalogEntry> kegEntries =
                mCatalog.lookup(
                        EXPANDED_NAMESPACE, EXPANDED_NAME, EXPECTED_VERSION, EXPANDED_SITE, null);
        TransformationCatalogEntry expanded = kegEntries.get(0);
        SysInfo info = expanded.getSysInfo();
        assertEquals(EXPANDED_NAMESPACE, expanded.getLogicalNamespace(), "Expected attribute ");
        assertEquals(EXPANDED_NAME, expanded.getLogicalName(), "Expected attribute ");
        assertEquals(EXPECTED_VERSION, expanded.getLogicalVersion(), "Expected attribute ");
        assertEquals(EXPANDED_SITE, expanded.getResourceId(), "Expected attribute ");
        assertEquals(EXPANDED_ARCH, info.getArchitecture().name(), "Expected attribute ");
        assertEquals(EXPANDED_OS, info.getOS().name(), "Expected attribute ");
        assertEquals(
                EXPANDED_KEG_PATH, expanded.getPhysicalTransformation(), "Expected attribute ");

        mLogger.logEventCompletion();
    }

    @Test
    public void testInvalidYAMLFile() {
        PegasusBag mBag = new PegasusBag();
        PegasusProperties mErrorProps =
                mTestSetup.loadPropertiesFromFile(PROPERTIES_BASENAME, new LinkedList());
        mBag.add(PegasusBag.PEGASUS_PROPERTIES, mErrorProps);
        mBag.add(PegasusBag.PEGASUS_LOGMANAGER, mLogger);
        mBag.add(PegasusBag.PEGASUS_PROPERTIES, mErrorProps);
        mLogger.logEventStart("test.catalog.transformation.impl.YAML", "setup", "0");

        YAML catalog = new YAML();
        mErrorProps.setProperty(
                PegasusProperties.PEGASUS_TRANSFORMATION_CATALOG_FILE_PROPERTY,
                new File(mTestSetup.getInputDirectory(), INVALID_YAML_FILE).getAbsolutePath());
        try {
            catalog.initialize(mBag);
        } catch (RuntimeException e) {
            assertTrue(
                    e.getCause()
                            .getMessage()
                            .contains(
                                    "{$.transformations[0].sites: is missing but it is required}"));
        }
    }

    private void testProfile(
            TransformationCatalogEntry entry, String namespace, String key, String value) {
        Profile p = new Profile(namespace, key, value);
        List profiles = entry.getProfiles(namespace);
        assertTrue(profiles.contains(p), "Entry " + entry);
    }
}
