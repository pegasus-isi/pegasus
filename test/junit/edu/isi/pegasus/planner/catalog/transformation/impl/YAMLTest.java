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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.LinkedList;
import java.util.List;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.planner.catalog.classes.SysInfo;
import edu.isi.pegasus.planner.catalog.classes.SysInfo.Architecture;
import edu.isi.pegasus.planner.catalog.classes.SysInfo.OS;
import edu.isi.pegasus.planner.catalog.transformation.TransformationCatalogEntry;
import edu.isi.pegasus.planner.catalog.transformation.classes.Container;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.classes.Profile;
import edu.isi.pegasus.planner.common.PegasusProperties;
import edu.isi.pegasus.planner.test.DefaultTestSetup;
import edu.isi.pegasus.planner.test.TestSetup;

/**
 *
 * A Test class to test the YAML format of the Transformation Catalog
 */
public class YAMLTest {

	private static final String CORRECT_FILE = "transformationcatalogue_test.yaml";

	private static final String ERROR_FILE = "transformationcatalogue_test_error.yaml";
	
	private static final String INVALID_YAML_FILE = "transformationcatalogue_test_invalid.yaml";

	private PegasusBag mBag;

	private PegasusProperties mProps;

	private LogManager mLogger;

	private TestSetup mTestSetup;

	private static int mTestNumber = 1;

	private YAML mCorrectCatalog;

	private static final String PROPERTIES_BASENAME = "properties";

	@BeforeClass
	public static void setUpClass() {
	}

	@AfterClass
	public static void tearDownClass() {
	}

	public YAMLTest() {

	}

	/**
	 * Setup the logger and properties that all test functions require
	 */
	@Before
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

		mProps.setProperty(PegasusProperties.PEGASUS_TRANSFORMATION_CATALOG_FILE_PROPERTY,
				new File(mTestSetup.getInputDirectory(), CORRECT_FILE).getAbsolutePath());
		mCorrectCatalog = new YAML();
		mCorrectCatalog.initialize(mBag);
		mLogger.logEventCompletion();
	}

	@Test
	public void testWholeCount() throws Exception {
		mLogger.logEventStart("test.catalog.transformation.impl.YAML", "whole-count-test",
				Integer.toString(mTestNumber++));
		List<TransformationCatalogEntry> entries = mCorrectCatalog.getContents();
		assertEquals("Expected total number of entries", 3, entries.size());
		List<TransformationCatalogEntry> kegEntries = mCorrectCatalog.lookup("example", "keg", "1.0", (String) null,
				null);
		assertEquals("Expected total number of keg entries", 2, kegEntries.size());
		mLogger.logEventCompletion();
	}

	@Test
	public void testKegCount() throws Exception {
		mLogger.logEventStart("test.catalog.transformation.impl.YAML", "keg-count-test",
				Integer.toString(mTestNumber++));
		List<TransformationCatalogEntry> kegEntries = mCorrectCatalog.lookup("example", "keg", "1.0", (String) null,
				null);
		assertEquals("Expected total number of keg entries", 2, kegEntries.size());
		mLogger.logEventCompletion();

	}

	@Test
	public void testContainer() throws Exception {
		mLogger.logEventStart("test.catalog.transformation.impl.YAML", "keg-site-test",
				Integer.toString(mTestNumber++));
		List<TransformationCatalogEntry> kegEntries = mCorrectCatalog.lookup(null, "myxform", null, "condorpool", null);
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
		assertTrue("Entry " + containerInfo, profiles.contains(p));
	}

	@Test
	public void testMetadataKeyword() throws Exception {
		mLogger.logEventStart("test.catalog.transformation.impl.YAML", "metadata-keyword",
				Integer.toString(mTestNumber++));
		List<TransformationCatalogEntry> entries = mCorrectCatalog.lookup(null, "myxform", null, "condorpool", null);
		TransformationCatalogEntry entry = entries.get(0);
		SysInfo info = entry.getSysInfo();
		assertEquals("Expected attribute ", "INSTALLED", entry.getType().name());
		assertEquals("Expected attribute ", null, entry.getLogicalNamespace());
		assertEquals("Expected attribute ", "myxform", entry.getLogicalName());
		assertEquals("Expected attribute ", null, entry.getLogicalVersion());
		assertEquals("Expected attribute ", "condorpool", entry.getResourceId());
		assertEquals("Expected attribute ", "/usr/bin/true", entry.getPhysicalTransformation());
		assertEquals("Expected attribute ", Architecture.x86_64.toString(), info.getArchitecture().name());
		assertEquals("Expected attribute ", OS.linux.toString(), info.getOS().name());
		testProfile(entry, Profile.METADATA, "key", "value");
		testProfile(entry, Profile.METADATA, "appmodel", "myxform.aspen");
		testProfile(entry, Profile.METADATA, "version", "2.0");
		mLogger.logEventCompletion();

	}

	@Test
	public void testErrorYAMLFile() {
		PegasusBag mBag = new PegasusBag();
		PegasusProperties mErrorProps = mTestSetup.loadPropertiesFromFile(PROPERTIES_BASENAME, new LinkedList());
		mBag.add(PegasusBag.PEGASUS_PROPERTIES, mErrorProps);
		mBag.add(PegasusBag.PEGASUS_LOGMANAGER, mLogger);
		mBag.add(PegasusBag.PEGASUS_PROPERTIES, mErrorProps);
		mLogger.logEventStart("test.catalog.transformation.impl.YAML", "setup", "0");

		YAML mCorrectCatalog = new YAML();
		mErrorProps.setProperty(PegasusProperties.PEGASUS_TRANSFORMATION_CATALOG_FILE_PROPERTY,
				new File(mTestSetup.getInputDirectory(), ERROR_FILE).getAbsolutePath());
		try {
			mCorrectCatalog.initialize(mBag);
		} catch (RuntimeException e) {
			assertTrue(e.getCause().getMessage().contains("Unknown fields [\"unknown\"] present in transformations details"));
			assertTrue(e.getCause().getMessage().contains("Missing required fields [\"arch\"] in transformations details"));
		}
	}
	

	@Test
	public void testInvalidYAMLFile() {
		PegasusBag mBag = new PegasusBag();
		PegasusProperties mErrorProps = mTestSetup.loadPropertiesFromFile(PROPERTIES_BASENAME, new LinkedList());
		mBag.add(PegasusBag.PEGASUS_PROPERTIES, mErrorProps);
		mBag.add(PegasusBag.PEGASUS_LOGMANAGER, mLogger);
		mBag.add(PegasusBag.PEGASUS_PROPERTIES, mErrorProps);
		mLogger.logEventStart("test.catalog.transformation.impl.YAML", "setup", "0");

		YAML mCorrectCatalog = new YAML();
		mErrorProps.setProperty(PegasusProperties.PEGASUS_TRANSFORMATION_CATALOG_FILE_PROPERTY,
				new File(mTestSetup.getInputDirectory(), INVALID_YAML_FILE).getAbsolutePath());
		try {
			mCorrectCatalog.initialize(mBag);
		} catch (RuntimeException e) {
			assertTrue(e.getCause().getMessage().contains("line 6: Problem in the line :6, column:4 with tag profile:^"));
		}
	}

	private void testProfile(TransformationCatalogEntry entry, String namespace, String key, String value) {
		Profile p = new Profile(namespace, key, value);
		List profiles = entry.getProfiles(namespace);
		assertTrue("Entry " + entry, profiles.contains(p));

	}

}
