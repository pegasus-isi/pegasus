/*
 * This file or a portion of this file is licensed under the terms of
 * the Globus Toolkit Public License, found in file GTPL, or at
 * http://www.globus.org/toolkit/download/license.html. This notice must
 * appear in redistributions of this file, with or without modification.
 *
 * Redistributions of this Software, with or without modification, must
 * reproduce the GTPL in: (1) the Software, or (2) the Documentation or
 * some other similar material which is provided with the Software (if
 * any).
 *
 * Copyright 1999-2020 University of Chicago and The University of
 * Southern California. All rights reserved.
 */
package edu.isi.pegasus.planner.catalog.site;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.*;

import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.planner.catalog.SiteCatalog;
import edu.isi.pegasus.planner.catalog.site.impl.XML;
import edu.isi.pegasus.planner.catalog.site.impl.YAML;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.common.PegasusProperties;
import edu.isi.pegasus.planner.test.DefaultTestSetup;
import edu.isi.pegasus.planner.test.TestSetup;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test class for testing the SiteFactory, and whether it loads correct backend classes based on
 * properties specified
 *
 * @author Karan Vahi
 */
public class SiteFactoryTest {

    private static final String XML_SITE_CATALOG_TYPE = "XML";

    private static final String YAML_SITE_CATALOG_TYPE = "YAML";

    private PegasusBag mBag;

    private LogManager mLogger;

    private TestSetup mTestSetup;

    private static int mTestNumber = 1;

    public SiteFactoryTest() {}

    @BeforeClass
    public static void setUpClass() {}

    @AfterClass
    public static void tearDownClass() {}

    @Before
    public void setUp() {
        mTestSetup = new DefaultTestSetup();
        mBag = new PegasusBag();
        mTestSetup.setInputDirectory(this.getClass());
        System.out.println("Input Test Dir is " + mTestSetup.getInputDirectory());
        mLogger = mTestSetup.loadLogger(PegasusProperties.nonSingletonInstance());
        mLogger.setLevel(LogManager.DEBUG_MESSAGE_LEVEL);
        mLogger.logEventStart("test.catalog.site.SiteFactory", "setup", "0");
        mBag.add(PegasusBag.PEGASUS_LOGMANAGER, mLogger);
        mLogger.logEventCompletion();
    }

    @After
    public void tearDown() {}

    @Test
    public void testWithTypeMentionedXML() throws Exception {
        mLogger.logEventStart(
                "test.catalog.site.SiteFactory", "type-xml-test", Integer.toString(mTestNumber++));
        PegasusProperties props = PegasusProperties.nonSingletonInstance();
        props.setProperty(PegasusProperties.PEGASUS_SITE_CATALOG_PROPERTY, XML_SITE_CATALOG_TYPE);
        props.setProperty(
                PegasusProperties.PEGASUS_SITE_CATALOG_FILE_PROPERTY,
                new File(mTestSetup.getInputDirectory(), "sites.xml").getAbsolutePath());
        mBag.add(PegasusBag.PEGASUS_PROPERTIES, props);
        SiteCatalog s = SiteFactory.loadInstance(mBag);
        assertThat(s, instanceOf(XML.class));
        mLogger.logEventCompletion();
    }

    @Test
    public void testWithOnlyPathToXML() throws Exception {
        mLogger.logEventStart(
                "test.catalog.site.SiteFactory", "path-xml-test", Integer.toString(mTestNumber++));
        PegasusProperties props = PegasusProperties.nonSingletonInstance();
        props.setProperty(
                PegasusProperties.PEGASUS_SITE_CATALOG_FILE_PROPERTY,
                new File(mTestSetup.getInputDirectory(), "sites.xml").getAbsolutePath());
        mBag.add(PegasusBag.PEGASUS_PROPERTIES, props);
        SiteCatalog s = SiteFactory.loadInstance(mBag);
        assertThat(s, instanceOf(XML.class));
        mLogger.logEventCompletion();
    }

    @Test
    public void testWithConflictingPropsXML() throws Exception {

        mLogger.logEventStart(
                "test.catalog.site.SiteFactory",
                "conflicting-props-xml",
                Integer.toString(mTestNumber++));
        PegasusProperties props = PegasusProperties.nonSingletonInstance();

        props.setProperty(PegasusProperties.PEGASUS_SITE_CATALOG_PROPERTY, YAML_SITE_CATALOG_TYPE);
        props.setProperty(
                PegasusProperties.PEGASUS_SITE_CATALOG_FILE_PROPERTY,
                new File(mTestSetup.getInputDirectory(), "sites.xml").getAbsolutePath());
        mBag.add(PegasusBag.PEGASUS_PROPERTIES, props);
        SiteCatalog s = SiteFactory.loadInstance(mBag);
        assertThat(s, instanceOf(YAML.class));
        mLogger.logEventCompletion();
    }

    @Test
    public void testWithConflictingPropsYAML() throws Exception {

        mLogger.logEventStart(
                "test.catalog.site.SiteFactory",
                "conflicting-props-yaml",
                Integer.toString(mTestNumber++));
        PegasusProperties props = PegasusProperties.nonSingletonInstance();

        props.setProperty(PegasusProperties.PEGASUS_SITE_CATALOG_PROPERTY, XML_SITE_CATALOG_TYPE);
        props.setProperty(
                PegasusProperties.PEGASUS_SITE_CATALOG_FILE_PROPERTY,
                new File(mTestSetup.getInputDirectory(), "sites.yml").getAbsolutePath());
        mBag.add(PegasusBag.PEGASUS_PROPERTIES, props);
        SiteCatalog s = SiteFactory.loadInstance(mBag);
        assertThat(s, instanceOf(XML.class));
        mLogger.logEventCompletion();
    }

    @Test
    public void testWithTypeMentionedYAML() throws Exception {

        mLogger.logEventStart(
                "test.catalog.site.SiteFactory", "type-yaml-test", Integer.toString(mTestNumber++));
        PegasusProperties props = PegasusProperties.nonSingletonInstance();
        props.setProperty(PegasusProperties.PEGASUS_SITE_CATALOG_PROPERTY, YAML_SITE_CATALOG_TYPE);
        props.setProperty(
                PegasusProperties.PEGASUS_SITE_CATALOG_FILE_PROPERTY,
                new File(mTestSetup.getInputDirectory(), "sites.yml").getAbsolutePath());
        mBag.add(PegasusBag.PEGASUS_PROPERTIES, props);
        SiteCatalog s = SiteFactory.loadInstance(mBag);
        assertThat(s, instanceOf(YAML.class));
        mLogger.logEventCompletion();
    }

    @Test
    public void testWithOnlyPathToYAML() throws Exception {

        mLogger.logEventStart(
                "test.catalog.site.SiteFactory", "path-yaml-test", Integer.toString(mTestNumber++));
        PegasusProperties props = PegasusProperties.nonSingletonInstance();
        props.setProperty(
                PegasusProperties.PEGASUS_SITE_CATALOG_FILE_PROPERTY,
                new File(mTestSetup.getInputDirectory(), "sites.yml").getAbsolutePath());
        mBag.add(PegasusBag.PEGASUS_PROPERTIES, props);
        SiteCatalog s = SiteFactory.loadInstance(mBag);
        assertThat(s, instanceOf(YAML.class));
        mLogger.logEventCompletion();
    }

    @Test
    public void testWithDefaultXMLFile() throws Exception {
        mLogger.logEventStart(
                "test.catalog.site.SiteFactory",
                "default-xml-file-test",
                Integer.toString(mTestNumber++));
        PegasusProperties props = PegasusProperties.nonSingletonInstance();
        PegasusBag bag = new PegasusBag();
        bag.add(PegasusBag.PEGASUS_PROPERTIES, props);
        bag.add(PegasusBag.PEGASUS_LOGMANAGER, mLogger);
        Path p = Files.createTempDirectory("pegasus");
        File dir = p.toFile();
        File xml = new File(dir, "sites.xml");
        BufferedWriter writer = new BufferedWriter(new FileWriter(xml));
        writer.write("xml\n");
        writer.close();
        bag.add(PegasusBag.PLANNER_DIRECTORY, dir);
        try {
            SiteCatalog s = SiteFactory.loadInstance(bag);
            assertThat(s, instanceOf(XML.class));
        } finally {
            dir.delete();
        }
        mLogger.logEventCompletion();
    }

    @Test
    public void testWithDefaultYAMLFile() throws Exception {
        mLogger.logEventStart(
                "test.catalog.site.SiteFactory",
                "default-yaml-file-test",
                Integer.toString(mTestNumber++));
        PegasusProperties props = PegasusProperties.nonSingletonInstance();
        PegasusBag bag = new PegasusBag();
        bag.add(PegasusBag.PEGASUS_PROPERTIES, props);
        bag.add(PegasusBag.PEGASUS_LOGMANAGER, mLogger);
        Path p = Files.createTempDirectory("pegasus");
        File dir = p.toFile();
        File yaml = new File(dir, "sites.yml");
        BufferedWriter writer = new BufferedWriter(new FileWriter(yaml));
        writer.write("pegasus:5.0\n");
        writer.close();
        bag.add(PegasusBag.PLANNER_DIRECTORY, dir);
        try {
            SiteCatalog s = SiteFactory.loadInstance(bag);
            assertThat(s, instanceOf(YAML.class));
        } finally {
            dir.delete();
        }
        mLogger.logEventCompletion();
    }

    @Test
    public void testWithDefaultYAMLAndXMLFiles() throws Exception {
        mLogger.logEventStart(
                "test.catalog.site.SiteFactory",
                "default-yaml-xml-test",
                Integer.toString(mTestNumber++));
        PegasusProperties props = PegasusProperties.nonSingletonInstance();
        PegasusBag bag = new PegasusBag();
        bag.add(PegasusBag.PEGASUS_PROPERTIES, props);
        bag.add(PegasusBag.PEGASUS_LOGMANAGER, mLogger);
        Path p = Files.createTempDirectory("pegasus");
        File dir = p.toFile();
        File yaml = new File(dir, "sites.yml");
        BufferedWriter writer = new BufferedWriter(new FileWriter(yaml));
        writer.write("pegasus:5.0\n");
        writer.close();
        File xml = new File(dir, "sites.xml");
        writer = new BufferedWriter(new FileWriter(xml));
        writer.write("xml\n");
        writer.close();
        bag.add(PegasusBag.PLANNER_DIRECTORY, dir);
        try {
            SiteCatalog s = SiteFactory.loadInstance(bag);
            assertThat(s, instanceOf(YAML.class));
        } finally {
            dir.delete();
        }
        mLogger.logEventCompletion();
    }

    @Test(expected = SiteFactoryException.class)
    public void testWithEmptyProperties() throws Exception {

        mLogger.logEventStart(
                "test.catalog.site.SiteFactory",
                "empty-props-test",
                Integer.toString(mTestNumber++));
        PegasusProperties props = PegasusProperties.nonSingletonInstance();
        mBag.add(PegasusBag.PEGASUS_PROPERTIES, props);
        SiteCatalog s = SiteFactory.loadInstance(mBag);
        mLogger.logEventCompletion();
    }
}
