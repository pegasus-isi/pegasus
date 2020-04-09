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
package edu.isi.pegasus.planner.catalog.transformation;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.*;

import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.planner.catalog.TransformationCatalog;
import edu.isi.pegasus.planner.catalog.transformation.impl.Text;
import edu.isi.pegasus.planner.catalog.transformation.impl.YAML;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.common.PegasusProperties;
import edu.isi.pegasus.planner.test.DefaultTestSetup;
import edu.isi.pegasus.planner.test.TestSetup;
import java.io.File;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test class for testing the TransformationFactory, and whether it loads correct backend classes
 * based on properties specified
 *
 * @author Karan Vahi
 */
public class TransformationFactoryTest {

    private static final String FILE_CATALOG_TYPE = "Text";

    private static final String YAML_CATALOG_TYPE = "YAML";

    private PegasusBag mBag;

    private LogManager mLogger;

    private TestSetup mTestSetup;
    private static int mTestNumber = 1;

    public TransformationFactoryTest() {}

    @BeforeClass
    public static void setUpClass() {}

    @AfterClass
    public static void tearDownClass() {}

    @Before
    public void setUp() {
        mTestSetup = new DefaultTestSetup();
        mBag = new PegasusBag();
        PegasusProperties properties = PegasusProperties.nonSingletonInstance();
        mBag.add(PegasusBag.PEGASUS_PROPERTIES, properties);
        // pick test files from etc dir of the pegasus install
        mTestSetup.setInputDirectory(properties.getSysConfDir().getAbsolutePath());
        System.out.println("Input Test Dir is " + mTestSetup.getInputDirectory());
        mLogger = mTestSetup.loadLogger(properties);
        mLogger.setLevel(LogManager.DEBUG_MESSAGE_LEVEL);
        mLogger.logEventStart("test.catalog.transformation.TransformationFactory", "setup", "0");
        mBag.add(PegasusBag.PEGASUS_LOGMANAGER, mLogger);
        mLogger.logEventCompletion();
    }

    @After
    public void tearDown() {}

    @Test
    public void testWithTypeMentionedFile() throws Exception {
        mLogger.logEventStart(
                "test.catalog.transformation.factory",
                "type-as-file",
                Integer.toString(mTestNumber++));
        PegasusProperties props = PegasusProperties.nonSingletonInstance();

        props.setProperty(
                PegasusProperties.PEGASUS_TRANSFORMATION_CATALOG_PROPERTY, FILE_CATALOG_TYPE);
        props.setProperty(
                PegasusProperties.PEGASUS_TRANSFORMATION_CATALOG_FILE_PROPERTY,
                new File(mTestSetup.getInputDirectory(), "sample.tc.text").getAbsolutePath());
        TransformationCatalog tc = TransformationFactory.loadInstance(props);
        assertThat(tc, instanceOf(Text.class));
        mLogger.logEventCompletion();
    }

    @Test
    public void testWithOnlyPathToFile() throws Exception {
        mLogger.logEventStart(
                "test.catalog.transformation.factory",
                "only-file-path",
                Integer.toString(mTestNumber++));
        PegasusProperties props = PegasusProperties.nonSingletonInstance();
        props.setProperty(
                PegasusProperties.PEGASUS_TRANSFORMATION_CATALOG_FILE_PROPERTY,
                new File(mTestSetup.getInputDirectory(), "sample.tc.text").getAbsolutePath());
        TransformationCatalog tc = TransformationFactory.loadInstance(props);
        assertThat(tc, instanceOf(Text.class));
        mLogger.logEventCompletion();
    }

    @Test
    public void testWithTypeMentionedYAML() throws Exception {
        mLogger.logEventStart(
                "test.catalog.transformation.factory",
                "type-as-yaml",
                Integer.toString(mTestNumber++));
        PegasusProperties props = PegasusProperties.nonSingletonInstance();
        props.setProperty(
                PegasusProperties.PEGASUS_TRANSFORMATION_CATALOG_PROPERTY, YAML_CATALOG_TYPE);
        props.setProperty(
                PegasusProperties.PEGASUS_TRANSFORMATION_CATALOG_FILE_PROPERTY,
                new File(mTestSetup.getInputDirectory() + "/sample-5.0-data/", "tc.yml")
                        .getAbsolutePath());
        TransformationCatalog tc = TransformationFactory.loadInstance(props);
        assertThat(tc, instanceOf(YAML.class));
        mLogger.logEventCompletion();
    }

    @Test
    public void testWithOnlyPathToYAMLFile() throws Exception {
        mLogger.logEventStart(
                "test.catalog.transformation.factory",
                "only-file-path-yaml",
                Integer.toString(mTestNumber++));
        PegasusProperties props = PegasusProperties.nonSingletonInstance();
        props.setProperty(
                PegasusProperties.PEGASUS_TRANSFORMATION_CATALOG_FILE_PROPERTY,
                new File(mTestSetup.getInputDirectory() + "/sample-5.0-data/", "tc.yml")
                        .getAbsolutePath());
        TransformationCatalog tc = TransformationFactory.loadInstance(props);
        assertThat(tc, instanceOf(YAML.class));
        mLogger.logEventCompletion();
    }
}