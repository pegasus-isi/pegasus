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
        mLogger = mTestSetup.loadLogger(properties);
        mLogger.setLevel(LogManager.DEBUG_MESSAGE_LEVEL);
        mLogger.logEventStart("test.catalog.transformation.TransformationFactory", "setup", "0");
        mBag.add(PegasusBag.PEGASUS_LOGMANAGER, mLogger);
        mLogger.log(
                "System conf dir " + properties.getSysConfDir(), LogManager.DEBUG_MESSAGE_LEVEL);
        // pick test files from etc dir of the pegasus install
        mTestSetup.setInputDirectory(properties.getSysConfDir().getAbsolutePath());
        System.out.println("Input Test Dir is " + mTestSetup.getInputDirectory());

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
        TransformationCatalog tc = TransformationFactory.loadInstance(getPegasusBag(props));
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
        TransformationCatalog tc = TransformationFactory.loadInstance(getPegasusBag(props));
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
                new File(mTestSetup.getInputDirectory() + "/yaml/", "tc.yml")
                        .getAbsolutePath());
        TransformationCatalog tc = TransformationFactory.loadInstance(getPegasusBag(props));
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
                new File(mTestSetup.getInputDirectory() + "/yaml/", "tc.yml")
                        .getAbsolutePath());
        TransformationCatalog tc = TransformationFactory.loadInstance(getPegasusBag(props));
        assertThat(tc, instanceOf(YAML.class));
        mLogger.logEventCompletion();
    }

    @Test
    public void testWithDefaultTextFile() throws Exception {
        mLogger.logEventStart(
                "test.catalog.transformation.factory",
                "default-text-file-test",
                Integer.toString(mTestNumber++));
        PegasusProperties props = PegasusProperties.nonSingletonInstance();
        PegasusBag bag = new PegasusBag();
        bag.add(PegasusBag.PEGASUS_PROPERTIES, props);
        bag.add(PegasusBag.PEGASUS_LOGMANAGER, mLogger);
        Path p = Files.createTempDirectory("pegasus");
        File dir = p.toFile();
        File text =
                new File(dir, TransformationFactory.DEFAULT_TEXT_TRANSFORMATION_CATALOG_BASENAME);
        BufferedWriter writer = new BufferedWriter(new FileWriter(text));
        writer.write(
                "tr black::analyze:1.0 {\n"
                        + "        site isi {\n"
                        + "                pfn \"/home/pegasus/2.0/bin/keg\"\n"
                        + "                arch \"x86\"\n"
                        + "                os \"LINUX\"\n"
                        + "                type \"INSTALLED\"\n"
                        + "        }\n"
                        + "}");
        writer.close();
        bag.add(PegasusBag.PLANNER_DIRECTORY, dir);
        try {
            TransformationCatalog s = TransformationFactory.loadInstance(bag);
            assertThat(s, instanceOf(Text.class));
        } finally {
            dir.delete();
        }
        mLogger.logEventCompletion();
    }

    @Test
    public void testWithDefaultYAMLFile() throws Exception {
        mLogger.logEventStart(
                "test.catalog.transformation.factory",
                "default-yaml-file-test",
                Integer.toString(mTestNumber++));
        PegasusProperties props = PegasusProperties.nonSingletonInstance();
        PegasusBag bag = new PegasusBag();
        bag.add(PegasusBag.PEGASUS_PROPERTIES, props);
        bag.add(PegasusBag.PEGASUS_LOGMANAGER, mLogger);
        Path p = Files.createTempDirectory("pegasus");
        File dir = p.toFile();
        File yaml =
                new File(dir, TransformationFactory.DEFAULT_YAML_TRANSFORMATION_CATALOG_BASENAME);
        BufferedWriter writer = new BufferedWriter(new FileWriter(yaml));
        writer.write(
                "pegasus: \"5.0\"\n"
                        + "transformations:\n"
                        + "  - name: foo\n"
                        + "    requires:\n"
                        + "      - bar\n"
                        + "    sites:\n"
                        + "      - name: local\n"
                        + "        pfn: /nfs/u2/ryan/bin/foo\n"
                        + "        type: stageable\n"
                        + "        arch: x86_64\n"
                        + "        os.type: linux");
        writer.close();
        bag.add(PegasusBag.PLANNER_DIRECTORY, dir);
        try {
            TransformationCatalog s = TransformationFactory.loadInstance(bag);
            assertThat(s, instanceOf(YAML.class));
        } finally {
            dir.delete();
        }
        mLogger.logEventCompletion();
    }

    @Test
    public void testWithDefaultYAMLAndTextFiles() throws Exception {
        mLogger.logEventStart(
                "test.catalog.transformation.factory",
                "default-yaml-xml-test",
                Integer.toString(mTestNumber++));
        PegasusProperties props = PegasusProperties.nonSingletonInstance();
        PegasusBag bag = new PegasusBag();
        bag.add(PegasusBag.PEGASUS_PROPERTIES, props);
        bag.add(PegasusBag.PEGASUS_LOGMANAGER, mLogger);
        Path p = Files.createTempDirectory("pegasus");
        File dir = p.toFile();
        File yaml =
                new File(dir, TransformationFactory.DEFAULT_YAML_TRANSFORMATION_CATALOG_BASENAME);
        BufferedWriter writer = new BufferedWriter(new FileWriter(yaml));
        writer.write(
                "pegasus: \"5.0\"\n"
                        + "transformations:\n"
                        + "  - name: foo\n"
                        + "    requires:\n"
                        + "      - bar\n"
                        + "    sites:\n"
                        + "      - name: local\n"
                        + "        pfn: /nfs/u2/ryan/bin/foo\n"
                        + "        type: stageable\n"
                        + "        arch: x86_64\n"
                        + "        os.type: linux");
        writer.close();
        File xml =
                new File(dir, TransformationFactory.DEFAULT_TEXT_TRANSFORMATION_CATALOG_BASENAME);
        writer = new BufferedWriter(new FileWriter(xml));
        writer.write(
                "tr black::analyze:1.0 {\n"
                        + "        site isi {\n"
                        + "                pfn \"/home/pegasus/2.0/bin/keg\"\n"
                        + "                arch \"x86\"\n"
                        + "                os \"LINUX\"\n"
                        + "                type \"INSTALLED\"\n"
                        + "        }\n"
                        + "}");
        writer.close();
        bag.add(PegasusBag.PLANNER_DIRECTORY, dir);
        try {
            TransformationCatalog s = TransformationFactory.loadInstance(bag);
            assertThat(s, instanceOf(YAML.class));
        } finally {
            dir.delete();
        }
        mLogger.logEventCompletion();
    }

    private PegasusBag getPegasusBag(PegasusProperties props) {
        PegasusBag bag = new PegasusBag();
        bag.add(PegasusBag.PEGASUS_PROPERTIES, props);
        bag.add(PegasusBag.PEGASUS_LOGMANAGER, mLogger);
        return bag;
    }
}
