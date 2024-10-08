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
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.*;

import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.common.util.PegasusURL;
import edu.isi.pegasus.planner.catalog.TransformationCatalog;
import edu.isi.pegasus.planner.catalog.classes.SysInfo;
import edu.isi.pegasus.planner.catalog.site.classes.SiteCatalogEntry;
import edu.isi.pegasus.planner.catalog.site.classes.SiteStore;
import edu.isi.pegasus.planner.catalog.transformation.classes.TCType;
import edu.isi.pegasus.planner.catalog.transformation.impl.Text;
import edu.isi.pegasus.planner.catalog.transformation.impl.YAML;
import edu.isi.pegasus.planner.classes.ADag;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.classes.PlannerOptions;
import edu.isi.pegasus.planner.common.PegasusProperties;
import edu.isi.pegasus.planner.test.DefaultTestSetup;
import edu.isi.pegasus.planner.test.TestSetup;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

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

    @BeforeAll
    public static void setUpClass() {}

    @AfterAll
    public static void tearDownClass() {}

    @BeforeEach
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

    @AfterEach
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
        assertFalse(tc.isTransient(), "loaded catalog should not be transient");
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
        assertFalse(tc.isTransient(), "loaded catalog should not be transient");
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
                new File(mTestSetup.getInputDirectory() + "/yaml/", "tc.yml").getAbsolutePath());
        TransformationCatalog tc = TransformationFactory.loadInstance(getPegasusBag(props));
        assertThat(tc, instanceOf(YAML.class));
        assertFalse(tc.isTransient(), "loaded catalog should not be transient");
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
                new File(mTestSetup.getInputDirectory() + "/yaml/", "tc.yml").getAbsolutePath());
        TransformationCatalog tc = TransformationFactory.loadInstance(getPegasusBag(props));
        assertThat(tc, instanceOf(YAML.class));
        assertFalse(tc.isTransient(), "loaded catalog should not be transient");
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
            assertFalse(s.isTransient(), "loaded catalog should not be transient");
        } finally {
            dir.delete();
        }
        mLogger.logEventCompletion();
    }

    @Test
    public void testWithEmptyTransientTextFile() throws Exception {
        mLogger.logEventStart(
                "test.catalog.transformation.factory",
                "default-text-empty-file-test",
                Integer.toString(mTestNumber++));
        PegasusProperties props = PegasusProperties.nonSingletonInstance();
        props.setProperty("pegasus.catalog.transformation.transient", "true");
        PegasusBag bag = new PegasusBag();
        bag.add(PegasusBag.PEGASUS_PROPERTIES, props);
        bag.add(PegasusBag.PEGASUS_LOGMANAGER, mLogger);
        Path p = Files.createTempDirectory("pegasus");
        File dir = p.toFile();
        File text =
                new File(dir, TransformationFactory.DEFAULT_TEXT_TRANSFORMATION_CATALOG_BASENAME);
        BufferedWriter writer = new BufferedWriter(new FileWriter(text));
        writer.close();
        bag.add(PegasusBag.PLANNER_DIRECTORY, dir);
        bag.add(PegasusBag.PLANNER_DIRECTORY, dir);
        try {
            TransformationCatalog s = TransformationFactory.loadInstance(bag);
            assertThat(s, instanceOf(Text.class));
            assertTrue(s.isTransient(), "catalog should be transient");
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
            assertFalse(s.isTransient(), "loaded catalog should not be transient");
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
            assertFalse(s.isTransient(), "loaded catalog should not be transient");
        } finally {
            dir.delete();
        }
        mLogger.logEventCompletion();
    }

    // @Test
    // entries in directory for same executable should over ride the TC
    public void testDirectoryAndCatalogOrder() throws Exception {
        mLogger.logEventStart(
                "test.catalog.transformation.factory",
                "default-directory-yaml-catalog-test",
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
        SysInfo sysInfo = new SysInfo();
        sysInfo.setArchitecture(SysInfo.Architecture.aarch64);
        sysInfo.setOS(SysInfo.OS.linux);

        writer.write(
                "pegasus: \"5.0\"\n"
                        + "transformations:\n"
                        + "  - name: pegasus-keg\n"
                        + "    requires:\n"
                        + "      - bar\n"
                        + "    sites:\n"
                        + "      - name: local\n"
                        + "        pfn: /catalog/pegasus-keg\n"
                        + "        type: stageable\n"
                        + "        arch: "
                        + sysInfo.getArchitecture().name()
                        + "\n"
                        + "        os.type: "
                        + sysInfo.getOS().name());
        writer.close();
        bag.add(PegasusBag.PLANNER_DIRECTORY, dir);

        TransformationCatalogEntry expected =
                new TransformationCatalogEntry(null, "pegasus-keg", null);

        // directory based tc entries are on site local
        expected.setResourceId("local");
        expected.setSysInfo(sysInfo);
        expected.setType(TCType.STAGEABLE);
        expected.setPhysicalTransformation(
                new PegasusURL(
                                new File(
                                                TransformationFactory
                                                        .DEFAULT_TRANSFORMATION_CATALOG_DIRECTORY,
                                                expected.getLogicalName())
                                        .getAbsolutePath())
                        .getURL());

        try {
            // number of entries retrieved should be actually 1 if overriding in TC worked perfectly
            this.testFromDirectory(expected, bag, "compute-site", 2);
        } finally {
            dir.delete();
        }
        mLogger.logEventCompletion();
    }

    @Test
    public void testFromDirectory() throws Exception {
        mLogger.logEventStart(
                "test.catalog.transformation.factory",
                "default-transformations-dir",
                Integer.toString(mTestNumber++));

        SysInfo sysInfo = new SysInfo();
        sysInfo.setArchitecture(SysInfo.Architecture.aarch64);
        sysInfo.setOS(SysInfo.OS.linux);

        TransformationCatalogEntry expected =
                new TransformationCatalogEntry(null, "pegasus-keg", null);

        // directory based tc entries are on site local
        expected.setResourceId("local");
        expected.setSysInfo(sysInfo);
        expected.setType(TCType.STAGEABLE);
        expected.setPhysicalTransformation(
                new PegasusURL(
                                new File(
                                                TransformationFactory
                                                        .DEFAULT_TRANSFORMATION_CATALOG_DIRECTORY,
                                                expected.getLogicalName())
                                        .getAbsolutePath())
                        .getURL());

        this.testFromDirectory(expected, null, "compute-site", 1);
    }

    private void testFromDirectory(
            TransformationCatalogEntry expected, PegasusBag bag, String computeSite, int num)
            throws Exception {
        mLogger.logEventStart(
                "test.catalog.transformation.factory",
                "default-transformations-dir",
                Integer.toString(mTestNumber++));

        bag = bag == null ? new PegasusBag() : bag;

        if (bag.getPegasusProperties() == null) {
            PegasusProperties props = PegasusProperties.nonSingletonInstance();
            bag.add(PegasusBag.PEGASUS_PROPERTIES, props);
        }
        if (bag.getLogger() == null) {
            bag.add(PegasusBag.PEGASUS_LOGMANAGER, mLogger);
        }
        // directory backend tc requires planner options set and also site store
        // populated for an execution site
        if (bag.getPlannerOptions() == null) {
            PlannerOptions options = new PlannerOptions();
            options.setExecutionSites(computeSite);
            bag.add(PegasusBag.PLANNER_OPTIONS, options);
        }
        SiteCatalogEntry entry = new SiteCatalogEntry(computeSite);
        entry.setSysInfo(expected.getSysInfo());
        SiteStore s = new SiteStore();
        s.addEntry(entry);
        bag.add(PegasusBag.SITE_STORE, s);

        File parent = bag.getPlannerDirectory();
        parent = parent == null ? new File(".") : parent;
        File dir = new File(parent, TransformationFactory.DEFAULT_TRANSFORMATION_CATALOG_DIRECTORY);
        dir.mkdir();
        File keg = new File(dir, expected.getLogicalName());
        keg.createNewFile();
        keg.setExecutable(true);
        try {
            TransformationCatalog c = TransformationFactory.loadInstanceWithStores(bag, new ADag());
            List<TransformationCatalogEntry> entries =
                    c.lookup(
                            null, expected.getLogicalName(), null, (String) null, TCType.STAGEABLE);
            assertEquals(num, entries.size(), "Number of entries retrieved from TC");
            assertEquals(expected, entries.get(0), "Transformation retrieved from TC as ");
            // maybe should be false. need to be revisited
            assertTrue(c.isTransient(), "loaded catalog should be transient");
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
