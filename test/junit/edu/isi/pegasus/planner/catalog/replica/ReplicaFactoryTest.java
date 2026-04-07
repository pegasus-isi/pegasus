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
package edu.isi.pegasus.planner.catalog.replica;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.*;

import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.planner.catalog.ReplicaCatalog;
import edu.isi.pegasus.planner.catalog.replica.impl.SimpleFile;
import edu.isi.pegasus.planner.catalog.replica.impl.YAML;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.common.PegasusProperties;
import edu.isi.pegasus.planner.test.DefaultTestSetup;
import edu.isi.pegasus.planner.test.TestSetup;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Test class for testing the ReplicaFactory, and whether it loads correct backend classes based on
 * properties specified
 *
 * @author Karan Vahi
 */
public class ReplicaFactoryTest {

    private static final String FILE_CATALOG_TYPE = "File";

    private static final String YAML_CATALOG_TYPE = "YAML";

    private PegasusBag mBag;

    private LogManager mLogger;

    private TestSetup mTestSetup;

    private static int mTestNumber = 1;

    public ReplicaFactoryTest() {}

    @BeforeEach
    public void setUp() {
        mTestSetup = new DefaultTestSetup();
        mBag = new PegasusBag();
        mTestSetup.setInputDirectory(this.getClass());
        System.out.println("Input Test Dir is " + mTestSetup.getInputDirectory());
        mLogger = mTestSetup.loadLogger(PegasusProperties.nonSingletonInstance());
        mLogger.setLevel(LogManager.DEBUG_MESSAGE_LEVEL);
        mLogger.logEventStart("test.catalog.replica.ReplicaFactory", "setup", "0");
        mBag.add(PegasusBag.PEGASUS_LOGMANAGER, mLogger);
        mLogger.logEventCompletion();
    }

    @Test
    public void testWithTypeMentionedFile() throws Exception {
        mLogger.logEventStart(
                "test.catalog.replica.factory",
                "testWithTypeMentionedFile",
                Integer.toString(mTestNumber++));

        PegasusProperties props = PegasusProperties.nonSingletonInstance();
        props.setProperty(PegasusProperties.PEGASUS_REPLICA_CATALOG_PROPERTY, FILE_CATALOG_TYPE);
        props.setProperty(
                PegasusProperties.PEGASUS_REPLICA_CATALOG_FILE_PROPERTY,
                new File(mTestSetup.getInputDirectory(), "rc.data").getAbsolutePath());
        props.setPropertiesFileBackend(mTestSetup.getInputDirectory());
        File tempFile = new File(props.writeOutProperties());
        try {
            ReplicaCatalog rc = ReplicaFactory.loadInstance(getPegasusBag(props));
            assertThat(rc, instanceOf(SimpleFile.class));
        } finally {
            if (tempFile != null) {
                tempFile.delete();
            }
        }
        mLogger.logEventCompletion();
    }

    @Test
    public void testWithOnlyPathToFile() throws Exception {
        mLogger.logEventStart(
                "test.catalog.replica.factory",
                "testWithOnlyPathToFile",
                Integer.toString(mTestNumber++));

        PegasusProperties props = PegasusProperties.nonSingletonInstance();
        props.setProperty(
                PegasusProperties.PEGASUS_REPLICA_CATALOG_FILE_PROPERTY,
                new File(mTestSetup.getInputDirectory(), "rc.data").getAbsolutePath());
        props.setPropertiesFileBackend(mTestSetup.getInputDirectory());
        File tempFile = new File(props.writeOutProperties());
        try {
            ReplicaCatalog rc = ReplicaFactory.loadInstance(getPegasusBag(props));
            assertThat(rc, instanceOf(SimpleFile.class));
        } finally {
            if (tempFile != null) {
                tempFile.delete();
            }
        }
        mLogger.logEventCompletion();
    }

    @Test
    public void testWithTypeMentionedYAML() throws Exception {
        mLogger.logEventStart(
                "test.catalog.replica.ReplicaFactory",
                "testWithOnlyPathToFile",
                Integer.toString(mTestNumber++));

        PegasusProperties props = PegasusProperties.nonSingletonInstance();
        props.setProperty(PegasusProperties.PEGASUS_REPLICA_CATALOG_PROPERTY, YAML_CATALOG_TYPE);
        props.setProperty(
                PegasusProperties.PEGASUS_REPLICA_CATALOG_FILE_PROPERTY,
                new File(mTestSetup.getInputDirectory(), "rc.yml").getAbsolutePath());
        props.setPropertiesFileBackend(mTestSetup.getInputDirectory());
        File tempFile = new File(props.writeOutProperties());
        try {
            ReplicaCatalog rc = ReplicaFactory.loadInstance(getPegasusBag(props));
            assertThat(rc, instanceOf(YAML.class));
        } finally {
            if (tempFile != null) {
                tempFile.delete();
            }
        }
        mLogger.logEventCompletion();
    }

    @Test
    public void testWithOnlyPathToYAMLFile() throws Exception {
        mLogger.logEventStart(
                "test.catalog.replica.ReplicaFactory",
                "testWithOnlyPathToFile",
                Integer.toString(mTestNumber++));

        PegasusProperties props = PegasusProperties.nonSingletonInstance();
        props.setProperty(
                PegasusProperties.PEGASUS_REPLICA_CATALOG_FILE_PROPERTY,
                new File(mTestSetup.getInputDirectory(), "rc.yml").getAbsolutePath());
        props.setPropertiesFileBackend(mTestSetup.getInputDirectory());
        File tempFile = new File(props.writeOutProperties());
        try {
            ReplicaCatalog rc = ReplicaFactory.loadInstance(getPegasusBag(props));
            assertThat(rc, instanceOf(YAML.class));
        } finally {
            if (tempFile != null) {
                tempFile.delete();
            }
        }
        mLogger.logEventCompletion();
    }

    @Test
    public void testWithDefaultTextFile() throws Exception {
        mLogger.logEventStart(
                "test.catalog.replica.factory",
                "default-text-file-test",
                Integer.toString(mTestNumber++));
        PegasusProperties props = PegasusProperties.nonSingletonInstance();
        PegasusBag bag = new PegasusBag();
        bag.add(PegasusBag.PEGASUS_PROPERTIES, props);
        bag.add(PegasusBag.PEGASUS_LOGMANAGER, mLogger);
        Path p = Files.createTempDirectory("pegasus");
        File dir = p.toFile();
        props.setPropertiesFileBackend(mTestSetup.getInputDirectory());
        File tempFile = new File(props.writeOutProperties());
        File text = new File(dir, ReplicaFactory.DEFAULT_FILE_REPLICA_CATALOG_BASENAME);
        BufferedWriter writer = new BufferedWriter(new FileWriter(text));
        writer.write("text\n");
        writer.close();
        bag.add(PegasusBag.PLANNER_DIRECTORY, dir);
        try {
            ReplicaCatalog c = ReplicaFactory.loadInstance(bag);
            assertThat(c, instanceOf(SimpleFile.class));
        } finally {
            text.delete();
            dir.delete();
            tempFile.delete();
        }
        mLogger.logEventCompletion();
    }

    @Test
    public void testWithDefaultYAMLFile() throws Exception {
        mLogger.logEventStart(
                "test.catalog.replica.factory",
                "default-yaml-file-test",
                Integer.toString(mTestNumber++));
        PegasusProperties props = PegasusProperties.nonSingletonInstance();
        PegasusBag bag = new PegasusBag();
        bag.add(PegasusBag.PEGASUS_PROPERTIES, props);
        bag.add(PegasusBag.PEGASUS_LOGMANAGER, mLogger);
        Path p = Files.createTempDirectory("pegasus");
        File dir = p.toFile();
        props.setPropertiesFileBackend(mTestSetup.getInputDirectory());
        File tempFile = new File(props.writeOutProperties());
        File yaml = new File(dir, ReplicaFactory.DEFAULT_YAML_REPLICA_CATALOG_BASENAME);
        BufferedWriter writer = new BufferedWriter(new FileWriter(yaml));
        writer.write("pegasus: \"5.0\"\n");
        writer.write("replicas: \n");
        writer.write(
                "# matches \"f.a\"\n"
                        + "  - lfn: \"f.a\"\n"
                        + "    pfns:\n"
                        + "      - pfn: \"file:///Volumes/data/input/f.a\"\n"
                        + "        site: \"local\"");
        writer.close();
        bag.add(PegasusBag.PLANNER_DIRECTORY, dir);
        try {
            ReplicaCatalog c = ReplicaFactory.loadInstance(bag);
            assertThat(c, instanceOf(YAML.class));
        } finally {
            yaml.delete();
            dir.delete();
            tempFile.delete();
        }
        mLogger.logEventCompletion();
    }

    @Test
    public void testWithDefaultYAMLAndTextFiles() throws Exception {
        mLogger.logEventStart(
                "test.catalog.replica.factory",
                "default-yaml-file-test",
                Integer.toString(mTestNumber++));
        PegasusProperties props = PegasusProperties.nonSingletonInstance();
        PegasusBag bag = new PegasusBag();
        bag.add(PegasusBag.PEGASUS_PROPERTIES, props);
        bag.add(PegasusBag.PEGASUS_LOGMANAGER, mLogger);
        Path p = Files.createTempDirectory("pegasus");
        File dir = p.toFile();
        props.setPropertiesFileBackend(mTestSetup.getInputDirectory());
        File tempFile = new File(props.writeOutProperties());
        File yaml = new File(dir, ReplicaFactory.DEFAULT_YAML_REPLICA_CATALOG_BASENAME);
        BufferedWriter writer = new BufferedWriter(new FileWriter(yaml));
        writer.write("pegasus: \"5.0\"\n");
        writer.write("replicas: \n");
        writer.write(
                "# matches \"f.a\"\n"
                        + "  - lfn: \"f.a\"\n"
                        + "    pfns:\n"
                        + "      - pfn: \"file:///Volumes/data/input/f.a\"\n"
                        + "        site: \"local\"");
        writer.close();

        File text = new File(dir, ReplicaFactory.DEFAULT_FILE_REPLICA_CATALOG_BASENAME);
        writer = new BufferedWriter(new FileWriter(text));
        writer.write("text\n");
        writer.close();
        bag.add(PegasusBag.PLANNER_DIRECTORY, dir);
        try {
            ReplicaCatalog c = ReplicaFactory.loadInstance(bag);
            assertThat(c, instanceOf(YAML.class));
        } finally {
            yaml.delete();
            text.delete();
            dir.delete();
            tempFile.delete();
        }
        mLogger.logEventCompletion();
    }

    @Test
    public void testExplicitImplementorLoadsShortNameSimpleFile() throws Exception {
        File text = createTempReplicaTextFile();
        Properties connectProps = new Properties();
        connectProps.setProperty(ReplicaCatalog.FILE_KEY, text.getAbsolutePath());

        try {
            mLogger.logEventStart("test.catalog.replica.factory", "short-name-simplefile", "0");
            ReplicaCatalog rc =
                    ReplicaFactory.loadInstance(
                            "SimpleFile", getPegasusBagWithPlannerDir(), connectProps);

            assertThat(rc, instanceOf(SimpleFile.class));
            assertThat(
                    connectProps.getProperty(ReplicaCatalog.PARSER_DOCUMENT_SIZE_PROPERTY_KEY),
                    equalTo(
                            Integer.toString(
                                    PegasusProperties.nonSingletonInstance()
                                            .getMaxSupportedYAMLDocSize())));
        } finally {
            mLogger.logEventCompletion();
            text.delete();
        }
    }

    @Test
    public void testExplicitImplementorLoadsFileAliasAsSimpleFile() throws Exception {
        File text = createTempReplicaTextFile();
        Properties connectProps = new Properties();
        connectProps.setProperty(ReplicaCatalog.FILE_KEY, text.getAbsolutePath());

        try {
            mLogger.logEventStart("test.catalog.replica.factory", "file-alias-simplefile", "0");
            ReplicaCatalog rc =
                    ReplicaFactory.loadInstance(
                            "File", getPegasusBagWithPlannerDir(), connectProps);

            assertThat(rc, instanceOf(SimpleFile.class));
        } finally {
            mLogger.logEventCompletion();
            text.delete();
        }
    }

    @Test
    public void testLoadInstanceWithNullImplementorThrows() {
        NullPointerException exception =
                assertThrows(
                        NullPointerException.class,
                        () ->
                                ReplicaFactory.loadInstance(
                                        null, getPegasusBagWithPlannerDir(), new Properties()));

        assertThat(exception.getMessage(), equalTo("Invalid catalog implementor passed"));
    }

    @Test
    public void testLoadInstanceWithNullPropertiesInBagThrows() {
        PegasusBag bag = new PegasusBag();
        bag.add(PegasusBag.PEGASUS_LOGMANAGER, mLogger);
        bag.add(PegasusBag.PLANNER_DIRECTORY, new File(mTestSetup.getInputDirectory()));

        NullPointerException exception =
                assertThrows(
                        NullPointerException.class, () -> ReplicaFactory.loadInstance(bag, null));

        assertThat(exception.getMessage(), equalTo("Invalid NULL properties passed"));
    }

    @Test
    public void testLoadInstanceWithoutConfiguredReplicaCatalogFallsBackToDefaultImplementor() {
        PegasusBag bag = new PegasusBag();
        bag.add(PegasusBag.PEGASUS_PROPERTIES, PegasusProperties.nonSingletonInstance());
        bag.add(PegasusBag.PEGASUS_LOGMANAGER, mLogger);

        try {
            mLogger.logEventStart(
                    "test.catalog.replica.factory", "default-implementor-fallback", "0");
            RuntimeException exception =
                    assertThrows(
                            RuntimeException.class, () -> ReplicaFactory.loadInstance(bag, null));

            assertThat(
                    exception.getMessage(),
                    containsString(ReplicaFactory.CONNECT_TO_RC_FAILED_MESSAGE));
            assertThat(
                    exception.getMessage(),
                    containsString(ReplicaFactory.DEFAULT_CATALOG_IMPLEMENTOR));
        } finally {
            mLogger.logEventCompletion();
        }
    }

    @Test
    public void testLoadInstanceWithNullLoggerInBagThrows() {
        PegasusBag bag = new PegasusBag();
        bag.add(PegasusBag.PEGASUS_PROPERTIES, PegasusProperties.nonSingletonInstance());
        bag.add(PegasusBag.PLANNER_DIRECTORY, new File(mTestSetup.getInputDirectory()));

        NullPointerException exception =
                assertThrows(
                        NullPointerException.class, () -> ReplicaFactory.loadInstance(bag, null));

        assertThat(exception.getMessage(), equalTo("Invalid Logger passed"));
    }

    private PegasusBag getPegasusBag(PegasusProperties props) {
        PegasusBag bag = new PegasusBag();
        bag.add(PegasusBag.PEGASUS_PROPERTIES, props);
        bag.add(PegasusBag.PEGASUS_LOGMANAGER, mLogger);
        return bag;
    }

    private PegasusBag getPegasusBagWithPlannerDir() {
        PegasusBag bag = new PegasusBag();
        PegasusProperties props = PegasusProperties.nonSingletonInstance();
        bag.add(PegasusBag.PEGASUS_PROPERTIES, props);
        bag.add(PegasusBag.PEGASUS_LOGMANAGER, mLogger);
        bag.add(PegasusBag.PLANNER_DIRECTORY, new File(mTestSetup.getInputDirectory()));
        return bag;
    }

    private File createTempReplicaTextFile() throws Exception {
        File text = File.createTempFile("replica-factory-", ".rc");
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(text))) {
            writer.write("f.a file:///tmp/f.a site=\"local\"");
        }
        return text;
    }

    private File createTempReplicaYAML() throws Exception {
        File yaml = File.createTempFile("replica-factory-", ".yml");
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(yaml))) {
            writer.write("pegasus: \"5.0\"\n");
            writer.write("replicas:\n");
            writer.write("  - lfn: \"f.a\"\n");
            writer.write("    pfns:\n");
            writer.write("      - pfn: \"file:///tmp/f.a\"\n");
            writer.write("        site: \"local\"\n");
        }
        return yaml;
    }
}
