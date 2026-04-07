/**
 * Copyright 2007-2013 University Of Southern California
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.isi.pegasus.planner.common;

import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.*;

import edu.isi.pegasus.common.logging.LogFormatter;
import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.planner.classes.ADag;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.classes.PlannerOptions;
import edu.isi.pegasus.planner.refiner.ReplicaCatalogBridge;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;
import org.apache.logging.log4j.Level;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests for PegasusDBAdmin static constants and structural properties.
 *
 * @author Rajiv Mayani
 */
public class PegasusDBAdminTest {

    private static final class NoOpLogManager extends LogManager {
        private int mLevel;

        @Override
        public void initialize(LogFormatter formatter, Properties properties) {}

        @Override
        public void configure(boolean prefixTimestamp) {}

        @Override
        protected void setLevel(int level, boolean info) {
            mLevel = level;
        }

        @Override
        public int getLevel() {
            return mLevel;
        }

        @Override
        public void setWriters(String out) {}

        @Override
        public void setWriter(STREAM_TYPE type, PrintStream ps) {}

        @Override
        public PrintStream getWriter(STREAM_TYPE type) {
            return null;
        }

        @Override
        public void log(String message, Exception e, int level) {}

        @Override
        public void log(String message, int level) {}

        @Override
        protected void logAlreadyFormattedMessage(String message, int level) {}

        @Override
        public void logEventCompletion(int level) {}
    }

    @TempDir Path mTempDir;

    private PegasusBag createBag(PegasusProperties props, String submitDir) {
        PegasusBag bag = new PegasusBag();
        PlannerOptions options = new PlannerOptions();
        options.setSubmitDirectory(submitDir);
        bag.add(PegasusBag.PEGASUS_PROPERTIES, props);
        bag.add(PegasusBag.PLANNER_OPTIONS, options);
        return bag;
    }

    @Test
    public void testMasterDatabasePropertyKey() {
        assertThat(PegasusDBAdmin.MASTER_DATABASE_PROPERTY_KEY, is("pegasus.catalog.master.url"));
    }

    @Test
    public void testMasterDatabaseDeprecatedPropertyKey() {
        assertThat(
                PegasusDBAdmin.MASTER_DATABASE_DEPRECATED_PROPERTY_KEY,
                is("pegasus.dashboard.output"));
    }

    @Test
    public void testWorkflowDatabasePropertyKey() {
        assertThat(
                PegasusDBAdmin.WORKFLOW_DATABASE_PROPERTY_KEY, is("pegasus.catalog.workflow.url"));
    }

    @Test
    public void testWorkflowDatabaseDeprecatedPropertyKey() {
        assertThat(
                PegasusDBAdmin.WORKFLOW_DATABASE_DEPRECATED_PROPERTY_KEY,
                is("pegasus.monitord.output"));
    }

    @Test
    public void testMasterDatabaseKeyIsNotEmpty() {
        assertThat(PegasusDBAdmin.MASTER_DATABASE_PROPERTY_KEY.isEmpty(), is(false));
    }

    @Test
    public void testWorkflowDatabaseKeyIsNotEmpty() {
        assertThat(PegasusDBAdmin.WORKFLOW_DATABASE_PROPERTY_KEY.isEmpty(), is(false));
    }

    @Test
    public void testPropertyKeysAreDistinct() {
        assertThat(
                PegasusDBAdmin.MASTER_DATABASE_PROPERTY_KEY,
                not(is(PegasusDBAdmin.WORKFLOW_DATABASE_PROPERTY_KEY)));
    }

    @Test
    public void testDefaultConstructorCreatesInstance() {
        assertThat(new PegasusDBAdmin(), notNullValue());
    }

    @Test
    public void testCreateJDBCRCWithNullPropertiesFileReturnsFalse() {
        assertThat(new PegasusDBAdmin().createJDBCRC(null), is(false));
    }

    @Test
    public void testCreateJDBCRCWithMissingPropertiesFileReturnsFalse() {
        String missing = mTempDir.resolve("missing.properties").toString();
        NoOpLogManager logger = new NoOpLogManager();
        logger.setLevel(Level.DEBUG);

        assertThat(new PegasusDBAdmin(logger).createJDBCRC(missing), is(false));
    }

    @Test
    public void testRemapOutputRCPropertiesReturnsCommandLineArguments() {
        PegasusProperties props = PegasusProperties.nonSingletonInstance();
        props.setProperty(
                ReplicaCatalogBridge.OUTPUT_REPLICA_CATALOG_PREFIX + ".db.url", "jdbc:test");
        props.setProperty(
                ReplicaCatalogBridge.OUTPUT_REPLICA_CATALOG_PREFIX + ".db.driver",
                "org.test.Driver");

        String arguments =
                new PegasusDBAdmin()
                        .remapOutputRCProperties(
                                props, ReplicaCatalogBridge.OUTPUT_REPLICA_CATALOG_PREFIX);

        assertThat(arguments, containsString("-Dpegasus.catalog.replica.db.url=jdbc:test "));
        assertThat(
                arguments, containsString("-Dpegasus.catalog.replica.db.driver=org.test.Driver "));
    }

    @Test
    public void testUpdatePropertiesUsesDeprecatedConfiguredValues() {
        PegasusProperties props = PegasusProperties.nonSingletonInstance();
        props.setProperty(
                PegasusDBAdmin.MASTER_DATABASE_DEPRECATED_PROPERTY_KEY, "sqlite:///tmp/master.db");
        props.setProperty(
                PegasusDBAdmin.WORKFLOW_DATABASE_DEPRECATED_PROPERTY_KEY,
                "sqlite:///tmp/workflow.db");

        PegasusBag bag = createBag(props, mTempDir.toString());
        PegasusDBAdmin.updateProperties(bag, new ADag());

        assertThat(
                props.getProperty(PegasusDBAdmin.MASTER_DATABASE_PROPERTY_KEY),
                is("sqlite:///tmp/master.db"));
        assertThat(
                props.getProperty(PegasusDBAdmin.WORKFLOW_DATABASE_PROPERTY_KEY),
                is("sqlite:///tmp/workflow.db"));
    }

    @Test
    public void testUpdatePropertiesConstructsDefaultMasterDatabaseUrl() throws Exception {
        PegasusProperties props = PegasusProperties.nonSingletonInstance();
        props.setProperty(
                PegasusDBAdmin.WORKFLOW_DATABASE_PROPERTY_KEY,
                "sqlite:///tmp/existing-workflow.db");

        Path submitDir = Files.createDirectory(mTempDir.resolve("submit"));
        PegasusBag bag = createBag(props, submitDir.toString());

        PegasusDBAdmin.updateProperties(bag, new ADag());

        String expected =
                "sqlite:///"
                        + System.getProperty("user.home")
                        + java.io.File.separator
                        + ".pegasus"
                        + java.io.File.separator
                        + "workflow.db";
        assertThat(props.getProperty(PegasusDBAdmin.MASTER_DATABASE_PROPERTY_KEY), is(expected));
        assertThat(
                props.getProperty(PegasusDBAdmin.WORKFLOW_DATABASE_PROPERTY_KEY),
                is("sqlite:///tmp/existing-workflow.db"));
    }
}
