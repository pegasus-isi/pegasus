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
package edu.isi.pegasus.planner.code.generator;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.*;

import edu.isi.pegasus.common.logging.LogFormatter;
import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.planner.catalog.replica.ReplicaCatalogEntry;
import edu.isi.pegasus.planner.catalog.replica.classes.ReplicaStore;
import edu.isi.pegasus.planner.classes.ADag;
import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.classes.PlannerOptions;
import edu.isi.pegasus.planner.code.CodeGeneratorException;
import edu.isi.pegasus.planner.common.PegasusProperties;
import java.io.File;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Collection;
import java.util.Properties;
import org.junit.jupiter.api.Test;

/** Tests for the DAXReplicaStore code generator class. */
public class DAXReplicaStoreTest {

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

    @Test
    public void testImplementsCodeGenerator() {
        assertThat(
                edu.isi.pegasus.planner.code.CodeGenerator.class.isAssignableFrom(
                        DAXReplicaStore.class),
                is(true));
    }

    @Test
    public void testDAXReplicaStoreCatalogKeyConstant() {
        assertThat(DAXReplicaStore.DAX_REPLICA_STORE_CATALOG_KEY, is("file"));
    }

    @Test
    public void testDAXReplicaStoreCatalogImplementerConstant() {
        assertThat(DAXReplicaStore.DAX_REPLICA_STORE_CATALOG_IMPLEMENTER, is("SimpleFile"));
    }

    @Test
    public void testDAXReplicaStoreIsInstantiable() {
        DAXReplicaStore store = new DAXReplicaStore();
        assertThat(store, notNullValue());
    }

    @Test
    public void testGetDAXReplicaStoreFileUsesBasenamePrefix() {
        PlannerOptions options = new PlannerOptions();
        options.setSubmitDirectory(new File("/tmp"));
        options.setBasenamePrefix("run");

        assertThat(
                DAXReplicaStore.getDAXReplicaStoreFile(options, "ignored", "0001"),
                is(new File("/tmp", "run.replica.store").getPath()));
    }

    @Test
    public void testGetDAXReplicaStoreFileFallsBackToLabelAndIndex() {
        PlannerOptions options = new PlannerOptions();
        options.setSubmitDirectory(new File("/tmp"));

        assertThat(
                DAXReplicaStore.getDAXReplicaStoreFile(options, "workflow", "0003"),
                is(new File("/tmp", "workflow-0003.replica.store").getPath()));
    }

    @Test
    public void testGenerateCodeReturnsEmptyCollectionForEmptyReplicaStore() throws Exception {
        File submitDir = Files.createTempDirectory("dax-replica-store-empty").toFile();
        DAXReplicaStore generator = initializedGenerator(submitDir);
        ADag dag = new ADag();
        dag.setLabel("workflow");
        dag.setIndex("0001");

        Collection<File> result = generator.generateCode(dag);

        assertThat(result.isEmpty(), is(true));
        assertThat(new File(submitDir, "workflow-0001.replica.store").exists(), is(false));
    }

    @Test
    public void testGenerateCodeWritesReplicaEntriesToReplicaStoreFile() throws Exception {
        File submitDir = Files.createTempDirectory("dax-replica-store-write").toFile();
        DAXReplicaStore generator = initializedGenerator(submitDir);
        ADag dag = new ADag();
        dag.setLabel("workflow");
        dag.setIndex("0002");
        ReplicaStore store = new ReplicaStore();
        store.add("input.dat", new ReplicaCatalogEntry("file:///data/input.dat", "local"));
        dag.setReplicaStore(store);

        Collection<File> result = generator.generateCode(dag);

        assertThat(result.size(), is(1));
        File generated = result.iterator().next();
        assertThat(
                generated.getAbsolutePath(),
                is(new File(submitDir, "workflow-0002.replica.store").getAbsolutePath()));
        assertThat(generated.exists(), is(true));

        String contents =
                new String(Files.readAllBytes(generated.toPath()), StandardCharsets.UTF_8);
        assertThat(contents, containsString("# file-based replica catalog:"));
        assertThat(contents, containsString("input.dat file:///data/input.dat site=\"local\""));
    }

    @Test
    public void testGenerateCodeForSingleJobThrows() {
        DAXReplicaStore generator = new DAXReplicaStore();
        CodeGeneratorException exception =
                assertThrows(
                        CodeGeneratorException.class,
                        () -> generator.generateCode(new ADag(), new Job()));

        assertThat(exception.getMessage(), containsString("whole workflow"));
    }

    @Test
    public void testStartMonitoringThrowsUnsupportedOperationException() {
        DAXReplicaStore generator = new DAXReplicaStore();

        UnsupportedOperationException exception =
                assertThrows(UnsupportedOperationException.class, generator::startMonitoring);

        assertThat(exception.getMessage(), containsString("Not supported"));
    }

    @Test
    public void testResetThrowsUnsupportedOperationException() {
        DAXReplicaStore generator = new DAXReplicaStore();

        UnsupportedOperationException exception =
                assertThrows(UnsupportedOperationException.class, generator::reset);

        assertThat(exception.getMessage(), containsString("Not supported"));
    }

    private DAXReplicaStore initializedGenerator(File submitDir) throws Exception {
        PegasusBag bag = new PegasusBag();
        PlannerOptions options = new PlannerOptions();
        options.setSubmitDirectory(submitDir);
        bag.add(PegasusBag.PLANNER_OPTIONS, options);
        bag.add(PegasusBag.PEGASUS_PROPERTIES, PegasusProperties.nonSingletonInstance());
        bag.add(PegasusBag.PEGASUS_LOGMANAGER, new NoOpLogManager());

        DAXReplicaStore generator = new DAXReplicaStore();
        generator.initialize(bag);
        return generator;
    }
}
