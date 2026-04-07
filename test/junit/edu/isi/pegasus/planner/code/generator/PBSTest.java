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
import edu.isi.pegasus.planner.classes.ADag;
import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.classes.PlannerOptions;
import edu.isi.pegasus.planner.code.CodeGeneratorException;
import edu.isi.pegasus.planner.common.PegasusProperties;
import java.io.File;
import java.io.PrintStream;
import java.nio.file.Files;
import java.util.Map;
import java.util.Properties;
import org.junit.jupiter.api.Test;

/** Tests for the PBS code generator class. */
public class PBSTest {

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

    private static final class TestPBS extends PBS {
        boolean initializeGridStartFlag() {
            return mInitializeGridStart;
        }

        String pbsBasenameFor(ADag dag) {
            return pbsBasename(dag);
        }

        String pathToPBSFileFor(ADag dag) {
            return getPathtoPBSFile(dag);
        }
    }

    @Test
    public void testPBSExtendsAbstract() {
        assertThat(Abstract.class.isAssignableFrom(PBS.class), is(true));
    }

    @Test
    public void testPBSImplementsCodeGenerator() {
        assertThat(
                edu.isi.pegasus.planner.code.CodeGenerator.class.isAssignableFrom(PBS.class),
                is(true));
    }

    @Test
    public void testPBSInstantiation() {
        PBS pbs = new PBS();
        assertThat(pbs, notNullValue());
    }

    @Test
    public void testConstructorInitializesGridStartFlag() {
        TestPBS pbs = new TestPBS();

        assertThat(pbs.initializeGridStartFlag(), is(true));
    }

    @Test
    public void testPbsBasenameUsesLabelAndIndex() {
        TestPBS pbs = new TestPBS();
        ADag dag = new ADag();
        dag.setLabel("workflow");
        dag.setIndex("0005");

        assertThat(pbs.pbsBasenameFor(dag), is("workflow-0005.pbs"));
    }

    @Test
    public void testGetPathtoPBSFileUsesSubmitDirectory() throws Exception {
        File submitDir = Files.createTempDirectory("pbs-generator-path").toFile();
        TestPBS pbs = initializedGenerator(submitDir);
        ADag dag = new ADag();
        dag.setLabel("workflow");
        dag.setIndex("0006");

        assertThat(
                pbs.pathToPBSFileFor(dag),
                is(new File(submitDir, "workflow-0006.pbs").getAbsolutePath()));
    }

    @Test
    public void testGetAdditionalBraindumpEntriesIncludesGeneratorTypeAndScriptPath()
            throws Exception {
        File submitDir = Files.createTempDirectory("pbs-braindump").toFile();
        TestPBS pbs = initializedGenerator(submitDir);
        ADag dag = new ADag();
        dag.setLabel("workflow");
        dag.setIndex("0007");

        Map<String, String> entries = pbs.getAdditionalBraindumpEntries(dag);

        assertThat(entries.get(Braindump.GENERATOR_TYPE_KEY), is("pbs"));
        assertThat(
                entries.get("script"),
                is(new File(submitDir, "workflow-0007.pbs").getAbsolutePath()));
    }

    @Test
    public void testGenerateCodeForSingleJobThrowsWorkflowLevelException() {
        PBS pbs = new PBS();
        CodeGeneratorException exception =
                assertThrows(
                        CodeGeneratorException.class,
                        () -> pbs.generateCode(new ADag(), new Job()));

        assertThat(exception.getMessage(), containsString("workflow level"));
    }

    @Test
    public void testGenerateCodeThrowsWhenPegasusMpiClusterIsNotOnPath() throws Exception {
        File submitDir = Files.createTempDirectory("pbs-generate").toFile();
        TestPBS pbs = initializedGenerator(submitDir);
        ADag dag = new ADag();
        dag.setLabel("workflow");
        dag.setIndex("0008");

        CodeGeneratorException exception =
                assertThrows(CodeGeneratorException.class, () -> pbs.generateCode(dag));

        assertThat(exception.getMessage(), containsString("pegasus-mpi-cluster"));
    }

    private TestPBS initializedGenerator(File submitDir) throws Exception {
        PegasusBag bag = new PegasusBag();
        PlannerOptions options = new PlannerOptions();
        options.setSubmitDirectory(submitDir);
        bag.add(PegasusBag.PLANNER_OPTIONS, options);
        bag.add(PegasusBag.PEGASUS_PROPERTIES, PegasusProperties.nonSingletonInstance());
        bag.add(PegasusBag.PEGASUS_LOGMANAGER, new NoOpLogManager());

        TestPBS pbs = new TestPBS();
        pbs.initialize(bag);
        return pbs;
    }
}
