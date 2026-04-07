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
import edu.isi.pegasus.planner.code.CodeGenerator;
import edu.isi.pegasus.planner.common.PegasusProperties;
import java.io.File;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import org.junit.jupiter.api.Test;

/** Tests for the Abstract code generator class. */
public class AbstractTest {

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

    private static final class TestGenerator extends Abstract {
        @Override
        public Collection<File> generateCode(ADag dag) {
            return Collections.emptyList();
        }

        @Override
        public void generateCode(ADag dag, Job job) {}

        @Override
        public Map<String, String> getAdditionalBraindumpEntries(ADag workflow) {
            return Collections.emptyMap();
        }

        String dagFilename(ADag dag, String suffix) {
            return getDAGFilename(dag, suffix);
        }

        String submitDirectory() {
            return mSubmitFileDir;
        }

        PegasusProperties properties() {
            return mProps;
        }

        PlannerOptions options() {
            return mPOptions;
        }
    }

    @Test
    public void testAbstractImplementsCodeGenerator() {
        assertThat(CodeGenerator.class.isAssignableFrom(Abstract.class), is(true));
    }

    @Test
    public void testPostscriptLogSuffixConstant() {
        assertThat(Abstract.POSTSCRIPT_LOG_SUFFIX, is(".exitcode.log"));
    }

    @Test
    public void testPBSExtendsAbstract() {
        assertThat(Abstract.class.isAssignableFrom(PBS.class), is(true));
    }

    @Test
    public void testPMCExtendsAbstract() {
        assertThat(Abstract.class.isAssignableFrom(PMC.class), is(true));
    }

    @Test
    public void testShellExtendsAbstract() {
        assertThat(Abstract.class.isAssignableFrom(Shell.class), is(true));
    }

    @Test
    public void testStartMonitoringReturnsFalseByDefault() {
        assertThat(new TestGenerator().startMonitoring(), is(false));
    }

    @Test
    public void testStaticGetDAGFilenameUsesBasenamePrefixWhenPresent() {
        PlannerOptions options = new PlannerOptions();
        options.setBasenamePrefix("prefix");

        assertThat(Abstract.getDAGFilename(options, "workflow", "0001", ".dag"), is("prefix.dag"));
    }

    @Test
    public void testStaticGetDAGFilenameFallsBackToNameAndIndex() {
        PlannerOptions options = new PlannerOptions();

        assertThat(
                Abstract.getDAGFilename(options, "workflow", "0001", ".dag"),
                is("workflow-0001.dag"));
    }

    @Test
    public void testInitializePopulatesCoreFields() throws Exception {
        TestGenerator generator = new TestGenerator();
        PegasusBag bag = bagWithSubmitDirectory();

        generator.initialize(bag);

        assertThat(generator.properties(), sameInstance(bag.getPegasusProperties()));
        assertThat(generator.options(), sameInstance(bag.getPlannerOptions()));
        assertThat(generator.submitDirectory(), is(bag.getPlannerOptions().getSubmitDirectory()));
    }

    @Test
    public void testInstanceGetDAGFilenameUsesInitializedOptions() throws Exception {
        TestGenerator generator = new TestGenerator();
        PegasusBag bag = bagWithSubmitDirectory();
        bag.getPlannerOptions().setBasenamePrefix("run");
        generator.initialize(bag);
        ADag dag = new ADag();
        dag.setLabel("workflow");
        dag.setIndex("0002");

        assertThat(generator.dagFilename(dag, ".dag"), is("run.dag"));
    }

    @Test
    public void testResetClearsInitializedFields() throws Exception {
        TestGenerator generator = new TestGenerator();
        generator.initialize(bagWithSubmitDirectory());

        generator.reset();

        assertThat(generator.submitDirectory(), nullValue());
        assertThat(generator.properties(), nullValue());
        assertThat(generator.options(), nullValue());
    }

    @Test
    public void testGetWriterCreatesFileInSubmitDirectory() throws Exception {
        TestGenerator generator = new TestGenerator();
        PegasusBag bag = bagWithSubmitDirectory();
        generator.initialize(bag);
        Job job = new Job();
        job.setName("jobA");

        try (PrintWriter writer = generator.getWriter(job, ".sub")) {
            writer.println("hello");
        }

        File generated = new File(bag.getPlannerOptions().getSubmitDirectory(), "jobA.sub");
        assertThat(generated.exists(), is(true));
        assertThat(
                new String(Files.readAllBytes(generated.toPath()), StandardCharsets.UTF_8),
                is("hello\n"));
    }

    private PegasusBag bagWithSubmitDirectory() throws Exception {
        PegasusBag bag = new PegasusBag();
        PlannerOptions options = new PlannerOptions();
        File submitDir = Files.createTempDirectory("cg-abstract-test").toFile();
        submitDir.deleteOnExit();
        options.setSubmitDirectory(submitDir);
        bag.add(PegasusBag.PLANNER_OPTIONS, options);
        bag.add(PegasusBag.PEGASUS_PROPERTIES, PegasusProperties.nonSingletonInstance());
        bag.add(PegasusBag.PEGASUS_LOGMANAGER, new NoOpLogManager());
        return bag;
    }
}
