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

/** Tests for the PMC code generator class. */
public class PMCTest {

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

    private static final class TestPMC extends PMC {
        boolean initializeGridStartFlag() {
            return mInitializeGridStart;
        }

        String pmcBasenameFor(ADag dag) {
            return pmcBasename(dag);
        }

        String pathToPMCFileFor(ADag dag) {
            return getPathtoPMCFile(dag);
        }
    }

    @Test
    public void testPMCExtendsAbstract() {
        assertThat(Abstract.class.isAssignableFrom(PMC.class), is(true));
    }

    @Test
    public void testPMCImplementsCodeGenerator() {
        assertThat(
                edu.isi.pegasus.planner.code.CodeGenerator.class.isAssignableFrom(PMC.class),
                is(true));
    }

    @Test
    public void testPMCInstantiation() {
        PMC pmc = new PMC();
        assertThat(pmc, notNullValue());
    }

    @Test
    public void testConstructorInitializesGridStartFlag() {
        TestPMC pmc = new TestPMC();

        assertThat(pmc.initializeGridStartFlag(), is(true));
    }

    @Test
    public void testPmcBasenameUsesLabelAndIndex() {
        TestPMC pmc = new TestPMC();
        ADag dag = new ADag();
        dag.setLabel("workflow");
        dag.setIndex("0009");

        assertThat(pmc.pmcBasenameFor(dag), is("workflow-0009.dag"));
    }

    @Test
    public void testGetPathtoPMCFileUsesSubmitDirectory() throws Exception {
        File submitDir = Files.createTempDirectory("pmc-path").toFile();
        TestPMC pmc = initializedGenerator(submitDir);
        ADag dag = new ADag();
        dag.setLabel("workflow");
        dag.setIndex("0010");

        assertThat(
                pmc.pathToPMCFileFor(dag),
                is(new File(submitDir, "workflow-0010.dag").getAbsolutePath()));
    }

    @Test
    public void testInitializeCreatesSubmitDirectory() throws Exception {
        File parent = Files.createTempDirectory("pmc-init").toFile();
        File submitDir = new File(parent, "nested/submit");
        assertThat(submitDir.exists(), is(false));

        initializedGenerator(submitDir);

        assertThat(submitDir.exists(), is(true));
        assertThat(submitDir.isDirectory(), is(true));
    }

    @Test
    public void testGetAdditionalBraindumpEntriesIncludesPbsAndDagPaths() throws Exception {
        File submitDir = Files.createTempDirectory("pmc-braindump").toFile();
        TestPMC pmc = initializedGenerator(submitDir);
        ADag dag = new ADag();
        dag.setLabel("workflow");
        dag.setIndex("0011");

        Map<String, String> entries = pmc.getAdditionalBraindumpEntries(dag);

        assertThat(entries.get(Braindump.GENERATOR_TYPE_KEY), is("pbs"));
        assertThat(
                entries.get("script"),
                is(new File(submitDir, "workflow-0011.pbs").getAbsolutePath()));
        assertThat(
                entries.get("dag"), is(new File(submitDir, "workflow-0011.dag").getAbsolutePath()));
    }

    @Test
    public void testGenerateCodeForSingleJobThrowsWorkflowLevelException() {
        PMC pmc = new PMC();
        CodeGeneratorException exception =
                assertThrows(
                        CodeGeneratorException.class,
                        () -> pmc.generateCode(new ADag(), new Job()));

        assertThat(exception.getMessage(), containsString("workflow level"));
    }

    @Test
    public void testGenerateCodeRequiresSiteStoreDuringGridstartEnablement() throws Exception {
        File submitDir = Files.createTempDirectory("pmc-gridstart").toFile();
        TestPMC pmc = initializedGenerator(submitDir);
        ADag dag = new ADag();

        Job job = new Job();
        job.setName("compute");
        job.setLogicalID("compute-id");
        job.setJobType(Job.COMPUTE_JOB);
        job.setSiteHandle("local");
        dag.add(job);

        NullPointerException exception =
                assertThrows(NullPointerException.class, () -> pmc.generateCode(dag));

        assertThat(exception.getMessage(), containsString("mSiteStore"));
    }

    private TestPMC initializedGenerator(File submitDir) throws Exception {
        PegasusBag bag = new PegasusBag();
        PlannerOptions options = new PlannerOptions();
        options.setSubmitDirectory(submitDir);
        bag.add(PegasusBag.PLANNER_OPTIONS, options);
        bag.add(PegasusBag.PEGASUS_PROPERTIES, PegasusProperties.nonSingletonInstance());
        bag.add(PegasusBag.PEGASUS_LOGMANAGER, new NoOpLogManager());

        TestPMC pmc = new TestPMC();
        pmc.initialize(bag);
        return pmc;
    }
}
