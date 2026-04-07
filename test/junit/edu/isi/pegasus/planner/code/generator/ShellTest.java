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
import edu.isi.pegasus.planner.catalog.site.classes.SiteStore;
import edu.isi.pegasus.planner.classes.ADag;
import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.classes.PlannerOptions;
import edu.isi.pegasus.planner.code.CodeGeneratorException;
import edu.isi.pegasus.planner.common.PegasusProperties;
import edu.isi.pegasus.planner.namespace.Dagman;
import java.io.File;
import java.io.PrintStream;
import java.nio.file.Files;
import java.util.Map;
import java.util.Properties;
import org.junit.jupiter.api.Test;

/** Tests for the Shell code generator class. */
public class ShellTest {

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

    private static final class TestShell extends Shell {
        boolean initializeGridStartFlag() {
            return mInitializeGridStart;
        }

        String pathToShellScript(ADag dag) {
            return getPathToShellScript(dag);
        }

        String checkExitCodeCall(Job job, String prefix) {
            return generateCallToCheckExitcode(job, prefix);
        }

        String executePostScriptCall(Job job, String directory) {
            return generateCallToExecutePostScript(job, directory);
        }

        boolean setExecutableBit(String file) {
            return setXBitOnFile(file);
        }
    }

    @Test
    public void testShellExtendsAbstract() {
        assertThat(Abstract.class.isAssignableFrom(Shell.class), is(true));
    }

    @Test
    public void testShellImplementsCodeGenerator() {
        assertThat(
                edu.isi.pegasus.planner.code.CodeGenerator.class.isAssignableFrom(Shell.class),
                is(true));
    }

    @Test
    public void testShellRunnerFunctionsBasenameConstant() {
        assertThat(Shell.PEGASUS_SHELL_RUNNER_FUNCTIONS_BASENAME, notNullValue());
        assertThat(Shell.PEGASUS_SHELL_RUNNER_FUNCTIONS_BASENAME, endsWith(".sh "));
    }

    @Test
    public void testConstructorInitializesGridStartFlag() {
        TestShell shell = new TestShell();

        assertThat(shell.initializeGridStartFlag(), is(true));
    }

    @Test
    public void testGetPathToShellScriptUsesWorkflowLabel() throws Exception {
        File submitDir = Files.createTempDirectory("shell-script-path").toFile();
        TestShell shell = initializedGenerator(submitDir);
        ADag dag = new ADag();
        dag.setLabel("workflow");
        dag.setIndex("0012");

        assertThat(
                shell.pathToShellScript(dag),
                is(new File(submitDir, "workflow.sh").getAbsolutePath()));
    }

    @Test
    public void testGetAdditionalBraindumpEntriesIncludesShellScriptPath() throws Exception {
        File submitDir = Files.createTempDirectory("shell-braindump").toFile();
        TestShell shell = initializedGenerator(submitDir);
        ADag dag = new ADag();
        dag.setLabel("workflow");

        Map<String, String> entries = shell.getAdditionalBraindumpEntries(dag);

        assertThat(entries.get(Braindump.GENERATOR_TYPE_KEY), is("shell"));
        assertThat(entries.get("script"), is(new File(submitDir, "workflow.sh").getAbsolutePath()));
    }

    @Test
    public void testGenerateCallToCheckExitcodeUsesJobIdPrefixAndShellStatus() {
        TestShell shell = new TestShell();
        Job job = new Job();
        job.setName("jobA");

        assertThat(
                shell.checkExitCodeCall(job, Shell.JOBSTATE_JOB_PREFIX),
                is("check_exitcode jobA JOB $?"));
    }

    @Test
    public void testGenerateCallToExecutePostScriptUsesDagmanProfiles() {
        TestShell shell = new TestShell();
        Job job = new Job();
        job.setName("jobA");
        job.dagmanVariables.construct(Dagman.POST_SCRIPT_KEY, "/bin/post");
        job.dagmanVariables.construct(Dagman.POST_SCRIPT_ARGUMENTS_KEY, "--flag");
        job.dagmanVariables.construct(Dagman.OUTPUT_KEY, "/tmp/dir/jobA.out");

        String call = shell.executePostScriptCall(job, "/submit/dir");

        assertThat(call, startsWith("execute_post_script jobA /submit/dir /bin/post "));
        assertThat(call, containsString("\"--flag jobA.out\""));
        assertThat(call, endsWith("\"\" "));
    }

    @Test
    public void testSetXBitOnFileReturnsFalseForMissingFile() {
        TestShell shell = new TestShell();
        shell.mLogger = new NoOpLogManager();

        assertThat(shell.setExecutableBit("/tmp/does-not-exist-shell-test"), is(false));
    }

    @Test
    public void testSetXBitOnFileReturnsTrueForExistingFile() throws Exception {
        TestShell shell = new TestShell();
        shell.mLogger = new NoOpLogManager();
        File script = Files.createTempFile("shell-xbit", ".sh").toFile();

        assertThat(shell.setExecutableBit(script.getAbsolutePath()), is(true));
        assertThat(script.canExecute(), is(true));
    }

    @Test
    public void testGenerateCodeForNonLocalJobThrows() throws Exception {
        File submitDir = Files.createTempDirectory("shell-nonlocal").toFile();
        TestShell shell = initializedGenerator(submitDir);
        ADag dag = new ADag();
        Job job = new Job();
        job.setName("jobA");
        job.setSiteHandle("remote");

        CodeGeneratorException exception =
                assertThrows(CodeGeneratorException.class, () -> shell.generateCode(dag, job));

        assertThat(exception.getMessage(), containsString("site local"));
    }

    private TestShell initializedGenerator(File submitDir) throws Exception {
        PegasusBag bag = new PegasusBag();
        PlannerOptions options = new PlannerOptions();
        options.setSubmitDirectory(submitDir);
        bag.add(PegasusBag.PLANNER_OPTIONS, options);
        bag.add(PegasusBag.PEGASUS_PROPERTIES, PegasusProperties.nonSingletonInstance());
        bag.add(PegasusBag.PEGASUS_LOGMANAGER, new NoOpLogManager());
        bag.add(PegasusBag.SITE_STORE, new SiteStore());

        TestShell shell = new TestShell();
        shell.initialize(bag);
        return shell;
    }
}
