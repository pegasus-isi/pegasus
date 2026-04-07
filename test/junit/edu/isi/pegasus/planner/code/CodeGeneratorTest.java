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
package edu.isi.pegasus.planner.code;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.*;

import edu.isi.pegasus.common.logging.LogFormatter;
import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.common.PegasusConfiguration;
import java.io.PrintStream;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Properties;
import org.junit.jupiter.api.Test;

/** Tests for CodeGenerator interface structure */
public class CodeGeneratorTest {

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
    public void testVersionConstantExists() {
        assertThat(CodeGenerator.VERSION, is("1.5"));
    }

    @Test
    public void testInterfaceIsPublic() {
        assertThat(
                java.lang.reflect.Modifier.isPublic(CodeGenerator.class.getModifiers()), is(true));
    }

    @Test
    public void testInterfaceHasInitializeMethod() throws NoSuchMethodException {
        // Verify initialize(PegasusBag) method is declared
        assertThat(
                CodeGenerator.class.getMethod(
                        "initialize", edu.isi.pegasus.planner.classes.PegasusBag.class),
                notNullValue());
    }

    @Test
    public void testInterfaceHasGenerateCodeMethod() throws NoSuchMethodException {
        assertThat(
                CodeGenerator.class.getMethod(
                        "generateCode", edu.isi.pegasus.planner.classes.ADag.class),
                notNullValue());
    }

    @Test
    public void testCondorGeneratorImplementsCodeGenerator() {
        assertThat(
                CodeGenerator.class.isAssignableFrom(
                        edu.isi.pegasus.planner.code.generator.condor.CondorGenerator.class),
                is(true));
    }

    @Test
    public void testTypeIsAnInterface() {
        assertThat(CodeGenerator.class.isInterface(), is(true));
    }

    @Test
    public void testInterfaceHasSingleJobGenerateCodeMethod() throws NoSuchMethodException {
        assertThat(
                CodeGenerator.class.getMethod(
                        "generateCode",
                        edu.isi.pegasus.planner.classes.ADag.class,
                        edu.isi.pegasus.planner.classes.Job.class),
                notNullValue());
    }

    @Test
    public void testInterfaceDeclaresStartMonitoringAndResetMethods() throws NoSuchMethodException {
        assertThat(CodeGenerator.class.getMethod("startMonitoring"), notNullValue());
        assertThat(CodeGenerator.class.getMethod("reset"), notNullValue());
    }

    @Test
    public void testMutatingMethodsDeclareCodeGeneratorException() throws NoSuchMethodException {
        assertDeclaresCodeGeneratorException(
                CodeGenerator.class.getMethod(
                        "initialize", edu.isi.pegasus.planner.classes.PegasusBag.class));
        assertDeclaresCodeGeneratorException(
                CodeGenerator.class.getMethod(
                        "generateCode", edu.isi.pegasus.planner.classes.ADag.class));
        assertDeclaresCodeGeneratorException(
                CodeGenerator.class.getMethod(
                        "generateCode",
                        edu.isi.pegasus.planner.classes.ADag.class,
                        edu.isi.pegasus.planner.classes.Job.class));
        assertDeclaresCodeGeneratorException(CodeGenerator.class.getMethod("reset"));
    }

    @Test
    public void testReplaceCondorScratchDirInArgumentsForSharedFsJob() {
        Job job = new Job();
        job.setName("jobA");
        job.setDataConfiguration(PegasusConfiguration.SHARED_FS_CONFIGURATION_VALUE);
        job.setArguments("--dir $_CONDOR_SCRATCH_DIR/run");

        CodeGenerator.replaceCondorScratchDirInArguments(job, new NoOpLogManager(), "/scratch");

        assertThat(job.getArguments(), is("--dir /scratch/run"));
    }

    @Test
    public void testReplaceCondorScratchDirInArgumentsLeavesNonSharedFsJobUntouched() {
        Job job = new Job();
        job.setName("jobB");
        job.setDataConfiguration("nonsharedfs");
        job.setArguments("--dir $_CONDOR_SCRATCH_DIR/run");

        CodeGenerator.replaceCondorScratchDirInArguments(job, new NoOpLogManager(), "/scratch");

        assertThat(job.getArguments(), is("--dir $_CONDOR_SCRATCH_DIR/run"));
    }

    private void assertDeclaresCodeGeneratorException(Method method) {
        assertThat(
                Arrays.asList(method.getExceptionTypes()).contains(CodeGeneratorException.class),
                is(true));
    }
}
