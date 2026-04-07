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
package edu.isi.pegasus.planner.code.generator.condor;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.*;

import edu.isi.pegasus.common.logging.LogFormatter;
import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.code.CodeGenerator;
import edu.isi.pegasus.planner.code.CodeGeneratorException;
import edu.isi.pegasus.planner.common.PegasusProperties;
import edu.isi.pegasus.planner.namespace.Condor;
import java.io.PrintStream;
import java.util.Properties;
import org.junit.jupiter.api.Test;

/** Tests for CondorGenerator class. */
public class CondorGeneratorTest {

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

    private static final class TestCondorGenerator extends CondorGenerator {
        void setLogger(LogManager logger) {
            mLogger = logger;
        }

        boolean initializeGridStartFlag() {
            return mInitializeGridStart;
        }

        String basename(String prefix, String suffix) {
            return getBasename(prefix, suffix);
        }

        String categoryKnobs(PegasusProperties properties) {
            return getCategoryDAGManKnobs(properties);
        }

        int priority(Job job, int depth) {
            return getJobPriority(job, depth);
        }

        String concurrencyLimit(Job job) throws CodeGeneratorException {
            return getConcurrencyLimit(job);
        }
    }

    @Test
    public void testCondorGeneratorImplementsCodeGenerator() {
        assertThat(CodeGenerator.class.isAssignableFrom(CondorGenerator.class), is(true));
    }

    @Test
    public void testCondorGeneratorClassExists() {
        assertThat(CondorGenerator.class, notNullValue());
    }

    @Test
    public void testCondorGeneratorInstantiation() {
        CondorGenerator gen = new CondorGenerator();
        assertThat(gen, notNullValue());
    }

    @Test
    public void testConstructorInitializesGridStartFlag() {
        TestCondorGenerator gen = new TestCondorGenerator();

        assertThat(gen.initializeGridStartFlag(), is(true));
    }

    @Test
    public void testGetBasenameConcatenatesPrefixAndSuffix() {
        TestCondorGenerator gen = new TestCondorGenerator();

        assertThat(gen.basename("workflow", ".dag"), is("workflow.dag"));
    }

    @Test
    public void testStartMonitoringReturnsTrue() throws Exception {
        assertThat(new CondorGenerator().startMonitoring(), is(true));
    }

    @Test
    public void testResetRestoresGridStartInitializationFlag() throws Exception {
        TestCondorGenerator gen = new TestCondorGenerator();
        gen.mInitializeGridStart = false;
        gen.mDone = true;

        gen.reset();

        assertThat(gen.initializeGridStartFlag(), is(true));
        assertThat(gen.mDone, is(false));
    }

    @Test
    public void testPopulatePeriodicReleaseAndRemoveInJobSetsDefaultsAndNormalizesIntegers() {
        TestCondorGenerator gen = new TestCondorGenerator();
        gen.setLogger(new NoOpLogManager());
        Job job = new Job();
        job.setName("jobA");
        job.condorVariables.construct(Condor.PERIODIC_RELEASE_KEY, "5");
        job.condorVariables.construct(Condor.PERIODIC_REMOVE_KEY, "10");

        gen.populatePeriodicReleaseAndRemoveInJob(job);

        assertThat(
                job.condorVariables.get(Condor.PERIODIC_RELEASE_KEY),
                is(CondorGenerator.DEFAULT_PERIODIC_RELEASE_VALUE));
        assertThat(
                job.condorVariables.get(Condor.PERIODIC_REMOVE_KEY),
                is(CondorGenerator.DEFAULT_PERIODIC_REMOVE_VALUE));
    }

    @Test
    public void testGetJobPriorityUsesDefaultAndCurrentReplicaRegBehavior() {
        TestCondorGenerator gen = new TestCondorGenerator();
        Job createDir = new Job();
        createDir.setJobType(Job.CREATE_DIR_JOB);
        Job replicaReg = new Job();
        replicaReg.setJobType(Job.REPLICA_REG_JOB);

        assertThat(gen.priority(createDir, 3), is(CondorGenerator.DEFAULT_CREATE_DIR_PRIORITY_KEY));
        assertThat(gen.priority(replicaReg, 4), is(40));
    }

    @Test
    public void testGetConcurrencyLimitReturnsExpectedGroupAndRejectsUnknownType()
            throws Exception {
        TestCondorGenerator gen = new TestCondorGenerator();
        Job compute = new Job();
        compute.setName("compute");
        compute.setJobType(Job.COMPUTE_JOB);
        Job unknown = new Job();
        unknown.setName("unknown");
        unknown.setJobType(Job.UNASSIGNED_JOB);

        assertThat(gen.concurrencyLimit(compute), is("pegasus_compute"));

        CodeGeneratorException exception =
                assertThrows(CodeGeneratorException.class, () -> gen.concurrencyLimit(unknown));
        assertThat(
                exception.getMessage(),
                containsString("Unable to determine Condor concurrency limit"));
    }

    @Test
    public void testGetCategoryDAGManKnobsIncludesConfiguredAndDefaultValues() {
        TestCondorGenerator gen = new TestCondorGenerator();
        PegasusProperties properties = PegasusProperties.nonSingletonInstance();
        properties.setProperty("dagman.bigjob.maxjobs", "25");

        String knobs = gen.categoryKnobs(properties);

        assertThat(knobs, containsString("MAXJOBS bigjob 25"));
        assertThat(knobs, containsString("MAXJOBS stagein 10"));
        assertThat(knobs, containsString("MAXJOBS stageout 10"));
    }
}
