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
import edu.isi.pegasus.planner.classes.AggregatedJob;
import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.classes.PegasusFile;
import edu.isi.pegasus.planner.classes.PlannerOptions;
import edu.isi.pegasus.planner.code.CodeGenerator;
import edu.isi.pegasus.planner.code.CodeGeneratorException;
import edu.isi.pegasus.planner.common.PegasusProperties;
import edu.isi.pegasus.planner.namespace.Dagman;
import java.io.File;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Files;
import java.util.Properties;
import org.junit.jupiter.api.Test;

/** Tests for the Stampede code generator class. */
public class StampedeTest {

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

    private static final class TestStampede extends Stampede {
        String daxTaskEvent(ADag dag, Job job) throws Exception {
            StringWriter buffer = new StringWriter();
            PrintWriter writer = new PrintWriter(buffer);
            generateEventsForDAXTask(writer, dag, job);
            writer.flush();
            return buffer.toString();
        }

        String executableJobEvent(ADag dag, Job job) throws Exception {
            StringWriter buffer = new StringWriter();
            PrintWriter writer = new PrintWriter(buffer);
            generateEventsForExecutableJob(writer, dag, job);
            writer.flush();
            return buffer.toString();
        }

        String taskMapEvents(ADag dag, Job job) {
            StringWriter buffer = new StringWriter();
            PrintWriter writer = new PrintWriter(buffer);
            generateTaskMapEvents(writer, dag, job);
            writer.flush();
            return buffer.toString();
        }

        String workflowMetadataEvents(ADag dag) {
            StringWriter buffer = new StringWriter();
            PrintWriter writer = new PrintWriter(buffer);
            generateMetadataEventsForWF(dag, writer);
            writer.flush();
            return buffer.toString();
        }
    }

    @Test
    public void testImplementsCodeGenerator() {
        assertThat(CodeGenerator.class.isAssignableFrom(Stampede.class), is(true));
    }

    @Test
    public void testNetloggerBPFileSuffixConstant() {
        assertThat(Stampede.NETLOGGER_BP_FILE_SUFFIX, is(".static.bp"));
    }

    @Test
    public void testNetloggerLogFormatterImplementorConstant() {
        assertThat(Stampede.NETLOGGER_LOG_FORMATTER_IMPLEMENTOR, is("Netlogger"));
    }

    @Test
    public void testWorkflowIdKeyConstant() {
        assertThat(Stampede.WORKFLOW_ID_KEY, is("xwf.id"));
    }

    @Test
    public void testTaskEventNameConstant() {
        assertThat(Stampede.TASK_EVENT_NAME, is("task.info"));
    }

    @Test
    public void testTaskIdKeyConstant() {
        assertThat(Stampede.TASK_ID_KEY, is("task.id"));
    }

    @Test
    public void testTypeKeyConstant() {
        assertThat(Stampede.TYPE_KEY, is("type"));
    }

    @Test
    public void testTaskTransformationKeyConstant() {
        assertThat(Stampede.TASK_TRANSFORMATION_KEY, is("transformation"));
    }

    @Test
    public void testBooleanToIntConvertsTrueAndFalse() {
        Stampede stampede = new Stampede();

        assertThat(stampede.booleanToInt(true), is("1"));
        assertThat(stampede.booleanToInt(false), is("0"));
    }

    @Test
    public void testGenerateEventsForDAXTaskIncludesArguments() throws Exception {
        TestStampede stampede = initializedGenerator();
        ADag dag = new ADag();
        dag.setWorkflowUUID("wf-uuid");
        Job job = new Job();
        job.setName("compute");
        job.setLogicalID("taskA");
        job.setJobType(Job.COMPUTE_JOB);
        job.setTransformation("ns", "preprocess", "1.0");
        job.setArguments("--input file.txt");

        String event = stampede.daxTaskEvent(dag, job);

        assertThat(event, containsString("event=task.info"));
        assertThat(event, containsString("xwf.id=wf-uuid"));
        assertThat(event, containsString("task.id=\"taskA\""));
        assertThat(event, containsString("type=\"1\""));
        assertThat(event, containsString("transformation=\"ns::preprocess:1.0\""));
        assertThat(event, containsString("argv=\"--input file.txt\""));
    }

    @Test
    public void testGenerateEventsForDAXTaskRejectsAuxiliaryJobType() throws Exception {
        TestStampede stampede = initializedGenerator();
        ADag dag = new ADag();
        Job job = new Job();
        job.setName("cleanup");
        job.setLogicalID("cleanup-task");
        job.setJobType(Job.CLEANUP_JOB);

        CodeGeneratorException exception =
                assertThrows(CodeGeneratorException.class, () -> stampede.daxTaskEvent(dag, job));

        assertThat(exception.getMessage(), containsString("Invalid Job Type"));
    }

    @Test
    public void testGenerateEventsForExecutableJobIncludesClusterFlagsAndRetry() throws Exception {
        TestStampede stampede = initializedGenerator();
        ADag dag = new ADag();
        dag.setWorkflowUUID("wf-uuid");
        AggregatedJob job = new AggregatedJob();
        job.setName("clustered");
        job.setJobType(Job.COMPUTE_JOB);
        job.setTransformation("ns", "cluster", "2.0");
        job.setRemoteExecutable("/bin/cluster");
        job.dagmanVariables.construct(Dagman.RETRY_KEY, "3");
        Job child = new Job();
        child.setName("child");
        child.setLogicalID("task1");
        child.setJobType(Job.COMPUTE_JOB);
        job.add(child);

        String event = stampede.executableJobEvent(dag, job);

        assertThat(event, containsString("event=job.info"));
        assertThat(event, containsString("job.id=\"clustered\""));
        assertThat(event, containsString("clustered=\"1\""));
        assertThat(event, containsString("max_retries=\"3\""));
        assertThat(event, containsString("task_count=\"1\""));
        assertThat(event, containsString("executable=\"/bin/cluster\""));
    }

    @Test
    public void testGenerateTaskMapEventsForAggregatedJobUsesRootJobId() throws Exception {
        TestStampede stampede = initializedGenerator();
        ADag dag = new ADag();
        dag.setWorkflowUUID("wf-uuid");
        AggregatedJob job = new AggregatedJob();
        job.setName("clustered");
        job.setLogicalID("cluster-task");
        job.setJobType(Job.COMPUTE_JOB);
        job.setTransformation("ns", "cluster", "2.0");
        Job childOne = new Job();
        childOne.setName("child1");
        childOne.setLogicalID("task1");
        childOne.setJobType(Job.COMPUTE_JOB);
        job.add(childOne);
        Job childTwo = new Job();
        childTwo.setName("child2");
        childTwo.setLogicalID("task2");
        childTwo.setJobType(Job.COMPUTE_JOB);
        job.add(childTwo);

        String events = stampede.taskMapEvents(dag, job);

        assertThat(events, containsString("event=wf.map.task_job"));
        assertThat(events, containsString("job.id=\"clustered\""));
        assertThat(events, containsString("task.id=\"task1\""));
        assertThat(events, containsString("task.id=\"task2\""));
    }

    @Test
    public void testGenerateMetadataEventsForWFIncludesWorkflowTaskAndFileMetadata()
            throws Exception {
        TestStampede stampede = initializedGenerator();
        ADag dag = new ADag();
        dag.setWorkflowUUID("wf-uuid");
        dag.addMetadata("wf-key", "wf-value");
        Job job = new Job();
        job.setName("compute");
        job.setLogicalID("taskA");
        job.setJobType(Job.COMPUTE_JOB);
        job.addMetadata("task-key", "task-value");
        PegasusFile file = new PegasusFile("input.dat");
        file.addMetadata("file-key", "file-value");
        job.addInputFile(file);
        dag.add(job);

        String events = stampede.workflowMetadataEvents(dag);

        assertThat(events, containsString("event=static.meta.start"));
        assertThat(events, containsString("event=xwf.meta"));
        assertThat(events, containsString("key=\"wf-key\""));
        assertThat(events, containsString("value=\"wf-value\""));
        assertThat(events, containsString("event=task.meta"));
        assertThat(events, containsString("task.id=\"taskA\""));
        assertThat(events, containsString("key=\"task-key\""));
        assertThat(events, containsString("event=rc.meta"));
        assertThat(events, containsString("lfn.id=\"input.dat\""));
        assertThat(events, containsString("key=\"file-key\""));
        assertThat(events, containsString("event=wf.map.file"));
        assertThat(events, containsString("event=static.meta.end"));
    }

    @Test
    public void testGenerateCodeForSingleJobThrowsWorkflowLevelException() {
        Stampede stampede = new Stampede();
        CodeGeneratorException exception =
                assertThrows(
                        CodeGeneratorException.class,
                        () -> stampede.generateCode(new ADag(), new Job()));

        assertThat(exception.getMessage(), containsString("whole workflow"));
    }

    @Test
    public void testStartMonitoringAndResetAreUnsupported() {
        Stampede stampede = new Stampede();

        assertThrows(UnsupportedOperationException.class, stampede::startMonitoring);
        assertThrows(UnsupportedOperationException.class, stampede::reset);
    }

    private TestStampede initializedGenerator() throws Exception {
        File submitDir = Files.createTempDirectory("stampede-test").toFile();
        PegasusBag bag = new PegasusBag();
        PlannerOptions options = new PlannerOptions();
        options.setSubmitDirectory(submitDir);
        bag.add(PegasusBag.PLANNER_OPTIONS, options);
        bag.add(PegasusBag.PEGASUS_PROPERTIES, PegasusProperties.nonSingletonInstance());
        bag.add(PegasusBag.PEGASUS_LOGMANAGER, new NoOpLogManager());

        TestStampede stampede = new TestStampede();
        stampede.initialize(bag);
        return stampede;
    }
}
