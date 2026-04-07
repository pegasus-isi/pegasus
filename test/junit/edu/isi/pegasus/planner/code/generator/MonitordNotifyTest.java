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
import edu.isi.pegasus.planner.classes.DAGJob;
import edu.isi.pegasus.planner.classes.DAXJob;
import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.classes.PlannerOptions;
import edu.isi.pegasus.planner.code.CodeGenerator;
import edu.isi.pegasus.planner.code.CodeGeneratorException;
import edu.isi.pegasus.planner.common.PegasusProperties;
import edu.isi.pegasus.planner.dax.Invoke;
import java.io.File;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Collection;
import java.util.Properties;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

/** Tests for the MonitordNotify code generator class. */
public class MonitordNotifyTest {

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
    public void testNotificationsFileSuffixConstant() {
        assertThat(MonitordNotify.NOTIFICATIONS_FILE_SUFFIX, is(".notify"));
    }

    @Test
    public void testWorkflowConstant() {
        assertThat(MonitordNotify.WORKFLOW, is("WORKFLOW"));
    }

    @Test
    public void testJobConstant() {
        assertThat(MonitordNotify.JOB, is("JOB"));
    }

    @Test
    public void testInvocationConstant() {
        assertThat(MonitordNotify.INVOCATION, is("INVOCATION"));
    }

    @Test
    public void testDagJobConstant() {
        assertThat(MonitordNotify.DAG_JOB, is("DAGJOB"));
    }

    @Test
    public void testDaxJobConstant() {
        assertThat(MonitordNotify.DAX_JOB, is("DAXJOB"));
    }

    @Test
    public void testDelimiterConstant() {
        assertThat(MonitordNotify.DELIMITER, is(" "));
    }

    @Test
    public void testImplementsCodeGenerator() {
        assertThat(CodeGenerator.class.isAssignableFrom(MonitordNotify.class), is(true));
    }

    @Test
    public void testGenerateCodeUsesBasenamePrefixForNotificationsFile() throws Exception {
        File submitDir = Files.createTempDirectory("monitord-notify-prefix").toFile();
        MonitordNotify generator = initializedGenerator(submitDir, "prefix");
        ADag dag = new ADag();
        dag.setLabel("workflow");
        dag.setIndex("0001");

        Collection<File> result = generator.generateCode(dag);

        assertThat(result.size(), is(1));
        assertThat(
                result.iterator().next().getAbsolutePath(),
                is(new File(submitDir, "prefix.notify").getAbsolutePath()));
    }

    @Test
    public void testGenerateCodeWritesWorkflowJobAndInvocationNotifications() throws Exception {
        File submitDir = Files.createTempDirectory("monitord-notify-output").toFile();
        MonitordNotify generator = initializedGenerator(submitDir, null);
        ADag dag = new ADag();
        dag.setLabel("workflow");
        dag.setIndex("0002");
        dag.setWorkflowUUID("wf-uuid");
        dag.addNotification(new Invoke(Invoke.WHEN.start, "/bin/date"));

        Job compute = new Job();
        compute.setName("computeJob");
        compute.setLogicalID("compute-id");
        compute.setJobType(Job.COMPUTE_JOB);
        compute.addNotification(new Invoke(Invoke.WHEN.success, "/bin/true"));
        dag.add(compute);

        DAGJob dagJob = new DAGJob();
        dagJob.setName("dagJob");
        dagJob.setLogicalID("dag-id");
        dagJob.addNotification(new Invoke(Invoke.WHEN.error, "/bin/false"));

        DAXJob daxJob = new DAXJob();
        daxJob.setName("daxJob");
        daxJob.setLogicalID("dax-id");
        daxJob.addNotification(new Invoke(Invoke.WHEN.end, "/bin/echo done"));

        AggregatedJob aggregated = new AggregatedJob();
        aggregated.setName("clustered");
        aggregated.setJobType(Job.COMPUTE_JOB);
        aggregated.addNotification(new Invoke(Invoke.WHEN.start, "/bin/cluster-start"));
        Job part1 = new Job();
        part1.setName("part1");
        part1.setLogicalID("part1-id");
        part1.setJobType(Job.COMPUTE_JOB);
        part1.addNotification(new Invoke(Invoke.WHEN.success, "/bin/part1"));
        aggregated.add(part1);
        Job part2 = new Job();
        part2.setName("part2");
        part2.setLogicalID("part2-id");
        part2.setJobType(Job.COMPUTE_JOB);
        aggregated.add(part2);
        dag.add(aggregated);

        Collection<File> result = generator.generateCode(dag);

        File file = result.iterator().next();
        String contents = new String(Files.readAllBytes(file.toPath()), StandardCharsets.UTF_8);
        StringWriter adHocBuffer = new StringWriter();
        injectNotificationsWriter(generator, new PrintWriter(adHocBuffer));
        generator.generateCode(dag, dagJob);
        generator.generateCode(dag, daxJob);
        String adHocContents = adHocBuffer.toString();
        assertThat(contents, containsString("WORKFLOW wf-uuid start /bin/date"));
        assertThat(contents, containsString("JOB computeJob success /bin/true"));
        assertThat(contents, containsString("JOB clustered start /bin/cluster-start"));
        assertThat(contents, containsString("INVOCATION clustered 1 success /bin/part1"));
        assertThat(adHocContents, containsString("DAGJOB dagJob error /bin/false"));
        assertThat(adHocContents, containsString("DAXJOB daxJob end /bin/echo done"));
    }

    @Test
    public void testStartMonitoringThrowsUnsupportedOperationException() {
        MonitordNotify generator = new MonitordNotify();

        UnsupportedOperationException exception =
                assertThrows(UnsupportedOperationException.class, generator::startMonitoring);

        assertThat(exception.getMessage(), containsString("Not supported"));
    }

    @Test
    public void testResetThrowsUnsupportedOperationException() {
        MonitordNotify generator = new MonitordNotify();

        UnsupportedOperationException exception =
                assertThrows(UnsupportedOperationException.class, generator::reset);

        assertThat(exception.getMessage(), containsString("Not supported"));
    }

    @Test
    public void testGenerateCodeForSingleJobWithoutWriterWithNoNotificationsDoesNotThrow() {
        MonitordNotify generator = new MonitordNotify();

        assertDoesNotThrow(() -> generator.generateCode(new ADag(), new Job()));
    }

    private MonitordNotify initializedGenerator(File submitDir, String basenamePrefix)
            throws CodeGeneratorException {
        PegasusBag bag = new PegasusBag();
        PlannerOptions options = new PlannerOptions();
        options.setSubmitDirectory(submitDir);
        options.setBasenamePrefix(basenamePrefix);
        bag.add(PegasusBag.PLANNER_OPTIONS, options);
        bag.add(PegasusBag.PEGASUS_PROPERTIES, PegasusProperties.nonSingletonInstance());
        bag.add(PegasusBag.PEGASUS_LOGMANAGER, new NoOpLogManager());

        MonitordNotify generator = new MonitordNotify();
        generator.initialize(bag);
        return generator;
    }

    private void injectNotificationsWriter(MonitordNotify generator, PrintStream stream)
            throws Exception {
        injectNotificationsWriter(generator, new PrintWriter(stream));
    }

    private void injectNotificationsWriter(MonitordNotify generator, PrintWriter writer)
            throws Exception {
        ReflectionTestUtils.setField(generator, "mNotificationsWriter", writer);
    }
}
