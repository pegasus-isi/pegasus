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
import java.io.PrintStream;
import java.io.StringWriter;
import java.lang.reflect.Method;
import java.util.Properties;
import org.junit.jupiter.api.Test;

/** Tests for the NetloggerJobMapper class. */
public class NetloggerJobMapperTest {

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
    public void testNetloggerLogFormatterImplementorConstant() {
        assertThat(NetloggerJobMapper.NETLOGGER_LOG_FORMATTER_IMPLEMENTOR, is("Netlogger"));
    }

    @Test
    public void testNetloggerJobMapperClassExists() {
        assertThat(NetloggerJobMapper.class, notNullValue());
    }

    @Test
    public void testConstantIsNotNull() {
        assertThat(NetloggerJobMapper.NETLOGGER_LOG_FORMATTER_IMPLEMENTOR, notNullValue());
    }

    @Test
    public void testWriteOutMappingsForComputeAndAggregatedJobs() throws Exception {
        ADag dag = new ADag();

        Job compute = new Job();
        compute.setName("compute");
        compute.setJobType(Job.COMPUTE_JOB);
        compute.setLogicalID("taskA");
        compute.setTransformation("ns", "preprocess", "1.0");
        dag.add(compute);

        AggregatedJob aggregated = new AggregatedJob();
        aggregated.setName("clustered");
        aggregated.setJobType(Job.COMPUTE_JOB);
        aggregated.setTransformation("ns", "cluster", "2.0");
        Job childOne = new Job();
        childOne.setName("child1");
        childOne.setJobType(Job.COMPUTE_JOB);
        childOne.setLogicalID("task1");
        childOne.setTransformation("child", "step1", "1.0");
        aggregated.add(childOne);
        Job childTwo = new Job();
        childTwo.setName("child2");
        childTwo.setJobType(Job.COMPUTE_JOB);
        childTwo.setLogicalID("task2");
        childTwo.setTransformation("child", "step2", "1.1");
        aggregated.add(childTwo);
        dag.add(aggregated);

        StringWriter writer = new StringWriter();
        new NetloggerJobMapper(new NoOpLogManager()).writeOutMappings(writer, dag);
        String output = writer.toString();

        assertThat(output, containsString("event=pegasus.job"));
        assertThat(output, containsString("job.id=compute"));
        assertThat(output, containsString("job.class=\"1\""));
        assertThat(output, containsString("job.xform=\"ns::preprocess:1.0\""));
        assertThat(output, containsString("task.count=\"1\""));
        assertThat(output, containsString("event=pegasus.job.map"));
        assertThat(output, containsString("task.id=\"taskA\""));
        assertThat(output, containsString("task.class=\"1\""));
        assertThat(output, containsString("task.xform=\"ns::preprocess:1.0\""));
        assertThat(output, containsString("job.id=clustered"));
        assertThat(output, containsString("job.xform=\"ns::cluster:2.0\""));
        assertThat(output, containsString("task.count=\"2\""));
        assertThat(output, containsString("task.id=\"task1\""));
        assertThat(output, containsString("task.xform=\"child::step1:1.0\""));
        assertThat(output, containsString("task.id=\"task2\""));
        assertThat(output, containsString("task.xform=\"child::step2:1.1\""));
    }

    @Test
    public void testWriteOutMappingsSkipsMapEventForComputeJobWithoutLogicalId() throws Exception {
        ADag dag = new ADag();
        Job compute = new Job();
        compute.setName("untar");
        compute.setJobType(Job.COMPUTE_JOB);
        compute.setLogicalID("");
        compute.setTransformation(null, "untar", null);
        dag.add(compute);

        StringWriter writer = new StringWriter();
        new NetloggerJobMapper(new NoOpLogManager()).writeOutMappings(writer, dag);
        String output = writer.toString();

        assertThat(output, containsString("job.id=untar"));
        assertThat(output, containsString("task.count=\"0\""));
        assertThat(output.contains("event=pegasus.job.map"), is(false));
    }

    @Test
    public void testGetTaskCountReturnsZeroForAggregatedCleanupJob() throws Exception {
        AggregatedJob cleanup = new AggregatedJob();
        cleanup.setName("cleanup");
        cleanup.setJobType(Job.CLEANUP_JOB);
        Job child = new Job();
        child.setName("cleanupChild");
        child.setJobType(Job.CLEANUP_JOB);
        child.setLogicalID("cleanup-task");
        cleanup.add(child);

        NetloggerJobMapper mapper = new NetloggerJobMapper(new NoOpLogManager());
        Method method = NetloggerJobMapper.class.getDeclaredMethod("getTaskCount", Job.class);
        method.setAccessible(true);

        assertThat(((Integer) method.invoke(mapper, cleanup)).intValue(), is(0));
    }
}
