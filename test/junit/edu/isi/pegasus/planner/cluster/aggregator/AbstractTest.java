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
package edu.isi.pegasus.planner.cluster.aggregator;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.*;

import edu.isi.pegasus.common.logging.LogFormatter;
import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.planner.classes.AggregatedJob;
import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.cluster.JobAggregator;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Properties;
import org.junit.jupiter.api.Test;

/** Tests for the Abstract aggregator class structure. */
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

    private static final class TestAggregator extends Abstract {
        private boolean mAbortOnFirstJobFailure;

        TestAggregator() {
            this.mLogger = new NoOpLogManager();
            this.mDefaultArguments = "";
        }

        @Override
        public String aggregatedJobArguments(AggregatedJob job) {
            return "";
        }

        @Override
        public void setAbortOnFirstJobFailure(boolean fail) {
            mAbortOnFirstJobFailure = fail;
        }

        @Override
        public boolean abortOnFristJobFailure() {
            return mAbortOnFirstJobFailure;
        }

        @Override
        public boolean entryNotInTC(String site) {
            return false;
        }

        @Override
        public String getClusterExecutableLFN() {
            return "test-cluster";
        }

        @Override
        public String getClusterExecutableBasename() {
            return "test-cluster";
        }

        @Override
        public boolean topologicalOrderingRequired() {
            return false;
        }

        String commentFor(Job job, int taskid) {
            return getCommentString(job, taskid);
        }

        String commentFor(int taskid, String transformationName, String daxId) {
            return getCommentString(taskid, transformationName, daxId);
        }

        void updateDirectory(String directory) {
            setDirectory(directory);
        }

        String directory() {
            return mDirectory;
        }
    }

    private final TestAggregator mAggregator = new TestAggregator();

    @Test
    public void testAbstractImplementsJobAggregator() {
        assertThat(JobAggregator.class.isAssignableFrom(Abstract.class), is(true));
    }

    @Test
    public void testSeqExecExtendsAbstract() {
        assertThat(Abstract.class.isAssignableFrom(SeqExec.class), is(true));
    }

    @Test
    public void testMPIExecExtendsAbstract() {
        assertThat(Abstract.class.isAssignableFrom(MPIExec.class), is(true));
    }

    @Test
    public void testAWSBatchExtendsAbstract() {
        assertThat(Abstract.class.isAssignableFrom(AWSBatch.class), is(true));
    }

    @Test
    public void testDecafExtendsAbstract() {
        assertThat(Abstract.class.isAssignableFrom(Decaf.class), is(true));
    }

    @Test
    public void testClusteredJobPrefixConstant() {
        assertThat(Abstract.CLUSTERED_JOB_PREFIX, is("merge_"));
    }

    @Test
    public void testGetCompleteTransformationNameUsesPegasusNamespace() {
        assertThat(Abstract.getCompleteTranformationName("seqexec"), is("pegasus::seqexec"));
    }

    @Test
    public void testSetDirectoryUsesCurrentDirectoryFallback() {
        mAggregator.updateDirectory(null);

        assertThat(mAggregator.directory(), is("."));
    }

    @Test
    public void testSetDirectoryPreservesExplicitPath() {
        mAggregator.updateDirectory("build/submit");

        assertThat(mAggregator.directory(), is("build/submit"));
    }

    @Test
    public void testGetCommentStringForExplicitValues() {
        assertThat(
                mAggregator.commentFor(3, "pegasus::keg:1.0", "dax-job"),
                is("#@ 3 pegasus::keg:1.0 dax-job "));
    }

    @Test
    public void testGetCommentStringForJobUsesTransformationAndDaxId() {
        Job job = new Job();
        job.namespace = "ns";
        job.logicalName = "task";
        job.version = "1.0";
        job.setJobType(Job.COMPUTE_JOB);
        job.setLogicalID("ID0001");

        assertThat(mAggregator.commentFor(job, 2), is("#@ 2 ns::task:1.0 ID0001 "));
    }

    @Test
    public void testConstructAbstractAggregatedJobReturnsNullForEmptyList() {
        assertThat(
                mAggregator.constructAbstractAggregatedJob(
                        java.util.Collections.emptyList(), "name", "id"),
                nullValue());
    }

    @Test
    public void testConstructAbstractAggregatedJobBuildsMergedJobMetadata() {
        Job first = new Job();
        first.setName("j1");
        first.setLogicalID("ID0001");
        first.setSiteHandle("local");
        first.setStagingSiteHandle("staging");
        first.inputFiles =
                new HashSet(Arrays.asList(new edu.isi.pegasus.planner.classes.PegasusFile("in1")));
        first.outputFiles =
                new HashSet(Arrays.asList(new edu.isi.pegasus.planner.classes.PegasusFile("out1")));

        Job second = new Job();
        second.setName("j2");
        second.setLogicalID("ID0002");
        second.setSiteHandle("local");
        second.setStagingSiteHandle("staging");
        second.inputFiles =
                new HashSet(Arrays.asList(new edu.isi.pegasus.planner.classes.PegasusFile("in2")));
        second.outputFiles =
                new HashSet(Arrays.asList(new edu.isi.pegasus.planner.classes.PegasusFile("out2")));

        AggregatedJob merged =
                mAggregator.constructAbstractAggregatedJob(
                        Arrays.asList(first, second), "task", "cluster-1");

        assertThat(merged, notNullValue());
        assertThat(merged.getName(), is("merge_task_cluster-1"));
        assertThat(merged.getLogicalID(), is("cluster-1"));
        assertThat(merged.getSiteHandle(), is("local"));
        assertThat(merged.getStagingSiteHandle(), is("staging"));
        assertThat(merged.getInputFiles().size(), is(2));
        assertThat(merged.getOutputFiles().size(), is(2));
        assertThat(merged.getJobAggregator(), sameInstance(mAggregator));
        assertThat(merged.namespace, is("pegasus"));
        assertThat(merged.logicalName, is("test-cluster"));
        assertThat(merged.version, nullValue());
    }
}
