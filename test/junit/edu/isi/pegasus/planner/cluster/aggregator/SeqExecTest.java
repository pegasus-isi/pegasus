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

import edu.isi.pegasus.planner.classes.ADag;
import edu.isi.pegasus.planner.classes.AggregatedJob;
import edu.isi.pegasus.planner.cluster.JobAggregator;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

/** Tests for the SeqExec aggregator class. */
public class SeqExecTest {

    private static final class TestSeqExec extends SeqExec {
        String logFileFor(AggregatedJob job) {
            return logFile(job);
        }
    }

    @Test
    public void testSeqExecExtendsAbstract() {
        assertThat(Abstract.class.isAssignableFrom(SeqExec.class), is(true));
    }

    @Test
    public void testSeqExecImplementsJobAggregator() {
        assertThat(JobAggregator.class.isAssignableFrom(SeqExec.class), is(true));
    }

    @Test
    public void testCollapseLogicalNameConstant() {
        assertThat(SeqExec.COLLAPSE_LOGICAL_NAME, is("seqexec"));
    }

    @Test
    public void testExecutableBasenameConstant() {
        assertThat(SeqExec.EXECUTABLE_BASENAME, is("pegasus-cluster"));
    }

    @Test
    public void testSeqexecProgressReportSuffixConstant() {
        assertThat(SeqExec.SEQEXEC_PROGRESS_REPORT_SUFFIX, is(".prg"));
    }

    @Test
    public void testDefaultInstantiation() {
        SeqExec seqExec = new SeqExec();
        assertThat(seqExec, notNullValue());
    }

    @Test
    public void testSeqExecIsPublicClass() {
        int modifiers = SeqExec.class.getModifiers();
        assertThat(java.lang.reflect.Modifier.isPublic(modifiers), is(true));
    }

    @Test
    public void testGetClusterExecutableLFNReturnsCollapseLogicalName() {
        SeqExec seqExec = new SeqExec();

        assertThat(seqExec.getClusterExecutableLFN(), is(SeqExec.COLLAPSE_LOGICAL_NAME));
    }

    @Test
    public void testGetClusterExecutableBasenameReturnsExecutableBasename() {
        SeqExec seqExec = new SeqExec();

        assertThat(seqExec.getClusterExecutableBasename(), is(SeqExec.EXECUTABLE_BASENAME));
    }

    @Test
    public void testTopologicalOrderingRequiredReturnsTrue() {
        SeqExec seqExec = new SeqExec();

        assertThat(seqExec.topologicalOrderingRequired(), is(true));
    }

    @Test
    public void testAbortOnFirstJobFailureFlagRoundTrips() {
        SeqExec seqExec = new SeqExec();

        seqExec.setAbortOnFirstJobFailure(true);
        assertThat(seqExec.abortOnFristJobFailure(), is(true));

        seqExec.setAbortOnFirstJobFailure(false);
        assertThat(seqExec.abortOnFristJobFailure(), is(false));
    }

    @Test
    public void testLogFileUsesJobNameWhenNotGlobal() throws Exception {
        TestSeqExec seqExec = new TestSeqExec();
        AggregatedJob job = new AggregatedJob();
        job.setName("merge_a");
        ReflectionTestUtils.setField(seqExec, "mGlobalLog", Boolean.FALSE);

        assertThat(seqExec.logFileFor(job), is("merge_a.prg"));
    }

    @Test
    public void testLogFileUsesWorkflowLabelWhenGlobal() throws Exception {
        TestSeqExec seqExec = new TestSeqExec();
        AggregatedJob job = new AggregatedJob();
        ADag dag = new ADag();
        dag.setLabel("workflow");
        ReflectionTestUtils.setField(seqExec, "mGlobalLog", Boolean.TRUE);
        ReflectionTestUtils.setField(seqExec, "mClusteredADag", dag);

        assertThat(seqExec.logFileFor(job), is("workflow.prg"));
    }

    @Test
    public void testAggregatedJobArgumentsIncludeFailFlagProgressAndExtraArgs() throws Exception {
        SeqExec seqExec = new SeqExec();
        AggregatedJob job = new AggregatedJob();
        job.setName("merge_a");
        job.vdsNS.construct(
                edu.isi.pegasus.planner.namespace.Pegasus.JOB_AGGREGATOR_ARGUMENTS_KEY, "--trace");

        seqExec.setAbortOnFirstJobFailure(true);
        ReflectionTestUtils.setField(seqExec, "mLogProgress", Boolean.TRUE);
        ReflectionTestUtils.setField(seqExec, "mGlobalLog", Boolean.FALSE);

        String arguments = seqExec.aggregatedJobArguments(job);

        assertThat(arguments.contains("-f"), is(true));
        assertThat(arguments.contains("-R merge_a.prg"), is(true));
        assertThat(arguments.contains("--trace"), is(true));
    }
}
