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
package edu.isi.pegasus.planner.classes;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

/** @author Rajiv Mayani */
public class WorkflowMetricsTest {

    // -----------------------------------------------------------------------
    // helpers
    // -----------------------------------------------------------------------

    private Job makeJob(int jobType) {
        Job j = new Job();
        j.logicalName = "test-job";
        j.setJobType(jobType);
        return j;
    }

    // -----------------------------------------------------------------------
    // construction / reset
    // -----------------------------------------------------------------------

    @Test
    public void testDefaultConstructorZeroesAllCounts() {
        WorkflowMetrics wm = new WorkflowMetrics();
        assertThat(wm.getJobCount(Job.COMPUTE_JOB), is(0));
        assertThat(wm.getJobCount(Job.STAGE_IN_JOB), is(0));
        assertThat(wm.getJobCount(Job.STAGE_OUT_JOB), is(0));
        assertThat(wm.getJobCount(Job.CLEANUP_JOB), is(0));
        assertThat(wm.getJobCount(Job.CREATE_DIR_JOB), is(0));
        assertThat(wm.getTaskCount(Job.COMPUTE_JOB), is(0));
        assertThat(wm.getTaskCount(Job.DAX_JOB), is(0));
        assertThat(wm.getTaskCount(Job.DAG_JOB), is(0));
        assertThat(wm.getNumDAXFiles(WorkflowMetrics.FILE_TYPE.input), is(0));
        assertThat(wm.getNumDAXFiles(WorkflowMetrics.FILE_TYPE.output), is(0));
        assertThat(wm.getNumDAXFiles(WorkflowMetrics.FILE_TYPE.intermediate), is(0));
        assertThat(wm.getNumDAXFiles(WorkflowMetrics.FILE_TYPE.total), is(0));
    }

    @Test
    public void testSetAndGetLabel() {
        WorkflowMetrics wm = new WorkflowMetrics();
        assertThat(wm.getLabel(), nullValue());
        wm.setLabel("mywf");
        assertThat(wm.getLabel(), is("mywf"));
    }

    @Test
    public void testResetTaskMetricsFalseKeepsTasks() {
        WorkflowMetrics wm = new WorkflowMetrics();
        Job j = makeJob(Job.COMPUTE_JOB);
        wm.increment(j);
        // reset without touching task metrics
        wm.reset(false);
        // job counters reset
        assertThat(wm.getJobCount(Job.COMPUTE_JOB), is(0));
        // task counters intact
        assertThat(wm.getTaskCount(Job.COMPUTE_JOB), is(1));
    }

    @Test
    public void testResetTaskMetricsTrueResetsEverything() {
        WorkflowMetrics wm = new WorkflowMetrics();
        Job j = makeJob(Job.COMPUTE_JOB);
        wm.increment(j);
        wm.reset(true);
        assertThat(wm.getJobCount(Job.COMPUTE_JOB), is(0));
        assertThat(wm.getTaskCount(Job.COMPUTE_JOB), is(0));
    }

    // -----------------------------------------------------------------------
    // increment / decrement – job types
    // -----------------------------------------------------------------------

    @Test
    public void testIncrementComputeJob() {
        WorkflowMetrics wm = new WorkflowMetrics();
        Job j = makeJob(Job.COMPUTE_JOB);
        wm.increment(j);
        assertThat(wm.getJobCount(Job.COMPUTE_JOB), is(1));
        assertThat(wm.getTaskCount(Job.COMPUTE_JOB), is(1));
    }

    @Test
    public void testIncrementMultipleComputeJobs() {
        WorkflowMetrics wm = new WorkflowMetrics();
        for (int i = 0; i < 3; i++) {
            wm.increment(makeJob(Job.COMPUTE_JOB));
        }
        assertThat(wm.getJobCount(Job.COMPUTE_JOB), is(3));
        assertThat(wm.getTaskCount(Job.COMPUTE_JOB), is(3));
    }

    @Test
    public void testIncrementAggregatedComputeJobCountsAsClusteredNotCompute() {
        WorkflowMetrics wm = new WorkflowMetrics();
        AggregatedJob aj = new AggregatedJob();
        aj.logicalName = "clustered-job";
        aj.setJobType(Job.COMPUTE_JOB);
        wm.increment(aj);
        // clustered jobs go to a separate bucket from regular compute jobs
        assertThat(wm.getJobCount(Job.COMPUTE_JOB), is(0));
        // but the task counter still increments
        assertThat(wm.getTaskCount(Job.COMPUTE_JOB), is(1));
    }

    @Test
    public void testIncrementStageInJobWithLockedTaskMetrics() {
        WorkflowMetrics wm = new WorkflowMetrics();
        wm.lockTaskMetrics(true);
        Job j = makeJob(Job.STAGE_IN_JOB);
        wm.increment(j);
        assertThat(wm.getJobCount(Job.STAGE_IN_JOB), is(1));
    }

    @Test
    public void testIncrementStageInWorkerPackageJobCountsAsStageIn() {
        WorkflowMetrics wm = new WorkflowMetrics();
        wm.lockTaskMetrics(true);

        wm.increment(makeJob(Job.STAGE_IN_WORKER_PACKAGE_JOB));

        assertThat(wm.getJobCount(Job.STAGE_IN_JOB), is(1));
    }

    @Test
    public void testIncrementStageOutJobWithLockedTaskMetrics() {
        WorkflowMetrics wm = new WorkflowMetrics();
        wm.lockTaskMetrics(true);
        Job j = makeJob(Job.STAGE_OUT_JOB);
        wm.increment(j);
        assertThat(wm.getJobCount(Job.STAGE_OUT_JOB), is(1));
    }

    @Test
    public void testIncrementInterPoolJobWithLockedTaskMetrics() {
        WorkflowMetrics wm = new WorkflowMetrics();
        wm.lockTaskMetrics(true);
        Job j = makeJob(Job.INTER_POOL_JOB);
        wm.increment(j);
        assertThat(wm.getJobCount(Job.INTER_POOL_JOB), is(1));
    }

    @Test
    public void testIncrementReplicaRegJobWithLockedTaskMetrics() {
        WorkflowMetrics wm = new WorkflowMetrics();
        wm.lockTaskMetrics(true);
        Job j = makeJob(Job.REPLICA_REG_JOB);
        wm.increment(j);
        assertThat(wm.getJobCount(Job.REPLICA_REG_JOB), is(1));
    }

    @Test
    public void testIncrementCleanupJobWithLockedTaskMetrics() {
        WorkflowMetrics wm = new WorkflowMetrics();
        wm.lockTaskMetrics(true);
        Job j = makeJob(Job.CLEANUP_JOB);
        wm.increment(j);
        assertThat(wm.getJobCount(Job.CLEANUP_JOB), is(1));
    }

    @Test
    public void testIncrementCreateDirJobWithLockedTaskMetrics() {
        WorkflowMetrics wm = new WorkflowMetrics();
        wm.lockTaskMetrics(true);
        Job j = makeJob(Job.CREATE_DIR_JOB);
        wm.increment(j);
        assertThat(wm.getJobCount(Job.CREATE_DIR_JOB), is(1));
    }

    @Test
    public void testIncrementChmodJobWithLockedTaskMetrics() {
        WorkflowMetrics wm = new WorkflowMetrics();
        wm.lockTaskMetrics(true);
        Job j = makeJob(Job.CHMOD_JOB);
        wm.increment(j);
        assertThat(wm.getJobCount(Job.CHMOD_JOB), is(1));
    }

    @Test
    public void testIncrementNonComputeJobWithoutLockThrows() {
        WorkflowMetrics wm = new WorkflowMetrics();
        assertThrows(RuntimeException.class, () -> wm.increment(makeJob(Job.STAGE_IN_JOB)));
    }

    @Test
    public void testIncrementDAXJob() {
        WorkflowMetrics wm = new WorkflowMetrics();
        DAXJob dj = new DAXJob();
        dj.logicalName = "dax-job";
        wm.increment(dj);
        assertThat(wm.getJobCount(Job.DAX_JOB), is(1));
        assertThat(wm.getTaskCount(Job.DAX_JOB), is(1));
    }

    @Test
    public void testIncrementDAGJob() {
        WorkflowMetrics wm = new WorkflowMetrics();
        DAGJob dg = new DAGJob();
        dg.logicalName = "dag-job";
        wm.increment(dg);
        assertThat(wm.getJobCount(Job.DAG_JOB), is(1));
        assertThat(wm.getTaskCount(Job.DAG_JOB), is(1));
    }

    @Test
    public void testDecrementComputeJob() {
        WorkflowMetrics wm = new WorkflowMetrics();
        Job j = makeJob(Job.COMPUTE_JOB);
        wm.increment(j);
        wm.increment(j);
        wm.decrement(j);
        assertThat(wm.getJobCount(Job.COMPUTE_JOB), is(1));
        assertThat(wm.getTaskCount(Job.COMPUTE_JOB), is(1));
    }

    @Test
    public void testDecrementDAXJob() {
        WorkflowMetrics wm = new WorkflowMetrics();
        DAXJob job = new DAXJob();
        job.logicalName = "dax-job";
        wm.increment(job);

        wm.decrement(job);

        assertThat(wm.getJobCount(Job.DAX_JOB), is(0));
        assertThat(wm.getTaskCount(Job.DAX_JOB), is(0));
    }

    @Test
    public void testIncrementNullJobIsNoOp() {
        WorkflowMetrics wm = new WorkflowMetrics();
        // should not throw
        wm.increment(null);
        assertThat(wm.getJobCount(Job.COMPUTE_JOB), is(0));
    }

    @Test
    public void testDecrementNullJobIsNoOp() {
        WorkflowMetrics wm = new WorkflowMetrics();
        // should not throw
        wm.decrement(null);
    }

    // -----------------------------------------------------------------------
    // task metrics lock
    // -----------------------------------------------------------------------

    @Test
    public void testLockTaskMetricsPreventsTaskUpdates() {
        WorkflowMetrics wm = new WorkflowMetrics();
        wm.lockTaskMetrics(true);
        wm.increment(makeJob(Job.COMPUTE_JOB));
        // job metrics still updated
        assertThat(wm.getJobCount(Job.COMPUTE_JOB), is(1));
        // task metrics should NOT be updated
        assertThat(wm.getTaskCount(Job.COMPUTE_JOB), is(0));
    }

    @Test
    public void testUnlockTaskMetricsAllowsUpdates() {
        WorkflowMetrics wm = new WorkflowMetrics();
        wm.lockTaskMetrics(true);
        wm.increment(makeJob(Job.COMPUTE_JOB));
        wm.lockTaskMetrics(false);
        wm.increment(makeJob(Job.COMPUTE_JOB));
        assertThat(wm.getTaskCount(Job.COMPUTE_JOB), is(1));
    }

    @Test
    public void testLockedTaskMetricsPreventsTaskDecrement() {
        WorkflowMetrics wm = new WorkflowMetrics();
        Job job = makeJob(Job.COMPUTE_JOB);
        wm.increment(job);
        wm.lockTaskMetrics(true);

        wm.decrement(job);

        assertThat(wm.getJobCount(Job.COMPUTE_JOB), is(0));
        assertThat(wm.getTaskCount(Job.COMPUTE_JOB), is(1));
    }

    // -----------------------------------------------------------------------
    // file counts
    // -----------------------------------------------------------------------

    @Test
    public void testSetAndGetNumDAXFilesInput() {
        WorkflowMetrics wm = new WorkflowMetrics();
        wm.setNumDAXFiles(WorkflowMetrics.FILE_TYPE.input, 7);
        assertThat(wm.getNumDAXFiles(WorkflowMetrics.FILE_TYPE.input), is(7));
    }

    @Test
    public void testSetAndGetNumDAXFilesOutput() {
        WorkflowMetrics wm = new WorkflowMetrics();
        wm.setNumDAXFiles(WorkflowMetrics.FILE_TYPE.output, 4);
        assertThat(wm.getNumDAXFiles(WorkflowMetrics.FILE_TYPE.output), is(4));
    }

    @Test
    public void testSetAndGetNumDAXFilesIntermediate() {
        WorkflowMetrics wm = new WorkflowMetrics();
        wm.setNumDAXFiles(WorkflowMetrics.FILE_TYPE.intermediate, 3);
        assertThat(wm.getNumDAXFiles(WorkflowMetrics.FILE_TYPE.intermediate), is(3));
    }

    @Test
    public void testSetAndGetNumDAXFilesTotal() {
        WorkflowMetrics wm = new WorkflowMetrics();
        wm.setNumDAXFiles(WorkflowMetrics.FILE_TYPE.total, 14);
        assertThat(wm.getNumDAXFiles(WorkflowMetrics.FILE_TYPE.total), is(14));
    }

    @Test
    public void testSetAndGetNumDeletedTasks() {
        WorkflowMetrics wm = new WorkflowMetrics();
        wm.setNumDeletedTasks(5);
        // verify via toString (no direct getter)
        String s = wm.toString();
        assertThat(s, containsString("deleted-tasks.count = 5"));
    }

    @Test
    public void testGetTaskCountWithInvalidJobTypeThrows() {
        WorkflowMetrics wm = new WorkflowMetrics();

        assertThrows(RuntimeException.class, () -> wm.getTaskCount(Job.STAGE_IN_JOB));
    }

    @Test
    public void testGetJobCountWithInvalidJobTypeThrows() {
        WorkflowMetrics wm = new WorkflowMetrics();

        assertThrows(RuntimeException.class, () -> wm.getJobCount(Job.UNASSIGNED_JOB));
    }

    // -----------------------------------------------------------------------
    // toString / toJson
    // -----------------------------------------------------------------------

    @Test
    public void testToStringContainsLabel() {
        WorkflowMetrics wm = new WorkflowMetrics();
        wm.setLabel("my-workflow");
        String s = wm.toString();
        assertThat(s, containsString("my-workflow"));
    }

    @Test
    public void testToStringContainsCounts() {
        WorkflowMetrics wm = new WorkflowMetrics();
        wm.increment(makeJob(Job.COMPUTE_JOB));
        String s = wm.toString();
        assertThat(s, containsString("total-jobs.count = 1"));
    }

    @Test
    public void testToStringUsesComputeJobCountForCreatedirJobsCurrentBehavior() {
        WorkflowMetrics wm = new WorkflowMetrics();
        wm.increment(makeJob(Job.COMPUTE_JOB));

        String s = wm.toString();

        assertThat(s, containsString("createdir-jobs.count = 1"));
    }

    @Test
    public void testToJsonReturnsNonEmptyString() {
        WorkflowMetrics wm = new WorkflowMetrics();
        String json = wm.toJson();
        assertThat(json, notNullValue());
        assertThat(json.trim(), not(emptyString()));
    }

    @Test
    public void testToPrettyJsonContainsComputeTasks() {
        WorkflowMetrics wm = new WorkflowMetrics();
        String json = wm.toPrettyJson();
        assertThat(json, containsString("compute_tasks"));
    }

    // -----------------------------------------------------------------------
    // clone
    // -----------------------------------------------------------------------

    @Test
    public void testCloneProducesIndependentObject() {
        WorkflowMetrics wm = new WorkflowMetrics();
        wm.setLabel("orig");
        wm.increment(makeJob(Job.COMPUTE_JOB));

        WorkflowMetrics clone = (WorkflowMetrics) wm.clone();

        // same initial values
        assertThat(clone.getLabel(), is("orig"));
        assertThat(clone.getJobCount(Job.COMPUTE_JOB), is(1));

        // mutating the clone should not affect original
        clone.setLabel("cloned");
        clone.increment(makeJob(Job.COMPUTE_JOB));
        assertThat(wm.getLabel(), is("orig"));
        assertThat(clone.getJobCount(Job.COMPUTE_JOB), is(2));
        assertThat(wm.getJobCount(Job.COMPUTE_JOB), is(1));
    }

    @Test
    public void testCloneUsesDagJobCountForDaxJobCountCurrentBehavior() {
        WorkflowMetrics wm = new WorkflowMetrics();
        DAXJob daxJob = new DAXJob();
        daxJob.logicalName = "dax";
        DAGJob dagJob = new DAGJob();
        dagJob.logicalName = "dag";
        wm.increment(daxJob);
        wm.increment(dagJob);
        wm.increment(dagJob);

        WorkflowMetrics clone = (WorkflowMetrics) wm.clone();

        assertThat(clone.getJobCount(Job.DAX_JOB), is(2));
        assertThat(clone.getJobCount(Job.DAG_JOB), is(2));
    }
}
