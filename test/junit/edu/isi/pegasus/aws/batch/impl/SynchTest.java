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
package edu.isi.pegasus.aws.batch.impl;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.batch.model.JobStatus;
import software.amazon.awssdk.services.batch.model.ListJobsRequest;

/** @author Rajiv Mayani */
public class SynchTest {

    private Synch synch;

    @BeforeAll
    public static void setUpClass() {}

    @AfterAll
    public static void tearDownClass() {}

    @BeforeEach
    public void setUp() {
        synch = new Synch();
    }

    @AfterEach
    public void tearDown() {}

    @Test
    public void testClassExists() {
        assertNotNull(Synch.class);
    }

    @Test
    public void testConstructorCreatesInstance() {
        assertNotNull(synch);
    }

    // --- constants: prefixes and identifiers ---

    @Test
    public void testArnAndS3PrefixConstants() {
        assertThat(Synch.ARN_PREFIX, is("arn:aws"));
        assertThat(Synch.S3_PREFIX, is("s3://"));
    }

    @Test
    public void testPropertyPrefixConstants() {
        assertThat(Synch.AWS_PROPERTY_PREFIX, is("aws"));
        assertThat(Synch.AWS_BATCH_PROPERTY_PREFIX, is("aws.batch"));
    }

    @Test
    public void testRegionConstant() {
        assertThat(Synch.US_EAST_1_REGION, is("us-east-1"));
    }

    // --- constants: naming suffixes ---

    @Test
    public void testNamingSuffixConstants() {
        assertThat(Synch.JOB_DEFINITION_SUFFIX, is("-job-definition"));
        assertThat(Synch.JOB_QUEUE_SUFFIX, is("-job-queue"));
        assertThat(Synch.COMPUTE_ENV_SUFFIX, is("-compute-env"));
        assertThat(Synch.S3_BUCKET_SUFFIX, is("-bucket"));
    }

    // --- constants: exit codes ---

    @Test
    public void testExitCodeConstants() {
        assertThat(Synch.TASK_FAILURE_EXITCODE, is(1));
        assertThat(Synch.NON_TASK_FAILURE_EXITCODE, is(2));
    }

    // --- constants: limits and timing ---

    @Test
    public void testAwsBatchMaxJobsConstant() {
        assertThat(Synch.AWS_BATCH_MAX_JOBS_SUPPORTED_IN_API, is(100));
    }

    @Test
    public void testMaxSleepTimeConstant() {
        assertThat(Synch.MAX_SLEEP_TIME, is(32_000L));
    }

    // --- constants: environment variables and sentinels ---

    @Test
    public void testEnvironmentVariableKeyConstants() {
        assertThat(Synch.TRANSFER_INPUT_FILES_KEY, is("TRANSFER_INPUT_FILES"));
        assertThat(Synch.PEGASUS_AWS_BATCH_ENV_KEY, is("PEGASUS_AWS_BATCH_BUCKET"));
        assertThat(Synch.PEGASUS_JOB_NAME_ENV_KEY, is("PEGASUS_JOB_NAME"));
    }

    @Test
    public void testNullValueConstant() {
        assertThat(Synch.NULL_VALUE, is("NULL"));
    }

    @Test
    public void testCloudWatchLogGroupConstant() {
        assertThat(Synch.CLOUD_WATCH_BATCH_LOG_GROUP, is("/aws/batch/job"));
    }

    // --- BATCH_ENTITY_TYPE enum ---

    @Test
    public void testBatchEntityTypeEnumValues() {
        Synch.BATCH_ENTITY_TYPE[] values = Synch.BATCH_ENTITY_TYPE.values();
        assertThat(
                values,
                arrayContainingInAnyOrder(
                        Synch.BATCH_ENTITY_TYPE.compute_environment,
                        Synch.BATCH_ENTITY_TYPE.job_definition,
                        Synch.BATCH_ENTITY_TYPE.job_queue,
                        Synch.BATCH_ENTITY_TYPE.s3_bucket));
    }

    // --- createListJobRequest ---

    @Test
    public void testCreateListJobRequestFields() {
        ListJobsRequest req = synch.createListJobRequest("my-queue-arn", JobStatus.RUNNING);
        assertThat(req.jobQueue(), is("my-queue-arn"));
        assertThat(req.jobStatus(), is(JobStatus.RUNNING.name()));
    }

    @Test
    public void testCreateListJobRequestWithDifferentStatuses() {
        for (JobStatus status :
                new JobStatus[] {
                    JobStatus.SUBMITTED,
                    JobStatus.PENDING,
                    JobStatus.RUNNABLE,
                    JobStatus.STARTING,
                    JobStatus.RUNNING,
                    JobStatus.SUCCEEDED,
                    JobStatus.FAILED
                }) {
            ListJobsRequest req = synch.createListJobRequest("arn:queue", status);
            assertThat(req.jobStatus(), is(status.name()));
        }
    }
}
