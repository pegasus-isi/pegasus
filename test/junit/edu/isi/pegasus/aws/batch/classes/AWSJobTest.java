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
package edu.isi.pegasus.aws.batch.classes;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.*;

import java.util.Iterator;
import java.util.Map;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.batch.model.SubmitJobRequest;

/** @author Rajiv Mayani */
public class AWSJobTest {

    private AWSJob mJob;

    @BeforeAll
    public static void setUpClass() {}

    @AfterAll
    public static void tearDownClass() {}

    @BeforeEach
    public void setUp() {
        mJob = new AWSJob();
    }

    @AfterEach
    public void tearDown() {}

    @Test
    public void testDefaultStateIsUnsubmitted() {
        assertThat(mJob.getJobState(), is(AWSJob.JOBSTATE.unsubmitted));
    }

    @Test
    public void testSetAndGetID() {
        mJob.setID("job-001");
        assertThat(mJob.getID(), is("job-001"));
    }

    @Test
    public void testSetAndGetState() {
        mJob.setState(AWSJob.JOBSTATE.running);
        assertThat(mJob.getJobState(), is(AWSJob.JOBSTATE.running));
    }

    @Test
    public void testSetCommandAndGetExecutable() {
        mJob.setCommand("/usr/bin/pegasus-kickstart", "-n job1");
        assertThat(mJob.getExecutable(), is("/usr/bin/pegasus-kickstart"));
    }

    @Test
    public void testSetCommandAndGetArguments() {
        mJob.setCommand("/usr/bin/pegasus-kickstart", "-n job1 -o out.txt");
        assertThat(mJob.getArguments(), is("-n job1 -o out.txt"));
    }

    @Test
    public void testSetAndGetAWSJobID() {
        mJob.setAWSJobID("aws-batch-id-abc123");
        assertThat(mJob.getAWSJobID(), is("aws-batch-id-abc123"));
    }

    @Test
    public void testSetAndGetSequenceID() {
        mJob.setSequenceID(42L);
        assertThat(mJob.getSequenceID(), is(42L));
    }

    @Test
    public void testSetAndGetStdout() {
        mJob.setStdout("/scratch/job.out");
        assertThat(mJob.getStdout(), is("/scratch/job.out"));
    }

    @Test
    public void testSetAndGetStderr() {
        mJob.setStderr("/scratch/job.err");
        assertThat(mJob.getStderr(), is("/scratch/job.err"));
    }

    @Test
    public void testSetAndGetJobQueueARN() {
        String arn = "arn:aws:batch:us-east-1:123456789012:job-queue/MyQueue";
        mJob.setJobQueueARN(arn);
        assertThat(mJob.getJobQueueARN(), is(arn));
    }

    @Test
    public void testSetAndGetJobDefinitionARN() {
        String arn = "arn:aws:batch:us-east-1:123456789012:job-definition/MyDef:1";
        mJob.setJobDefinitionARN(arn);
        assertThat(mJob.getJobDefinitionARN(), is(arn));
    }

    @Test
    public void testAddAndGetEnvironmentVariable() {
        mJob.addEnvironmentVariable("MY_VAR", "my_value");
        assertThat(mJob.getEnvironmentVariable("MY_VAR"), is("my_value"));
    }

    @Test
    public void testGetEnvironmentVariableMissingKeyReturnsNull() {
        assertNull(mJob.getEnvironmentVariable("NO_SUCH_VAR"));
    }

    @Test
    public void testAddMultipleEnvironmentVariables() {
        mJob.addEnvironmentVariable("VAR1", "val1");
        mJob.addEnvironmentVariable("VAR2", "val2");
        assertThat(mJob.getEnvironmentVariable("VAR1"), is("val1"));
        assertThat(mJob.getEnvironmentVariable("VAR2"), is("val2"));
    }

    @Test
    public void testGetEnvironmentVariablesIteratorNotNull() {
        mJob.addEnvironmentVariable("K", "V");
        Iterator<Map.Entry<String, String>> it = mJob.getEnvironmentVariablesIterator();
        assertNotNull(it);
        assertTrue(it.hasNext());
    }

    @Test
    public void testGetEnvironmentVariablesIteratorEmpty() {
        Iterator<Map.Entry<String, String>> it = mJob.getEnvironmentVariablesIterator();
        assertNotNull(it);
        assertFalse(it.hasNext());
    }

    @Test
    public void testGetTaskSummaryNotNull() {
        mJob.setID("test-job");
        mJob.setAWSJobID("aws-123");
        mJob.setSequenceID(5L);
        mJob.setCommand("/bin/sh", "-c echo hello");
        String summary = mJob.getTaskSummary();
        assertNotNull(summary);
    }

    @Test
    public void testGetTaskSummaryContainsJobID() {
        mJob.setID("myjob");
        mJob.setAWSJobID("aws-abc");
        mJob.setSequenceID(1L);
        mJob.setCommand("/bin/sh", "");
        String summary = mJob.getTaskSummary();
        assertThat(summary, containsString("myjob"));
    }

    @Test
    public void testGetTaskSummaryContainsState() {
        mJob.setID("statejob");
        mJob.setAWSJobID("aws-xyz");
        mJob.setSequenceID(2L);
        mJob.setCommand("/bin/ls", "");
        mJob.setState(AWSJob.JOBSTATE.succeeded);
        String summary = mJob.getTaskSummary();
        assertThat(summary, containsString("succeeded"));
    }

    @Test
    public void testGetTaskSummaryStatusZeroForSucceeded() {
        mJob.setID("successjob");
        mJob.setAWSJobID("aws-ok");
        mJob.setSequenceID(3L);
        mJob.setCommand("/bin/true", "");
        mJob.setState(AWSJob.JOBSTATE.succeeded);
        String summary = mJob.getTaskSummary();
        assertThat(summary, containsString("status=0"));
    }

    @Test
    public void testGetTaskSummaryStatusOneForFailed() {
        mJob.setID("failedjob");
        mJob.setAWSJobID("aws-fail");
        mJob.setSequenceID(4L);
        mJob.setCommand("/bin/false", "");
        mJob.setState(AWSJob.JOBSTATE.failed);
        String summary = mJob.getTaskSummary();
        assertThat(summary, containsString("status=1"));
    }

    @Test
    public void testAllJobStatesCanBeSet() {
        for (AWSJob.JOBSTATE state : AWSJob.JOBSTATE.values()) {
            mJob.setState(state);
            assertThat(mJob.getJobState(), is(state));
        }
    }

    @Test
    public void testCreateAWSBatchSubmitRequestCoreFields() {
        mJob.setID("my-job");
        mJob.setJobDefinitionARN("arn:aws:batch::111:job-definition/my-def:1");
        mJob.setJobQueueARN("arn:aws:batch::111:job-queue/my-queue");
        mJob.setCommand("myjob.sh", "arg1 arg2");

        SubmitJobRequest req = mJob.createAWSBatchSubmitRequest();

        assertThat(req.jobName(), is("my-job"));
        assertThat(req.jobDefinition(), is("arn:aws:batch::111:job-definition/my-def:1"));
        assertThat(req.jobQueue(), is("arn:aws:batch::111:job-queue/my-queue"));
        assertThat(req.containerOverrides().command(), contains("myjob.sh", "arg1 arg2"));
    }

    @Test
    public void testCreateAWSBatchSubmitRequestEnvironmentVariables() {
        mJob.setID("env-job");
        mJob.setJobDefinitionARN("arn:aws:batch::111:job-definition/my-def:1");
        mJob.setJobQueueARN("arn:aws:batch::111:job-queue/my-queue");
        mJob.setCommand("myjob.sh", "");
        mJob.addEnvironmentVariable("PEGASUS_HOME", "/usr");
        mJob.addEnvironmentVariable("JAVA_HOME", "/usr/lib/jvm/java");

        SubmitJobRequest req = mJob.createAWSBatchSubmitRequest();

        assertThat(req.containerOverrides().environment(), hasSize(2));
        assertTrue(
                req.containerOverrides().environment().stream()
                        .anyMatch(
                                kvp ->
                                        "PEGASUS_HOME".equals(kvp.name())
                                                && "/usr".equals(kvp.value())));
        assertTrue(
                req.containerOverrides().environment().stream()
                        .anyMatch(
                                kvp ->
                                        "JAVA_HOME".equals(kvp.name())
                                                && "/usr/lib/jvm/java".equals(kvp.value())));
    }

    @Test
    public void testCreateAWSBatchSubmitRequestNoEnvironmentVariables() {
        mJob.setID("no-env-job");
        mJob.setJobDefinitionARN("arn:aws:batch::111:job-definition/my-def:1");
        mJob.setJobQueueARN("arn:aws:batch::111:job-queue/my-queue");
        mJob.setCommand("myjob.sh", "");

        SubmitJobRequest req = mJob.createAWSBatchSubmitRequest();

        assertThat(req.containerOverrides().environment(), is(empty()));
    }
}
