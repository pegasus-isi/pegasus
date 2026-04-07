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
package edu.isi.pegasus.aws.batch.common;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.*;

import edu.isi.pegasus.aws.batch.classes.AWSJob;
import edu.isi.pegasus.aws.batch.classes.Tuple;
import java.lang.reflect.Method;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.batch.model.AttemptContainerDetail;
import software.amazon.awssdk.services.batch.model.AttemptDetail;
import software.amazon.awssdk.services.batch.model.JobDetail;

/** @author Rajiv Mayani */
public class CloudWatchLogTest {

    private CloudWatchLog log;

    @BeforeEach
    public void setUp() {
        log = new CloudWatchLog();
    }

    @Test
    public void testTaskStderrSeparatorConstant() {
        assertThat(
                CloudWatchLog.TASK_STDERR_SEPARATOR,
                is("########################[AWS BATCH] TASK STDERR ########################"));
    }

    @Test
    public void testRetrieveByJobNameThrowsWhenNotInitialized() {
        // mLogger and mBatchClient are null until initialze() is called
        assertThrows(Exception.class, () -> log.retrieve("aws-job-id-123", "task-summary"));
    }

    @Test
    public void testRetrieveByAWSJobThrowsWhenNotInitialized() {
        AWSJob job = new AWSJob();
        job.setAWSJobID("aws-job-id-123");
        job.setID("my-job");
        job.setCommand("/bin/sh", "");
        assertThrows(Exception.class, () -> log.retrieve(job));
    }

    @Test
    public void testRetrieveWithStreamThrowsWhenNotInitialized() {
        // mLogger is null until initialze() is called, so mLogger.info() throws
        assertThrows(
                Exception.class,
                () -> log.retrieve("my-job", "/aws/batch/job", "my-job/default/abc123", "summary"));
    }

    @Test
    public void testDeleteThrowsWhenNotInitialized() {
        // mCWL is null until initialze() is called; the NPE from mLogger.error() in the
        // catch block is not swallowed, so delete() throws rather than returning false
        assertThrows(Exception.class, () -> log.delete("/aws/batch/job", "stream-name"));
    }

    @Test
    public void testDefaultDeleteLogstreamAfterRetrievalFlagIsTrue() throws Exception {
        assertThat(ReflectionTestUtils.getField(log, "mDeleteLogstreamAfterRetrieval"), is(true));
    }

    @Test
    public void testDetermineLogBuildsExpectedTupleFromLastAttempt() throws Exception {
        log.initialze(Region.US_EAST_1, org.apache.logging.log4j.Level.INFO, "/aws/batch/job");

        AttemptDetail firstAttempt =
                AttemptDetail.builder()
                        .container(
                                AttemptContainerDetail.builder()
                                        .taskArn("arn:aws:ecs:region:acct:task/first-task")
                                        .build())
                        .build();
        AttemptDetail lastAttempt =
                AttemptDetail.builder()
                        .container(
                                AttemptContainerDetail.builder()
                                        .taskArn("arn:aws:ecs:region:acct:task/final-task")
                                        .build())
                        .build();
        JobDetail jobDetail =
                JobDetail.builder()
                        .jobDefinition("arn:aws:batch:region:acct:job-definition/sample-jd:3")
                        .attempts(firstAttempt, lastAttempt)
                        .build();

        Method determineLog =
                CloudWatchLog.class.getDeclaredMethod("determineLog", JobDetail.class);
        determineLog.setAccessible(true);

        Tuple<String, String> result = (Tuple<String, String>) determineLog.invoke(log, jobDetail);

        assertThat(result.getKey(), is("/aws/batch/job"));
        assertThat(result.getValue(), is("sample-jd/default/final-task"));
    }

    @Test
    public void testDeleteMethodReturnTypeIsBoolean() throws Exception {
        assertThat(
                CloudWatchLog.class.getMethod("delete", String.class, String.class).getReturnType(),
                is(boolean.class));
    }
}
