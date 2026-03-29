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
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** @author Rajiv Mayani */
public class CloudWatchLogTest {

    private CloudWatchLog log;

    @BeforeAll
    public static void setUpClass() {}

    @AfterAll
    public static void tearDownClass() {}

    @BeforeEach
    public void setUp() {
        log = new CloudWatchLog();
    }

    @AfterEach
    public void tearDown() {}

    @Test
    public void testClassCanBeReferenced() {
        assertNotNull(CloudWatchLog.class);
    }

    @Test
    public void testConstructorCreatesInstance() {
        assertNotNull(log);
    }

    @Test
    public void testTaskStderrSeparatorConstant() {
        assertThat(
                CloudWatchLog.TASK_STDERR_SEPARATOR,
                is("########################[AWS BATCH] TASK STDERR ########################"));
    }

    @Test
    public void testTaskStderrSeparatorContainsAwsBatchLabel() {
        assertThat(CloudWatchLog.TASK_STDERR_SEPARATOR, containsString("[AWS BATCH] TASK STDERR"));
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
}
