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
package edu.isi.pegasus.aws.batch.builder;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.List;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import software.amazon.awssdk.services.batch.model.CreateJobQueueRequest;
import software.amazon.awssdk.services.batch.model.JQState;

/** @author Rajiv Mayani */
public class JobQueueTest {

    @TempDir File tempDir;

    private JobQueue jq;

    @BeforeAll
    public static void setUpClass() {}

    @AfterAll
    public static void tearDownClass() {}

    @BeforeEach
    public void setUp() {
        jq = new JobQueue();
    }

    @AfterEach
    public void tearDown() {}

    @Test
    public void testClassLoads() {
        assertNotNull(JobQueue.class);
    }

    @Test
    public void testCreateFromHTTPSpecNullFileReturnsDefault() {
        CreateJobQueueRequest req =
                jq.createJobQueueRequestFromHTTPSpec(
                        null, "arn:aws:batch::111:ce/my-ce", "my-queue");
        assertThat(req.jobQueueName(), is("my-queue"));
        assertThat(req.priority(), is(1));
        assertThat(req.state(), is(JQState.ENABLED.name()));
        assertThat(req.computeEnvironmentOrder(), hasSize(1));
        assertThat(
                req.computeEnvironmentOrder().get(0).computeEnvironment(),
                is("arn:aws:batch::111:ce/my-ce"));
        assertThat(req.computeEnvironmentOrder().get(0).order(), is(1));
    }

    @Test
    public void testCreateFromHTTPSpecNonExistentFileReturnsDefault() {
        File missing = new File(tempDir, "does-not-exist.json");
        CreateJobQueueRequest req =
                jq.createJobQueueRequestFromHTTPSpec(
                        missing, "arn:aws:batch::111:ce/my-ce", "my-queue");
        assertThat(req.jobQueueName(), is("my-queue"));
        assertThat(req.priority(), is(1));
        assertThat(req.state(), is(JQState.ENABLED.name()));
        assertThat(
                req.computeEnvironmentOrder().get(0).computeEnvironment(),
                is("arn:aws:batch::111:ce/my-ce"));
    }

    @Test
    public void testCreateFromHTTPSpecParsesBasicFields() throws IOException {
        String json =
                "{"
                        + "\"jobQueueName\": \"parsed-queue\","
                        + "\"priority\": 10,"
                        + "\"state\": \"DISABLED\","
                        + "\"computeEnvironmentOrder\": ["
                        + "  {\"computeEnvironment\": \"arn:aws:batch::111:ce/my-ce\", \"order\": 2}"
                        + "]"
                        + "}";
        File f = writeToTempFile("jq-http.json", json);
        CreateJobQueueRequest req =
                jq.createJobQueueRequestFromHTTPSpec(
                        f, "arn:aws:batch::111:ce/fallback-ce", "fallback");
        assertThat(req.jobQueueName(), is("parsed-queue"));
        assertThat(req.priority(), is(10));
        assertThat(req.state(), is(JQState.DISABLED.name()));
        assertThat(req.computeEnvironmentOrder(), hasSize(1));
        assertThat(
                req.computeEnvironmentOrder().get(0).computeEnvironment(),
                is("arn:aws:batch::111:ce/my-ce"));
        assertThat(req.computeEnvironmentOrder().get(0).order(), is(2));
    }

    @Test
    public void testCreateFromHTTPSpecUsesNameWhenAbsent() throws IOException {
        String json = "{\"priority\": 1, \"state\": \"ENABLED\"}";
        File f = writeToTempFile("jq-http-noname.json", json);
        CreateJobQueueRequest req = jq.createJobQueueRequestFromHTTPSpec(f, null, "provided-name");
        assertThat(req.jobQueueName(), is("provided-name"));
    }

    @Test
    public void testCreateFromHTTPSpecDefaultsStateToEnabledWhenAbsent() throws IOException {
        String json = "{\"jobQueueName\": \"my-queue\", \"priority\": 1}";
        File f = writeToTempFile("jq-http-nostate.json", json);
        CreateJobQueueRequest req = jq.createJobQueueRequestFromHTTPSpec(f, null, "fallback");
        assertThat(req.state(), is(JQState.ENABLED.name()));
    }

    @Test
    public void testCreateFromHTTPSpecCeArnFallbackInComputeEnvironmentOrder() throws IOException {
        String json =
                "{"
                        + "\"jobQueueName\": \"my-queue\","
                        + "\"priority\": 1,"
                        + "\"computeEnvironmentOrder\": ["
                        + "  {\"order\": 1}"
                        + "]"
                        + "}";
        File f = writeToTempFile("jq-http-no-ce.json", json);
        CreateJobQueueRequest req =
                jq.createJobQueueRequestFromHTTPSpec(
                        f, "arn:aws:batch::111:ce/fallback-ce", "test");
        assertThat(
                req.computeEnvironmentOrder().get(0).computeEnvironment(),
                is("arn:aws:batch::111:ce/fallback-ce"));
    }

    @Test
    public void testCreateFromHTTPSpecMultipleComputeEnvironmentOrders() throws IOException {
        String json =
                "{"
                        + "\"jobQueueName\": \"my-queue\","
                        + "\"priority\": 1,"
                        + "\"computeEnvironmentOrder\": ["
                        + "  {\"computeEnvironment\": \"arn:ce/ce-1\", \"order\": 1},"
                        + "  {\"computeEnvironment\": \"arn:ce/ce-2\", \"order\": 2}"
                        + "]"
                        + "}";
        File f = writeToTempFile("jq-http-multi-ce.json", json);
        CreateJobQueueRequest req = jq.createJobQueueRequestFromHTTPSpec(f, null, "test");
        assertThat(req.computeEnvironmentOrder(), hasSize(2));
        assertThat(req.computeEnvironmentOrder().get(0).computeEnvironment(), is("arn:ce/ce-1"));
        assertThat(req.computeEnvironmentOrder().get(1).computeEnvironment(), is("arn:ce/ce-2"));
    }

    @Test
    public void testCreateFromBatchSpecReturnsList() throws IOException {
        String json =
                "{"
                        + "\"CreateJobQueue\": ["
                        + "  {\"input\": {\"jobQueueName\": \"jq-1\", \"priority\": 1,"
                        + "    \"computeEnvironmentOrder\": [{\"computeEnvironment\": \"arn:ce/ce-1\", \"order\": 1}]}},"
                        + "  {\"input\": {\"jobQueueName\": \"jq-2\", \"priority\": 2,"
                        + "    \"computeEnvironmentOrder\": [{\"computeEnvironment\": \"arn:ce/ce-2\", \"order\": 1}]}}"
                        + "]"
                        + "}";
        File f = writeToTempFile("jq-batch.json", json);
        List<CreateJobQueueRequest> reqs = jq.createJobQueueRequest(f);
        assertThat(reqs, hasSize(2));
        assertThat(reqs.get(0).jobQueueName(), is("jq-1"));
        assertThat(reqs.get(1).jobQueueName(), is("jq-2"));
    }

    @Test
    public void testCreateFromBatchSpecEmptyWhenNoKey() throws IOException {
        String json = "{\"Other\": []}";
        File f = writeToTempFile("jq-batch-empty.json", json);
        List<CreateJobQueueRequest> reqs = jq.createJobQueueRequest(f);
        assertThat(reqs, is(empty()));
    }

    @Test
    public void testCreateFromBatchSpecThrowsForNonArray() throws IOException {
        String json = "{\"CreateJobQueue\": {}}";
        File f = writeToTempFile("jq-batch-nonarray.json", json);
        assertThrows(RuntimeException.class, () -> jq.createJobQueueRequest(f));
    }

    @Test
    public void testCreateFromBatchSpecFileNotFound() {
        File missing = new File(tempDir, "does-not-exist.json");
        assertThrows(RuntimeException.class, () -> jq.createJobQueueRequest(missing));
    }

    private File writeToTempFile(String filename, String content) throws IOException {
        File f = new File(tempDir, filename);
        Files.write(f.toPath(), content.getBytes(StandardCharsets.UTF_8));
        return f;
    }
}
