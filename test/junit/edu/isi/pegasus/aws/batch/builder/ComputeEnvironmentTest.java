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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import software.amazon.awssdk.services.batch.model.CEState;
import software.amazon.awssdk.services.batch.model.CEType;
import software.amazon.awssdk.services.batch.model.CRType;
import software.amazon.awssdk.services.batch.model.CreateComputeEnvironmentRequest;

/** @author Rajiv Mayani */
public class ComputeEnvironmentTest {

    @TempDir File tempDir;

    private ComputeEnvironment ce;

    @BeforeEach
    public void setUp() {
        ce = new ComputeEnvironment();
    }

    @Test
    public void testGetTestComputeEnvironmentRequest() {
        CreateComputeEnvironmentRequest req =
                ce.getTestComputeEnvironmentRequest("123456789", "test-ce", "-v1");
        assertThat(req.computeEnvironmentName(), is("test-ce-v1"));
        assertThat(req.type(), is(CEType.MANAGED.name()));
        assertThat(req.state(), is(CEState.ENABLED.name()));
        assertThat(
                req.serviceRole(),
                is("arn:aws:iam::123456789:role/" + ComputeEnvironment.AWS_BATCH_SERVICE_ROLE));
        assertThat(req.computeResources().type(), is(CRType.EC2.name()));
        assertThat(
                req.computeResources().instanceRole(),
                is(
                        "arn:aws:iam::123456789:instance-profile/"
                                + ComputeEnvironment.ECS_INSTANCE_ROLE));
        assertThat(req.computeResources().minvCpus(), is(0));
        assertThat(req.computeResources().maxvCpus(), is(2));
        assertThat(req.computeResources().desiredvCpus(), is(0));
    }

    @Test
    public void testCreateComputeEnvironmentRequestFromHTTPSpecBasicFields() throws IOException {
        String json =
                "{"
                        + "\"type\": \"MANAGED\","
                        + "\"computeEnvironmentName\": \"my-ce\","
                        + "\"serviceRole\": \"arn:aws:iam::111:role/AWSBatchServiceRole\","
                        + "\"state\": \"ENABLED\""
                        + "}";
        File f = writeToTempFile("ce-http.json", json);
        CreateComputeEnvironmentRequest req =
                ce.createComputeEnvironmentRequestFromHTTPSpec(f, "fallback-name");
        assertThat(req.computeEnvironmentName(), is("my-ce"));
        assertThat(req.type(), is(CEType.MANAGED.name()));
        assertThat(req.state(), is(CEState.ENABLED.name()));
        assertThat(req.serviceRole(), is("arn:aws:iam::111:role/AWSBatchServiceRole"));
    }

    @Test
    public void testCreateComputeEnvironmentRequestFromHTTPSpecUsesNameWhenAbsent()
            throws IOException {
        String json = "{\"type\": \"MANAGED\", \"state\": \"ENABLED\"}";
        File f = writeToTempFile("ce-http-noname.json", json);
        CreateComputeEnvironmentRequest req =
                ce.createComputeEnvironmentRequestFromHTTPSpec(f, "provided-name");
        assertThat(req.computeEnvironmentName(), is("provided-name"));
    }

    @Test
    public void testCreateComputeEnvironmentRequestFromHTTPSpecComputeResources()
            throws IOException {
        String json =
                "{"
                        + "\"type\": \"MANAGED\","
                        + "\"computeResources\": {"
                        + "  \"type\": \"EC2\","
                        + "  \"minvCpus\": 0,"
                        + "  \"maxvCpus\": 8,"
                        + "  \"desiredvCpus\": 2,"
                        + "  \"instanceTypes\": [\"optimal\"],"
                        + "  \"subnets\": [\"subnet-abc\", \"subnet-def\"],"
                        + "  \"securityGroupIds\": [\"sg-abc\"],"
                        + "  \"instanceRole\": \"arn:aws:iam::111:instance-profile/ecsInstanceRole\""
                        + "}"
                        + "}";
        File f = writeToTempFile("ce-http-cr.json", json);
        CreateComputeEnvironmentRequest req =
                ce.createComputeEnvironmentRequestFromHTTPSpec(f, "test");
        assertThat(req.computeResources().type(), is(CRType.EC2.name()));
        assertThat(req.computeResources().minvCpus(), is(0));
        assertThat(req.computeResources().maxvCpus(), is(8));
        assertThat(req.computeResources().desiredvCpus(), is(2));
        assertThat(req.computeResources().instanceTypes(), contains("optimal"));
        assertThat(
                req.computeResources().subnets(), containsInAnyOrder("subnet-abc", "subnet-def"));
        assertThat(req.computeResources().securityGroupIds(), contains("sg-abc"));
    }

    @Test
    public void testCreateComputeEnvironmentRequestFromHTTPSpecWithTags() throws IOException {
        String json =
                "{"
                        + "\"type\": \"MANAGED\","
                        + "\"computeResources\": {"
                        + "  \"type\": \"EC2\","
                        + "  \"instanceTypes\": [\"optimal\"],"
                        + "  \"subnets\": [\"subnet-abc\"],"
                        + "  \"securityGroupIds\": [\"sg-abc\"],"
                        + "  \"instanceRole\": \"arn:aws:iam::111:instance-profile/ecsInstanceRole\","
                        + "  \"tags\": {\"Project\": \"Pegasus\", \"Env\": \"test\"}"
                        + "}"
                        + "}";
        File f = writeToTempFile("ce-http-tags.json", json);
        CreateComputeEnvironmentRequest req =
                ce.createComputeEnvironmentRequestFromHTTPSpec(f, "test");
        assertThat(req.computeResources().tags(), hasEntry("Project", "Pegasus"));
        assertThat(req.computeResources().tags(), hasEntry("Env", "test"));
    }

    @Test
    public void testCreateComputeEnvironmentRequestFromHTTPSpecFileNotFound() {
        File missing = new File(tempDir, "does-not-exist.json");
        assertThrows(
                RuntimeException.class,
                () -> ce.createComputeEnvironmentRequestFromHTTPSpec(missing, "test"));
    }

    @Test
    public void testCreateComputeEnvironmentRequestBatchSpecReturnsList() throws IOException {
        String json =
                "{"
                        + "\"CreateComputeEnvironment\": ["
                        + "  {\"input\": {\"type\": \"MANAGED\", \"computeEnvironmentName\": \"ce-1\", \"state\": \"ENABLED\"}},"
                        + "  {\"input\": {\"type\": \"MANAGED\", \"computeEnvironmentName\": \"ce-2\", \"state\": \"DISABLED\"}}"
                        + "]"
                        + "}";
        File f = writeToTempFile("ce-batch.json", json);
        List<CreateComputeEnvironmentRequest> reqs = ce.createComputeEnvironmentRequest(f, "test");
        assertThat(reqs, hasSize(2));
    }

    @Test
    public void testCreateComputeEnvironmentRequestBatchSpecNames() throws IOException {
        String json =
                "{"
                        + "\"CreateComputeEnvironment\": ["
                        + "  {\"input\": {\"type\": \"MANAGED\", \"computeEnvironmentName\": \"ce-1\", \"state\": \"ENABLED\"}}"
                        + "]"
                        + "}";
        File f = writeToTempFile("ce-batch-name.json", json);
        List<CreateComputeEnvironmentRequest> reqs = ce.createComputeEnvironmentRequest(f, "test");
        assertThat(reqs.get(0).computeEnvironmentName(), is("ce-1"));
    }

    @Test
    public void testCreateComputeEnvironmentRequestBatchSpecUsesNameWhenAbsent()
            throws IOException {
        String json =
                "{"
                        + "\"CreateComputeEnvironment\": ["
                        + "  {\"input\": {\"type\": \"MANAGED\", \"state\": \"ENABLED\"}}"
                        + "]"
                        + "}";
        File f = writeToTempFile("ce-batch-noname.json", json);
        List<CreateComputeEnvironmentRequest> reqs =
                ce.createComputeEnvironmentRequest(f, "fallback");
        assertThat(reqs.get(0).computeEnvironmentName(), is("fallback"));
    }

    @Test
    public void testCreateComputeEnvironmentRequestBatchSpecEmptyWhenNoKey() throws IOException {
        String json = "{\"Other\": []}";
        File f = writeToTempFile("ce-batch-empty.json", json);
        List<CreateComputeEnvironmentRequest> reqs = ce.createComputeEnvironmentRequest(f, "test");
        assertThat(reqs, is(empty()));
    }

    @Test
    public void testCreateComputeEnvironmentRequestBatchSpecFileNotFound() {
        File missing = new File(tempDir, "does-not-exist.json");
        assertThrows(
                RuntimeException.class, () -> ce.createComputeEnvironmentRequest(missing, "test"));
    }

    @Test
    public void testCreateComputeEnvironmentRequestBatchSpecThrowsForNonArray() throws IOException {
        String json = "{\"CreateComputeEnvironment\": {}}";
        File f = writeToTempFile("ce-batch-nonarray.json", json);
        assertThrows(RuntimeException.class, () -> ce.createComputeEnvironmentRequest(f, "test"));
    }

    @Test
    public void testAWSBatchServiceRoleConstant() {
        assertThat(ComputeEnvironment.AWS_BATCH_SERVICE_ROLE, is("AWSBatchServiceRole"));
    }

    @Test
    public void testECSInstanceRoleConstant() {
        assertThat(ComputeEnvironment.ECS_INSTANCE_ROLE, is("ecsInstanceRole"));
    }

    private File writeToTempFile(String filename, String content) throws IOException {
        File f = new File(tempDir, filename);
        Files.write(f.toPath(), content.getBytes(StandardCharsets.UTF_8));
        return f;
    }
}
