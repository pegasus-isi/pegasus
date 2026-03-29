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
import software.amazon.awssdk.services.batch.model.JobDefinitionType;
import software.amazon.awssdk.services.batch.model.RegisterJobDefinitionRequest;

/** @author Rajiv Mayani */
public class JobDefinitionTest {

    @TempDir File tempDir;

    private JobDefinition jd;

    @BeforeAll
    public static void setUpClass() {}

    @AfterAll
    public static void tearDownClass() {}

    @BeforeEach
    public void setUp() {
        jd = new JobDefinition();
    }

    @AfterEach
    public void tearDown() {}

    @Test
    public void testClassLoads() {
        assertNotNull(JobDefinition.class);
    }

    @Test
    public void testCreateFromHTTPSpecBasicFields() throws IOException {
        String json =
                "{"
                        + "\"jobDefinitionName\": \"my-job-def\","
                        + "\"type\": \"container\","
                        + "\"retryStrategy\": {\"attempts\": 3}"
                        + "}";
        File f = writeToTempFile("jd-http.json", json);
        RegisterJobDefinitionRequest req =
                jd.createRegisterJobDefinitionRequestFromHTTPSpec(f, "fallback");
        assertThat(req.jobDefinitionName(), is("my-job-def"));
        assertThat(req.type(), is(JobDefinitionType.Container.name().toLowerCase()));
        assertThat(req.retryStrategy().attempts(), is(3));
    }

    @Test
    public void testCreateFromHTTPSpecUsesNameWhenAbsent() throws IOException {
        String json = "{\"type\": \"container\"}";
        File f = writeToTempFile("jd-http-noname.json", json);
        RegisterJobDefinitionRequest req =
                jd.createRegisterJobDefinitionRequestFromHTTPSpec(f, "provided-name");
        assertThat(req.jobDefinitionName(), is("provided-name"));
    }

    @Test
    public void testCreateFromHTTPSpecParameters() throws IOException {
        String json =
                "{"
                        + "\"type\": \"container\","
                        + "\"parameters\": {\"input\": \"s3://bucket/input\", \"output\": \"s3://bucket/output\"}"
                        + "}";
        File f = writeToTempFile("jd-http-params.json", json);
        RegisterJobDefinitionRequest req =
                jd.createRegisterJobDefinitionRequestFromHTTPSpec(f, "test");
        assertThat(req.parameters(), hasEntry("input", "s3://bucket/input"));
        assertThat(req.parameters(), hasEntry("output", "s3://bucket/output"));
    }

    @Test
    public void testCreateFromHTTPSpecContainerPropertiesBasic() throws IOException {
        String json =
                "{"
                        + "\"type\": \"container\","
                        + "\"containerProperties\": {"
                        + "  \"image\": \"busybox\","
                        + "  \"memory\": 512,"
                        + "  \"vcpus\": 2,"
                        + "  \"user\": \"nobody\","
                        + "  \"jobRoleArn\": \"arn:aws:iam::111:role/batch-role\","
                        + "  \"command\": [\"sleep\", \"10\"]"
                        + "}"
                        + "}";
        File f = writeToTempFile("jd-http-container.json", json);
        RegisterJobDefinitionRequest req =
                jd.createRegisterJobDefinitionRequestFromHTTPSpec(f, "test");
        assertThat(req.containerProperties().image(), is("busybox"));
        assertThat(req.containerProperties().memory(), is(512));
        assertThat(req.containerProperties().vcpus(), is(2));
        assertThat(req.containerProperties().user(), is("nobody"));
        assertThat(req.containerProperties().jobRoleArn(), is("arn:aws:iam::111:role/batch-role"));
        assertThat(req.containerProperties().command(), contains("sleep", "10"));
    }

    @Test
    public void testCreateFromHTTPSpecContainerEnvironment() throws IOException {
        String json =
                "{"
                        + "\"type\": \"container\","
                        + "\"containerProperties\": {"
                        + "  \"image\": \"busybox\","
                        + "  \"memory\": 128,"
                        + "  \"vcpus\": 1,"
                        + "  \"environment\": ["
                        + "    {\"name\": \"PEGASUS_HOME\", \"value\": \"/usr\"},"
                        + "    {\"name\": \"JAVA_HOME\", \"value\": \"/usr/lib/jvm/java\"}"
                        + "  ]"
                        + "}"
                        + "}";
        File f = writeToTempFile("jd-http-env.json", json);
        RegisterJobDefinitionRequest req =
                jd.createRegisterJobDefinitionRequestFromHTTPSpec(f, "test");
        assertThat(req.containerProperties().environment(), hasSize(2));
        assertTrue(
                req.containerProperties().environment().stream()
                        .anyMatch(
                                kvp ->
                                        "PEGASUS_HOME".equals(kvp.name())
                                                && "/usr".equals(kvp.value())));
        assertTrue(
                req.containerProperties().environment().stream()
                        .anyMatch(
                                kvp ->
                                        "JAVA_HOME".equals(kvp.name())
                                                && "/usr/lib/jvm/java".equals(kvp.value())));
    }

    @Test
    public void testCreateFromHTTPSpecContainerMountPoints() throws IOException {
        String json =
                "{"
                        + "\"type\": \"container\","
                        + "\"containerProperties\": {"
                        + "  \"image\": \"busybox\","
                        + "  \"memory\": 128,"
                        + "  \"vcpus\": 1,"
                        + "  \"mountPoints\": ["
                        + "    {\"containerPath\": \"/data\", \"readOnly\": false, \"sourceVolume\": \"data-vol\"}"
                        + "  ]"
                        + "}"
                        + "}";
        File f = writeToTempFile("jd-http-mounts.json", json);
        RegisterJobDefinitionRequest req =
                jd.createRegisterJobDefinitionRequestFromHTTPSpec(f, "test");
        assertThat(req.containerProperties().mountPoints(), hasSize(1));
        assertThat(req.containerProperties().mountPoints().get(0).containerPath(), is("/data"));
        assertThat(req.containerProperties().mountPoints().get(0).readOnly(), is(false));
        assertThat(req.containerProperties().mountPoints().get(0).sourceVolume(), is("data-vol"));
    }

    @Test
    public void testCreateFromHTTPSpecContainerUlimits() throws IOException {
        String json =
                "{"
                        + "\"type\": \"container\","
                        + "\"containerProperties\": {"
                        + "  \"image\": \"busybox\","
                        + "  \"memory\": 128,"
                        + "  \"vcpus\": 1,"
                        + "  \"ulimits\": ["
                        + "    {\"name\": \"nofile\", \"hardLimit\": 1024, \"softLimit\": 512}"
                        + "  ]"
                        + "}"
                        + "}";
        File f = writeToTempFile("jd-http-ulimits.json", json);
        RegisterJobDefinitionRequest req =
                jd.createRegisterJobDefinitionRequestFromHTTPSpec(f, "test");
        assertThat(req.containerProperties().ulimits(), hasSize(1));
        assertThat(req.containerProperties().ulimits().get(0).name(), is("nofile"));
        assertThat(req.containerProperties().ulimits().get(0).hardLimit(), is(1024));
        assertThat(req.containerProperties().ulimits().get(0).softLimit(), is(512));
    }

    @Test
    public void testCreateFromHTTPSpecContainerVolumes() throws IOException {
        String json =
                "{"
                        + "\"type\": \"container\","
                        + "\"containerProperties\": {"
                        + "  \"image\": \"busybox\","
                        + "  \"memory\": 128,"
                        + "  \"vcpus\": 1,"
                        + "  \"volumes\": ["
                        + "    {\"name\": \"data-vol\", \"host\": {\"sourcePath\": \"/mnt/data\"}}"
                        + "  ]"
                        + "}"
                        + "}";
        File f = writeToTempFile("jd-http-volumes.json", json);
        RegisterJobDefinitionRequest req =
                jd.createRegisterJobDefinitionRequestFromHTTPSpec(f, "test");
        assertThat(req.containerProperties().volumes(), hasSize(1));
        assertThat(req.containerProperties().volumes().get(0).name(), is("data-vol"));
        assertThat(req.containerProperties().volumes().get(0).host().sourcePath(), is("/mnt/data"));
    }

    @Test
    public void testCreateFromHTTPSpecRetryStrategyMissingAttemptsThrows() throws IOException {
        String json = "{\"type\": \"container\", \"retryStrategy\": {}}";
        File f = writeToTempFile("jd-http-retry.json", json);
        assertThrows(
                RuntimeException.class,
                () -> jd.createRegisterJobDefinitionRequestFromHTTPSpec(f, "test"));
    }

    @Test
    public void testCreateFromHTTPSpecFileNotFound() {
        File missing = new File(tempDir, "does-not-exist.json");
        assertThrows(
                RuntimeException.class,
                () -> jd.createRegisterJobDefinitionRequestFromHTTPSpec(missing, "test"));
    }

    @Test
    public void testCreateFromBatchSpecReturnsList() throws IOException {
        String json =
                "{"
                        + "\"RegisterJobDefinition\": ["
                        + "  {\"input\": {\"type\": \"container\", \"jobDefinitionName\": \"jd-1\"}},"
                        + "  {\"input\": {\"type\": \"container\", \"jobDefinitionName\": \"jd-2\"}}"
                        + "]"
                        + "}";
        File f = writeToTempFile("jd-batch.json", json);
        List<RegisterJobDefinitionRequest> reqs = jd.createRegisterJobDefinitionRequest(f, "test");
        assertThat(reqs, hasSize(2));
        assertThat(reqs.get(0).jobDefinitionName(), is("jd-1"));
        assertThat(reqs.get(1).jobDefinitionName(), is("jd-2"));
    }

    @Test
    public void testCreateFromBatchSpecUsesNameWhenAbsent() throws IOException {
        String json =
                "{"
                        + "\"RegisterJobDefinition\": ["
                        + "  {\"input\": {\"type\": \"container\"}}"
                        + "]"
                        + "}";
        File f = writeToTempFile("jd-batch-noname.json", json);
        List<RegisterJobDefinitionRequest> reqs =
                jd.createRegisterJobDefinitionRequest(f, "fallback");
        assertThat(reqs.get(0).jobDefinitionName(), is("fallback"));
    }

    @Test
    public void testCreateFromBatchSpecEmptyWhenNoKey() throws IOException {
        String json = "{\"Other\": []}";
        File f = writeToTempFile("jd-batch-empty.json", json);
        List<RegisterJobDefinitionRequest> reqs = jd.createRegisterJobDefinitionRequest(f, "test");
        assertThat(reqs, is(empty()));
    }

    @Test
    public void testCreateFromBatchSpecThrowsForNonArray() throws IOException {
        String json = "{\"RegisterJobDefinition\": {}}";
        File f = writeToTempFile("jd-batch-nonarray.json", json);
        assertThrows(
                RuntimeException.class, () -> jd.createRegisterJobDefinitionRequest(f, "test"));
    }

    @Test
    public void testCreateFromBatchSpecFileNotFound() {
        File missing = new File(tempDir, "does-not-exist.json");
        assertThrows(
                RuntimeException.class,
                () -> jd.createRegisterJobDefinitionRequest(missing, "test"));
    }

    private File writeToTempFile(String filename, String content) throws IOException {
        File f = new File(tempDir, filename);
        Files.write(f.toPath(), content.getBytes(StandardCharsets.UTF_8));
        return f;
    }
}
