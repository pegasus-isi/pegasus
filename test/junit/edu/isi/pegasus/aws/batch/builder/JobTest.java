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

import edu.isi.pegasus.aws.batch.classes.AWSJob;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** @author Rajiv Mayani */
public class JobTest {

    @TempDir File tempDir;

    private Job job;

    @BeforeEach
    public void setUp() {
        job = new Job();
    }

    @Test
    public void testCreateJobParsesAllFields() throws IOException {
        String json =
                "{"
                        + "\"SubmitJob\": [{"
                        + "  \"jobName\": \"my-job\","
                        + "  \"jobDefinition\": \"arn:aws:batch::111:job-definition/my-def\","
                        + "  \"jobQueue\": \"arn:aws:batch::111:job-queue/my-queue\","
                        + "  \"executable\": \"myjob.sh\","
                        + "  \"arguments\": \"arg1 arg2\""
                        + "}]}";
        File f = writeToTempFile("job.json", json);
        List<AWSJob> jobs = job.createJob(f);
        assertThat(jobs, hasSize(1));
        AWSJob j = jobs.get(0);
        assertThat(j.getID(), is("my-job"));
        assertThat(j.getJobDefinitionARN(), is("arn:aws:batch::111:job-definition/my-def"));
        assertThat(j.getJobQueueARN(), is("arn:aws:batch::111:job-queue/my-queue"));
        assertThat(j.getExecutable(), is("myjob.sh"));
        assertThat(j.getArguments(), is("arg1 arg2"));
    }

    @Test
    public void testCreateJobMultipleJobs() throws IOException {
        String json =
                "{"
                        + "\"SubmitJob\": ["
                        + "  {\"jobName\": \"job-1\", \"executable\": \"run1.sh\"},"
                        + "  {\"jobName\": \"job-2\", \"executable\": \"run2.sh\"},"
                        + "  {\"jobName\": \"job-3\", \"executable\": \"run3.sh\"}"
                        + "]}";
        File f = writeToTempFile("jobs-multi.json", json);
        List<AWSJob> jobs = job.createJob(f);
        assertThat(jobs, hasSize(3));
        assertThat(jobs.get(0).getID(), is("job-1"));
        assertThat(jobs.get(1).getID(), is("job-2"));
        assertThat(jobs.get(2).getID(), is("job-3"));
    }

    @Test
    public void testCreateJobParsesEnvironmentVariables() throws IOException {
        String json =
                "{"
                        + "\"SubmitJob\": [{"
                        + "  \"jobName\": \"my-job\","
                        + "  \"environment\": ["
                        + "    {\"name\": \"PEGASUS_HOME\", \"value\": \"/usr\"},"
                        + "    {\"name\": \"JAVA_HOME\",    \"value\": \"/usr/lib/jvm/java\"}"
                        + "  ]"
                        + "}]}";
        File f = writeToTempFile("job-env.json", json);
        List<AWSJob> jobs = job.createJob(f);
        AWSJob j = jobs.get(0);
        assertThat(j.getEnvironmentVariable("PEGASUS_HOME"), is("/usr"));
        assertThat(j.getEnvironmentVariable("JAVA_HOME"), is("/usr/lib/jvm/java"));
    }

    @Test
    public void testCreateJobMissingExecutableAndArguments() throws IOException {
        String json = "{\"SubmitJob\": [{\"jobName\": \"my-job\"}]}";
        File f = writeToTempFile("job-noexec.json", json);
        List<AWSJob> jobs = job.createJob(f);
        AWSJob j = jobs.get(0);
        assertThat(j.getExecutable(), is(nullValue()));
        assertThat(j.getArguments(), is(nullValue()));
    }

    @Test
    public void testCreateJobEmptyWhenNoKey() throws IOException {
        String json = "{\"Other\": []}";
        File f = writeToTempFile("job-nokey.json", json);
        List<AWSJob> jobs = job.createJob(f);
        assertThat(jobs, is(empty()));
    }

    @Test
    public void testCreateJobThrowsForNonArray() throws IOException {
        String json = "{\"SubmitJob\": {}}";
        File f = writeToTempFile("job-nonarray.json", json);
        assertThrows(RuntimeException.class, () -> job.createJob(f));
    }

    @Test
    public void testCreateJobFileNotFound() {
        File missing = new File(tempDir, "does-not-exist.json");
        assertThrows(RuntimeException.class, () -> job.createJob(missing));
    }

    private File writeToTempFile(String filename, String content) throws IOException {
        File f = new File(tempDir, filename);
        Files.write(f.toPath(), content.getBytes(StandardCharsets.UTF_8));
        return f;
    }
}
