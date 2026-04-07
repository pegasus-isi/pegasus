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
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

import edu.isi.pegasus.planner.classes.ADag;
import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.cluster.JobAggregator;
import edu.isi.pegasus.planner.partitioner.graph.GraphNode;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import org.junit.jupiter.api.Test;

/** Tests for the AWSBatch aggregator class. */
public class AWSBatchTest {

    @Test
    public void testAWSBatchExtendsAbstract() {
        assertThat(Abstract.class.isAssignableFrom(AWSBatch.class), is(true));
    }

    @Test
    public void testAWSBatchImplementsJobAggregator() {
        assertThat(JobAggregator.class.isAssignableFrom(AWSBatch.class), is(true));
    }

    @Test
    public void testCollapseLogicalNameConstant() {
        assertThat(AWSBatch.COLLAPSE_LOGICAL_NAME, is("aws-batch"));
    }

    @Test
    public void testExecutableBasenameConstant() {
        assertThat(AWSBatch.EXECUTABLE_BASENAME, is("pegasus-aws-batch"));
    }

    @Test
    public void testPegasusAWSBatchLaunchBasenameConstant() {
        assertThat(AWSBatch.PEGASUS_AWS_BATCH_LAUNCH_BASENAME, is("pegasus-aws-batch-launch.sh"));
    }

    @Test
    public void testBatchFileTypeKeyConstant() {
        assertThat(AWSBatch.BATCH_FILE_TYPE_KEY, is("BATCH_FILE_TYPE"));
    }

    @Test
    public void testBatchFileS3URLKeyConstant() {
        assertThat(AWSBatch.BATCH_FILE_S3_URL_KEY, is("BATCH_FILE_S3_URL"));
    }

    @Test
    public void testPegasusAWSBatchBucketKeyConstant() {
        assertThat(AWSBatch.PEGASUS_AWS_BATCH_BUCKET_KEY, is("PEGASUS_AWS_BATCH_BUCKET"));
    }

    @Test
    public void testDefaultInstantiation() {
        AWSBatch awsBatch = new AWSBatch();
        assertThat(awsBatch, is(notNullValue()));
    }

    @Test
    public void testGetClusterExecutableLFNMatchesConstant() {
        AWSBatch awsBatch = new AWSBatch();

        assertThat(awsBatch.getClusterExecutableLFN(), is(AWSBatch.COLLAPSE_LOGICAL_NAME));
    }

    @Test
    public void testGetClusterExecutableBasenameMatchesConstant() {
        AWSBatch awsBatch = new AWSBatch();

        assertThat(awsBatch.getClusterExecutableBasename(), is(AWSBatch.EXECUTABLE_BASENAME));
    }

    @Test
    public void testGenerateAWSBatchInputFileWritesJobDefinitionJson() throws Exception {
        AWSBatch awsBatch = new AWSBatch();
        ADag dag = new ADag();
        Job job = new Job();
        job.setName("job-1");
        job.setJobType(Job.COMPUTE_JOB);
        job.setRemoteExecutable("/bin/echo");
        job.setArguments("hello world");
        job.envVariables.construct("HELLO", "WORLD");
        dag.addNode(new GraphNode(job.getID(), job));

        File input = File.createTempFile("awsbatch", ".json");
        input.deleteOnExit();

        File result = awsBatch.generateAWSBatchInputFile(dag, input, true);
        String json = new String(Files.readAllBytes(result.toPath()), StandardCharsets.UTF_8);

        assertThat(result, is(input));
        assertThat(json.contains("\"SubmitJob\""), is(true));
        assertThat(json.contains("\"jobName\" : \"job-1\""), is(true));
        assertThat(json.contains("\"executable\" : \"/bin/echo\""), is(true));
        assertThat(json.contains("\"arguments\" : \"hello world\""), is(true));
        assertThat(json.contains("\"name\" : \"HELLO\""), is(true));
        assertThat(json.contains("\"value\" : \"WORLD\""), is(true));
    }
}
