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

import static org.junit.jupiter.api.Assertions.*;

import edu.isi.pegasus.planner.cluster.JobAggregator;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Tests for the AWSBatch aggregator class. */
public class AWSBatchTest {

    @BeforeAll
    public static void setUpClass() {}

    @AfterAll
    public static void tearDownClass() {}

    @BeforeEach
    public void setUp() {}

    @AfterEach
    public void tearDown() {}

    @Test
    public void testAWSBatchExtendsAbstract() {
        assertTrue(Abstract.class.isAssignableFrom(AWSBatch.class));
    }

    @Test
    public void testAWSBatchImplementsJobAggregator() {
        assertTrue(JobAggregator.class.isAssignableFrom(AWSBatch.class));
    }

    @Test
    public void testCollapseLogicalNameConstant() {
        assertEquals("aws-batch", AWSBatch.COLLAPSE_LOGICAL_NAME);
    }

    @Test
    public void testExecutableBasenameConstant() {
        assertEquals("pegasus-aws-batch", AWSBatch.EXECUTABLE_BASENAME);
    }

    @Test
    public void testPegasusAWSBatchLaunchBasenameConstant() {
        assertEquals("pegasus-aws-batch-launch.sh", AWSBatch.PEGASUS_AWS_BATCH_LAUNCH_BASENAME);
    }

    @Test
    public void testBatchFileTypeKeyConstant() {
        assertEquals("BATCH_FILE_TYPE", AWSBatch.BATCH_FILE_TYPE_KEY);
    }

    @Test
    public void testBatchFileS3URLKeyConstant() {
        assertEquals("BATCH_FILE_S3_URL", AWSBatch.BATCH_FILE_S3_URL_KEY);
    }

    @Test
    public void testPegasusAWSBatchBucketKeyConstant() {
        assertEquals("PEGASUS_AWS_BATCH_BUCKET", AWSBatch.PEGASUS_AWS_BATCH_BUCKET_KEY);
    }

    @Test
    public void testDefaultInstantiation() {
        AWSBatch awsBatch = new AWSBatch();
        assertNotNull(awsBatch);
    }
}
