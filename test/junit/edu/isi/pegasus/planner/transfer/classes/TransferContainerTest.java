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
package edu.isi.pegasus.planner.transfer.classes;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

import edu.isi.pegasus.planner.classes.FileTransfer;
import edu.isi.pegasus.planner.classes.Job;
import java.util.Arrays;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

/** Tests for {@link TransferContainer}. */
public class TransferContainerTest {

    private TransferContainer container;

    @BeforeEach
    public void setUp() {
        container = new TransferContainer();
    }

    @Test
    public void testDefaultTXNameIsNull() {
        assertThat(container.getTXName(), nullValue());
    }

    @Test
    public void testDefaultRegNameIsNull() {
        assertThat(container.getRegName(), nullValue());
    }

    @Test
    public void testSetAndGetTXName() {
        container.setTXName("stage_in_local_0");
        assertThat(container.getTXName(), equalTo("stage_in_local_0"));
    }

    @Test
    public void testSetAndGetRegName() {
        container.setRegName("register_local_0");
        assertThat(container.getRegName(), equalTo("register_local_0"));
    }

    @Test
    public void testInitialFileTransfersEmpty() {
        assertThat(container.getFileTransfers().isEmpty(), is(true));
    }

    @Test
    public void testAddSingleTransfer() {
        FileTransfer ft = new FileTransfer();
        container.addTransfer(ft);
        assertThat(container.getFileTransfers().size(), equalTo(1));
    }

    @Test
    public void testInitialRegistrationFilesEmpty() {
        assertThat(container.getRegistrationFiles().isEmpty(), is(true));
    }

    @Test
    public void testAddRegistrationFile() {
        FileTransfer ft = new FileTransfer();
        container.addRegistrationFiles(ft);
        assertThat(container.getRegistrationFiles().size(), equalTo(1));
    }

    @Test
    public void testAddComputeJob() {
        Job job = new Job();
        job.setJobType(Job.COMPUTE_JOB);
        container.addComputeJob(job);
        assertThat(container.getAssociatedComputeJobs().size(), equalTo(1));
    }

    @Test
    public void testSetTransferType() {
        container.setTransferType(Job.STAGE_OUT_JOB);
        // No getter exposed, but ensure no exception is thrown
    }

    @Test
    public void testAddTransferCollectionAddsAllEntries() {
        FileTransfer first = new FileTransfer();
        FileTransfer second = new FileTransfer();

        container.addTransfer(Arrays.asList(first, second));

        assertThat(container.getFileTransfers().size(), equalTo(2));
        assertThat(container.getFileTransfers().contains(first), is(true));
        assertThat(container.getFileTransfers().contains(second), is(true));
    }

    @Test
    public void testAddRegistrationFilesCollectionAddsAllEntries() {
        FileTransfer first = new FileTransfer();
        FileTransfer second = new FileTransfer();

        container.addRegistrationFiles(Arrays.asList(first, second));

        assertThat(container.getRegistrationFiles().size(), equalTo(2));
        assertThat(container.getRegistrationFiles().contains(first), is(true));
        assertThat(container.getRegistrationFiles().contains(second), is(true));
    }

    @Test
    public void testAddComputeJobFiltersDuplicateJobs() {
        Job job = new Job();
        job.setJobType(Job.COMPUTE_JOB);

        container.addComputeJob(job);
        container.addComputeJob(job);

        assertThat(container.getAssociatedComputeJobs().size(), equalTo(1));
    }

    @Test
    public void testDefaultAndUpdatedTransferTypeFieldValue() throws Exception {
        assertThat(
                (Integer) ReflectionTestUtils.getField(container, "mTransferType"),
                equalTo(Job.STAGE_IN_JOB));

        container.setTransferType(Job.STAGE_OUT_JOB);

        assertThat(
                (Integer) ReflectionTestUtils.getField(container, "mTransferType"),
                equalTo(Job.STAGE_OUT_JOB));
    }
}
