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

import static org.junit.jupiter.api.Assertions.*;

import edu.isi.pegasus.planner.classes.FileTransfer;
import edu.isi.pegasus.planner.classes.Job;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Tests for {@link TransferContainer}. */
public class TransferContainerTest {

    private TransferContainer container;

    @BeforeAll
    public static void setUpClass() {}

    @AfterAll
    public static void tearDownClass() {}

    @BeforeEach
    public void setUp() {
        container = new TransferContainer();
    }

    @AfterEach
    public void tearDown() {}

    @Test
    public void testDefaultTXNameIsNull() {
        assertNull(container.getTXName());
    }

    @Test
    public void testDefaultRegNameIsNull() {
        assertNull(container.getRegName());
    }

    @Test
    public void testSetAndGetTXName() {
        container.setTXName("stage_in_local_0");
        assertEquals("stage_in_local_0", container.getTXName());
    }

    @Test
    public void testSetAndGetRegName() {
        container.setRegName("register_local_0");
        assertEquals("register_local_0", container.getRegName());
    }

    @Test
    public void testInitialFileTransfersEmpty() {
        assertTrue(container.getFileTransfers().isEmpty());
    }

    @Test
    public void testAddSingleTransfer() {
        FileTransfer ft = new FileTransfer();
        container.addTransfer(ft);
        assertEquals(1, container.getFileTransfers().size());
    }

    @Test
    public void testInitialRegistrationFilesEmpty() {
        assertTrue(container.getRegistrationFiles().isEmpty());
    }

    @Test
    public void testAddRegistrationFile() {
        FileTransfer ft = new FileTransfer();
        container.addRegistrationFiles(ft);
        assertEquals(1, container.getRegistrationFiles().size());
    }

    @Test
    public void testAddComputeJob() {
        Job job = new Job();
        job.setJobType(Job.COMPUTE_JOB);
        container.addComputeJob(job);
        assertEquals(1, container.getAssociatedComputeJobs().size());
    }

    @Test
    public void testSetTransferType() {
        container.setTransferType(Job.STAGE_OUT_JOB);
        // No getter exposed, but ensure no exception is thrown
    }
}
