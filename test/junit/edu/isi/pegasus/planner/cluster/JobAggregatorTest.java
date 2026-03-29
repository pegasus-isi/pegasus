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
package edu.isi.pegasus.planner.cluster;

import static org.junit.jupiter.api.Assertions.*;

import edu.isi.pegasus.planner.cluster.aggregator.MPIExec;
import edu.isi.pegasus.planner.cluster.aggregator.SeqExec;
import org.junit.jupiter.api.Test;

/**
 * Tests for the JobAggregator interface. Exercises known constants and verifies that concrete
 * implementations conform to the interface.
 */
public class JobAggregatorTest {

    @Test
    public void testVersionConstantNotNull() {
        assertNotNull(JobAggregator.VERSION, "JobAggregator.VERSION should not be null");
    }

    @Test
    public void testVersionConstantNotEmpty() {
        assertFalse(JobAggregator.VERSION.isEmpty(), "JobAggregator.VERSION should not be empty");
    }

    @Test
    public void testSeqExecImplementsJobAggregator() {
        SeqExec seqExec = new SeqExec();
        assertInstanceOf(JobAggregator.class, seqExec, "SeqExec should implement JobAggregator");
    }

    @Test
    public void testMPIExecImplementsJobAggregator() {
        MPIExec mpiExec = new MPIExec();
        assertInstanceOf(JobAggregator.class, mpiExec, "MPIExec should implement JobAggregator");
    }

    @Test
    public void testSeqExecCanBeInstantiated() {
        assertDoesNotThrow(SeqExec::new, "SeqExec should be instantiatable with no-arg ctor");
    }

    @Test
    public void testMPIExecCanBeInstantiated() {
        assertDoesNotThrow(MPIExec::new, "MPIExec should be instantiatable with no-arg ctor");
    }

    @Test
    public void testSeqExecAndMPIExecAreDifferentClasses() {
        assertNotEquals(
                SeqExec.class, MPIExec.class, "SeqExec and MPIExec should be different classes");
    }
}
