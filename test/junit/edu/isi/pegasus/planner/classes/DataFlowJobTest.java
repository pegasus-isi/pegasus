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
package edu.isi.pegasus.planner.classes;

import static org.junit.jupiter.api.Assertions.*;

import edu.isi.pegasus.planner.namespace.Pegasus;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** @author Rajiv Mayani */
public class DataFlowJobTest {
    @BeforeAll
    public static void setUpClass() {}

    @AfterAll
    public static void tearDownClass() {}

    @BeforeEach
    public void setUp() {}

    @AfterEach
    public void tearDown() {}

    @Test
    public void testDefaultConstructorNotNull() {
        DataFlowJob job = new DataFlowJob();
        assertNotNull(job);
    }

    @Test
    public void testDefaultConstructorGridstartIsNone() {
        DataFlowJob job = new DataFlowJob();
        // DataFlowJob sets gridstart key to "none" in default constructor
        String gridstart = (String) job.vdsNS.get(Pegasus.GRIDSTART_KEY);
        assertEquals("none", gridstart);
    }

    @Test
    public void testDefaultPartiallyCreatedIsFalse() {
        DataFlowJob job = new DataFlowJob();
        assertFalse(job.isPartiallyCreated());
    }

    @Test
    public void testSetPartiallyCreated() {
        DataFlowJob job = new DataFlowJob();
        job.setPartiallyCreated();
        assertTrue(job.isPartiallyCreated());
    }

    @Test
    public void testSetPartiallyCreatedFalse() {
        DataFlowJob job = new DataFlowJob();
        job.setPartiallyCreated(true);
        job.setPartiallyCreated(false);
        assertFalse(job.isPartiallyCreated());
    }

    @Test
    public void testSetPartiallyCreatedTrue() {
        DataFlowJob job = new DataFlowJob();
        job.setPartiallyCreated(true);
        assertTrue(job.isPartiallyCreated());
    }

    @Test
    public void testConstructorWithJob() {
        Job baseJob = new Job();
        baseJob.logicalName = "transform";
        baseJob.jobClass = Job.COMPUTE_JOB;
        DataFlowJob dfJob = new DataFlowJob(baseJob);
        assertNotNull(dfJob);
        assertFalse(dfJob.isPartiallyCreated());
        // GridStart key should still be "none"
        assertEquals("none", dfJob.vdsNS.get(Pegasus.GRIDSTART_KEY));
    }

    @Test
    public void testConstructorWithJobAndNum() {
        Job baseJob = new Job();
        baseJob.logicalName = "transform";
        baseJob.jobClass = Job.COMPUTE_JOB;
        DataFlowJob dfJob = new DataFlowJob(baseJob, 3);
        assertNotNull(dfJob);
        assertFalse(dfJob.isPartiallyCreated());
    }

    @Test
    public void testAddEdge() {
        DataFlowJob job = new DataFlowJob();
        DataFlowJob.Link link = new DataFlowJob.Link();
        link.setLink("parentJob", "childJob");
        job.addEdge(link);
        // No exception thrown means edge was added
    }

    @Test
    public void testAddMultipleEdges() {
        DataFlowJob job = new DataFlowJob();
        DataFlowJob.Link link1 = new DataFlowJob.Link();
        link1.setLink("job1", "job2");
        DataFlowJob.Link link2 = new DataFlowJob.Link();
        link2.setLink("job2", "job3");
        job.addEdge(link1);
        job.addEdge(link2);
        // No exception thrown means both edges were added
    }

    @Test
    public void testLinkDefaultConstructor() {
        DataFlowJob.Link link = new DataFlowJob.Link();
        assertNotNull(link);
    }

    @Test
    public void testLinkSetAndGetParentID() {
        DataFlowJob.Link link = new DataFlowJob.Link();
        link.setLink("parentJob", "childJob");
        assertEquals("parentJob", link.getParentID());
    }

    @Test
    public void testLinkSetAndGetChildID() {
        DataFlowJob.Link link = new DataFlowJob.Link();
        link.setLink("parentJob", "childJob");
        assertEquals("childJob", link.getChildID());
    }

    @Test
    public void testLinkToStringThrowsUnsupportedOperationException() {
        DataFlowJob.Link link = new DataFlowJob.Link();
        assertThrows(UnsupportedOperationException.class, () -> link.toString());
    }

    @Test
    public void testDataFlowJobIsAnAggregatedJob() {
        DataFlowJob job = new DataFlowJob();
        assertTrue(job instanceof AggregatedJob);
    }

    @Test
    public void testDataFlowJobIsAJob() {
        DataFlowJob job = new DataFlowJob();
        assertTrue(job instanceof Job);
    }

    @Test
    public void testPartiallyCreatedDefaultConstructorFromBaseJob() {
        Job baseJob = new Job();
        baseJob.logicalName = "myTransform";
        baseJob.jobClass = Job.COMPUTE_JOB;
        DataFlowJob dfJob = new DataFlowJob(baseJob);
        // newly created from a base job should not be partially created
        assertFalse(dfJob.isPartiallyCreated());
    }
}
