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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;

import edu.isi.pegasus.planner.namespace.Pegasus;
import org.junit.jupiter.api.Test;

/** @author Rajiv Mayani */
public class DataFlowJobTest {

    @Test
    public void testDefaultConstructorNotNull() {
        DataFlowJob job = new DataFlowJob();
        assertThat(job, is(notNullValue()));
    }

    @Test
    public void testDefaultConstructorGridstartIsNone() {
        DataFlowJob job = new DataFlowJob();
        // DataFlowJob sets gridstart key to "none" in default constructor
        String gridstart = (String) job.vdsNS.get(Pegasus.GRIDSTART_KEY);
        assertThat(gridstart, is("none"));
    }

    @Test
    public void testDefaultPartiallyCreatedIsFalse() {
        DataFlowJob job = new DataFlowJob();
        assertThat(job.isPartiallyCreated(), is(false));
    }

    @Test
    public void testSetPartiallyCreated() {
        DataFlowJob job = new DataFlowJob();
        job.setPartiallyCreated();
        assertThat(job.isPartiallyCreated(), is(true));
    }

    @Test
    public void testSetPartiallyCreatedFalse() {
        DataFlowJob job = new DataFlowJob();
        job.setPartiallyCreated(true);
        job.setPartiallyCreated(false);
        assertThat(job.isPartiallyCreated(), is(false));
    }

    @Test
    public void testSetPartiallyCreatedTrue() {
        DataFlowJob job = new DataFlowJob();
        job.setPartiallyCreated(true);
        assertThat(job.isPartiallyCreated(), is(true));
    }

    @Test
    public void testConstructorWithJob() {
        Job baseJob = new Job();
        baseJob.logicalName = "transform";
        baseJob.jobClass = Job.COMPUTE_JOB;
        DataFlowJob dfJob = new DataFlowJob(baseJob);
        assertThat(dfJob, is(notNullValue()));
        assertThat(dfJob.isPartiallyCreated(), is(false));
        // GridStart key should still be "none"
        assertThat(dfJob.vdsNS.get(Pegasus.GRIDSTART_KEY), is("none"));
    }

    @Test
    public void testConstructorWithJobAndNum() {
        Job baseJob = new Job();
        baseJob.logicalName = "transform";
        baseJob.jobClass = Job.COMPUTE_JOB;
        DataFlowJob dfJob = new DataFlowJob(baseJob, 3);
        assertThat(dfJob, is(notNullValue()));
        assertThat(dfJob.isPartiallyCreated(), is(false));
    }

    @Test
    public void testConstructorWithJobCopiesNameAndLogicalName() {
        Job baseJob = new Job();
        baseJob.jobName = "base";
        baseJob.logicalName = "logical-base";

        DataFlowJob dfJob = new DataFlowJob(baseJob);

        assertThat(dfJob.getName(), is("base"));
        assertThat(dfJob.logicalName, is("logical-base"));
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
        assertThat(link, is(notNullValue()));
    }

    @Test
    public void testLinkSetAndGetParentID() {
        DataFlowJob.Link link = new DataFlowJob.Link();
        link.setLink("parentJob", "childJob");
        assertThat(link.getParentID(), is("parentJob"));
    }

    @Test
    public void testLinkSetAndGetChildID() {
        DataFlowJob.Link link = new DataFlowJob.Link();
        link.setLink("parentJob", "childJob");
        assertThat(link.getChildID(), is("childJob"));
    }

    @Test
    public void testLinkToStringThrowsUnsupportedOperationException() {
        DataFlowJob.Link link = new DataFlowJob.Link();
        assertThrows(UnsupportedOperationException.class, () -> link.toString());
    }

    @Test
    public void testDataFlowJobIsAnAggregatedJob() {
        DataFlowJob job = new DataFlowJob();
        assertThat(job instanceof AggregatedJob, is(true));
    }

    @Test
    public void testDataFlowJobIsAJob() {
        DataFlowJob job = new DataFlowJob();
        assertThat(job instanceof Job, is(true));
    }

    @Test
    public void testDefaultConstructorStartsWithNoConstituentJobs() {
        DataFlowJob job = new DataFlowJob();
        assertThat(job.numberOfConsitutentJobs(), is(0));
        assertThat(job.isEmpty(), is(true));
    }

    @Test
    public void testInheritedAddSupportsConstituentJobs() {
        DataFlowJob job = new DataFlowJob();
        Job constituent = new Job();
        constituent.jobName = "child";
        constituent.logicalName = "child";
        constituent.logicalId = "child-id";

        job.add(constituent);

        assertThat(job.numberOfConsitutentJobs(), is(1));
        assertThat(job.getConstituentJob(0), is(notNullValue()));
        assertThat(job.getConstituentJob(0).getName(), is("child"));
    }

    @Test
    public void testLinkSetLinkOverwritesPreviousParentAndChild() {
        DataFlowJob.Link link = new DataFlowJob.Link();
        link.setLink("parent1", "child1");
        link.setLink("parent2", "child2");

        assertThat(link.getParentID(), is("parent2"));
        assertThat(link.getChildID(), is("child2"));
    }

    @Test
    public void testLinkIsAJob() {
        DataFlowJob.Link link = new DataFlowJob.Link();
        assertThat(link instanceof Job, is(true));
    }

    @Test
    public void testPartiallyCreatedDefaultConstructorFromBaseJob() {
        Job baseJob = new Job();
        baseJob.logicalName = "myTransform";
        baseJob.jobClass = Job.COMPUTE_JOB;
        DataFlowJob dfJob = new DataFlowJob(baseJob);
        // newly created from a base job should not be partially created
        assertThat(dfJob.isPartiallyCreated(), is(false));
    }
}
