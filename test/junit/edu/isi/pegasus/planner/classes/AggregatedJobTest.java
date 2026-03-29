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
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.*;

import java.util.Iterator;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Tests for the AggregatedJob class. */
public class AggregatedJobTest {

    @BeforeAll
    public static void setUpClass() {}

    @AfterAll
    public static void tearDownClass() {}

    @BeforeEach
    public void setUp() {}

    @AfterEach
    public void tearDown() {}

    // -----------------------------------------------------------------------
    // Construction — default constructor
    // -----------------------------------------------------------------------

    @Test
    public void testDefaultConstructorCreatesEmptyJob() {
        AggregatedJob aj = new AggregatedJob();
        assertThat(aj.numberOfConsitutentJobs(), is(0));
        assertTrue(aj.isEmpty());
    }

    @Test
    public void testDefaultConstructorRenderedFlagIsFalse() {
        AggregatedJob aj = new AggregatedJob();
        assertFalse(aj.renderedToExecutableForm());
    }

    @Test
    public void testDefaultConstructorAggregatorIsNull() {
        AggregatedJob aj = new AggregatedJob();
        assertNull(aj.getJobAggregator());
    }

    // -----------------------------------------------------------------------
    // Construction — int capacity constructor
    // -----------------------------------------------------------------------

    @Test
    public void testCapacityConstructorCreatesEmptyJob() {
        AggregatedJob aj = new AggregatedJob(5);
        assertThat(aj.numberOfConsitutentJobs(), is(0));
    }

    // -----------------------------------------------------------------------
    // Construction — from a Job
    // -----------------------------------------------------------------------

    @Test
    public void testJobConstructorCopiesJobName() {
        Job base = createJob("baseJob");
        AggregatedJob aj = new AggregatedJob(base);
        assertThat(aj.getName(), is("baseJob"));
    }

    @Test
    public void testJobConstructorCopiesLogicalName() {
        Job base = createJob("logicJob");
        AggregatedJob aj = new AggregatedJob(base);
        assertThat(aj.logicalName, is("logicJob"));
    }

    @Test
    public void testJobConstructorStartsWithNoConstituentJobs() {
        Job base = createJob("baseForAgg");
        AggregatedJob aj = new AggregatedJob(base);
        assertThat(aj.numberOfConsitutentJobs(), is(0));
    }

    // -----------------------------------------------------------------------
    // Adding constituent jobs
    // -----------------------------------------------------------------------

    @Test
    public void testAddSingleConstituentJob() {
        AggregatedJob aj = new AggregatedJob();
        aj.add(createJobWithLogicalId("c1", "lid-c1"));
        assertThat(aj.numberOfConsitutentJobs(), is(1));
    }

    @Test
    public void testAddMultipleConstituentJobs() {
        AggregatedJob aj = new AggregatedJob();
        aj.add(createJobWithLogicalId("c1", "lid-c1"));
        aj.add(createJobWithLogicalId("c2", "lid-c2"));
        aj.add(createJobWithLogicalId("c3", "lid-c3"));
        assertThat(aj.numberOfConsitutentJobs(), is(3));
    }

    @Test
    public void testIsEmptyAfterAddReturnsFalse() {
        AggregatedJob aj = new AggregatedJob();
        aj.add(createJobWithLogicalId("c1", "lid-c1"));
        assertFalse(aj.isEmpty());
    }

    // -----------------------------------------------------------------------
    // Iterating constituent jobs
    // -----------------------------------------------------------------------

    @Test
    public void testConstituentJobsIteratorVisitsAddedJobs() {
        AggregatedJob aj = new AggregatedJob();
        aj.add(createJobWithLogicalId("j1", "lid-j1"));
        aj.add(createJobWithLogicalId("j2", "lid-j2"));

        int count = 0;
        for (Iterator<Job> it = aj.constituentJobsIterator(); it.hasNext(); ) {
            it.next();
            count++;
        }
        assertThat(count, is(2));
    }

    @Test
    public void testGetConstituentJobByIndex() {
        AggregatedJob aj = new AggregatedJob();
        Job first = createJobWithLogicalId("first", "lid-first");
        Job second = createJobWithLogicalId("second", "lid-second");
        aj.add(first);
        aj.add(second);

        Job retrieved = aj.getConstituentJob(0);
        assertNotNull(retrieved);
        assertThat(retrieved.getName(), is("first"));
    }

    @Test
    public void testGetConstituentJobOutOfBoundsReturnsNull() {
        AggregatedJob aj = new AggregatedJob();
        aj.add(createJobWithLogicalId("only", "lid-only"));
        assertNull(aj.getConstituentJob(5));
    }

    // -----------------------------------------------------------------------
    // Rendered flag
    // -----------------------------------------------------------------------

    @Test
    public void testSetRenderedToExecutableForm() {
        AggregatedJob aj = new AggregatedJob();
        aj.setRenderedToExecutableForm(true);
        assertTrue(aj.renderedToExecutableForm());
    }

    @Test
    public void testSetRenderedToExecutableFormFalse() {
        AggregatedJob aj = new AggregatedJob();
        aj.setRenderedToExecutableForm(true);
        aj.setRenderedToExecutableForm(false);
        assertFalse(aj.renderedToExecutableForm());
    }

    // -----------------------------------------------------------------------
    // getDAXID always returns null for clustered jobs
    // -----------------------------------------------------------------------

    @Test
    public void testGetDAXIDReturnsNull() {
        AggregatedJob aj = new AggregatedJob();
        assertNull(aj.getDAXID());
    }

    // -----------------------------------------------------------------------
    // Graph-level delegation
    // -----------------------------------------------------------------------

    @Test
    public void testSizeEqualsNumberOfConstituentJobs() {
        AggregatedJob aj = new AggregatedJob();
        aj.add(createJobWithLogicalId("g1", "lid-g1"));
        aj.add(createJobWithLogicalId("g2", "lid-g2"));
        assertThat(aj.size(), is(aj.numberOfConsitutentJobs()));
    }

    // -----------------------------------------------------------------------
    // Clone — MapGraph.clone() is not implemented (returns a CloneNotSupportedException
    // object), so AggregatedJob.clone() will throw ClassCastException at runtime.
    // We document that behavior here rather than silently testing a broken path.
    // -----------------------------------------------------------------------

    @Test
    public void testCloneThrowsDueToUnimplementedMapGraphClone() {
        Job base = createJob("cloneBase");
        AggregatedJob aj = new AggregatedJob(base);
        assertThrows(ClassCastException.class, aj::clone);
    }

    // -----------------------------------------------------------------------
    // toString
    // -----------------------------------------------------------------------

    @Test
    public void testToStringContainsMainJobSection() {
        Job base = createJob("mainJobName");
        AggregatedJob aj = new AggregatedJob(base);
        String str = aj.toString();
        assertThat(str, containsString("[MAIN JOB]"));
    }

    @Test
    public void testToStringContainsConstituentJobsSection() {
        AggregatedJob aj = new AggregatedJob();
        aj.jobName = "aggJob";
        aj.logicalName = "aggJob";
        String str = aj.toString();
        assertThat(str, containsString("[CONSTITUENT JOBS]"));
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    private Job createJob(String name) {
        Job job = new Job();
        job.jobName = name;
        job.logicalName = name;
        job.logicalId = name;
        return job;
    }

    private Job createJobWithLogicalId(String name, String logicalId) {
        Job job = new Job();
        job.jobName = name;
        job.logicalName = name;
        job.logicalId = logicalId;
        return job;
    }
}
