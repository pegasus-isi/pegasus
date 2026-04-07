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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.*;

import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.partitioner.Partition;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import org.junit.jupiter.api.Test;

/** Tests for the Abstract cluster class structure. */
public class AbstractTest {

    private static final class TestClusterer extends Abstract {
        TestClusterer() {
            this.mSubInfoMap = new HashMap<String, Job>();
            this.mPartitionClusterMap = new HashMap<String, Job>();
        }

        @Override
        public String description() {
            return "test";
        }

        @Override
        public List<String> order(Partition p) {
            return Collections.emptyList();
        }

        @Override
        public void determineInputOutputFiles(
                edu.isi.pegasus.planner.classes.AggregatedJob job, List<Job> orderedJobs) {}

        Job getJobById(String id) {
            return getJob(id);
        }

        void mapPartition(Partition partition, Job job) {
            associate(partition, job);
        }

        Job getClusteredJob(Partition partition) {
            return clusteredJob(partition);
        }

        Job getClusteredJob(String id) {
            return clusteredJob(id);
        }

        String clusteredJobIdFor(Partition partition) {
            return constructClusteredJobID(partition);
        }

        void add(Job job) {
            addJob(job);
        }

        String logicalNameFor(List<Job> jobs) {
            return getLogicalNameForJobs(jobs);
        }
    }

    @Test
    public void testAbstractImplementsClusterer() {
        // edu.isi.pegasus.planner.cluster.Abstract implements Clusterer
        assertThat(
                Clusterer.class.isAssignableFrom(edu.isi.pegasus.planner.cluster.Abstract.class),
                is(true));
    }

    @Test
    public void testHorizontalImplementsClustererDirectly() {
        // Horizontal implements Clusterer directly (not via Abstract)
        assertThat(
                edu.isi.pegasus.planner.cluster.Abstract.class.isAssignableFrom(Horizontal.class),
                is(false));
        assertThat(Clusterer.class.isAssignableFrom(Horizontal.class), is(true));
    }

    @Test
    public void testVerticalExtendsAbstract() {
        assertThat(
                edu.isi.pegasus.planner.cluster.Abstract.class.isAssignableFrom(Vertical.class),
                is(true));
    }

    @Test
    public void testHorizontalInstantiation() {
        Horizontal h = new Horizontal();
        assertThat(h, notNullValue());
    }

    @Test
    public void testVerticalInstantiation() {
        Vertical v = new Vertical();
        assertThat(v, notNullValue());
    }

    @Test
    public void testAbstractConstructorInitializesAggregatorFactory() {
        TestClusterer clusterer = new TestClusterer();

        assertThat(clusterer.mJobAggregatorFactory, notNullValue());
    }

    @Test
    public void testAddJobIndexesByLogicalId() {
        TestClusterer clusterer = new TestClusterer();
        Job job = new Job();
        job.setLogicalID("job-1");

        clusterer.add(job);

        assertThat(clusterer.getJobById("job-1"), sameInstance(job));
    }

    @Test
    public void testAssociateMapsPartitionToClusteredJob() {
        TestClusterer clusterer = new TestClusterer();
        Partition partition = new Partition();
        partition.setID("partition-1");
        Job job = new Job();

        clusterer.mapPartition(partition, job);

        assertThat(clusterer.getClusteredJob(partition), sameInstance(job));
        assertThat(clusterer.getClusteredJob("partition-1"), sameInstance(job));
    }

    @Test
    public void testConstructClusteredJobIdDefaultsToPartitionId() {
        TestClusterer clusterer = new TestClusterer();
        Partition partition = new Partition();
        partition.setID("cluster-7");

        assertThat(clusterer.clusteredJobIdFor(partition), is("cluster-7"));
    }

    @Test
    public void testGetLogicalNameForJobsUsesFirstJobAndDagmanCompliantName() {
        TestClusterer clusterer = new TestClusterer();
        Job first = new Job();
        first.namespace = "ns";
        first.logicalName = "job.a+b=c";
        first.version = "1.0";
        Job second = new Job();
        second.namespace = "ns";
        second.logicalName = "other";
        second.version = "1.0";

        String result = clusterer.logicalNameFor(Arrays.asList(first, second));

        assertThat(result, is(Job.makeDAGManCompliant(first.getStagedExecutableBaseName())));
        assertThat(result.contains("."), is(false));
        assertThat(result.contains("+"), is(false));
        assertThat(result.contains("="), is(false));
    }
}
