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

import edu.isi.pegasus.planner.classes.AggregatedJob;
import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.classes.PegasusFile;
import edu.isi.pegasus.planner.partitioner.Partition;
import edu.isi.pegasus.planner.partitioner.graph.GraphNode;
import edu.isi.pegasus.planner.partitioner.graph.LabelBag;
import java.util.Arrays;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Tests for the Vertical clusterer class. */
public class VerticalTest {

    private static final class TestVertical extends Vertical {
        String clusteredJobIdFor(Partition partition) {
            return constructClusteredJobID(partition);
        }

        String logicalNameFor(java.util.List<Job> jobs) {
            return getLogicalNameForJobs(jobs);
        }
    }

    private TestVertical mVertical;

    @BeforeEach
    public void setUp() {
        mVertical = new TestVertical();
    }

    @Test
    public void testInstantiation() {
        assertThat(mVertical, notNullValue());
    }

    @Test
    public void testImplementsClusterer() {
        assertThat(mVertical, instanceOf(Clusterer.class));
    }

    @Test
    public void testDescriptionNotNull() {
        assertThat(mVertical.description(), notNullValue());
    }

    @Test
    public void testDescriptionNotEmpty() {
        assertThat(mVertical.description().isEmpty(), is(false));
    }

    @Test
    public void testDefaultConstructorDoesNotThrow() {
        assertDoesNotThrow(Vertical::new);
    }

    @Test
    public void testDescriptionIsDifferentFromHorizontal() {
        Horizontal h = new Horizontal();
        assertThat(mVertical.description(), not(equalTo(h.description())));
    }

    @Test
    public void testIsInstanceOfVertical() {
        assertThat(mVertical, instanceOf(Vertical.class));
    }

    @Test
    public void testDescriptionMatchesDeclaredConstant() {
        assertThat(mVertical.description(), is(Vertical.DESCRIPTION));
    }

    @Test
    public void testGetLogicalNameForJobsReturnsNull() {
        assertThat(mVertical.logicalNameFor(Arrays.asList(new Job())), nullValue());
    }

    @Test
    public void testConstructClusteredJobIdUsesLabelWhenPresent() {
        Partition partition = new Partition();
        partition.setID("partition-1");
        GraphNode node = new GraphNode("n1");
        LabelBag bag = new LabelBag();
        bag.add(LabelBag.LABEL_KEY, "label-a");
        node.setBag(bag);
        partition.addNode(node);

        assertThat(mVertical.clusteredJobIdFor(partition), is("label-a"));
    }

    @Test
    public void testConstructClusteredJobIdFallsBackToPartitionIdWhenLabelMissing() {
        Partition partition = new Partition();
        partition.setID("partition-2");
        GraphNode node = new GraphNode("n1");
        node.setBag(new LabelBag());
        partition.addNode(node);

        assertThat(mVertical.clusteredJobIdFor(partition), is("partition-2"));
    }

    @Test
    public void testDetermineInputOutputFilesUsesTopologicalMaterializationRules() {
        AggregatedJob aggregatedJob = new AggregatedJob();
        Job first = new Job();
        Job second = new Job();

        PegasusFile shared = new PegasusFile("shared");
        PegasusFile finalOutput = new PegasusFile("final");

        first.inputFiles.add(shared);
        first.outputFiles.add(shared);
        second.inputFiles.add(shared);
        second.outputFiles.add(finalOutput);

        mVertical.determineInputOutputFiles(aggregatedJob, Arrays.asList(first, second));

        assertThat(aggregatedJob.getInputFiles().contains(shared), is(true));
        assertThat(aggregatedJob.getOutputFiles().contains(shared), is(true));
        assertThat(aggregatedJob.getOutputFiles().contains(finalOutput), is(true));
    }
}
