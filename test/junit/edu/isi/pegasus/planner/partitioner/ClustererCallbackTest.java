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
package edu.isi.pegasus.planner.partitioner;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.*;

import edu.isi.pegasus.planner.cluster.Clusterer;
import edu.isi.pegasus.planner.cluster.ClustererException;
import edu.isi.pegasus.planner.common.PegasusProperties;
import edu.isi.pegasus.planner.partitioner.graph.GraphNode;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

/** @author Rajiv Mayani */
public class ClustererCallbackTest {

    @Test
    public void testInitializeStoresPropertiesAndClusterer() throws Exception {
        ClustererCallback callback = new ClustererCallback();
        PegasusProperties properties = PegasusProperties.nonSingletonInstance();
        RecordingClusterer clusterer = new RecordingClusterer();

        callback.initialize(properties, clusterer);

        assertThat(ReflectionTestUtils.getField(callback, "mProps"), is(notNullValue()));
        assertThat(
                ReflectionTestUtils.getField(callback, "mClusterer"), is(sameInstance(clusterer)));
    }

    @Test
    public void testCbPartitionDelegatesToClusterer() {
        ClustererCallback callback = new ClustererCallback();
        RecordingClusterer clusterer = new RecordingClusterer();
        callback.initialize(PegasusProperties.nonSingletonInstance(), clusterer);

        Partition partition = new Partition(Arrays.asList(new GraphNode("jobA")), "p1");

        callback.cbPartition(partition);

        assertThat(clusterer.lastPartition, is(notNullValue()));
    }

    @Test
    public void testCbParentsDelegatesToClusterer() {
        ClustererCallback callback = new ClustererCallback();
        RecordingClusterer clusterer = new RecordingClusterer();
        callback.initialize(PegasusProperties.nonSingletonInstance(), clusterer);

        List parents = Arrays.asList("parent-1");

        callback.cbParents("child-1", parents);

        assertThat(clusterer.lastChildPartitionId, is("child-1"));
        assertThat(clusterer.lastParents, is(notNullValue()));
    }

    @Test
    public void testCbPartitionThrowsIfUninitialized() {
        ClustererCallback callback = new ClustererCallback();
        Partition partition = new Partition(Arrays.asList(new GraphNode("jobA")), "p1");

        RuntimeException exception =
                assertThrows(RuntimeException.class, () -> callback.cbPartition(partition));

        assertThat(
                exception.getMessage(), is("Callback needs to be initialized before being used"));
    }

    @Test
    public void testCbParentsWrapsClustererException() {
        ClustererCallback callback = new ClustererCallback();
        RecordingClusterer clusterer = new RecordingClusterer();
        clusterer.parentsException = new ClustererException("boom");
        callback.initialize(PegasusProperties.nonSingletonInstance(), clusterer);

        RuntimeException exception =
                assertThrows(
                        RuntimeException.class,
                        () -> callback.cbParents("child-1", Collections.singletonList("parent-1")));

        assertThat(
                exception.getMessage(),
                containsString("ClustererCallback cbParents( String, List )"));
        assertThat(exception.getCause(), is(sameInstance(clusterer.parentsException)));
    }

    @Test
    public void testCbDoneIsNoOp() {
        ClustererCallback callback = new ClustererCallback();

        assertDoesNotThrow(callback::cbDone);
    }

    private static final class RecordingClusterer implements Clusterer {
        private Partition lastPartition;
        private String lastChildPartitionId;
        private List lastParents;
        private ClustererException parentsException;

        @Override
        public void initialize(
                edu.isi.pegasus.planner.classes.ADag dag,
                edu.isi.pegasus.planner.classes.PegasusBag bag)
                throws ClustererException {}

        @Override
        public void determineClusters(Partition partition) throws ClustererException {
            lastPartition = partition;
        }

        @Override
        public void parents(String partitionID, List parents) throws ClustererException {
            if (parentsException != null) {
                throw parentsException;
            }
            lastChildPartitionId = partitionID;
            lastParents = parents;
        }

        @Override
        public edu.isi.pegasus.planner.classes.ADag getClusteredDAG() throws ClustererException {
            return null;
        }

        @Override
        public String description() {
            return "recording";
        }
    }
}
