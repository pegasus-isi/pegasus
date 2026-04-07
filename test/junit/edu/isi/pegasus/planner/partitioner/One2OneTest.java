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

import edu.isi.pegasus.common.logging.LogFormatter;
import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.planner.common.PegasusProperties;
import edu.isi.pegasus.planner.partitioner.graph.GraphNode;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.junit.jupiter.api.Test;

/** @author Rajiv Mayani */
public class One2OneTest {

    @Test
    public void testDescriptionReturnsConstant() {
        One2One partitioner = new One2One(new GraphNode("root"), new LinkedHashMap(), null);
        partitioner.mLogger = new NoOpLogManager();

        assertThat(partitioner.description(), is(One2One.DESCRIPTION));
    }

    @Test
    public void testDeterminePartitionsCreatesOnePartitionPerNonRootNode() {
        GraphNode root = new GraphNode("root");
        GraphNode alpha = new GraphNode("alpha");
        GraphNode beta = new GraphNode("beta");

        alpha.addParent(root);
        root.addChild(alpha);

        beta.addParent(alpha);
        alpha.addChild(beta);

        Map graph = new LinkedHashMap();
        graph.put(root.getID(), root);
        graph.put(alpha.getID(), alpha);
        graph.put(beta.getID(), beta);

        RecordingCallback callback = new RecordingCallback();
        One2One partitioner = new One2One(root, graph, PegasusProperties.nonSingletonInstance());
        partitioner.mLogger = new NoOpLogManager();

        partitioner.determinePartitions(callback);

        assertThat(callback.partitionIds, is(Arrays.asList("alpha", "beta")));
        assertThat(callback.partitionIndexes, is(Arrays.asList(1, 2)));
        assertThat(
                callback.partitionNodeIds,
                is(Arrays.asList(Arrays.asList("alpha"), Arrays.asList("beta"))));
        assertThat(callback.doneCalled, is(true));
    }

    @Test
    public void testDeterminePartitionsBuildsParentRelationsUsingOriginalNodeIds() {
        GraphNode root = new GraphNode("root");
        GraphNode alpha = new GraphNode("alpha");
        GraphNode beta = new GraphNode("beta");
        GraphNode gamma = new GraphNode("gamma");

        alpha.addParent(root);
        root.addChild(alpha);

        beta.addParent(alpha);
        alpha.addChild(beta);

        gamma.addParent(alpha);
        alpha.addChild(gamma);
        gamma.addParent(beta);
        beta.addChild(gamma);

        Map graph = new LinkedHashMap();
        graph.put(root.getID(), root);
        graph.put(alpha.getID(), alpha);
        graph.put(beta.getID(), beta);
        graph.put(gamma.getID(), gamma);

        RecordingCallback callback = new RecordingCallback();
        One2One partitioner = new One2One(root, graph, PegasusProperties.nonSingletonInstance());
        partitioner.mLogger = new NoOpLogManager();

        partitioner.determinePartitions(callback);

        assertThat(callback.partitionIds, is(Arrays.asList("alpha", "beta", "gamma")));
        assertThat(callback.parentRelationChildren, is(Arrays.asList("alpha", "beta", "gamma")));
        assertThat(
                callback.parentRelationParents,
                is(
                        Arrays.asList(
                                Arrays.asList("root"),
                                Arrays.asList("alpha"),
                                Arrays.asList("alpha", "beta"))));
        assertThat(callback.doneCalled, is(true));
    }

    private static final class RecordingCallback implements Callback {
        private final List<String> partitionIds = new ArrayList<>();
        private final List<Integer> partitionIndexes = new ArrayList<>();
        private final List<List<String>> partitionNodeIds = new ArrayList<>();
        private final List<String> parentRelationChildren = new ArrayList<>();
        private final List<List<String>> parentRelationParents = new ArrayList<>();
        private boolean doneCalled;

        @Override
        public void cbPartition(Partition p) {
            partitionIds.add(p.getID());
            partitionIndexes.add(p.getIndex());
            partitionNodeIds.add(new ArrayList<>(p.getNodeIDs()));
        }

        @Override
        public void cbParents(String child, List parents) {
            parentRelationChildren.add(child);
            parentRelationParents.add(new ArrayList<>(parents));
        }

        @Override
        public void cbDone() {
            doneCalled = true;
        }
    }

    private static final class NoOpLogManager extends LogManager {
        @Override
        public void initialize(LogFormatter formatter, Properties properties) {}

        @Override
        public void configure(boolean prefixTimestamp) {}

        @Override
        public void log(String message, Exception e, int level) {}

        @Override
        public void log(String message, int level) {}

        @Override
        protected void logAlreadyFormattedMessage(String message, int level) {}

        @Override
        protected void setLevel(int level, boolean info) {}

        @Override
        public int getLevel() {
            return LogManager.DEBUG_MESSAGE_LEVEL;
        }

        @Override
        public void setWriters(String out) {}

        @Override
        public void setWriter(STREAM_TYPE type, PrintStream ps) {}

        @Override
        public PrintStream getWriter(STREAM_TYPE type) {
            return null;
        }

        @Override
        public void logEventCompletion(int level) {}
    }
}
