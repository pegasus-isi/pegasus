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
import edu.isi.pegasus.planner.partitioner.graph.LabelBag;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.junit.jupiter.api.Test;

/** @author Rajiv Mayani */
public class WholeTest {

    @Test
    public void testDescriptionReturnsConstant() {
        Whole partitioner = new Whole(new GraphNode("root"), new LinkedHashMap(), null);
        partitioner.mLogger = new NoOpLogManager();

        assertThat(partitioner.description(), is(Whole.DESCRIPTION));
    }

    @Test
    public void testDeterminePartitionsCreatesSingleWholePartitionAndSignalsDone() {
        GraphNode root = new GraphNode("root");
        GraphNode alpha = new GraphNode("alpha");
        GraphNode beta = new GraphNode("beta");

        beta.addParent(alpha);
        alpha.addChild(beta);

        Map graph = new LinkedHashMap();
        graph.put(root.getID(), root);
        graph.put(alpha.getID(), alpha);
        graph.put(beta.getID(), beta);

        RecordingCallback callback = new RecordingCallback();
        Whole partitioner = new Whole(root, graph, PegasusProperties.nonSingletonInstance());
        partitioner.mLogger = new NoOpLogManager();

        partitioner.determinePartitions(callback);

        assertThat(callback.partition, is(notNullValue()));
        assertThat(callback.partition.getID(), is("1"));
        assertThat(callback.partition.getIndex(), is(1));
        assertThat(
                new ArrayList<>(callback.partition.getNodeIDs()),
                is(Arrays.asList("alpha", "beta")));
        assertThat(callback.doneCalled, is(true));
        assertThat(callback.parentCalls.isEmpty(), is(true));
    }

    @Test
    public void testDeterminePartitionsAddsWholeWorkflowLabelToLastAddedNode() {
        GraphNode root = new GraphNode("root");
        GraphNode alpha = new GraphNode("alpha");
        GraphNode beta = new GraphNode("beta");

        Map graph = new LinkedHashMap();
        graph.put(root.getID(), root);
        graph.put(alpha.getID(), alpha);
        graph.put(beta.getID(), beta);

        RecordingCallback callback = new RecordingCallback();
        Whole partitioner = new Whole(root, graph, PegasusProperties.nonSingletonInstance());
        partitioner.mLogger = new NoOpLogManager();

        partitioner.determinePartitions(callback);

        assertThat(beta.getBag() instanceof LabelBag, is(true));
        assertThat(((LabelBag) beta.getBag()).get(LabelBag.LABEL_KEY), is("whole-wf"));
    }

    private static final class RecordingCallback implements Callback {
        private Partition partition;
        private final List<String> parentCalls = new ArrayList<>();
        private boolean doneCalled;

        @Override
        public void cbPartition(Partition p) {
            partition = p;
        }

        @Override
        public void cbParents(String child, List parents) {
            parentCalls.add(child);
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
        public void log(String message, Exception e, int level) {}

        @Override
        public void log(String message, int level) {}

        @Override
        protected void logAlreadyFormattedMessage(String message, int level) {}

        @Override
        public void logEventCompletion(int level) {}
    }
}
