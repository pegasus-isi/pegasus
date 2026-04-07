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
package edu.isi.pegasus.planner.parser.dax;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.*;

import edu.isi.pegasus.common.logging.LogFormatter;
import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.common.PegasusProperties;
import edu.isi.pegasus.planner.partitioner.graph.Bag;
import edu.isi.pegasus.planner.partitioner.graph.GraphNode;
import edu.isi.pegasus.planner.partitioner.graph.LabelBag;
import java.io.PrintStream;
import java.util.Map;
import java.util.Properties;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

/** @author Rajiv Mayani */
public class DAX2LabelGraphTest {

    @Test
    public void testInitializeSetsDefaultLabelKey() throws Exception {
        DAX2LabelGraph callback = new DAX2LabelGraph();

        callback.initialize(createBag(), "workflow.dax");

        assertThat(
                "initialize should default the label key",
                getField(callback, "mLabelKey"),
                is(DAX2LabelGraph.DEFAULT_LABEL_KEY));
        assertThat(
                "initialize should propagate the default label key to LabelBag",
                LabelBag.LABEL_KEY,
                is(DAX2LabelGraph.DEFAULT_LABEL_KEY));
    }

    @Test
    public void testSetLabelKeyFallsBackToDefaultAndPropagatesCustomValue() throws Exception {
        DAX2LabelGraph callback = new DAX2LabelGraph();
        callback.initialize(createBag(), "workflow.dax");

        callback.setLabelKey("group");
        assertThat(
                "setLabelKey should store the custom key",
                getField(callback, "mLabelKey"),
                is("group"));
        assertThat("setLabelKey should update LabelBag.LABEL_KEY", LabelBag.LABEL_KEY, is("group"));

        callback.setLabelKey(null);
        assertThat(
                "null should reset the label key to the default",
                getField(callback, "mLabelKey"),
                is(DAX2LabelGraph.DEFAULT_LABEL_KEY));
        assertThat(
                "reset should also restore LabelBag.LABEL_KEY",
                LabelBag.LABEL_KEY,
                is(DAX2LabelGraph.DEFAULT_LABEL_KEY));
    }

    @Test
    public void testCbJobStoresLabelBagUsingConfiguredKey() {
        DAX2LabelGraph callback = new DAX2LabelGraph();
        callback.initialize(createBag(), "workflow.dax");
        callback.setLabelKey("group");

        Job job = new Job();
        job.setLogicalID("ID0001");
        job.logicalName = "preprocess";
        job.vdsNS.construct("group", "cluster-a");

        callback.cbJob(job);

        GraphNode node = (GraphNode) callback.get("ID0001");
        Bag bag = node.getBag();
        assertThat("cbJob should index the graph node by logical id", node, notNullValue());
        assertThat("cbJob should use the job logical name", node.getName(), is("preprocess"));
        assertThat(
                "cbJob should associate a LabelBag with the graph node",
                bag,
                instanceOf(LabelBag.class));
        assertThat(
                "cbJob should store the profile value under the label key",
                bag.get("group"),
                is("cluster-a"));
    }

    @Test
    public void testCbDoneRequiresCompletionAndAddsLabelBagToDummyRoot() throws Exception {
        DAX2LabelGraph callback = new DAX2LabelGraph();
        callback.initialize(createBag(), "workflow.dax");

        RuntimeException exception =
                assertThrows(
                        RuntimeException.class,
                        callback::getConstructedObject,
                        "constructed graph should not be available before cbDone");
        assertThat(
                "guard message should match the current behavior",
                exception.getMessage(),
                is("Method called before the abstract dag  for the partition was fully generated"));

        Job rootJob = new Job();
        rootJob.setLogicalID("ID0001");
        rootJob.logicalName = "root";
        callback.cbJob(rootJob);

        callback.cbDone();

        Map<?, ?> graph = (Map<?, ?>) callback.getConstructedObject();
        GraphNode dummy = (GraphNode) graph.get(DAX2Graph.DUMMY_NODE_ID);
        assertThat("cbDone should add the dummy root node", dummy, notNullValue());
        assertThat(
                "cbDone should associate a LabelBag with the dummy root",
                dummy.getBag(),
                instanceOf(LabelBag.class));
        assertThat(
                "cbDone should store the dummy root on the callback",
                getField(callback, "mRoot"),
                sameInstance(dummy));
    }

    private PegasusBag createBag() {
        PegasusBag bag = new PegasusBag();
        bag.add(PegasusBag.PEGASUS_PROPERTIES, PegasusProperties.nonSingletonInstance());
        bag.add(PegasusBag.PEGASUS_LOGMANAGER, new NoOpLogManager());
        return bag;
    }

    private Object getField(Object target, String name) throws Exception {
        return ReflectionTestUtils.getField(target, name);
    }

    private static final class NoOpLogManager extends LogManager {

        @Override
        public void initialize(LogFormatter formatter, Properties properties) {
            this.mLogFormatter = formatter;
        }

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
        public void log(String message, int level) {}

        @Override
        public void log(String message, Exception e, int level) {}

        @Override
        protected void logAlreadyFormattedMessage(String message, int level) {}

        @Override
        public void logEventCompletion(int level) {}
    }
}
