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

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.*;

import edu.isi.pegasus.common.logging.LogFormatter;
import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.common.PegasusProperties;
import edu.isi.pegasus.planner.partitioner.graph.GraphNode;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

/** @author Rajiv Mayani */
public class DAX2GraphTest {

    @Test
    public void testInitializeSetsExpectedDefaults() throws Exception {
        DAX2Graph callback = new DAX2Graph();
        PegasusBag bag = createBag();

        callback.initialize(bag, "workflow.dax");

        assertThat(
                "initialize should store the bag properties",
                getField(callback, "mProps"),
                sameInstance(bag.getPegasusProperties()));
        assertThat(
                "initialize should store the bag logger",
                getField(callback, "mLogger"),
                sameInstance(bag.getLogger()));
        assertThat(
                "initialize should create the graph map",
                getField(callback, "mAbstractGraph"),
                notNullValue());
        assertThat("initialize should reset mDone", getBooleanField(callback, "mDone"), is(false));
        assertThat("initialize should clear the label", getField(callback, "mLabel"), nullValue());
        assertThat(
                "initialize should clear the root node", getField(callback, "mRoot"), nullValue());
    }

    @Test
    public void testCbDocumentUsesNameOrFallbackLabel() {
        DAX2Graph callback = new DAX2Graph();
        callback.initialize(createBag(), "workflow.dax");

        Map<String, String> attributes = new HashMap<String, String>();
        attributes.put("name", "example-wf");
        callback.cbDocument(attributes);
        assertThat("cbDocument should use the DAX name", callback.getNameOfDAX(), is("example-wf"));

        callback.cbDocument(new HashMap<String, String>());
        assertThat(
                "cbDocument should fall back to the current default",
                callback.getNameOfDAX(),
                is("test"));

        callback.cbDocument(null);
        assertThat("cbDocument should handle null attributes", callback.getNameOfDAX(), is("test"));
    }

    @Test
    public void testCbJobParentsAndChildrenWireGraphNodes() {
        DAX2Graph callback = new DAX2Graph();
        callback.initialize(createBag(), "workflow.dax");

        Job parent = createJob("ID0001", "preprocess");
        Job child = createJob("ID0002", "analyze");

        callback.cbJob(parent);
        callback.cbJob(child);
        callback.cbParents("ID0002", Arrays.asList("ID0001"));

        GraphNode parentNode = (GraphNode) callback.get("ID0001");
        GraphNode childNode = (GraphNode) callback.get("ID0002");

        assertThat("cbJob should index nodes by logical id", parentNode.getID(), is("ID0001"));
        assertThat(
                "cbJob should use the job transformation name",
                parentNode.getName(),
                is("preprocess"));
        assertThat(
                "cbParents should attach the parent node",
                childNode.getParents().contains(parentNode),
                is(true));
        assertThat(
                "cbParents should update the parent's child set",
                parentNode.getChildren().contains(childNode),
                is(true));

        callback.cbChildren("ID0001", Arrays.asList("ID0002"));

        assertThat(
                "cbChildren should currently repopulate the parent's child set with the parent node",
                parentNode.getChildren().contains(parentNode),
                is(true));
        assertThat(
                "cbChildren should preserve the parent linkage on the child node",
                childNode.getParents().contains(parentNode),
                is(true));
    }

    @Test
    public void testGetConstructedObjectRequiresDoneAndCbDoneAddsDummyRoot() throws Exception {
        DAX2Graph callback = new DAX2Graph();
        callback.initialize(createBag(), "workflow.dax");

        RuntimeException exception =
                assertThrows(
                        RuntimeException.class,
                        callback::getConstructedObject,
                        "Constructed graph should not be available before cbDone");
        assertThat(
                "Guard message should match the current behavior",
                exception.getMessage(),
                is("Method called before the abstract dag  for the partition was fully generated"));

        Job root = createJob("ID0001", "root");
        Job child = createJob("ID0002", "child");
        callback.cbJob(root);
        callback.cbJob(child);
        callback.cbParents("ID0002", Arrays.asList("ID0001"));

        callback.cbDone();

        Map<?, ?> graph = (Map<?, ?>) callback.getConstructedObject();
        GraphNode dummy = (GraphNode) graph.get(DAX2Graph.DUMMY_NODE_ID);
        GraphNode rootNode = (GraphNode) graph.get("ID0001");

        assertThat("cbDone should add a dummy root node", dummy, notNullValue());
        assertThat(
                "cbDone should store the dummy node as mRoot",
                getField(callback, "mRoot"),
                sameInstance(dummy));
        assertThat(
                "dummy root should point at nodes without parents",
                dummy.getChildren().contains(rootNode),
                is(true));
        assertThat(
                "dummy root should not point at non-root nodes",
                dummy.getChildren().contains(graph.get("ID0002")),
                is(false));
    }

    private Job createJob(String logicalId, String txName) {
        Job job = new Job();
        job.setLogicalID(logicalId);
        job.setTransformation(null, txName, null);
        job.setName(logicalId);
        return job;
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

    private boolean getBooleanField(Object target, String name) throws Exception {
        return ((Boolean) getField(target, name)).booleanValue();
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
