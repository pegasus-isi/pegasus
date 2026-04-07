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

import edu.isi.pegasus.planner.common.PegasusProperties;
import edu.isi.pegasus.planner.partitioner.graph.GraphNode;
import edu.isi.pegasus.planner.partitioner.graph.LabelBag;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

/** @author Rajiv Mayani */
public class LabelTest {

    @Test
    public void testDescriptionReturnsConstantDescription() {
        Label label =
                new Label(
                        new GraphNode("root"),
                        new HashMap<>(),
                        PegasusProperties.nonSingletonInstance());

        assertThat(label.description(), is(Label.DESCRIPTION));
    }

    @Test
    public void testConstructorInitializesPartitionMapAndQueue() throws Exception {
        Label label =
                new Label(
                        new GraphNode("root"),
                        new HashMap<>(),
                        PegasusProperties.nonSingletonInstance());

        Object partitionMap = ReflectionTestUtils.getField(label, "mPartitionMap");
        Object queue = ReflectionTestUtils.getField(label, "mQueue");

        assertThat(partitionMap, is(notNullValue()));
        assertThat(partitionMap instanceof Map, is(true));
        assertThat(queue, is(notNullValue()));
        assertThat(queue instanceof LinkedList, is(true));
    }

    @Test
    public void testPrivateGetPartitionIDFormatsIdentifier() throws Exception {
        Label label =
                new Label(
                        new GraphNode("root"),
                        new HashMap<>(),
                        PegasusProperties.nonSingletonInstance());
        Method method = Label.class.getDeclaredMethod("getPartitionID", int.class);
        method.setAccessible(true);

        assertThat(method.invoke(label, 7), is("ID7"));
    }

    @Test
    public void testPrivateGetLabelUsesBagValueOrFallsBackToNodeId() throws Exception {
        Label label =
                new Label(
                        new GraphNode("root"),
                        new HashMap<>(),
                        PegasusProperties.nonSingletonInstance());
        Method method = Label.class.getDeclaredMethod("getLabel", GraphNode.class);
        method.setAccessible(true);

        GraphNode unlabeled = new GraphNode("jobA");
        unlabeled.setBag(new LabelBag());
        assertThat(method.invoke(label, unlabeled), is("jobA"));

        GraphNode labeled = new GraphNode("jobB");
        LabelBag bag = new LabelBag();
        bag.add(LabelBag.LABEL_KEY, "shared-label");
        labeled.setBag(bag);
        assertThat(method.invoke(label, labeled), is("shared-label"));
    }
}
