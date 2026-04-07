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
import java.util.Arrays;
import java.util.HashMap;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

/** @author Rajiv Mayani */
public class HorizontalTest {

    @Test
    public void testDescriptionReturnsConstantDescription() {
        Horizontal horizontal =
                new Horizontal(
                        new GraphNode("root"),
                        new HashMap<>(),
                        PegasusProperties.nonSingletonInstance());

        assertThat(horizontal.description(), is(Horizontal.DESCRIPTION));
    }

    @Test
    public void testConstructorInitializesPartitionMapAndCounter() throws Exception {
        Horizontal horizontal =
                new Horizontal(
                        new GraphNode("root"),
                        new HashMap<>(),
                        PegasusProperties.nonSingletonInstance());

        assertThat(ReflectionTestUtils.getField(horizontal, "mPartitionMap"), is(notNullValue()));
        assertThat(
                ReflectionTestUtils.getField(horizontal, "mPartitionMap") instanceof HashMap,
                is(true));
        assertThat(ReflectionTestUtils.getField(horizontal, "mIDCounter"), is(0));
    }

    @Test
    public void testGetCollapseFactorDefaultsToWholeClusterSize() {
        Horizontal horizontal =
                new Horizontal(
                        new GraphNode("root"),
                        new HashMap<>(),
                        PegasusProperties.nonSingletonInstance());

        int[] result = horizontal.getCollapseFactor("tx", 5);

        assertThat(result[0], is(5));
        assertThat(result[1], is(0));
    }

    @Test
    public void testCreatePartitionAssignsIdIndexAndNodeBagsBeforeCurrentLoggerFailure()
            throws Exception {
        Horizontal horizontal =
                new Horizontal(
                        new GraphNode("root"),
                        new HashMap<>(),
                        PegasusProperties.nonSingletonInstance());
        GraphNode a = new GraphNode("a", "jobA");
        GraphNode b = new GraphNode("b", "jobB");

        java.util.EmptyStackException ex =
                assertThrows(
                        java.util.EmptyStackException.class,
                        () -> horizontal.createPartition(Arrays.asList(a, b)));

        assertThat(ex, is(notNullValue()));
        assertThat(a.getBag().get(LabelBag.PARTITION_KEY), is("ID1"));
        assertThat(b.getBag().get(LabelBag.PARTITION_KEY), is("ID1"));

        assertThat(ReflectionTestUtils.getField(horizontal, "mIDCounter"), is(1));
        HashMap<?, ?> partitionMap =
                (HashMap<?, ?>) ReflectionTestUtils.getField(horizontal, "mPartitionMap");
        assertThat(partitionMap.containsKey("ID1"), is(true));
        Partition partition = (Partition) partitionMap.get("ID1");
        assertThat(partition.getID(), is("ID1"));
        assertThat(partition.getIndex(), is(1));
    }

    @Test
    public void testPrivateGetPartitionIDFormatsIdentifier() throws Exception {
        Horizontal horizontal =
                new Horizontal(
                        new GraphNode("root"),
                        new HashMap<>(),
                        PegasusProperties.nonSingletonInstance());
        Method method = Horizontal.class.getDeclaredMethod("getPartitionID", int.class);
        method.setAccessible(true);

        assertThat(method.invoke(horizontal, 9), is("ID9"));
    }
}
