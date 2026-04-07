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

import edu.isi.pegasus.planner.partitioner.graph.GraphNode;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

/** @author Rajiv Mayani */
public class TopologicalTest {

    private Partition simpleChainPartition() {
        GraphNode a = new GraphNode("A");
        GraphNode b = new GraphNode("B");
        GraphNode c = new GraphNode("C");

        b.addParent(a);
        a.addChild(b);
        c.addParent(b);
        b.addChild(c);

        Partition partition = new Partition(Arrays.asList(a, b, c), "ID1");
        partition.constructPartition();
        return partition;
    }

    @Test
    public void testInitializeBuildsExpectedIndegreeArray() throws Exception {
        Topological topological = new Topological(simpleChainPartition());

        assertArrayEquals(
                new int[] {0, 1, 1},
                (int[]) ReflectionTestUtils.getField(topological, "mInDegree"));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testChildrenRepresentationBuildsAdjacencyLists() {
        Topological topological = new Topological(simpleChainPartition());

        Map<String, List<String>> children = topological.childrenRepresentation();

        assertThat(children.get("A"), is(Arrays.asList("B")));
        assertThat(children.get("B"), is(Arrays.asList("C")));
        assertThat(children.containsKey("C"), is(false));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testSortReturnsTopologicalOrderForSimpleChain() {
        Topological topological = new Topological(simpleChainPartition());

        List<String> order = topological.sort();

        assertThat(order, is(Arrays.asList("A", "B", "C")));
    }

    @Test
    public void testPrivateIndexReturnsMappedArrayPosition() throws Exception {
        Topological topological = new Topological(simpleChainPartition());
        Method method = Topological.class.getDeclaredMethod("index", String.class);
        method.setAccessible(true);

        assertThat(method.invoke(topological, "A"), is(0));
        assertThat(method.invoke(topological, "B"), is(1));
        assertThat(method.invoke(topological, "C"), is(2));
    }
}
