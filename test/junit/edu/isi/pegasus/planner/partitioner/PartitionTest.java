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
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;

/** @author Rajiv Mayani */
public class PartitionTest {

    @Test
    public void testConstructPartitionKeepsOnlyInternalParents() {
        GraphNode external = new GraphNode("external");
        GraphNode alpha = new GraphNode("alpha");
        GraphNode beta = new GraphNode("beta");

        beta.addParent(alpha);
        beta.addParent(external);

        Partition partition = new Partition(Arrays.asList(alpha, beta), "p1");

        partition.constructPartition();

        assertThat(partition.getParents("beta"), is(Arrays.asList("alpha")));
        assertThat(partition.getParents("alpha").isEmpty(), is(true));
    }

    @Test
    public void testGetRootNodesReturnsNodesWithoutInternalParents() {
        GraphNode alpha = new GraphNode("alpha");
        GraphNode beta = new GraphNode("beta");
        GraphNode gamma = new GraphNode("gamma");

        beta.addParent(alpha);
        gamma.addParent(beta);

        Partition partition = new Partition(Arrays.asList(alpha, beta, gamma), "p1");

        partition.constructPartition();

        List roots = partition.getRootNodes();
        assertThat(roots.size(), is(1));
        assertThat(roots.get(0), is(notNullValue()));
    }

    @Test
    public void testAddParentsOnlyUpdatesNodesAlreadyInPartition() {
        GraphNode alpha = new GraphNode("alpha");
        Partition partition = new Partition(Arrays.asList(alpha), "p1");

        partition.addParents("alpha", Arrays.asList("parent-1", "parent-2"));
        partition.addParents("missing", Collections.singletonList("ignored"));

        assertThat(partition.getParents("alpha"), is(Arrays.asList("parent-1", "parent-2")));
        assertThat(partition.getParents("missing").isEmpty(), is(true));
        assertThat(partition.getRelations().containsKey("missing"), is(false));
    }

    @Test
    public void testToXMLRendersJobsAndDependencies() throws IOException {
        GraphNode alpha = new GraphNode("alpha");
        GraphNode beta = new GraphNode("beta");
        beta.addParent(alpha);

        Partition partition = new Partition(Arrays.asList(alpha, beta), "p1");
        partition.setName("wf");
        partition.setIndex(7);
        partition.constructPartition();

        String xml = partition.toXML();

        assertThat(xml.contains("<partition name=\"wf\" index=\"7\" id=\"p1\">"), is(true));
        assertThat(xml.contains("<job name=\"\" id=\"alpha\"/>"), is(true));
        assertThat(xml.contains("<job name=\"\" id=\"beta\"/>"), is(true));
        assertThat(xml.contains("<child ref=\"beta\">"), is(true));
        assertThat(xml.contains("<parent ref=\"alpha\"/>"), is(true));
    }

    @Test
    public void testCloneThrowsCloneNotSupportedException() {
        Partition partition = new Partition();

        CloneNotSupportedException exception =
                assertThrows(CloneNotSupportedException.class, () -> partition.clone());

        assertThat(exception.getMessage(), is("Clone method not implemented in Partition"));
    }
}
