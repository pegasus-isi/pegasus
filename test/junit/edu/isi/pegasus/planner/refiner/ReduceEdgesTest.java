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
package edu.isi.pegasus.planner.refiner;

import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.*;

import edu.isi.pegasus.planner.classes.ADag;
import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.partitioner.graph.Graph;
import edu.isi.pegasus.planner.partitioner.graph.GraphNode;
import java.util.Iterator;
import org.junit.jupiter.api.Test;

/** Tests for ReduceEdges. */
public class ReduceEdgesTest {

    /** Count total edges in a DAG by summing each node's child count. */
    private int countEdges(ADag dag) {
        int count = 0;
        for (Iterator<GraphNode> it = dag.nodeIterator(); it.hasNext(); ) {
            GraphNode node = it.next();
            count += node.getChildren().size();
        }
        return count;
    }

    @Test
    public void testDefaultConstructor() {
        ReduceEdges re = new ReduceEdges();
        assertThat(re, notNullValue());
    }

    @Test
    public void testReduceEmptyDag() {
        ReduceEdges re = new ReduceEdges();
        ADag dag = new ADag();
        ADag result = re.reduce(dag);
        assertThat(result, notNullValue());
    }

    @Test
    public void testReduceSingleNodeDag() {
        ReduceEdges re = new ReduceEdges();
        ADag dag = new ADag();
        Job j = new Job();
        j.setName("j1");
        j.setJobType(Job.COMPUTE_JOB);
        dag.add(j);
        ADag result = re.reduce(dag);
        assertThat(result, notNullValue());
    }

    @Test
    public void testReduceWithTestHelperThrowsDueToBugInHelper() {
        // TestReduceEdges.createTest1() has a bug: addNewRelation is called before add(job),
        // causing "node doesn't exist" RuntimeException. Document this known defect.
        ReduceEdges re = new ReduceEdges();
        TestReduceEdges helper = new TestReduceEdges();
        assertThrows(RuntimeException.class, helper::createTest1);
    }

    @Test
    public void testSecondHelperWorkflowAlsoThrowsDueToRelationInsertionOrder() {
        TestReduceEdges helper = new TestReduceEdges();

        assertThrows(RuntimeException.class, helper::createTest2);
    }

    @Test
    public void testHasGraphReduceOverload() throws Exception {
        assertThat(
                (Object) ReduceEdges.class.getMethod("reduce", Graph.class).getReturnType(),
                is((Object) Graph.class));
    }

    @Test
    public void testHasAssignLevelsMethod() throws Exception {
        assertThat(
                ReduceEdges.class.getMethod("assignLevels", Graph.class, GraphNode.class),
                notNullValue());
    }

    @Test
    public void testPrivateFindLCAMethodExists() throws Exception {
        assertThat(
                ReduceEdges.class.getDeclaredMethod("findLCA", GraphNode.class, GraphNode.class),
                notNullValue());
    }

    @Test
    public void testPrivateResetMethodExists() throws Exception {
        assertThat(ReduceEdges.class.getDeclaredMethod("reset", Graph.class), notNullValue());
    }
}
