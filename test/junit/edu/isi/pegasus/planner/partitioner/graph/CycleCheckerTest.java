/*
 * Copyright 2007-2016 University Of Southern California
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.isi.pegasus.planner.partitioner.graph;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.planner.classes.NameValue;
import edu.isi.pegasus.planner.test.DefaultTestSetup;
import edu.isi.pegasus.planner.test.TestSetup;
import java.util.LinkedList;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit test class for checking cycles in a DAG
 *
 * @author Karan Vahi
 */
public class CycleCheckerTest {

    private LogManager mLogger;

    private TestSetup mTestSetup;

    private int mTestNumber = 1;

    @BeforeAll
    public static void setUpAll() {}

    @AfterAll
    public static void tearDownClass() {}

    @BeforeEach
    public void setUp() {
        mTestSetup = new DefaultTestSetup();

        mTestSetup.setInputDirectory(this.getClass());
        mLogger =
                mTestSetup.loadLogger(
                        mTestSetup.loadPropertiesFromFile(".properties", new LinkedList()));
        mLogger.logEventStart("test.planner.partitioner.graph.CycleChecker", "setup", "0");
    }

    @Test
    public void testEmptyWorkflow() {

        mLogger.logEventStart(
                "test.planner.partitioner.graph.CycleChecker",
                "set",
                Integer.toString(mTestNumber++));
        Graph g = new MapGraph();

        CycleChecker c = new CycleChecker(g);
        boolean cyclic = c.hasCycles();
        assertEquals(false, cyclic, "Input Test Case should be determined cycle free");
        mLogger.logEventCompletion();
    }

    @Test
    public void testSingleNode() {

        mLogger.logEventStart(
                "test.planner.partitioner.graph.CycleChecker",
                "set",
                Integer.toString(mTestNumber++));
        Graph g = new MapGraph();

        g.addNode(new GraphNode("A", "A"));

        CycleChecker c = new CycleChecker(g);
        boolean cyclic = c.hasCycles();
        assertEquals(false, cyclic, "Input Test Case should be determined cycle free");
        mLogger.logEventCompletion();
    }

    @Test
    public void testSingleNodeCyclic() {

        mLogger.logEventStart(
                "test.planner.partitioner.graph.CycleChecker",
                "set",
                Integer.toString(mTestNumber++));
        Graph g = new MapGraph();

        g.addNode(new GraphNode("A", "A"));
        assertThrows(IllegalArgumentException.class, () -> g.addEdge("A", "A"));
        mLogger.logEventCompletion();
    }

    @Test
    public void testBlackDiamond() {

        mLogger.logEventStart(
                "test.planner.partitioner.graph.CycleChecker",
                "set",
                Integer.toString(mTestNumber++));
        Graph g = new MapGraph();

        g.addNode(new GraphNode("A", "A"));
        g.addNode(new GraphNode("B", "B"));
        g.addNode(new GraphNode("C", "C"));
        g.addNode(new GraphNode("D", "D"));

        g.addEdge("A", "B");
        g.addEdge("A", "C");
        g.addEdge("B", "D");
        g.addEdge("C", "D");

        CycleChecker c = new CycleChecker(g);
        boolean cyclic = c.hasCycles();
        assertEquals(false, cyclic, "Input Test Case should be determined cycle free");
        mLogger.logEventCompletion();
    }

    @Test
    public void testBlackDiamondWholeCyclic() {

        mLogger.logEventStart(
                "test.planner.partitioner.graph.CycleChecker",
                "set",
                Integer.toString(mTestNumber++));
        Graph g = new MapGraph();

        g.addNode(new GraphNode("A", "A"));
        g.addNode(new GraphNode("B", "B"));
        g.addNode(new GraphNode("C", "C"));
        g.addNode(new GraphNode("D", "D"));

        g.addEdge("A", "B");
        g.addEdge("A", "C");
        g.addEdge("B", "D");
        g.addEdge("C", "D");
        g.addEdge("D", "A");

        CycleChecker c = new CycleChecker(g);
        boolean cyclic = c.hasCycles();
        assertEquals(true, cyclic, "Input Test Case should be determined to be cyclic");

        if (cyclic) {
            // cyclic edge is null since the whole workflow constitutes a cycle
            NameValue cyclicEdge = c.getCyclicEdge();
            assertEquals(null, cyclicEdge, "Cyclic Edge does not match");
        }

        mLogger.logEventCompletion();
    }

    @Test
    public void testBlackDiamondCyclic() {

        mLogger.logEventStart(
                "test.planner.partitioner.graph.CycleChecker",
                "set",
                Integer.toString(mTestNumber++));
        Graph g = new MapGraph();

        g.addNode(new GraphNode("A", "A"));
        g.addNode(new GraphNode("B", "B"));
        g.addNode(new GraphNode("C", "C"));
        g.addNode(new GraphNode("D", "D"));
        g.addNode(new GraphNode("E", "E"));

        g.addEdge("A", "B");
        g.addEdge("A", "C");
        g.addEdge("B", "D");
        g.addEdge("C", "D");
        g.addEdge("D", "E");
        g.addEdge("E", "D");

        CycleChecker c = new CycleChecker(g);
        boolean cyclic = c.hasCycles();
        assertEquals(true, cyclic, "Input Test Case should be determined to be cyclic");

        if (cyclic) {
            // cyclic edge is null since the whole workflow constitutes a cycle
            NameValue cyclicEdge = c.getCyclicEdge();
            assertEquals(new NameValue("E", "D"), cyclicEdge, "Cyclic Edge does not match");
        }

        mLogger.logEventCompletion();
    }

    @Test
    public void testPipeline() {

        mLogger.logEventStart(
                "test.planner.partitioner.graph.CycleChecker",
                "set",
                Integer.toString(mTestNumber++));
        Graph g = new MapGraph();

        g.addNode(new GraphNode("A", "A"));
        g.addNode(new GraphNode("B", "B"));
        g.addNode(new GraphNode("C", "C"));
        g.addNode(new GraphNode("D", "D"));
        g.addNode(new GraphNode("E", "E"));

        g.addEdge("A", "B");
        g.addEdge("B", "C");
        g.addEdge("C", "D");
        g.addEdge("D", "E");

        CycleChecker c = new CycleChecker(g);
        boolean cyclic = c.hasCycles();
        assertEquals(false, cyclic, "Input Test Case should be determined cycle free");

        mLogger.logEventCompletion();
    }

    @Test
    public void testPipelineCyclic() {

        mLogger.logEventStart(
                "test.planner.partitioner.graph.CycleChecker",
                "set",
                Integer.toString(mTestNumber++));
        Graph g = new MapGraph();

        g.addNode(new GraphNode("A", "A"));
        g.addNode(new GraphNode("B", "B"));
        g.addNode(new GraphNode("C", "C"));
        g.addNode(new GraphNode("D", "D"));
        g.addNode(new GraphNode("E", "E"));

        g.addEdge("A", "B");
        g.addEdge("B", "C");
        g.addEdge("C", "D");
        g.addEdge("D", "E");
        g.addEdge("E", "B");

        CycleChecker c = new CycleChecker(g);
        boolean cyclic = c.hasCycles();
        assertEquals(true, cyclic, "Input Test Case should be determined to be cyclic");

        if (cyclic) {
            // cyclic edge is null since the whole workflow constitutes a cycle
            NameValue cyclicEdge = c.getCyclicEdge();
            assertEquals(new NameValue("E", "B"), cyclicEdge, "Cyclic Edge does not match");
        }

        mLogger.logEventCompletion();
    }

    @Test
    public void testPipelineForest() {

        mLogger.logEventStart(
                "test.planner.partitioner.graph.CycleChecker",
                "set",
                Integer.toString(mTestNumber++));
        Graph g = new MapGraph();

        g.addNode(new GraphNode("A", "A"));
        g.addNode(new GraphNode("B", "B"));
        g.addNode(new GraphNode("C", "C"));
        g.addNode(new GraphNode("D", "D"));
        g.addNode(new GraphNode("E", "E"));

        g.addEdge("A", "B");
        g.addEdge("B", "C");
        g.addEdge("C", "D");
        g.addEdge("D", "E");

        g.addNode(new GraphNode("A'", "A'"));
        g.addNode(new GraphNode("B'", "B'"));
        g.addNode(new GraphNode("C'", "C'"));
        g.addNode(new GraphNode("D'", "D'"));
        g.addNode(new GraphNode("E'", "E'"));

        g.addEdge("A'", "B'");
        g.addEdge("B'", "C'");
        g.addEdge("C'", "D'");
        g.addEdge("D'", "E'");
        g.addEdge("E'", "B'");

        CycleChecker c = new CycleChecker(g);
        boolean cyclic = c.hasCycles();
        assertEquals(true, cyclic, "Input Test Case should be determined to be cyclic");

        if (cyclic) {
            // cyclic edge is null since the whole workflow constitutes a cycle
            NameValue cyclicEdge = c.getCyclicEdge();
            assertEquals(new NameValue("E'", "B'"), cyclicEdge, "Cyclic Edge does not match");
        }

        mLogger.logEventCompletion();
    }
}
