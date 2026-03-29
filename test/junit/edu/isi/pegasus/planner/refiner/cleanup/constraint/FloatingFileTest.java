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
package edu.isi.pegasus.planner.refiner.cleanup.constraint;

import static org.junit.jupiter.api.Assertions.*;

import edu.isi.pegasus.planner.classes.PegasusFile;
import edu.isi.pegasus.planner.partitioner.graph.GraphNode;
import java.util.HashSet;
import java.util.Set;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Tests for {@link FloatingFile}. */
public class FloatingFileTest {

    @BeforeAll
    public static void setUpClass() {}

    @AfterAll
    public static void tearDownClass() {}

    @BeforeEach
    public void setUp() {}

    @AfterEach
    public void tearDown() {}

    @Test
    public void testConstructorSetsDependencies() {
        Set<GraphNode> deps = new HashSet<>();
        deps.add(new GraphNode("node-1"));
        PegasusFile pf = new PegasusFile("output.txt");
        FloatingFile ff = new FloatingFile(deps, pf);
        assertSame(deps, ff.dependencies);
    }

    @Test
    public void testConstructorSetsFile() {
        Set<GraphNode> deps = new HashSet<>();
        PegasusFile pf = new PegasusFile("data.bin");
        FloatingFile ff = new FloatingFile(deps, pf);
        assertSame(pf, ff.file);
    }

    @Test
    public void testFileReturnsCorrectLFN() {
        Set<GraphNode> deps = new HashSet<>();
        PegasusFile pf = new PegasusFile("result.txt");
        FloatingFile ff = new FloatingFile(deps, pf);
        assertEquals("result.txt", ff.file.getLFN());
    }

    @Test
    public void testEmptyDependenciesAllowed() {
        Set<GraphNode> deps = new HashSet<>();
        PegasusFile pf = new PegasusFile("root-output.txt");
        FloatingFile ff = new FloatingFile(deps, pf);
        assertTrue(ff.dependencies.isEmpty());
    }

    @Test
    public void testMultipleDependencies() {
        Set<GraphNode> deps = new HashSet<>();
        deps.add(new GraphNode("parent-1"));
        deps.add(new GraphNode("parent-2"));
        deps.add(new GraphNode("parent-3"));
        PegasusFile pf = new PegasusFile("output.txt");
        FloatingFile ff = new FloatingFile(deps, pf);
        assertEquals(3, ff.dependencies.size());
    }
}
