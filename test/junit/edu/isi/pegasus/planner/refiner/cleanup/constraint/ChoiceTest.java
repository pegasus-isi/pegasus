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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Tests for {@link Choice}. */
public class ChoiceTest {

    @BeforeAll
    public static void setUpClass() {}

    @AfterAll
    public static void tearDownClass() {}

    @BeforeEach
    public void setUp() {}

    @AfterEach
    public void tearDown() {}

    private Choice makeChoice(long isr, long balance) {
        List<GraphNode> jobs = new ArrayList<>();
        jobs.add(new GraphNode("job-1"));
        Map<Long, List<FloatingFile>> ff = new HashMap<>();
        return new Choice(isr, balance, jobs, ff);
    }

    @Test
    public void testIntermediateSpaceRequirement() {
        Choice c = makeChoice(1024L, 512L);
        assertEquals(1024L, c.intermediateSpaceRequirement);
    }

    @Test
    public void testBalance() {
        Choice c = makeChoice(1024L, 512L);
        assertEquals(512L, c.balance);
    }

    @Test
    public void testListOfJobsSize() {
        Choice c = makeChoice(0L, 0L);
        assertEquals(1, c.listOfJobs.size());
    }

    @Test
    public void testEmptyFloatingFiles() {
        Choice c = makeChoice(0L, 0L);
        assertTrue(c.floatingFiles.isEmpty());
    }

    @Test
    public void testToStringContainsJobId() {
        Choice c = makeChoice(100L, 50L);
        String s = c.toString();
        assertTrue(s.contains("job-1"));
    }

    @Test
    public void testChoiceWithFloatingFiles() {
        List<GraphNode> jobs = new ArrayList<>();
        jobs.add(new GraphNode("job-A"));
        Map<Long, List<FloatingFile>> ff = new HashMap<>();
        Set<GraphNode> deps = new HashSet<>();
        PegasusFile pf = new PegasusFile("file.txt");
        List<FloatingFile> ffList = new ArrayList<>();
        ffList.add(new FloatingFile(deps, pf));
        ff.put(100L, ffList);
        Choice c = new Choice(200L, 100L, jobs, ff);
        assertEquals(1, c.floatingFiles.size());
    }
}
