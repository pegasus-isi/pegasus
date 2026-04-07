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

import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.*;

import edu.isi.pegasus.planner.classes.PegasusFile;
import edu.isi.pegasus.planner.partitioner.graph.GraphNode;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

/** Tests for {@link Choice}. */
public class ChoiceTest {

    private Choice makeChoice(long isr, long balance) {
        List<GraphNode> jobs = new ArrayList<>();
        jobs.add(new GraphNode("job-1"));
        Map<Long, List<FloatingFile>> ff = new HashMap<>();
        return new Choice(isr, balance, jobs, ff);
    }

    @Test
    public void testIntermediateSpaceRequirement() {
        Choice c = makeChoice(1024L, 512L);
        assertThat(c.intermediateSpaceRequirement, is(1024L));
    }

    @Test
    public void testBalance() {
        Choice c = makeChoice(1024L, 512L);
        assertThat(c.balance, is(512L));
    }

    @Test
    public void testListOfJobsSize() {
        Choice c = makeChoice(0L, 0L);
        assertThat(c.listOfJobs.size(), is(1));
    }

    @Test
    public void testEmptyFloatingFiles() {
        Choice c = makeChoice(0L, 0L);
        assertThat(c.floatingFiles.isEmpty(), is(true));
    }

    @Test
    public void testToStringContainsJobId() {
        Choice c = makeChoice(100L, 50L);
        String s = c.toString();
        assertThat(s, containsString("job-1"));
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
        assertThat(c.floatingFiles.size(), is(1));
    }

    @Test
    public void testConstructorKeepsProvidedCollectionReferences() {
        List<GraphNode> jobs = new ArrayList<>();
        Map<Long, List<FloatingFile>> ff = new HashMap<>();

        Choice c = new Choice(1L, 2L, jobs, ff);

        assertThat(c.listOfJobs, sameInstance(jobs));
        assertThat(c.floatingFiles, sameInstance(ff));
    }

    @Test
    public void testToStringExactFormatWithEmptyFloatingFiles() {
        Choice c = makeChoice(7L, 3L);
        assertThat(
                c.toString(),
                is(
                        "Choice{intermediateSpaceRequirement=7, balance=3, listOfJobs={job-1}, floatingFiles={}}"));
    }

    @Test
    public void testToStringContainsFloatingFileAndReservationKey() {
        List<GraphNode> jobs = new ArrayList<>();
        jobs.add(new GraphNode("job-B"));

        Map<Long, List<FloatingFile>> ff = new HashMap<>();
        List<FloatingFile> ffList = new ArrayList<>();
        ffList.add(new FloatingFile(new HashSet<GraphNode>(), new PegasusFile("data.dat")));
        ff.put(42L, ffList);

        String value = new Choice(9L, 4L, jobs, ff).toString();

        assertThat(value, allOf(containsString("data.dat:42"), containsString("job-B")));
    }
}
