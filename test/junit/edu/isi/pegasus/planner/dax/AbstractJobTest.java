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
package edu.isi.pegasus.planner.dax;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import org.junit.jupiter.api.Test;

/** Tests for the AbstractJob class, exercised via the concrete Job and DAG subclasses. */
public class AbstractJobTest {

    @Test
    public void testJobExtendsAbstractJob() {
        Job job = new Job("ID001", "test");
        assertInstanceOf(AbstractJob.class, job, "Job should extend AbstractJob");
    }

    @Test
    public void testDAGExtendsAbstractJob() {
        DAG dag = new DAG("DAG001", "workflow.dag");
        assertInstanceOf(AbstractJob.class, dag, "DAG should extend AbstractJob");
    }

    @Test
    public void testDAXExtendsAbstractJob() {
        DAX dax = new DAX("DAX001", "workflow.dax");
        assertInstanceOf(AbstractJob.class, dax, "DAX should extend AbstractJob");
    }

    @Test
    public void testAddArgument() {
        Job job = new Job("ID001", "test");
        job.addArgument("--input");
        List args = job.getArguments();
        assertFalse(args.isEmpty(), "Arguments should not be empty after addArgument");
        assertEquals("--input", args.get(0), "First argument should match");
    }

    @Test
    public void testAddMultipleArguments() {
        Job job = new Job("ID001", "test");
        job.addArgument("-a").addArgument("-b").addArgument("-c");
        List args = job.getArguments();
        assertEquals(3, args.size(), "Should have 3 arguments");
    }

    @Test
    public void testAddArgumentWithFile() {
        Job job = new Job("ID001", "test");
        job.addArgument(new File("input.txt"));
        List args = job.getArguments();
        assertEquals(1, args.size(), "Should have 1 argument (file)");
        assertInstanceOf(File.class, args.get(0), "Argument should be a File instance");
    }

    @Test
    public void testAddProfile() {
        Job job = new Job("ID001", "test");
        job.addProfile("pegasus", "runtime", "100");
        List<Profile> profiles = job.getProfiles();
        assertFalse(profiles.isEmpty(), "Profiles should not be empty after addProfile");
    }

    @Test
    public void testAddInvoke() {
        Job job = new Job("ID001", "test");
        job.addInvoke(Invoke.WHEN.start, "/usr/bin/notify");
        List<Invoke> invokes = job.getInvoke();
        assertEquals(1, invokes.size(), "Should have 1 invoke");
    }

    @Test
    public void testEmptyArgumentsInitially() {
        Job job = new Job("ID001", "test");
        List args = job.getArguments();
        assertTrue(args.isEmpty(), "Arguments should be empty initially");
    }

    @Test
    public void testAddFileToUses() {
        Job job = new Job("ID001", "test");
        File f = new File("output.txt", File.LINK.OUTPUT);
        job.uses(f, File.LINK.OUTPUT);
        assertFalse(job.getUses().isEmpty(), "Uses should not be empty after adding file");
    }
}
