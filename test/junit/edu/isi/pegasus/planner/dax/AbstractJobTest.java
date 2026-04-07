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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import org.junit.jupiter.api.Test;

/** Tests for the AbstractJob class, exercised via the concrete Job and DAG subclasses. */
public class AbstractJobTest {

    @Test
    public void testJobExtendsAbstractJob() {
        Job job = new Job("ID001", "test");
        assertThat(job, instanceOf(AbstractJob.class));
    }

    @Test
    public void testDAGExtendsAbstractJob() {
        DAG dag = new DAG("DAG001", "workflow.dag");
        assertThat(dag, instanceOf(AbstractJob.class));
    }

    @Test
    public void testDAXExtendsAbstractJob() {
        DAX dax = new DAX("DAX001", "workflow.dax");
        assertThat(dax, instanceOf(AbstractJob.class));
    }

    @Test
    public void testAddArgument() {
        Job job = new Job("ID001", "test");
        job.addArgument("--input");
        List args = job.getArguments();
        assertThat(args.isEmpty(), is(false));
        assertThat(args.get(0), is("--input"));
    }

    @Test
    public void testAddMultipleArguments() {
        Job job = new Job("ID001", "test");
        job.addArgument("-a").addArgument("-b").addArgument("-c");
        List args = job.getArguments();
        assertThat(args.size(), is(3));
    }

    @Test
    public void testAddArgumentWithFile() {
        Job job = new Job("ID001", "test");
        job.addArgument(new File("input.txt"));
        List args = job.getArguments();
        assertThat(args.size(), is(1));
        assertThat(args.get(0), instanceOf(File.class));
    }

    @Test
    public void testAddProfile() {
        Job job = new Job("ID001", "test");
        job.addProfile("pegasus", "runtime", "100");
        List<Profile> profiles = job.getProfiles();
        assertThat(profiles.isEmpty(), is(false));
    }

    @Test
    public void testAddInvoke() {
        Job job = new Job("ID001", "test");
        job.addInvoke(Invoke.WHEN.start, "/usr/bin/notify");
        List<Invoke> invokes = job.getInvoke();
        assertThat(invokes.size(), is(1));
    }

    @Test
    public void testEmptyArgumentsInitially() {
        Job job = new Job("ID001", "test");
        List args = job.getArguments();
        assertThat(args.isEmpty(), is(true));
    }

    @Test
    public void testAddFileToUses() {
        Job job = new Job("ID001", "test");
        File f = new File("output.txt", File.LINK.OUTPUT);
        job.uses(f, File.LINK.OUTPUT);
        assertThat(job.getUses().isEmpty(), is(false));
    }

    @Test
    public void testAddArgumentIgnoresNullString() {
        Job job = new Job("ID001", "test");

        job.addArgument((String) null);

        assertThat(job.getArguments().isEmpty(), is(true));
    }

    @Test
    public void testAddArgumentWithFileArrayAndDelimiters() {
        Job job = new Job("ID001", "test");
        File[] files = {new File("f.a1", File.LINK.INPUT), new File("f.a2", File.LINK.INPUT)};

        job.addArgument("-i", files, "=", ",");
        List args = job.getArguments();

        assertThat(args.size(), is(4));
        assertThat(args.get(0), is("-i="));
        assertThat(args.get(1), is(files[0]));
        assertThat(args.get(2), is(","));
        assertThat(args.get(3), is(files[1]));
    }

    @Test
    public void testSetStdinWithFlagsPropagatesToFile() {
        Job job = new Job("ID001", "test");

        job.setStdin("stdin.txt", File.TRANSFER.OPTIONAL, false, true);
        File stdin = job.getStdin();

        assertThat(stdin.getName(), is("stdin.txt"));
        assertThat(stdin.getLink(), is(File.LINK.INPUT));
        assertThat(stdin.getTransfer(), is(File.TRANSFER.OPTIONAL));
        assertThat(stdin.getRegister(), is(false));
        assertThat(stdin.getOptional(), is(true));
    }

    @Test
    public void testAddInvokeClonesPassedInvokeInstance() {
        Job job = new Job("ID001", "test");
        Invoke invoke = new Invoke(Invoke.WHEN.start, "/bin/date");

        job.addInvoke(invoke);
        List<Invoke> invokes = job.getInvoke();

        assertThat(invokes.size(), is(1));
        assertThat(invokes.get(0), not(sameInstance(invoke)));
        assertThat(invokes.get(0).getWhat(), is(invoke.getWhat()));
        assertThat(invokes.get(0).getWhen(), is(invoke.getWhen()));
    }

    @Test
    public void testNodeLabelEqualsAndHashCodeDependOnId() {
        Job first = new Job("ID001", "first");
        Job second = new Job("ID001", "second");
        Job third = new Job("ID999", "third");

        first.setNodeLabel("label-a");
        second.setNodeLabel("label-b");

        assertThat(first.getNodeLabel(), is("label-a"));
        assertThat(first, is(second));
        assertThat(first.hashCode(), is(second.hashCode()));
        assertThat(first, not(is(third)));
    }
}
