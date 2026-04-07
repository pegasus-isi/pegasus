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
package edu.isi.pegasus.planner.code.generator.condor.style;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.code.generator.condor.CondorStyle;
import edu.isi.pegasus.planner.code.generator.condor.CondorStyleException;
import edu.isi.pegasus.planner.namespace.Condor;
import edu.isi.pegasus.planner.namespace.ENV;
import edu.isi.pegasus.planner.namespace.Pegasus;
import org.junit.jupiter.api.Test;

/** Tests for the CondorGlideinWMS style class. */
public class CondorGlideinWMSTest {

    private static final class TestCondorGlideinWMS extends CondorGlideinWMS {
        private boolean mCredentialsApplied;

        @Override
        protected void applyCredentialsForRemoteExec(Job job) throws CondorStyleException {
            mCredentialsApplied = true;
        }

        boolean credentialsApplied() {
            return mCredentialsApplied;
        }
    }

    @Test
    public void testCondorGlideinWMSExtendsCondor() {
        assertThat(
                edu.isi.pegasus.planner.code.generator.condor.style.Condor.class.isAssignableFrom(
                        CondorGlideinWMS.class),
                is(true));
    }

    @Test
    public void testCondorGlideinWMSImplementsCondorStyle() {
        assertThat(CondorStyle.class.isAssignableFrom(CondorGlideinWMS.class), is(true));
    }

    @Test
    public void testStyleNameConstant() {
        assertThat(CondorGlideinWMS.STYLE_NAME, is("CondorGlideinWMS"));
    }

    @Test
    public void testInstantiation() {
        CondorGlideinWMS style = new CondorGlideinWMS();
        assertThat(style, is(org.hamcrest.Matchers.notNullValue()));
    }

    @Test
    public void testApplySetsGlideinRequirementsRankAndTransferDefaults() throws Exception {
        TestCondorGlideinWMS style = new TestCondorGlideinWMS();
        Job job = new Job();
        job.setSiteHandle("remote");
        job.setDirectory("/scratch/work");

        style.apply(job);

        assertThat(job.condorVariables.get(Condor.UNIVERSE_KEY), is(Condor.VANILLA_UNIVERSE));
        assertThat(job.vdsNS.get(Pegasus.CHANGE_DIR_KEY), is("true"));
        assertThat(job.condorVariables.get("remote_initialdir"), is("/scratch/work"));
        assertThat(job.envVariables.get(ENV.PEGASUS_SCRATCH_DIR_KEY), is("/scratch/work"));
        assertThat(job.condorVariables.get("should_transfer_files"), is("YES"));
        assertThat(job.condorVariables.get(Condor.WHEN_TO_TRANSFER_OUTPUT_KEY), is("ON_EXIT"));
        assertThat(
                job.condorVariables.get("requirements"),
                is(
                        "(IS_MONITOR_VM == False) && (Arch != \"\") && (OpSys != \"\") && (Disk != -42)"
                                + " && (Memory > 1) && (FileSystemDomain != \"\")"));
        assertThat(job.condorVariables.get("rank"), is("DaemonStartTime"));
        assertThat(job.condorVariables.get("+WantIOProxy"), is("True"));
        assertThat(style.credentialsApplied(), is(true));
    }

    @Test
    public void testApplyPreservesExplicitWhenToTransferOutput() throws Exception {
        TestCondorGlideinWMS style = new TestCondorGlideinWMS();
        Job job = new Job();
        job.setSiteHandle("remote");
        job.condorVariables.construct(Condor.WHEN_TO_TRANSFER_OUTPUT_KEY, "ON_SUCCESS");

        style.apply(job);

        assertThat(job.condorVariables.get(Condor.WHEN_TO_TRANSFER_OUTPUT_KEY), is("ON_SUCCESS"));
    }

    @Test
    public void testApplyWithStandardUniverseUsesInitialdirButStillAddsGlideinKnobs()
            throws Exception {
        TestCondorGlideinWMS style = new TestCondorGlideinWMS();
        Job job = new Job();
        job.setSiteHandle("remote");
        job.setDirectory("/scratch/work");
        job.setJobType(Job.COMPUTE_JOB);
        job.condorVariables.construct(Condor.UNIVERSE_KEY, Condor.STANDARD_UNIVERSE);

        style.apply(job);

        assertThat(job.condorVariables.get(Condor.UNIVERSE_KEY), is(Condor.STANDARD_UNIVERSE));
        assertThat(job.condorVariables.get("initialdir"), is("/scratch/work"));
        assertThat(job.condorVariables.containsKey("remote_initialdir"), is(false));
        assertThat(job.condorVariables.get("rank"), is("DaemonStartTime"));
        assertThat(style.credentialsApplied(), is(true));
    }

    @Test
    public void testApplyRejectsUnsupportedUniverse() {
        TestCondorGlideinWMS style = new TestCondorGlideinWMS();
        Job job = new Job();
        job.setSiteHandle("remote");
        job.condorVariables.construct(Condor.UNIVERSE_KEY, Condor.GRID_UNIVERSE);

        CondorStyleException e = assertThrows(CondorStyleException.class, () -> style.apply(job));

        assertThat(e.getMessage(), containsString("(Condor,grid,remote)"));
        assertThat(style.credentialsApplied(), is(false));
    }
}
