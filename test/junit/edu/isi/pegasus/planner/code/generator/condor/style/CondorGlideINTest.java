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
import edu.isi.pegasus.planner.classes.TransferJob;
import edu.isi.pegasus.planner.code.generator.condor.CondorStyle;
import edu.isi.pegasus.planner.code.generator.condor.CondorStyleException;
import edu.isi.pegasus.planner.namespace.Condor;
import edu.isi.pegasus.planner.namespace.ENV;
import edu.isi.pegasus.planner.namespace.Pegasus;
import org.junit.jupiter.api.Test;

/** Tests for the CondorGlideIN style class. */
public class CondorGlideINTest {

    private static final class TestCondorGlideIN extends CondorGlideIN {
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
    public void testCondorGlideINExtendsAbstract() {
        assertThat(Abstract.class.isAssignableFrom(CondorGlideIN.class), is(true));
    }

    @Test
    public void testCondorGlideINImplementsCondorStyle() {
        assertThat(CondorStyle.class.isAssignableFrom(CondorGlideIN.class), is(true));
    }

    @Test
    public void testStyleNameConstant() {
        assertThat(CondorGlideIN.STYLE_NAME, is("CondorGlideIN"));
    }

    @Test
    public void testInstantiation() {
        CondorGlideIN style = new CondorGlideIN();
        assertThat(style, is(org.hamcrest.Matchers.notNullValue()));
    }

    @Test
    public void testApplyForVanillaUniverseSetsDirectoryAndTransferDefaults() throws Exception {
        TestCondorGlideIN style = new TestCondorGlideIN();
        Job job = new Job();
        job.setDirectory("/scratch/work");

        style.apply(job);

        assertThat(job.vdsNS.get(Pegasus.CHANGE_DIR_KEY), is("true"));
        assertThat(job.condorVariables.get("remote_initialdir"), is("/scratch/work"));
        assertThat(job.envVariables.get(ENV.PEGASUS_SCRATCH_DIR_KEY), is("/scratch/work"));
        assertThat(job.condorVariables.get("should_transfer_files"), is("YES"));
        assertThat(job.condorVariables.get(Condor.WHEN_TO_TRANSFER_OUTPUT_KEY), is("ON_EXIT"));
        assertThat(job.condorVariables.get(Condor.UNIVERSE_KEY), is(Condor.VANILLA_UNIVERSE));
        assertThat(style.credentialsApplied(), is(true));
    }

    @Test
    public void testApplyPreservesExplicitWhenToTransferOutput() throws Exception {
        TestCondorGlideIN style = new TestCondorGlideIN();
        Job job = new Job();
        job.condorVariables.construct(Condor.WHEN_TO_TRANSFER_OUTPUT_KEY, "ON_SUCCESS");

        style.apply(job);

        assertThat(job.condorVariables.get(Condor.WHEN_TO_TRANSFER_OUTPUT_KEY), is("ON_SUCCESS"));
    }

    @Test
    public void testApplyForTransferJobDoesNotSetChangeDirOrScratchEnv() throws Exception {
        TestCondorGlideIN style = new TestCondorGlideIN();
        TransferJob job = new TransferJob();
        job.setDirectory("/scratch/work");

        style.apply(job);

        assertThat(job.vdsNS.containsKey(Pegasus.CHANGE_DIR_KEY), is(false));
        assertThat(job.condorVariables.containsKey("remote_initialdir"), is(false));
        assertThat(job.envVariables.containsKey(ENV.PEGASUS_SCRATCH_DIR_KEY), is(false));
        assertThat(job.condorVariables.get("should_transfer_files"), is("YES"));
        assertThat(job.condorVariables.get(Condor.WHEN_TO_TRANSFER_OUTPUT_KEY), is("ON_EXIT"));
        assertThat(style.credentialsApplied(), is(true));
    }

    @Test
    public void testApplyRejectsUnsupportedUniverse() {
        TestCondorGlideIN style = new TestCondorGlideIN();
        Job job = new Job();
        job.condorVariables.construct(Condor.UNIVERSE_KEY, Condor.GRID_UNIVERSE);

        CondorStyleException e = assertThrows(CondorStyleException.class, () -> style.apply(job));

        assertThat(e.getMessage(), containsString(CondorGlideIN.STYLE_NAME));
        assertThat(style.credentialsApplied(), is(false));
    }
}
