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
package edu.isi.pegasus.planner.cluster.aggregator;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.*;

import edu.isi.pegasus.planner.classes.AggregatedJob;
import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.cluster.JobAggregator;
import edu.isi.pegasus.planner.namespace.Pegasus;
import org.junit.jupiter.api.Test;

/** Tests for the MPIExec aggregator class. */
public class MPIExecTest {

    @Test
    public void testMPIExecExtendsAbstract() {
        assertThat(Abstract.class.isAssignableFrom(MPIExec.class), is(true));
    }

    @Test
    public void testMPIExecImplementsJobAggregator() {
        assertThat(JobAggregator.class.isAssignableFrom(MPIExec.class), is(true));
    }

    @Test
    public void testCollapseLogicalNameConstant() {
        assertThat(MPIExec.COLLAPSE_LOGICAL_NAME, is("mpiexec"));
    }

    @Test
    public void testExecutableBasenameConstant() {
        assertThat(MPIExec.EXECUTABLE_BASENAME, is("pegasus-mpi-cluster"));
    }

    @Test
    public void testDefaultInstantiation() {
        MPIExec mpiExec = new MPIExec();
        assertThat(mpiExec, notNullValue());
    }

    @Test
    public void testMPIExecIsPublicClass() {
        int modifiers = MPIExec.class.getModifiers();
        assertThat(java.lang.reflect.Modifier.isPublic(modifiers), is(true));
    }

    @Test
    public void testGetClusterExecutableLFNReturnsCollapseLogicalName() {
        MPIExec mpiExec = new MPIExec();

        assertThat(mpiExec.getClusterExecutableLFN(), is(MPIExec.COLLAPSE_LOGICAL_NAME));
    }

    @Test
    public void testGetClusterExecutableBasenameReturnsExecutableBasename() {
        MPIExec mpiExec = new MPIExec();

        assertThat(mpiExec.getClusterExecutableBasename(), is(MPIExec.EXECUTABLE_BASENAME));
    }

    @Test
    public void testTopologicalOrderingRequiredReturnsFalse() {
        MPIExec mpiExec = new MPIExec();

        assertThat(mpiExec.topologicalOrderingRequired(), is(false));
    }

    @Test
    public void testAbortOnFirstJobFailureIsAlwaysFalse() {
        MPIExec mpiExec = new MPIExec();

        assertDoesNotThrow(() -> mpiExec.setAbortOnFirstJobFailure(true));
        assertThat(mpiExec.abortOnFristJobFailure(), is(false));
    }

    @Test
    public void testGetCPURequirementsArgumentPrefersPMCSpecificKey() {
        MPIExec mpiExec = new MPIExec();
        Job job = new Job();
        job.vdsNS.construct(Pegasus.PMC_REQUEST_CPUS_KEY, "8");

        assertThat(mpiExec.getCPURequirementsArgument(job), is("-c 8 "));
    }

    @Test
    public void testGetCPURequirementsArgumentFallsBackToCores() {
        MPIExec mpiExec = new MPIExec();
        Job job = new Job();
        job.vdsNS.construct(Pegasus.CORES_KEY, "4");

        assertThat(mpiExec.getCPURequirementsArgument(job), is("-c 4 "));
    }

    @Test
    public void testGetMemoryRequirementsArgumentFallsBackToMemoryProfile() {
        MPIExec mpiExec = new MPIExec();
        Job job = new Job();
        job.vdsNS.construct(Pegasus.MEMORY_KEY, "512");

        assertThat(mpiExec.getMemoryRequirementsArgument(job), is("-m 512 "));
    }

    @Test
    public void testGetPriorityArgumentAllowsNegativePriority() {
        MPIExec mpiExec = new MPIExec();
        Job job = new Job();
        job.vdsNS.construct(Pegasus.PMC_PRIORITY_KEY, "-3");

        assertThat(mpiExec.getPriorityArgument(job), is("-p -3 "));
    }

    @Test
    public void testGetExtraArgumentsReturnsEmptyStringWhenUnset() {
        MPIExec mpiExec = new MPIExec();

        assertThat(mpiExec.getExtraArguments(new Job()), is(""));
    }

    @Test
    public void testAggregatedJobArgumentsUsesRuntimeAndExtraArguments() {
        MPIExec mpiExec = new MPIExec();
        AggregatedJob job = new AggregatedJob();
        job.setStdIn("cluster.in");
        job.vdsNS.construct(Pegasus.RUNTIME_KEY, "900");
        job.vdsNS.construct(Pegasus.JOB_AGGREGATOR_ARGUMENTS_KEY, "--trace");

        assertThat(
                mpiExec.aggregatedJobArguments(job), is("--max-wall-time 10 --trace cluster.in"));
    }
}
