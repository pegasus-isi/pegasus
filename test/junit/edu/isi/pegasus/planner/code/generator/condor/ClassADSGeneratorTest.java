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
package edu.isi.pegasus.planner.code.generator.condor;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.*;

import edu.isi.pegasus.planner.classes.ADag;
import edu.isi.pegasus.planner.classes.AggregatedJob;
import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.namespace.Pegasus;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Files;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** Tests for ClassADSGenerator class constants and structure. */
public class ClassADSGeneratorTest {

    @Test
    public void testGeneratorConstant() {
        assertThat(ClassADSGenerator.GENERATOR, is("Pegasus"));
    }

    @Test
    public void testGeneratorAdKeyConstant() {
        assertThat(ClassADSGenerator.GENERATOR_AD_KEY, is("pegasus_generator"));
    }

    @Test
    public void testVersionAdKeyConstant() {
        assertThat(ClassADSGenerator.VERSION_AD_KEY, is("pegasus_version"));
    }

    @Test
    public void testRootWfUuidKeyConstant() {
        assertThat(ClassADSGenerator.ROOT_WF_UUID_KEY, is("pegasus_root_wf_uuid"));
    }

    @Test
    public void testWfUuidKeyConstant() {
        assertThat(ClassADSGenerator.WF_UUID_KEY, is("pegasus_wf_uuid"));
    }

    @Test
    public void testWfNameAdKeyConstant() {
        assertThat(ClassADSGenerator.WF_NAME_AD_KEY, is("pegasus_wf_name"));
    }

    @Test
    public void testWfTimeAdKeyConstant() {
        assertThat(ClassADSGenerator.WF_TIME_AD_KEY, is("pegasus_wf_time"));
    }

    @Test
    public void testXformationAdKeyConstant() {
        assertThat(ClassADSGenerator.XFORMATION_AD_KEY, is("pegasus_wf_xformation"));
    }

    @Test
    public void testPegasusClassAdKeysToPegasusProfilesContainsExpectedMappings() {
        Map<String, String> mappings = ClassADSGenerator.pegasusClassAdKeysToPegasusProfiles();

        assertThat(mappings.get(ClassADSGenerator.MEMORY_KEY), is(Pegasus.MEMORY_KEY));
        assertThat(mappings.get(ClassADSGenerator.CORES_KEY), is(Pegasus.CORES_KEY));
        assertThat(mappings.get(ClassADSGenerator.GPUS_KEY), is(Pegasus.GPUS_KEY));
        assertThat(mappings.get(ClassADSGenerator.DISKSPACE_KEY), is(Pegasus.DISKSPACE_KEY));
        assertThat(mappings.get(ClassADSGenerator.JOB_RUNTIME_AD_KEY), is(Pegasus.RUNTIME_KEY));
    }

    @Test
    public void testMapPegasusResourceProfileToPegasusClassAdVariable() {
        assertThat(
                ClassADSGenerator.mapPegasusResourceProfileToPegasusClassAdVariable(
                        Pegasus.MEMORY_KEY),
                is("$(my.pegasus_memory_mb)"));
    }

    @Test
    public void testMapPegasusResourceProfileToPegasusClassAdVariableRejectsUnknownKey() {
        RuntimeException exception =
                assertThrows(
                        RuntimeException.class,
                        () ->
                                ClassADSGenerator.mapPegasusResourceProfileToPegasusClassAdVariable(
                                        "unknown"));

        assertThat(exception.getMessage(), containsString("Unable to map pegasus profile key"));
    }

    @Test
    public void testGenerateWorkflowClassAdsIncludesAppAndTime() throws Exception {
        ADag dag = new ADag();
        dag.setRootWorkflowUUID("root-uuid");
        dag.setWorkflowUUID("wf-uuid");
        dag.setLabel("workflow");
        dag.setReleaseVersion();
        dag.setFlowTimestamp("2026-03-30T12:00:00Z");
        java.io.File dax = Files.createTempFile("classads-dax", ".dax").toFile();
        dag.setDAXMTime(dax);
        StringWriter buffer = new StringWriter();
        PrintWriter writer = new PrintWriter(buffer);

        ClassADSGenerator.generate(writer, dag, "my-app");
        writer.flush();
        String output = buffer.toString();

        assertThat(output, containsString("+pegasus_generator = \"Pegasus\""));
        assertThat(output, containsString("+pegasus_root_wf_uuid = \"root-uuid\""));
        assertThat(output, containsString("+pegasus_wf_uuid = \"wf-uuid\""));
        assertThat(output, containsString("+pegasus_wf_name = \"\""));
        assertThat(output, containsString("+pegasus_wf_app = \"my-app\""));
        assertThat(output, containsString("+pegasus_wf_time = \"2026-03-30T12:00:00Z\""));
    }

    @Test
    public void testGenerateJobClassAdsUsesJobProfilesAndClusterSize() {
        ADag dag = new ADag();
        dag.setRootWorkflowUUID("root-uuid");
        dag.setWorkflowUUID("wf-uuid");
        dag.setLabel("workflow");
        dag.setReleaseVersion();
        AggregatedJob job = new AggregatedJob();
        job.setName("clustered");
        job.setLogicalID("taskA");
        job.setJobType(Job.COMPUTE_JOB);
        job.setTransformation("ns", "xform", "1.0");
        job.setSiteHandle("local");
        job.setRemoteExecutable("/bin/true");
        job.vdsNS.construct(Pegasus.RUNTIME_KEY, "180");
        job.vdsNS.construct(Pegasus.CORES_KEY, "8");
        job.vdsNS.construct(Pegasus.GPUS_KEY, "2");
        job.vdsNS.construct(Pegasus.MEMORY_KEY, "1024");
        job.vdsNS.construct(Pegasus.DISKSPACE_KEY, "2048");
        Job childOne = new Job();
        childOne.setName("child1");
        childOne.setLogicalID("task1");
        childOne.setJobType(Job.COMPUTE_JOB);
        job.add(childOne);
        Job childTwo = new Job();
        childTwo.setName("child2");
        childTwo.setLogicalID("task2");
        childTwo.setJobType(Job.COMPUTE_JOB);
        job.add(childTwo);
        StringWriter buffer = new StringWriter();
        PrintWriter writer = new PrintWriter(buffer);

        ClassADSGenerator.generate(writer, dag, job, "my-app");
        writer.flush();
        String output = buffer.toString();

        assertThat(output, containsString("+pegasus_wf_xformation = \"ns::xform:1.0\""));
        assertThat(output, containsString("+pegasus_wf_dax_job_id = \"null\""));
        assertThat(output, containsString("+pegasus_wf_dag_job_id = \"clustered\""));
        assertThat(output, containsString("+pegasus_job_class = 1"));
        assertThat(output, containsString("+pegasus_site = \"local\""));
        assertThat(output, containsString("+pegasus_job_runtime = 180"));
        assertThat(output, containsString("+pegasus_cores = 8"));
        assertThat(output, containsString("+pegasus_gpus = 2"));
        assertThat(output, containsString("+pegasus_memory_mb = 1024"));
        assertThat(output, containsString("+pegasus_diskspace_mb = 2048"));
        assertThat(output, containsString("+pegasus_cluster_size = 2"));
    }

    @Test
    public void testGenerateJobClassAdsUsesPrepopulatedPlusSiteKey() {
        ADag dag = new ADag();
        dag.setWorkflowUUID("wf-uuid");
        Job job = new Job();
        job.setName("compute");
        job.setLogicalID("taskA");
        job.setJobType(Job.COMPUTE_JOB);
        job.setTransformation("ns", "xform", "1.0");
        job.setSiteHandle("ignored-site");
        job.condorVariables.construct(ClassADSGenerator.PLUS_RESOURCE_AD_KEY, "preselected-site");
        StringWriter buffer = new StringWriter();
        PrintWriter writer = new PrintWriter(buffer);

        ClassADSGenerator.generate(writer, dag, job, null);
        writer.flush();

        assertThat(buffer.toString(), containsString("+pegasus_site = \"preselected-site\""));
    }
}
