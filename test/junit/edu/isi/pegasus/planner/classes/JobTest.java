/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.isi.pegasus.planner.classes;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.io.IOException;
import java.util.Collection;
import org.junit.jupiter.api.Test;

/**
 * Test class for testing creation of jobs
 *
 * @author Karan Vahi
 */
public class JobTest {

    public JobTest() {}

    @Test
    public void testDAGManCompliantJobNameNoSub() throws IOException {
        assertThat(Job.makeDAGManCompliant("preprocess"), is("preprocess"));
    }

    @Test
    public void testDAGManCompliantJobNameWithDots() throws IOException {
        assertThat(Job.makeDAGManCompliant("pre.proc.ess"), is("pre_proc_ess"));
    }

    @Test
    public void testDAGManCompliantJobNameWithPlus() throws IOException {
        assertThat(Job.makeDAGManCompliant("pre+proc+ess"), is("pre_proc_ess"));
    }

    @Test
    public void testDAGManCompliantJobNameWithEquals() throws IOException {
        assertThat(Job.makeDAGManCompliant("pre=proc+ess"), is("pre_proc_ess"));
    }

    @Test
    public void testSimpleJobCreation() throws IOException {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        mapper.configure(MapperFeature.ALLOW_COERCION_OF_SCALARS, false);

        String test =
                "id: ID000001\n"
                        + "name: preprocess\n"
                        + "namespace: diamond\n"
                        + "version: \"2.0\"";

        Job job = mapper.readValue(test, Job.class);
        assertThat(job, is(notNullValue()));
        assertThat(job.getLogicalID(), is("ID000001"));
        assertThat(job.getTXName(), is("preprocess"));
        assertThat(job.getTXNamespace(), is("diamond"));
        assertThat(job.getTXVersion(), is("2.0"));
    }

    @Test
    public void testSimpleJobWithArgumentsCreation() throws IOException {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        mapper.configure(MapperFeature.ALLOW_COERCION_OF_SCALARS, false);

        String test =
                "id: ID000001\n"
                        + "name: preprocess\n"
                        + "arguments:\n"
                        + "  [\"-a\", \"preprocess\", \"-T60\", \"-i\", \"f.a\", \"-o\", \"f.b1\", \"f.b2\"]";

        Job job = mapper.readValue(test, Job.class);
        assertThat(job, is(notNullValue()));
        assertThat(job.getLogicalID(), is("ID000001"));
        assertThat(job.getTXName(), is("preprocess"));
        assertThat(job.getArguments(), is("-a preprocess -T60 -i f.a -o f.b1 f.b2"));
    }

    @Test
    public void testSimpleJobWithNodeLabel() throws IOException {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        mapper.configure(MapperFeature.ALLOW_COERCION_OF_SCALARS, false);

        String test = "id: ID000001\n" + "name: preprocess\n" + "nodeLabel: pre-process";

        Job job = mapper.readValue(test, Job.class);
        assertThat(job, is(notNullValue()));
        assertThat(job.getLogicalID(), is("ID000001"));
        assertThat(job.getTXName(), is("preprocess"));
        // nodeLabel is not parsed into by the planner. making sure no error is thrown
    }

    @Test
    public void testSimpleJobWithInput() throws IOException {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        mapper.configure(MapperFeature.ALLOW_COERCION_OF_SCALARS, false);

        String test =
                "id: ID000001\n"
                        + "name: preprocess\n"
                        + "uses:\n"
                        + "  - lfn: f.b2\n"
                        + "    type: output\n"
                        + "    registerReplica: false\n"
                        + "    stageOut: true\n"
                        + "  - lfn: f.b1\n"
                        + "    type: input\n"
                        + "    registerReplica: false\n"
                        + "    stageOut: true";

        Job job = mapper.readValue(test, Job.class);
        assertThat(job, is(notNullValue()));
        assertThat(job.getLogicalID(), is("ID000001"));
        assertThat(job.getTXName(), is("preprocess"));
        Collection<PegasusFile> inputs = job.getInputFiles();
        assertThat(inputs, is(notNullValue()));
        assertThat(inputs.size(), is(1));

        PegasusFile expectedInput = new PegasusFile();
        expectedInput.setLinkage(PegasusFile.LINKAGE.input);
        expectedInput.setLFN("f.b1");
        expectedInput.setRegisterFlag(false);
        expectedInput.setTransferFlag(true);

        testPegasusFile(expectedInput, (PegasusFile) inputs.toArray()[0]);
    }

    @Test
    public void testSimpleJobWithOutput() throws IOException {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        mapper.configure(MapperFeature.ALLOW_COERCION_OF_SCALARS, false);

        String test =
                "id: ID000001\n"
                        + "name: preprocess\n"
                        + "uses:\n"
                        + "  - lfn: f.b2\n"
                        + "    type: output\n"
                        + "    registerReplica: true\n"
                        + "    stageOut: true\n";

        Job job = mapper.readValue(test, Job.class);
        assertThat(job, is(notNullValue()));
        assertThat(job.getLogicalID(), is("ID000001"));
        assertThat(job.getTXName(), is("preprocess"));
        Collection<PegasusFile> outputs = job.getOutputFiles();
        assertThat(outputs, is(notNullValue()));
        assertThat(outputs.size(), is(1));

        PegasusFile expectedOutput = new PegasusFile();
        expectedOutput.setLinkage(PegasusFile.LINKAGE.output);
        expectedOutput.setLFN("f.b2");
        expectedOutput.setRegisterFlag(true);
        expectedOutput.setTransferFlag(true);

        testPegasusFile(expectedOutput, (PegasusFile) outputs.toArray()[0]);
    }

    @Test
    public void testSimpleJobWithStdin() throws IOException {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        mapper.configure(MapperFeature.ALLOW_COERCION_OF_SCALARS, false);

        String test = "id: ID000001\n" + "name: preprocess\n" + "stdin: job.in";

        Job job = mapper.readValue(test, Job.class);
        assertThat(job, is(notNullValue()));
        assertThat(job.getLogicalID(), is("ID000001"));
        assertThat(job.getTXName(), is("preprocess"));
        assertThat(job.getStdIn(), is("job.in"));
    }

    @Test
    public void testSimpleJobWithStderr() throws IOException {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        mapper.configure(MapperFeature.ALLOW_COERCION_OF_SCALARS, false);

        String test = "id: ID000001\n" + "name: preprocess\n" + "stderr: job.err";

        Job job = mapper.readValue(test, Job.class);
        assertThat(job, is(notNullValue()));
        assertThat(job.getLogicalID(), is("ID000001"));
        assertThat(job.getTXName(), is("preprocess"));
        assertThat(job.getStdErr(), is("job.err"));
    }

    @Test
    public void testSimpleJobWithStdout() throws IOException {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        mapper.configure(MapperFeature.ALLOW_COERCION_OF_SCALARS, false);

        String test = "id: ID000001\n" + "name: preprocess\n" + "stdout: job.out";

        Job job = mapper.readValue(test, Job.class);
        assertThat(job, is(notNullValue()));
        assertThat(job.getLogicalID(), is("ID000001"));
        assertThat(job.getTXName(), is("preprocess"));
        assertThat(job.getStdOut(), is("job.out"));
    }

    @Test
    public void testDAXJob() throws IOException {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        mapper.configure(MapperFeature.ALLOW_COERCION_OF_SCALARS, false);

        String test =
                "type: pegasusWorkflow\n"
                        + "file: finalization.dax\n"
                        + "id: ID000002\n"
                        + "uses:\n"
                        + "  - lfn: finalization.dax\n"
                        + "    type: input\n"
                        + "    stageOut: True\n"
                        + "    registerReplica: False";

        DAXJob job = (DAXJob) mapper.readValue(test, Job.class);
        assertThat(job, is(notNullValue()));
        assertThat(job.getLogicalID(), is("ID000002"));
        assertThat(job.getDAXLFN(), is("finalization.dax"));
        assertThat(job.getJobType(), is(Job.DAX_JOB));
        Collection<PegasusFile> inputs = job.getInputFiles();
        assertThat(inputs, is(notNullValue()));
        assertThat(inputs.size(), is(1));

        PegasusFile expectedInput = new PegasusFile();
        expectedInput.setLinkage(PegasusFile.LINKAGE.input);
        expectedInput.setLFN("finalization.dax");
        expectedInput.setRegisterFlag(false);
        expectedInput.setTransferFlag(true);

        testPegasusFile(expectedInput, (PegasusFile) inputs.toArray()[0]);
    }

    @Test
    public void testDAXJobWihtoutFile() throws IOException {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        mapper.configure(MapperFeature.ALLOW_COERCION_OF_SCALARS, false);

        String test =
                "type: pegasusWorkflow\n"
                        + "id: ID000002\n"
                        + "uses:\n"
                        + "  - lfn: finalization.dax\n"
                        + "    type: input\n"
                        + "    stageOut: True\n"
                        + "    registerReplica: False";

        assertThrows(RuntimeException.class, () -> mapper.readValue(test, Job.class));
    }

    @Test
    public void testDAGJob() throws IOException {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        mapper.configure(MapperFeature.ALLOW_COERCION_OF_SCALARS, false);

        String test =
                "type: condorWorkflow\n"
                        + "file: finalization.dag\n"
                        + "id: ID000002\n"
                        + "uses:\n"
                        + "  - lfn: finalization.dag\n"
                        + "    type: input\n"
                        + "    stageOut: True\n"
                        + "    registerReplica: False";

        DAGJob job = (DAGJob) mapper.readValue(test, Job.class);
        assertThat(job, is(notNullValue()));
        assertThat(job.getLogicalID(), is("ID000002"));
        assertThat(job.getDAGLFN(), is("finalization.dag"));
        assertThat(job.getJobType(), is(Job.DAG_JOB));
        Collection<PegasusFile> inputs = job.getInputFiles();
        assertThat(inputs, is(notNullValue()));
        assertThat(inputs.size(), is(1));

        PegasusFile expectedInput = new PegasusFile();
        expectedInput.setLinkage(PegasusFile.LINKAGE.input);
        expectedInput.setLFN("finalization.dag");
        expectedInput.setRegisterFlag(false);
        expectedInput.setTransferFlag(true);

        testPegasusFile(expectedInput, (PegasusFile) inputs.toArray()[0]);
    }

    @Test
    public void testDAGJobWihtoutFile() throws IOException {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        mapper.configure(MapperFeature.ALLOW_COERCION_OF_SCALARS, false);

        String test =
                "type: condorWorkflow\n"
                        + "id: ID000002\n"
                        + "uses:\n"
                        + "  - lfn: finalization.dag\n"
                        + "    type: input\n"
                        + "    stageOut: True\n"
                        + "    registerReplica: False";

        assertThrows(RuntimeException.class, () -> mapper.readValue(test, Job.class));
    }

    @Test
    public void testStagedExecutableBasename() throws IOException {
        Job j = new Job();
        j.setTransformation("pegasus", "keg", "5.0");
        assertThat(j.getStagedExecutableBaseName(), is("pegasus-keg-5.0"));
    }

    // PM-1806
    @Test
    public void testStagedExecutableBasenameWithDots() throws IOException {
        Job j = new Job();
        j.setTransformation("pegasus.namespace", "keg.rajiv", "5.0");
        assertThat(j.getStagedExecutableBaseName(), is("pegasus.namespace-keg.rajiv-5.0"));
    }

    // PM-1806
    @Test
    public void testStagedExecutableBasenameWithDotInName() throws IOException {
        Job j = new Job();
        j.setTransformation(null, "keg.rajiv", null);
        assertThat(j.getStagedExecutableBaseName(), is("keg.rajiv"));
    }

    private void testPegasusFile(PegasusFile expected, PegasusFile actual) {
        assertThat(actual, is(notNullValue()));
        assertThat(actual.getLFN(), is(expected.getLFN()));
        assertThat(actual.getLinkage(), is(expected.getLinkage()));
        assertThat(actual.getTransferFlag(), is(expected.getTransferFlag()));
        assertThat(expected.getRegisterFlag() == actual.getRegisterFlag(), is(true));
    }
}
