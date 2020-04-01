/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.isi.pegasus.planner.classes;

import static org.junit.Assert.*;

import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.io.IOException;
import java.util.Collection;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test class for testing creation of jobs
 *
 * @author Karan Vahi
 */
public class JobTest {

    public JobTest() {}

    @BeforeClass
    public static void setUpClass() {}

    @AfterClass
    public static void tearDownClass() {}

    @Before
    public void setUp() {}

    @After
    public void tearDown() {}

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
        assertNotNull(job);
        assertEquals("ID000001", job.getLogicalID());
        assertEquals("preprocess", job.getTXName());
        assertEquals("diamond", job.getTXNamespace());
        assertEquals("2.0", job.getTXVersion());
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
        assertNotNull(job);
        assertEquals("ID000001", job.getLogicalID());
        assertEquals("preprocess", job.getTXName());
        assertEquals("-a preprocess -T60 -i f.a -o f.b1 f.b2 ", job.getArguments());
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
        assertNotNull(job);
        assertEquals("ID000001", job.getLogicalID());
        assertEquals("preprocess", job.getTXName());
        Collection<PegasusFile> inputs = job.getInputFiles();
        assertNotNull(inputs);
        assertEquals(1, inputs.size());

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
        assertNotNull(job);
        assertEquals("ID000001", job.getLogicalID());
        assertEquals("preprocess", job.getTXName());
        Collection<PegasusFile> outputs = job.getOutputFiles();
        assertNotNull(outputs);
        assertEquals(1, outputs.size());

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
        assertNotNull(job);
        assertEquals("ID000001", job.getLogicalID());
        assertEquals("preprocess", job.getTXName());
        assertEquals("job.in", job.getStdIn());
    }

    @Test
    public void testSimpleJobWithStderr() throws IOException {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        mapper.configure(MapperFeature.ALLOW_COERCION_OF_SCALARS, false);

        String test = "id: ID000001\n" + "name: preprocess\n" + "stderr: job.err";

        Job job = mapper.readValue(test, Job.class);
        assertNotNull(job);
        assertEquals("ID000001", job.getLogicalID());
        assertEquals("preprocess", job.getTXName());
        assertEquals("job.err", job.getStdErr());
    }

    @Test
    public void testSimpleJobWithStdout() throws IOException {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        mapper.configure(MapperFeature.ALLOW_COERCION_OF_SCALARS, false);

        String test = "id: ID000001\n" + "name: preprocess\n" + "stdout: job.out";

        Job job = mapper.readValue(test, Job.class);
        assertNotNull(job);
        assertEquals("ID000001", job.getLogicalID());
        assertEquals("preprocess", job.getTXName());
        assertEquals("job.out", job.getStdOut());
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
        assertNotNull(job);
        assertEquals("ID000002", job.getLogicalID());
        assertEquals("finalization.dax", job.getDAXLFN());
        assertEquals(Job.DAX_JOB, job.getJobType());
        Collection<PegasusFile> inputs = job.getInputFiles();
        assertNotNull(inputs);
        assertEquals(1, inputs.size());

        PegasusFile expectedInput = new PegasusFile();
        expectedInput.setLinkage(PegasusFile.LINKAGE.input);
        expectedInput.setLFN("finalization.dax");
        expectedInput.setRegisterFlag(false);
        expectedInput.setTransferFlag(true);

        testPegasusFile(expectedInput, (PegasusFile) inputs.toArray()[0]);
    }

    @Test(expected = RuntimeException.class)
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

        DAXJob job = (DAXJob) mapper.readValue(test, Job.class);
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
        assertNotNull(job);
        assertEquals("ID000002", job.getLogicalID());
        assertEquals("finalization.dag", job.getDAGLFN());
        assertEquals(Job.DAG_JOB, job.getJobType());
        Collection<PegasusFile> inputs = job.getInputFiles();
        assertNotNull(inputs);
        assertEquals(1, inputs.size());

        PegasusFile expectedInput = new PegasusFile();
        expectedInput.setLinkage(PegasusFile.LINKAGE.input);
        expectedInput.setLFN("finalization.dag");
        expectedInput.setRegisterFlag(false);
        expectedInput.setTransferFlag(true);

        testPegasusFile(expectedInput, (PegasusFile) inputs.toArray()[0]);
    }

    @Test(expected = RuntimeException.class)
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

        DAGJob job = (DAGJob) mapper.readValue(test, Job.class);
    }

    private void testPegasusFile(PegasusFile expected, PegasusFile actual) {
        assertNotNull(actual);
        assertEquals(expected.getLFN(), actual.getLFN());
        assertEquals(expected.getLinkage(), actual.getLinkage());
        assertEquals(expected.getTransferFlag(), actual.getTransferFlag());
        assertTrue(expected.getRegisterFlag() == actual.getRegisterFlag());
    }
}
