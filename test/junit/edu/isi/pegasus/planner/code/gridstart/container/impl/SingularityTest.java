/**
 * Copyright 2007-2024 University Of Southern California
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
package edu.isi.pegasus.planner.code.gridstart.container.impl;

import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.*;

import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.planner.catalog.transformation.classes.Container;
import edu.isi.pegasus.planner.classes.ADag;
import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.classes.PegasusFile;
import edu.isi.pegasus.planner.classes.PlannerOptions;
import edu.isi.pegasus.planner.code.gridstart.container.ContainerShellWrapper;
import edu.isi.pegasus.planner.code.gridstart.container.ContainerShellWrapperFactory;
import edu.isi.pegasus.planner.common.PegasusProperties;
import edu.isi.pegasus.planner.namespace.Condor;
import edu.isi.pegasus.planner.namespace.Pegasus;
import edu.isi.pegasus.planner.test.DefaultTestSetup;
import edu.isi.pegasus.planner.test.TestSetup;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** @author Karan Vahi */
public class SingularityTest {

    private static final String TEST_JOB_ID = "preprocess_ID1";

    private PegasusBag mBag;

    private PegasusProperties mProps;

    private LogManager mLogger;

    private TestSetup mTestSetup;

    private ADag mDAG;

    private static int mTestNumber = 1;

    @BeforeAll
    public static void setUpClass() {}

    @AfterAll
    public static void tearDownClass() {}

    private ContainerShellWrapperFactory mFactory;

    @BeforeEach
    public void setUp() {
        mTestSetup = new DefaultTestSetup();
        mBag = new PegasusBag();
        mTestSetup.setInputDirectory(this.getClass());
        System.out.println("Input Test Dir is " + mTestSetup.getInputDirectory());

        mProps = PegasusProperties.nonSingletonInstance();
        mBag.add(PegasusBag.PEGASUS_PROPERTIES, mProps);

        PlannerOptions options = new PlannerOptions();
        options.setExecutionSites("compute");
        options.setBaseSubmitDirectory("/tmp");
        mBag.add(PegasusBag.PLANNER_OPTIONS, options);

        mLogger = mTestSetup.loadLogger(mProps);
        mLogger.setLevel(LogManager.DEBUG_MESSAGE_LEVEL);
        mLogger.logEventStart("test.code.generator.container.Singulartiy", "setup", "0");
        mBag.add(PegasusBag.PEGASUS_LOGMANAGER, mLogger);

        /**
         * SiteStore store = this.constructTestSiteStore(); store.setForPlannerUse(mProps,
         * mBag.getPlannerOptions()); mBag.add(PegasusBag.SITE_STORE, store);
         *
         * <p>// we don't care for type of staging mapper
         * mBag.add(PegasusBag.PEGASUS_STAGING_MAPPER, StagingMapperFactory.loadInstance(mBag));
         */
        // we only need a workflow with one job
        mDAG = constructTestWorkflow();

        mFactory = new ContainerShellWrapperFactory();
        mFactory.initialize(mBag, mDAG);

        mLogger.logEventCompletion();
    }

    @AfterEach
    public void tearDown() {}

    @Test
    public void testSingularityInit() {
        mLogger.logEventStart(
                "test.code.generator.container.Singularity",
                "set",
                Integer.toString(mTestNumber++));
        Job j = (Job) mDAG.getNode(TEST_JOB_ID).getContent();
        assertEquals(
                "singularity_init python3-minimal.sif",
                singularityInstance(j).containerInit(j).toString(),
                "singularity initiation");
        mLogger.logEventCompletion();
    }

    @Test
    public void testSingularityRun() {
        mLogger.logEventStart(
                "test.code.generator.container.Singularity",
                "set",
                Integer.toString(mTestNumber++));
        Job j = (Job) mDAG.getNode(TEST_JOB_ID).getContent();
        String expected =
                "$singularity_exec exec --no-home --bind $PWD:/srv --bind $_CONDOR_SCRATCH_DIR:$_CONDOR_SCRATCH_DIR python3-minimal.sif /srv/preprocess_ID1-cont.sh";
        assertEquals(
                expected,
                singularityInstance(j).containerRun(j).toString(),
                "singularity run command");
        mLogger.logEventCompletion();
    }

    @Test
    public void testSingularityRunWithMount() {
        mLogger.logEventStart(
                "test.code.generator.container.Singularity",
                "set",
                Integer.toString(mTestNumber++));
        Job j = (Job) mDAG.getNode(TEST_JOB_ID).getContent();
        Container c = j.getContainer();
        c.addMountPoint("/shared/scratch:/scratch");
        // get the part of singularity run with the options
        assertEquals(
                "$singularity_exec exec --no-home --bind $PWD:/srv --bind $_CONDOR_SCRATCH_DIR:$_CONDOR_SCRATCH_DIR --bind /shared/scratch:/scratch python3-minimal.sif /srv/preprocess_ID1-cont.sh",
                singularityInstance(j).containerRun(j).toString(),
                "--bind option should be in singularity exec invocation");
        mLogger.logEventCompletion();
    }

    @Test
    public void testSingularityRunWithShellWrapper() {
        mLogger.logEventStart(
                "test.code.generator.container.Singularity",
                "set",
                Integer.toString(mTestNumber++));
        Job j = (Job) mDAG.getNode(TEST_JOB_ID).getContent();
        j.vdsNS.construct(Pegasus.CONTAINER_LAUNCHER_KEY, "srun");
        j.vdsNS.construct(Pegasus.CONTAINER_LAUNCHER_ARGUMENTS_KEY, "--kill-on-bad-exit");
        // get the part of singularity run with the options
        assertTrue(
                singularityInstance(j)
                        .containerRun(j)
                        .toString()
                        .startsWith("srun --kill-on-bad-exit"));
        mLogger.logEventCompletion();
    }

    @Test
    public void testSingularityRunWithGPUsPegasusProfile() {
        mLogger.logEventStart(
                "test.code.generator.container.Singularity",
                "set",
                Integer.toString(mTestNumber++));
        Job j = (Job) mDAG.getNode(TEST_JOB_ID).getContent();
        j.vdsNS.construct(Pegasus.GPUS_KEY, "5");
        assertEquals(
                "$singularity_exec exec --no-home --nv --bind $PWD:/srv --bind $_CONDOR_SCRATCH_DIR:$_CONDOR_SCRATCH_DIR python3-minimal.sif /srv/preprocess_ID1-cont.sh",
                singularityInstance(j).containerRun(j).toString(),
                "singularity exec should have -nv option");
        mLogger.logEventCompletion();
    }

    @Test
    public void testSingularityRunWithGPUsCondorProfile() {
        mLogger.logEventStart(
                "test.code.generator.container.Singularity",
                "set",
                Integer.toString(mTestNumber++));
        Job j = (Job) mDAG.getNode(TEST_JOB_ID).getContent();
        j.condorVariables.construct(Condor.REQUEST_GPUS_KEY, "5");
        assertEquals(
                "$singularity_exec exec --no-home --nv --bind $PWD:/srv --bind $_CONDOR_SCRATCH_DIR:$_CONDOR_SCRATCH_DIR python3-minimal.sif /srv/preprocess_ID1-cont.sh",
                singularityInstance(j).containerRun(j).toString(),
                "singluarity exec");
        mLogger.logEventCompletion();
    }

    @Test
    public void testSingularityRemove() {
        mLogger.logEventStart(
                "test.code.generator.container.Singularity",
                "set",
                Integer.toString(mTestNumber++));
        Job j = (Job) mDAG.getNode(TEST_JOB_ID).getContent();
        // nothing to rm, as cleanup taken care by pegasus lite removal
        // of workdir
        assertEquals(
                "", singularityInstance(j).containerRemove(j).toString(), "singularity rm command");
        mLogger.logEventCompletion();
    }

    @Test
    public void testContainerWorkingDirectory() {
        mLogger.logEventStart(
                "test.code.generator.container.Singularity",
                "set",
                Integer.toString(mTestNumber++));
        Job j = (Job) mDAG.getNode(TEST_JOB_ID).getContent();
        assertEquals(
                "/srv",
                singularityInstance(j).getContainerWorkingDirectory(),
                "container working dir");
        mLogger.logEventCompletion();
    }

    private ADag constructTestWorkflow() {
        ADag dag = new ADag();
        dag.setLabel("test");
        dag.add(this.vanillaTestJob());
        return dag;
    }

    private Job vanillaTestJob() {
        Job j = new Job();

        j.setTXName("preprocess");
        j.setLogicalID("ID1");
        j.setName(TEST_JOB_ID);
        j.setRemoteExecutable("/usr/bin/pegasus-keg");
        j.setSiteHandle("compute");
        j.setJobType(Job.COMPUTE_JOB);
        j.addInputFile(new PegasusFile("f.in"));
        PegasusFile output = new PegasusFile("f.out");
        output.setLinkage(PegasusFile.LINKAGE.output);
        j.addOutputFile(output);

        Container c = new Container("python3-minimal.sif");
        c.setType(Container.TYPE.singularity);
        j.setContainer(c);

        return j;
    }

    private Singularity singularityInstance(Job j) {
        ContainerShellWrapper wrapper = mFactory.loadInstance(j);
        assertThat(wrapper, instanceOf(Singularity.class));
        return (Singularity) wrapper;
    }
}
