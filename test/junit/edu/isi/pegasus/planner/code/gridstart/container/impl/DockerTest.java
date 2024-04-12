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
package edu.isi.pegasus.planner.code.gridstart.container.impl;

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
import edu.isi.pegasus.planner.test.DefaultTestSetup;
import edu.isi.pegasus.planner.test.TestSetup;



import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.CoreMatchers.*;


import static org.junit.jupiter.api.Assertions.*;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

/*
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
*/
import org.junit.Test;


//import org.junit.jupiter.api.Test;

/**
 * 
 * @author Karan Vahi
 */
public class DockerTest {
    
    private static final String TEST_JOB_ID = "preprocess_ID1";
    
    private PegasusBag mBag;

    private PegasusProperties mProps;

    private LogManager mLogger;

    private TestSetup mTestSetup;

    private ADag mDAG;

    private static int mTestNumber = 1;

    @BeforeClass
    public static void setUpClass() {}

    @AfterClass
    public static void tearDownClass() {}
    
    private ContainerShellWrapperFactory mFactory;

    @Before
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
        mLogger.logEventStart("test.code.generator.container.Docker", "setup", "0");
        mBag.add(PegasusBag.PEGASUS_LOGMANAGER, mLogger);

        /**
        SiteStore store = this.constructTestSiteStore();
        store.setForPlannerUse(mProps, mBag.getPlannerOptions());
        mBag.add(PegasusBag.SITE_STORE, store);
        
        // we don't care for type of staging mapper
        mBag.add(PegasusBag.PEGASUS_STAGING_MAPPER, StagingMapperFactory.loadInstance(mBag));
        */
        // we only need a workflow with one job
        mDAG = constructTestWorkflow();
        
        mFactory = new ContainerShellWrapperFactory();
        mFactory.initialize(mBag, mDAG);
        
        
        mLogger.logEventCompletion();
    }

    @After
    public void tearDown() {}

    
    @Test
    public void testDockerInit() {
        Job j =   (Job) mDAG.getNode(TEST_JOB_ID).getContent();
        ContainerShellWrapper wrapper = mFactory.loadInstance(j);
        assertThat(wrapper, instanceOf(Docker.class));
        Docker docker = (Docker)wrapper; 
        assertEquals("docker_init centos-osgvo-el8", docker.containerInit(j).toString() , "docker initiation"); 
    }
    
    @Test
    public void testDockerRun() {
        Job j =   (Job) mDAG.getNode(TEST_JOB_ID).getContent();
        ContainerShellWrapper wrapper = mFactory.loadInstance(j);
        assertThat(wrapper, instanceOf(Docker.class));
        Docker docker = (Docker)wrapper; 
        String expected = "docker run --user root -v $PWD:/scratch -w=/scratch --entrypoint /bin/sh --name $cont_name  $cont_image -c \"set -e ;export root_path=\\$PATH ;if ! grep -q -E  \"^$cont_group:\" /etc/group ; then groupadd -f --gid $c"
                + "ont_groupid $cont_group ;fi; if ! id $cont_user 2>/dev/null >/dev/null; then    if id $cont_userid 2>/dev/null >/dev/null; then        useradd -o --uid $cont_userid --gid $cont_groupid $cont_user;    else        "
                + "useradd --uid $cont_userid --gid $cont_groupid $cont_user;    fi; fi; su $cont_user -c \\\"./preprocess_ID1-cont.sh \\\"\"";
        assertEquals(expected, docker.containerRun(j).toString(), "docker run command"); 
    }
    
    @Test
    public void testContainerWorkingDirectory() {
        Job j =   (Job) mDAG.getNode(TEST_JOB_ID).getContent();
        ContainerShellWrapper wrapper = mFactory.loadInstance(j);
        assertThat(wrapper, instanceOf(Docker.class));
        Docker docker = (Docker)wrapper; 
        assertEquals("/scratch", docker.getContainerWorkingDirectory(), "container working dir"); 
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
        
        Container c = new Container("centos-osgvo-el8");
        c.setType(Container.TYPE.docker);
        j.setContainer(c);
        
        return j;
    }
    
    
}
