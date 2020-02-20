/*
 * Copyright 2007-2020 University Of Southern California
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.isi.pegasus.planner.code;

import static org.junit.Assert.*;

import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.planner.catalog.SiteCatalog;
import edu.isi.pegasus.planner.catalog.classes.SysInfo;
import edu.isi.pegasus.planner.catalog.site.classes.SiteCatalogEntry;
import edu.isi.pegasus.planner.catalog.site.classes.SiteStore;
import edu.isi.pegasus.planner.classes.ADag;
import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.classes.PlannerOptions;
import edu.isi.pegasus.planner.classes.Profile;
import edu.isi.pegasus.planner.common.PegasusProperties;
import edu.isi.pegasus.planner.namespace.Pegasus;
import edu.isi.pegasus.planner.test.DefaultTestSetup;
import edu.isi.pegasus.planner.test.TestSetup;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Unit test class for the various Gridstart implementations
 *
 * @author Karan Vahi
 */
public class GridStartTest {

    private String DEFAULT_SITE_NAME = "condor_pool";

    private static final String USER_JOB_EXECUTABLE = "/bin/echo";
    private static final String USER_JOB_ARGS = "user-job args";
    private static final String PEGASUS_KICKSTART_PATH = "/usr/bin/pegasus-kickstart";
    private static final Profile PEGASUS_HOME = new Profile("env", "PEGASUS_HOME", "/usr/");

    private PegasusBag mBag;

    private PegasusProperties mProps;

    private LogManager mLogger;

    private TestSetup mTestSetup;

    private static int mTestNumber = 1;
    private SiteCatalog mCatalog;
    private static final String PROPERTIES_BASENAME = "properties";
    private Job mTestJob;
    private GridStartFactory mGSFactory;

    public GridStartTest() {}

    @BeforeClass
    public static void setUpClass() {}

    @AfterClass
    public static void tearDownClass() {}

    @Before
    public void setUp() {
        mTestSetup = new DefaultTestSetup();
        mBag = new PegasusBag();
        mTestSetup.setInputDirectory(this.getClass());

        mProps = PegasusProperties.nonSingletonInstance();

        mLogger = mTestSetup.loadLogger(mProps);
        mLogger.setLevel(LogManager.DEBUG_MESSAGE_LEVEL);
        mLogger.logEventStart("test.code.generator.GridStart", "setup", "0");

        PlannerOptions options = new PlannerOptions();
        options.setBaseSubmitDirectory("/tmp");

        mBag.add(PegasusBag.PLANNER_OPTIONS, options);
        mBag.add(PegasusBag.PEGASUS_LOGMANAGER, mLogger);
        mBag.add(PegasusBag.PEGASUS_PROPERTIES, mProps);

        SiteStore store = new SiteStore();
        SiteCatalogEntry entry = new SiteCatalogEntry();
        entry.setSiteHandle(DEFAULT_SITE_NAME);
        entry.setArchitecture(SysInfo.Architecture.x86_64);
        entry.setOS(SysInfo.OS.linux);
        entry.addProfile(PEGASUS_HOME);
        store.addEntry(entry);
        store.setForPlannerUse(mProps, options);
        mBag.add(PegasusBag.SITE_STORE, store);

        mGSFactory = new GridStartFactory();
        mGSFactory.initialize(mBag, new ADag(), "/tmp/ps.log");

        mTestJob = new Job();
        mTestJob.setName("test");
        mTestJob.setTransformation("", "test", "");
        mTestJob.setArguments(USER_JOB_ARGS);
        mTestJob.setRemoteExecutable(USER_JOB_EXECUTABLE);
        mTestJob.setSiteHandle(DEFAULT_SITE_NAME);

        mLogger.logEventCompletion();
    }

    @Test
    public void testKickStartEnabledJob() {

        mLogger.logEventStart(
                "test.code.generator.GridStart", "kickstart", Integer.toString(mTestNumber++));
        Job j = (Job) mTestJob.clone();

        GridStart ks = mGSFactory.loadGridStart(mTestJob, PROPERTIES_BASENAME);

        assertTrue(ks.enable(j, true));
        assertEquals(PEGASUS_KICKSTART_PATH, j.getRemoteExecutable());
        assertEquals(
                " -n test -N null -R condor_pool  -L  -T  "
                        + USER_JOB_EXECUTABLE
                        + " "
                        + USER_JOB_ARGS,
                j.getArguments());
        mLogger.logEventCompletion();
    }

    @Test
    public void testKickStartEnabledJobWithJSRun() {

        mLogger.logEventStart(
                "test.code.generator.GridStart",
                "js-run-kickstart",
                Integer.toString(mTestNumber++));
        Job j = (Job) mTestJob.clone();

        j.addProfile(new Profile("pegasus", Pegasus.GRIDSTART_LAUNCHER_KEY, "jsrun"));

        GridStart ks = mGSFactory.loadGridStart(j, PROPERTIES_BASENAME);
        assertTrue(ks.enable(j, true));
        assertEquals("jsrun", j.getRemoteExecutable());
        assertEquals(
                PEGASUS_KICKSTART_PATH
                        + " -n test -N null -R condor_pool  -L  -T  "
                        + USER_JOB_EXECUTABLE
                        + " "
                        + USER_JOB_ARGS,
                j.getArguments());
        mLogger.logEventCompletion();
    }

    @Test
    public void testKickStartEnabledJobWithJSRunArgs() {

        mLogger.logEventStart(
                "test.code.generator.GridStart",
                "js-run-kickstart",
                Integer.toString(mTestNumber++));
        Job j = (Job) mTestJob.clone();
        String jsrunArgs = "-n 1 -a 1 -c 42 -g 0";
        j.addProfile(new Profile("pegasus", Pegasus.GRIDSTART_LAUNCHER_KEY, "jsrun"));
        j.addProfile(new Profile("pegasus", Pegasus.GRIDSTART_LAUNCHER_ARGUMENTS_KEY, jsrunArgs));

        GridStart ks = mGSFactory.loadGridStart(j, PROPERTIES_BASENAME);
        assertTrue(ks.enable(j, true));
        assertEquals("jsrun", j.getRemoteExecutable());
        assertEquals(
                jsrunArgs
                        + " "
                        + PEGASUS_KICKSTART_PATH
                        + " -n test -N null -R condor_pool  -L  -T  "
                        + USER_JOB_EXECUTABLE
                        + " "
                        + USER_JOB_ARGS,
                j.getArguments());
        mLogger.logEventCompletion();
    }

    @Test
    public void testNoKickStartEnabledJob() {

        mLogger.logEventStart(
                "test.code.generator.GridStart", "no-kickstart", Integer.toString(mTestNumber++));
        Job j = (Job) mTestJob.clone();
        j.addProfile(new Profile("pegasus", Pegasus.GRIDSTART_KEY, "None"));
        GridStart ks = mGSFactory.loadGridStart(j, PROPERTIES_BASENAME);
        assertTrue(ks.enable(j, true));

        assertEquals(USER_JOB_EXECUTABLE, j.getRemoteExecutable());
        assertEquals(USER_JOB_ARGS, j.getArguments());
        mLogger.logEventCompletion();
    }

    @Test
    public void testNoKickStartEnabledJobWithJSRun() {

        mLogger.logEventStart(
                "test.code.generator.GridStart",
                "js-run-no-kickstart",
                Integer.toString(mTestNumber++));
        Job j = (Job) mTestJob.clone();
        j.addProfile(new Profile("pegasus", Pegasus.GRIDSTART_KEY, "None"));

        j.addProfile(new Profile("pegasus", Pegasus.GRIDSTART_LAUNCHER_KEY, "jsrun"));

        GridStart ks = mGSFactory.loadGridStart(j, PROPERTIES_BASENAME);
        assertTrue(ks.enable(j, true));
        assertEquals("jsrun", j.getRemoteExecutable());
        assertEquals(USER_JOB_EXECUTABLE + " " + USER_JOB_ARGS, j.getArguments());
        mLogger.logEventCompletion();
    }

    @Test
    public void testNoKickStartEnabledJobWithJSRunArgs() {

        mLogger.logEventStart(
                "test.code.generator.GridStart",
                "js-run-args-no-kickstart",
                Integer.toString(mTestNumber++));
        Job j = (Job) mTestJob.clone();
        String jsrunArgs = "-n 1 -a 1 -c 42 -g 0";
        j.addProfile(new Profile("pegasus", Pegasus.GRIDSTART_KEY, "None"));

        j.addProfile(new Profile("pegasus", Pegasus.GRIDSTART_LAUNCHER_KEY, "jsrun"));
        j.addProfile(new Profile("pegasus", Pegasus.GRIDSTART_LAUNCHER_ARGUMENTS_KEY, jsrunArgs));

        GridStart ks = mGSFactory.loadGridStart(j, PROPERTIES_BASENAME);
        assertTrue(ks.enable(j, true));
        assertEquals("jsrun", j.getRemoteExecutable());
        assertEquals(jsrunArgs + " " + USER_JOB_EXECUTABLE + " " + USER_JOB_ARGS, j.getArguments());
        mLogger.logEventCompletion();
    }

    @After
    public void tearDown() {}
}
