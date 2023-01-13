/**
 * Copyright 2007-2023 University Of Southern California
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License
 */
package edu.isi.pegasus.planner.code.generator.condor;

import static org.junit.Assert.*;

import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.classes.PlannerOptions;
import edu.isi.pegasus.planner.common.PegasusProperties;
import edu.isi.pegasus.planner.namespace.ENV;
import edu.isi.pegasus.planner.test.DefaultTestSetup;
import edu.isi.pegasus.planner.test.TestSetup;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/** @author vahi */
public class PegasusSubmitDAGTest {

    private TestSetup mTestSetup;
    private LogManager mLogger;
    private PegasusBag mBag;

    private PegasusProperties mProps;

    private static int mTestNum = 1;

    public PegasusSubmitDAGTest() {}

    @BeforeClass
    public static void setUpClass() {}

    @AfterClass
    public static void tearDownClass() {}

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
        mBag.add(PegasusBag.PLANNER_OPTIONS, options);

        mLogger = mTestSetup.loadLogger(mProps);
        mLogger.setLevel(LogManager.DEBUG_MESSAGE_LEVEL);
        mLogger.logEventStart("test.code.generator.condor.PegasusSubmitDAG", "setup", "0");
    }

    @After
    public void tearDown() {}

    @Test
    public void testEmpty() {
        String existing = "";
        String expected = "environment=\"PEGASUS_METRICS=true \"";
        this.testEnvironmentClassad(existing, expected);
    }

    @Test
    public void testPreCondor_10_2_0() {
        String existing =
                "environment	= _CONDOR_SCHEDD_ADDRESS_FILE=condor/spool/.schedd_address;_CONDOR_DAGMAN_LOG=blackƊiamond-0.dag.dagman.out";
        String expected = existing + ";PEGASUS_METRICS=true;";
        this.testEnvironmentClassad(existing, expected);
    }

    @Test
    public void testCondor_10_2_0() {
        String existing =
                "environment	= \"_CONDOR_SCHEDD_ADDRESS_FILE=/condor/spool/.schedd_address _CONDOR_DAGMAN_LOG=diamond-0.dag.dagman.out\"";
        String expected =
                "environment	= \"_CONDOR_SCHEDD_ADDRESS_FILE=/condor/spool/.schedd_address _CONDOR_DAGMAN_LOG=diamond-0.dag.dagman.out PEGASUS_METRICS=true \"";
        this.testEnvironmentClassad(existing, expected);
    }

    public void testEnvironmentClassad(String existing, String expected) {
        mLogger.logEventStart(
                "test.code.generator.condor.PegasusSubmitDAG", "set", Integer.toString(mTestNum++));
        PegasusSubmitDAG d = new PegasusSubmitDAG();
        d.intialize(mBag);
        ENV envProfiles = new ENV();
        envProfiles.construct("PEGASUS_METRICS", "true");
        String actual = d.getUpdatedDAGManEnv(existing, envProfiles);
        // System.err.println(actual);
        assertEquals(expected, actual);
        mLogger.logEventCompletion();
    }
}
