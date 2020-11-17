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
package edu.isi.pegasus.planner.refiner;

import static org.junit.Assert.*;

import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.planner.classes.ADag;
import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.classes.PlannerOptions;
import edu.isi.pegasus.planner.common.PegasusProperties;
import edu.isi.pegasus.planner.namespace.Hints;
import edu.isi.pegasus.planner.namespace.Selector;
import edu.isi.pegasus.planner.test.DefaultTestSetup;
import edu.isi.pegasus.planner.test.TestSetup;
import java.util.LinkedList;
import java.util.List;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * A JUnit Test to test the Interpool Engine
 *
 * @author Karan Vahi
 */
public class InterPoolEngineTest {

    /** The properties used for this test. */
    private static final String PROPERTIES_BASENAME = "properties";

    private PegasusBag mBag;

    private PegasusProperties mProps;

    private LogManager mLogger;

    private TestSetup mTestSetup;

    private static int mTestNumber = 1;

    @BeforeClass
    public static void setUpClass() {}

    @AfterClass
    public static void tearDownClass() {}

    public InterPoolEngineTest() {}

    /** Setup the logger and properties that all test functions require */
    @Before
    public final void setUp() {
        mTestSetup = new DefaultTestSetup();
        mBag = new PegasusBag();
        mTestSetup.setInputDirectory(this.getClass());
        System.out.println("Input Test Dir is " + mTestSetup.getInputDirectory());

        mProps =
                mTestSetup.loadPropertiesFromFile(
                        PROPERTIES_BASENAME, this.getPropertyKeysForSanitization());
        mBag.add(PegasusBag.PEGASUS_PROPERTIES, mProps);

        mLogger = mTestSetup.loadLogger(mProps);
        mLogger.setLevel(LogManager.DEBUG_MESSAGE_LEVEL);
        mLogger.logEventStart("test.refiner.interpoolengineD", "setup", "0");
        mBag.add(PegasusBag.PEGASUS_LOGMANAGER, mLogger);

        mBag.add(PegasusBag.PLANNER_OPTIONS, new PlannerOptions());

        mLogger.logEventCompletion();
    }

    private List<String> getPropertyKeysForSanitization() {
        return new LinkedList();
    }

    @Test
    public void incorporateExecutionSiteWithNoHints() {
        Job j = new Job();
        j.setJobType(Job.COMPUTE_JOB);
        ADag dag = new ADag();
        dag.add(j);
        mLogger.logEventStart(
                "test.refiner.interpoolengine", "set", Integer.toString(mTestNumber++));
        InterPoolEngine engine = new InterPoolEngine(dag, mBag);
        assertFalse(engine.incorporateHint(j, Selector.EXECUTION_SITE_KEY));
        mLogger.logEventCompletion();
    }

    @Test
    public void incorporateExecutionSiteWithSelector() {
        Job j = new Job();
        j.setJobType(Job.COMPUTE_JOB);
        String site = "local";
        j.getSelectorProfiles().construct(Selector.EXECUTION_SITE_KEY, site);
        ADag dag = new ADag();
        dag.add(j);
        mLogger.logEventStart(
                "test.refiner.interpoolengine", "set", Integer.toString(mTestNumber++));
        InterPoolEngine engine = new InterPoolEngine(dag, mBag);
        assertTrue(engine.incorporateHint(j, Selector.EXECUTION_SITE_KEY));
        assertEquals(site, j.getSiteHandle());
        mLogger.logEventCompletion();
    }

    @Test
    public void incorporatePFNWithSelector() {
        Job j = new Job();
        j.setJobType(Job.COMPUTE_JOB);
        String pfn = "/tmp/pegasus-keg";
        j.getSelectorProfiles().construct(Selector.PFN_HINT_KEY, pfn);
        ADag dag = new ADag();
        dag.add(j);
        mLogger.logEventStart(
                "test.refiner.interpoolengine", "set", Integer.toString(mTestNumber++));
        InterPoolEngine engine = new InterPoolEngine(dag, mBag);
        assertTrue(engine.incorporateHint(j, Selector.PFN_HINT_KEY));
        assertEquals(pfn, j.getRemoteExecutable());
        mLogger.logEventCompletion();
    }

    @Test
    public void incorporateExecutionSiteAndPFNWithSelector() {
        Job j = new Job();
        j.setJobType(Job.COMPUTE_JOB);
        String site = "local";
        j.getSelectorProfiles().construct(Selector.EXECUTION_SITE_KEY, site);
        String pfn = "/tmp/pegasus-keg";
        j.getSelectorProfiles().construct(Selector.PFN_HINT_KEY, pfn);
        ADag dag = new ADag();
        dag.add(j);
        mLogger.logEventStart(
                "test.refiner.interpoolengine", "set", Integer.toString(mTestNumber++));
        InterPoolEngine engine = new InterPoolEngine(dag, mBag);
        // execution site will also incorporate pfn if present
        assertTrue(engine.incorporateHint(j, Selector.EXECUTION_SITE_KEY));
        assertEquals(pfn, j.getRemoteExecutable());
        assertEquals(pfn, j.getRemoteExecutable());
        mLogger.logEventCompletion();
    }

    @Test
    public void incorporateExecutionSiteWithHints() {
        Job j = new Job();
        j.setJobType(Job.COMPUTE_JOB);
        String site = "local";
        j.hints.construct(Hints.EXECUTION_SITE_KEY, site);
        ADag dag = new ADag();
        dag.add(j);
        mLogger.logEventStart(
                "test.refiner.interpoolengine", "set", Integer.toString(mTestNumber++));
        InterPoolEngine engine = new InterPoolEngine(dag, mBag);
        assertTrue(engine.incorporateHint(j, Selector.EXECUTION_SITE_KEY));
        assertEquals(site, j.getSiteHandle());
        mLogger.logEventCompletion();
    }

    @Test
    public void incorporateExecutionSiteWithSelectorAndHints() {
        Job j = new Job();
        j.setJobType(Job.COMPUTE_JOB);
        String site = "local";
        j.getSelectorProfiles().construct(Selector.EXECUTION_SITE_KEY, site);
        j.hints.construct(Hints.EXECUTION_SITE_KEY, "local1");
        ADag dag = new ADag();
        dag.add(j);
        mLogger.logEventStart(
                "test.refiner.interpoolengine", "set", Integer.toString(mTestNumber++));
        InterPoolEngine engine = new InterPoolEngine(dag, mBag);
        assertTrue(engine.incorporateHint(j, Selector.EXECUTION_SITE_KEY));
        // selector profile is preferred
        assertEquals(site, j.getSiteHandle());
        mLogger.logEventCompletion();
    }
}
