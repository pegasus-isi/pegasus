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

import static org.junit.jupiter.api.Assertions.*;

import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.common.util.FindExecutable;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.classes.PlannerOptions;
import edu.isi.pegasus.planner.common.PegasusProperties;
import edu.isi.pegasus.planner.test.DefaultTestSetup;
import edu.isi.pegasus.planner.test.TestSetup;
import java.io.File;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

// import org.junit.jupiter.api.Test;

/** @author Rajiv Mayani */
public class DeployWorkerPackageTest {

    private TestSetup mTestSetup;

    private PegasusBag mBag;

    private PegasusProperties mProps;

    private LogManager mLogger;

    private static int mTestNumber = 1;

    @BeforeAll
    public static void setUpClass() {}

    @AfterAll
    public static void tearDownClass() {}

    @BeforeEach
    public void setUp() {
        mTestSetup = new DefaultTestSetup();
        mTestSetup.setInputDirectory(this.getClass());
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
        mLogger.logEventStart("test.refiner.DeployWorkerPackage", "setup", "0");
        mBag.add(PegasusBag.PEGASUS_LOGMANAGER, mLogger);
    }

    @AfterEach
    public void tearDown() {}

    @Test
    public void testDefaultUntarPathWithPATH() {
        File expectedPath = FindExecutable.findExec("tar");
        if (expectedPath == null) {
            fail("Unable to find executable tar in PATH");
        }
        DeployWorkerPackage dwp = new DeployWorkerPackage(this.mBag);

        String actual = dwp.defaultUntarPath("local");
        assertEquals(expectedPath.getAbsolutePath(), actual);
    }

    @Test
    public void testDefaultUntarPathWithoutPATH() {

        DeployWorkerPackage dwp = new DeployWorkerPackage(this.mBag);
        // any site other than local disables reliance on PATH
        String actual = dwp.defaultUntarPath("remote");
        assertEquals("/bin/tar", actual);
    }
}
