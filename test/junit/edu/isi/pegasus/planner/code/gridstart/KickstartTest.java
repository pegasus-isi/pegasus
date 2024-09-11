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
package edu.isi.pegasus.planner.code.gridstart;

import static org.junit.jupiter.api.Assertions.*;

import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.planner.classes.ADag;
import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.classes.PlannerOptions;
import edu.isi.pegasus.planner.common.PegasusProperties;
import edu.isi.pegasus.planner.namespace.Pegasus;
import edu.isi.pegasus.planner.test.DefaultTestSetup;
import edu.isi.pegasus.planner.test.TestSetup;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

// import org.junit.jupiter.api.Test;

/** @author Karan Vahi */
public class KickstartTest {

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
        mLogger.logEventStart("test.code.generator.container.Singulartiy", "setup", "0");
        mBag.add(PegasusBag.PEGASUS_LOGMANAGER, mLogger);
    }

    @AfterEach
    public void tearDown() {}

    @ParameterizedTest
    @CsvSource(
            value = {
                "1000, 10, 50, 3000",
                "1000, 10, null, 700", // expected value is (walltime - 5 mins)
                "290, 10, null, 280", // job will run for less than 5 minutes. fallback to mindiff
                "9, 10, null, 9223372036854775807", // job will run for less than min diff.
                // Long.MAX_VALUE is returned in case of error
            },
            nullValues = {"null"})
    public void testJobCheckpointTime(
            String maxwalltimeInSeconds,
            String minDiff,
            String checkpointTimeProfileValueInMins,
            String expectedCheckpointValue) {
        Job j = new Job("pegasus", "test", "5.1");
        if (checkpointTimeProfileValueInMins != null) {
            j.vdsNS.checkKeyInNS(Pegasus.CHECKPOINT_TIME_KEY, checkpointTimeProfileValueInMins);
        }
        Kickstart ks = new Kickstart();
        ks.initialize(mBag, new ADag());
        assertEquals(
                Long.parseLong(expectedCheckpointValue),
                ks.getJobCheckpointTimeInSeconds(
                        j, Long.parseLong(maxwalltimeInSeconds), Long.parseLong(minDiff)));
    }

    @ParameterizedTest
    @CsvSource(
            value = {
                "10, 2, -k 120 -K 240", // -K parameter to half the difference
                // between maxwalltime - checkpointTime
                "null, 2, -k 120", // only -k to trigger checkpoint is sent. no hard kill
                "10, null, -k 300 -K 150", // no checkpoint time passed. so set to 5 mins
                // before maxwalltime
                "null, null, '' ", // nothing is generated as both values are null
            },
            nullValues = {"null"})
    public void testKickstartTimeoutOptions(
            String maxwalltimeInMins, String checkpointTimeInMins, String expectedOptions) {
        Job j = new Job("pegasus", "test", "5.1");
        if (maxwalltimeInMins != null) {
            j.vdsNS.checkKeyInNS(Pegasus.MAX_WALLTIME, String.valueOf(maxwalltimeInMins));
        }
        if (checkpointTimeInMins != null) {
            j.vdsNS.checkKeyInNS(Pegasus.CHECKPOINT_TIME_KEY, String.valueOf(checkpointTimeInMins));
        }
        Kickstart ks = new Kickstart();
        ks.initialize(mBag, new ADag());
        assertEquals(expectedOptions, ks.getKickstartTimeoutOptions(j).trim());
    }

    @ParameterizedTest
    @CsvSource(
            value = {
                "10, 10",
            },
            nullValues = {"null"})
    public void testKickstartTimeoutWithError(
            String maxwalltimeInMins, String checkpointTimeInMins) {
        Job j = new Job("pegasus", "test", "5.1");
        if (maxwalltimeInMins != null) {
            j.vdsNS.checkKeyInNS(Pegasus.MAX_WALLTIME, String.valueOf(maxwalltimeInMins));
        }
        if (checkpointTimeInMins != null) {
            j.vdsNS.checkKeyInNS(Pegasus.CHECKPOINT_TIME_KEY, String.valueOf(checkpointTimeInMins));
        }
        Kickstart ks = new Kickstart();
        ks.initialize(mBag, new ADag());

        Exception e =
                Assertions.assertThrows(
                        RuntimeException.class,
                        () -> {
                            ks.getKickstartTimeoutOptions(j);
                        });
        assertTrue(
                e.getMessage().contains("Insufficient difference between maxwalltime"),
                "Exception thrown was " + e);
    }
}
