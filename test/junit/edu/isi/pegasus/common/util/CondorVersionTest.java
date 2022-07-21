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
package edu.isi.pegasus.common.util;

import static org.junit.Assert.*;

import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.planner.test.DefaultTestSetup;
import edu.isi.pegasus.planner.test.TestSetup;
import java.util.LinkedList;
import java.util.regex.Matcher;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Unit tests to test parsing of output of condor_version
 *
 * @author vahi
 */
public class CondorVersionTest {
    private static int mTestNumber = 1;

    private TestSetup mTestSetup;
    private LogManager mLogger;
    private CondorVersion mVersion;

    public CondorVersionTest() {}

    @BeforeClass
    public static void setUpClass() {}

    @AfterClass
    public static void tearDownClass() {}

    @Before
    public void setUp() {
        mTestSetup = new DefaultTestSetup();

        mTestSetup.setInputDirectory(this.getClass());
        System.out.println("Input Test Dir is " + mTestSetup.getInputDirectory());

        mLogger =
                mTestSetup.loadLogger(
                        mTestSetup.loadPropertiesFromFile(".properties", new LinkedList()));
        mLogger.logEventStart("test.common.util.CondorVersion", "setup", "0");
        mVersion = CondorVersion.getInstance();
    }

    @Test
    public void testNumericValue10_0_0() {
        this.testNumericValue((long) 100000, mVersion.numericValue("10.0.0"));
    }

    @Test
    public void testNumericValue7_1_2() {
        this.testNumericValue((long) 70102, mVersion.numericValue("7.1.2"));
    }

    @Test
    public void testNumericValue7_1_18() {
        this.testNumericValue((long) 70118, mVersion.numericValue("7.1.18"));
    }

    @Test
    public void testNumericValue6_99_9() {
        this.testNumericValue((long) 69909, mVersion.numericValue("6.99.9"));
    }

    @Test
    public void testNumericValue7_2_2() {
        this.testNumericValue((long) 70202, mVersion.numericValue("7.2.2"));
    }

    @Test
    public void testCondorVersionUWCSPRE() {
        this.testVersionString("7.4.1", "$CondorVersion: 7.4.1 Dec 17 2009 UWCS-PRE $");
    }

    @Test
    public void testCondorVersionWithBuildID() {
        this.testVersionString("7.4.1", "$CondorVersion: 7.4.1 Dec 17 2009 BuildID: 204351 $");
    }

    @Test
    public void testCondorVersionForPM1878() {
        this.testVersionString(
                "9.10.0",
                "$CondorVersion: 9.10.0 2022-07-14 BuildID: 596547 PackageID: 9.10.0-1 $");
    }

    public void testVersionString(String expected, String input) {
        Matcher matcher = mVersion.mPattern.matcher(input);
        if (matcher.matches()) {
            assertEquals(
                    "Version computed from " + input + " does not match",
                    expected,
                    matcher.group(1));
            return;
        }
        throw new RuntimeException("Unable to parse condor version from " + input);
    }

    private void testNumericValue(long expected, long actual) {
        mLogger.logEventStart(
                "test.common.util.CondorVersion", "set", Integer.toString(mTestNumber++));
        assertEquals("Computed numeric value of condor version mismatch", expected, actual);
        mLogger.logEventCompletion();
    }
}
