/**
 * Copyright 2007-2015 University Of Southern California
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

import static org.junit.Assert.assertEquals;

import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.planner.test.DefaultTestSetup;
import edu.isi.pegasus.planner.test.EnvSetup;
import edu.isi.pegasus.planner.test.TestSetup;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/** @author Karan Vahi */
public class VariableExpanderTest {

    private int mTestNumber = 1;

    private LogManager mLogger;

    private TestSetup mTestSetup;

    public VariableExpanderTest() {}

    @BeforeClass
    public static void setUpClass() {
        Map<String, String> testEnvVariables = new HashMap();
        testEnvVariables.put("USER", "bamboo");
        EnvSetup.setEnvironmentVariables(testEnvVariables);
    }

    @AfterClass
    public static void tearDownClass() {}

    @Before
    public void setUp() {
        mTestSetup = new DefaultTestSetup();

        mTestSetup.setInputDirectory(this.getClass());
        mLogger =
                mTestSetup.loadLogger(
                        mTestSetup.loadPropertiesFromFile(".properties", new LinkedList()));
        mLogger.logEventStart("test.common.util.VariableExpander", "setup", "0");
    }

    @Test
    public void testKnownVariableCaseSensitive() {

        mLogger.logEventStart(
                "test.common.util.VariableExpander", "set", Integer.toString(mTestNumber++));
        VariableExpander exp = new VariableExpander(true);
        String variable = "${USER}";
        String value = System.getenv("USER");
        String input = "Pegasus " + variable + " rocks . Says who? " + variable;
        String expected = "Pegasus " + value + " rocks . Says who? " + value;

        assertEquals("Invalid Expansion", expected, exp.expand(input));
        mLogger.logEventCompletion();
    }

    @Test(expected = RuntimeException.class)
    public void testKnownVariableCaseSensitiveError() {

        mLogger.logEventStart(
                "test.common.util.VariableExpander", "set", Integer.toString(mTestNumber++));
        VariableExpander exp = new VariableExpander(true);
        String variable = "${USeR}";
        String value = System.getenv("USER");
        String input = "Pegasus " + variable + " rocks . Says who? " + variable;
        String expected = "Pegasus " + value + " rocks . Says who? " + value;

        assertEquals("Invalid Expansion", expected, exp.expand(input));
        mLogger.logEventCompletion();
    }

    @Test(expected = RuntimeException.class)
    public void testUnKnownVariableCaseSensitive() {

        mLogger.logEventStart(
                "test.common.util.VariableExpander", "set", Integer.toString(mTestNumber++));
        VariableExpander exp = new VariableExpander(true);
        String variable = "${GIBBERISH}";
        String value = System.getenv("USER");
        String input = "Pegasus " + variable + " rocks . Says who? " + variable;
        String expected = "Pegasus " + value + " rocks . Says who? " + value;

        assertEquals("Invalid Expansion", expected, exp.expand(input));
        mLogger.logEventCompletion();
    }

    @Test
    public void testKnownVariableCaseInSensitive() {

        mLogger.logEventStart(
                "test.common.util.VariableExpander", "set", Integer.toString(mTestNumber++));
        VariableExpander exp = new VariableExpander();
        String variable = "${USER}";
        String value = System.getenv("USER");
        String input = "Pegasus " + variable + " rocks . Says who? " + variable;
        String expected = "Pegasus " + value + " rocks . Says who? " + value;

        assertEquals("Invalid Expansion", expected, exp.expand(input));
        mLogger.logEventCompletion();
    }

    @Test
    public void testSimilarVariableCaseInSensitive() {

        mLogger.logEventStart(
                "test.common.util.VariableExpander", "set", Integer.toString(mTestNumber++));
        VariableExpander exp = new VariableExpander();
        String variable = "${USeR}";
        String value = System.getenv("USER");
        String input = "Pegasus " + variable + " rocks . Says who? " + variable;
        String expected = "Pegasus " + value + " rocks . Says who? " + value;

        assertEquals("Invalid Expansion", expected, exp.expand(input));
        mLogger.logEventCompletion();
    }

    @Test(expected = RuntimeException.class)
    public void testUnKnownVariableCaseInSensitive() {

        mLogger.logEventStart(
                "test.common.util.VariableExpander", "set", Integer.toString(mTestNumber++));
        VariableExpander exp = new VariableExpander();
        String variable = "${GIBBERISH}";
        String value = System.getenv("USER");
        String input = "Pegasus " + variable + " rocks . Says who? " + variable;
        String expected = "Pegasus " + value + " rocks . Says who? " + value;

        assertEquals("Invalid Expansion", expected, exp.expand(input));
        mLogger.logEventCompletion();
    }

    @Test
    public void testKnownVariableEscaping() {

        mLogger.logEventStart(
                "test.common.util.VariableExpander", "set", Integer.toString(mTestNumber++));
        VariableExpander exp = new VariableExpander();
        String variable = "${USER}";
        String value = System.getenv("USER");
        String input = "Pegasus " + variable + " rocks . Says who? \\" + variable;
        String expected = "Pegasus " + value + " rocks . Says who? " + variable;

        assertEquals("Invalid Expansion", expected, exp.expand(input));
        mLogger.logEventCompletion();
    }
}
