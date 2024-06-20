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

import static org.junit.jupiter.api.Assertions.*;

import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.planner.test.DefaultTestSetup;
import edu.isi.pegasus.planner.test.TestSetup;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Properties;
import org.junit.jupiter.api.*;

/**
 * Test class to test the Common Properties that loads the various properties
 *
 * @author Karan Vahi
 */
public class CommonPropertiesTest {

    private static int mTestNumber = 1;

    private TestSetup mTestSetup;
    private LogManager mLogger;

    private Map<String, String> mOriginalEnv;

    public CommonPropertiesTest() {}

    @BeforeEach
    public void setUp() {
        mTestSetup = new DefaultTestSetup();
        mOriginalEnv = System.getenv();
        mTestSetup.setInputDirectory(this.getClass());
        System.out.println("Input Test Dir is " + mTestSetup.getInputDirectory());

        mLogger =
                mTestSetup.loadLogger(
                        mTestSetup.loadPropertiesFromFile(".properties", new LinkedList()));
        mLogger.logEventStart("test.pegasus.url", "setup", "0");
    }

    @AfterEach
    public void tearDown() throws Exception {
        mLogger = null;
        mTestSetup = null;
        CommonPropertiesTest.setEnv(this.mOriginalEnv);
    }

    @Test
    public void testPropertiesFromEnv() throws IOException, Exception {
        mLogger.logEventStart(
                "test.common.util.CommonProperties", "set", Integer.toString(mTestNumber++));

        Map<String, String> envs = new HashMap();
        String envVariable = "_PEGASUS__CATALOG__REPLICA";
        String expectedKey = "pegasus.catalog.replica";
        String expectedValue = "replicas-test.yml";
        envs.put(envVariable, expectedValue);
        CommonPropertiesTest.setEnv(envs);

        CommonProperties p = new CommonProperties(null);
        Properties envP = p.retrievePropertiesFromEnvironment();

        assertTrue(envP.containsKey(expectedKey));
        assertEquals(expectedValue, envP.getProperty(expectedKey));

        mLogger.logEventCompletion();
    }

    @Test
    public void testOrderPropsFileandEnv() throws Exception {
        mLogger.logEventStart(
                "test.common.util.CommonProperties", "set", Integer.toString(mTestNumber++));

        Map<String, String> envs = new HashMap();
        String envVariable = "_PEGASUS__CATALOG__REPLICA";
        String expectedKey = "pegasus.catalog.replica";
        String expectedValue = "replicas-test.yml";
        envs.put(envVariable, expectedValue);
        CommonPropertiesTest.setEnv(envs);

        File f = File.createTempFile("pegasus", ".properties");
        try {
            PrintWriter pw = new PrintWriter(new BufferedWriter(new FileWriter(f)));
            pw.println(expectedKey + " = props.replica.yml");
            pw.close();
            CommonProperties p = new CommonProperties(f.getAbsolutePath());

            // property in env should overrides property in property file
            assertEquals(expectedValue, p.getProperty(expectedKey));
            mLogger.logEventCompletion();
        } finally {
            f.delete();
        }
    }

    @Test
    public void testOrderSystemandEnv() throws Exception {
        mLogger.logEventStart(
                "test.common.util.CommonProperties", "set", Integer.toString(mTestNumber++));

        Map<String, String> envs = new HashMap();
        String envVariable = "_PEGASUS__CATALOG__REPLICA";
        String envValue = "replicas-test.yml";

        String expectedKey = "pegasus.catalog.replica";
        // property in System (mimicking -D) should overrides property picked from job env
        String expectedValue = "replicas.yml";
        envs.put(envVariable, envValue);
        CommonPropertiesTest.setEnv(envs);

        System.setProperty(expectedKey, expectedValue);
        CommonProperties p = new CommonProperties(null);

        try {
            assertEquals(expectedValue, p.getProperty(expectedKey));
        } finally {
            // important, as we are setting the property in the JVM
            // if not clear other unit tests get affected!
            System.clearProperty(expectedKey);
        }
        mLogger.logEventCompletion();
    }

    @Test
    // PM-1917
    public void testOrderPropsFileandHostWideProps() throws Exception {

        mLogger.logEventStart(
                "test.common.util.CommonProperties", "set", Integer.toString(mTestNumber++));
        String etcPropKey = "pegasus.home.sysconfdir";
        String existingEtc = System.getProperty(etcPropKey);

        File newEtc = Files.createTempDirectory("pegasus-etc").toFile();
        System.setProperty(etcPropKey, newEtc.getAbsolutePath());
        // write out a pegasus.properties file in the test etc dir
        File etcFile = new File(newEtc, "pegasus.properties");
        PrintWriter pw = new PrintWriter(new FileWriter(etcFile));
        String testKey = "from";
        String etcValue = "hostwide";
        pw.println(testKey + "=" + etcValue);
        pw.close();

        // write out a temp props file
        String propValue = "confFile";
        File propsFile = File.createTempFile("pegasus", ".properties");
        pw = new PrintWriter(new BufferedWriter(new FileWriter(propsFile)));
        pw.println(testKey + "=" + propValue);
        pw.close();
        CommonProperties p = new CommonProperties(propsFile.getAbsolutePath());
        try {
            // the value from the conf properties file overrides the hostwide
            // props in the pegasus install etc dir
            assertEquals(propValue, p.getProperty(testKey));
        } finally {
            // important, as we are setting the property in the JVM
            // if not clear other unit tests get affected!
            if (existingEtc != null) {
                System.setProperty(etcPropKey, existingEtc);
            }
            etcFile.delete();
            newEtc.delete();
            propsFile.delete();
        }
        mLogger.logEventCompletion();
    }

    @Test
    // PM-1917
    public void testUnionPropsFileandHostWideProps() throws Exception {

        mLogger.logEventStart(
                "test.common.util.CommonProperties", "set", Integer.toString(mTestNumber++));
        String etcPropKey = "pegasus.home.sysconfdir";
        String existingEtc = System.getProperty(etcPropKey);

        File newEtc = Files.createTempDirectory("pegasus-etc").toFile();
        System.setProperty(etcPropKey, newEtc.getAbsolutePath());
        // write out a pegasus.properties file in the test etc dir
        File etcFile = new File(newEtc, "pegasus.properties");
        PrintWriter pw = new PrintWriter(new FileWriter(etcFile));
        String etcKey = "etcKey";
        String etcValue = "etcValue";
        pw.println(etcKey + "=" + etcValue);
        pw.close();

        // write out a temp props file
        String propKey = "propKey";
        String propValue = "propValue";
        File propsFile = File.createTempFile("pegasus", ".properties");
        pw = new PrintWriter(new BufferedWriter(new FileWriter(propsFile)));
        pw.println(propKey + "=" + propValue);
        pw.close();
        CommonProperties p = new CommonProperties(propsFile.getAbsolutePath());
        try {
            assertEquals(etcValue, p.getProperty(etcKey));
            assertEquals(propValue, p.getProperty(propKey));
        } finally {
            // important, as we are setting the property in the JVM
            // if not clear other unit tests get affected!
            if (existingEtc != null) {
                System.setProperty(etcPropKey, existingEtc);
            }
            etcFile.delete();
            newEtc.delete();
            propsFile.delete();
        }
        mLogger.logEventCompletion();
    }

    @Test
    // PM-1917
    public void testHostWideProps() throws Exception {

        mLogger.logEventStart(
                "test.common.util.CommonProperties", "set", Integer.toString(mTestNumber++));
        String etcPropKey = "pegasus.home.sysconfdir";
        String existingEtc = System.getProperty(etcPropKey);

        File newEtc = Files.createTempDirectory("pegasus-etc").toFile();
        System.setProperty(etcPropKey, newEtc.getAbsolutePath());
        // write out a pegasus.properties file in the test etc dir
        File propsFile = new File(newEtc, "pegasus.properties");
        PrintWriter pw = new PrintWriter(new FileWriter(propsFile));
        String testKey = "from";
        String testValue = "hostwide";
        pw.println(testKey + "=" + testValue);
        pw.close();

        CommonProperties p = new CommonProperties(null);
        try {
            assertEquals(testValue, p.getProperty(testKey));
        } finally {
            // important, as we are setting the property in the JVM
            // if not clear other unit tests get affected!
            if (existingEtc != null) {
                System.setProperty(etcPropKey, existingEtc);
            }
            propsFile.delete();
            newEtc.delete();
        }
        mLogger.logEventCompletion();
    }

    /**
     * A hack to set environment variables, that java does not support directly. Copied from
     *
     * <p>https://stackoverflow.com/questions/318239/how-do-i-set-environment-variables-from-java
     *
     * @param newenv
     * @throws Exception
     */
    protected static void setEnv(Map<String, String> newenv) throws Exception {
        try {
            Class<?> processEnvironmentClass = Class.forName("java.lang.ProcessEnvironment");
            Field theEnvironmentField = processEnvironmentClass.getDeclaredField("theEnvironment");
            theEnvironmentField.setAccessible(true);
            Map<String, String> env = (Map<String, String>) theEnvironmentField.get(null);
            env.putAll(newenv);
            Field theCaseInsensitiveEnvironmentField =
                    processEnvironmentClass.getDeclaredField("theCaseInsensitiveEnvironment");
            theCaseInsensitiveEnvironmentField.setAccessible(true);
            Map<String, String> cienv =
                    (Map<String, String>) theCaseInsensitiveEnvironmentField.get(null);
            cienv.putAll(newenv);
        } catch (NoSuchFieldException e) {
            Class[] classes = Collections.class.getDeclaredClasses();
            Map<String, String> env = System.getenv();
            for (Class cl : classes) {
                if ("java.util.Collections$UnmodifiableMap".equals(cl.getName())) {
                    Field field = cl.getDeclaredField("m");
                    field.setAccessible(true);
                    Object obj = field.get(env);
                    Map<String, String> map = (Map<String, String>) obj;
                    map.clear();
                    map.putAll(newenv);
                }
            }
        }
    }
}
