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
package edu.isi.pegasus.planner.common;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.common.util.*;
import edu.isi.pegasus.planner.catalog.classes.Profiles;
import edu.isi.pegasus.planner.namespace.Namespace;
import edu.isi.pegasus.planner.test.DefaultTestSetup;
import edu.isi.pegasus.planner.test.TestSetup;
import java.io.IOException;
import java.util.LinkedList;
import java.util.Map;
import org.junit.*;

/**
 * Test class to test the Pegasus Properties class
 *
 * @author Karan Vahi
 */
public class PegasusPropertiesTest {

    private static int mTestNumber = 1;

    private TestSetup mTestSetup;
    private LogManager mLogger;

    private Map<String, String> mOriginalEnv;

    public PegasusPropertiesTest() {}

    @Before
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

    @After
    public void tearDown() throws Exception {
        mLogger = null;
        mTestSetup = null;
    }

    @Test
    public void testSiteProfilesInProps() throws IOException, Exception {
        mLogger.logEventStart(
                "test.planner.common.PegasusProperties", "set", Integer.toString(mTestNumber++));

        PegasusProperties properties = PegasusProperties.nonSingletonInstance();
        String key = "+testKey";
        String value = "true";
        String site = "CCG";
        properties.setProperty(
                "pegasus.catalog.site.sites." + site + ".profiles.condor." + key, value);

        Profiles p = properties.getSiteProfiles(site);
        assertNotNull(p);
        for (Profiles.NAMESPACES ns : Profiles.NAMESPACES.values()) {
            Namespace n = p.get(ns);
            if (ns.toString().equals("condor")) {
                assertEquals("Namspace should be of size", 1, n.size());
                assertEquals("Value of key " + key, value, n.get(key));
            } else {
                assertTrue("Namepsace " + n.namespaceName() + " should be empty", n.isEmpty());
            }
        }
        mLogger.logEventCompletion();
    }

}
