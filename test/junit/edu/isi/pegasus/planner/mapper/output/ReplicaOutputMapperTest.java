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
package edu.isi.pegasus.planner.mapper.output;

import static org.junit.Assert.*;

import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.planner.catalog.site.classes.FileServer;
import edu.isi.pegasus.planner.catalog.site.classes.SiteStore;
import edu.isi.pegasus.planner.classes.ADag;
import edu.isi.pegasus.planner.classes.NameValue;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.common.PegasusProperties;
import edu.isi.pegasus.planner.mapper.OutputMapper;
import edu.isi.pegasus.planner.mapper.OutputMapperFactory;
import edu.isi.pegasus.planner.test.TestSetup;
import java.util.LinkedList;
import java.util.List;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * A JUnit Test to test the Replica Output Mapper interface.
 *
 * @author Karan Vahi
 */
public class ReplicaOutputMapperTest {

    /** The properties used for this test. */
    private static final String PROPERTIES_BASENAME = "replica.properties";

    private PegasusBag mBag;

    private PegasusProperties mProps;

    private LogManager mLogger;

    private TestSetup mTestSetup;

    @BeforeClass
    public static void setUpClass() {}

    @AfterClass
    public static void tearDownClass() {}

    /** Setup the logger and properties that all test functions require */
    @Before
    public final void setUp() {
        mTestSetup = new OutputMapperTestSetup();
        mBag = new PegasusBag();

        mTestSetup.setInputDirectory(this.getClass());
        System.out.println("Input Test Dir is " + mTestSetup.getInputDirectory());

        mProps =
                mTestSetup.loadPropertiesFromFile(
                        PROPERTIES_BASENAME, this.getPropertyKeysForSanitization());
        mBag.add(PegasusBag.PEGASUS_PROPERTIES, mProps);

        mLogger = mTestSetup.loadLogger(mProps);
        mLogger.logEventStart("test.output.mapper", "setup", "0");
        mBag.add(PegasusBag.PEGASUS_LOGMANAGER, mLogger);

        mBag.add(PegasusBag.PLANNER_OPTIONS, mTestSetup.loadPlannerOptions());

        List<String> sites = new LinkedList();
        sites.add("*");
        SiteStore store = mTestSetup.loadSiteStoreFromFile(mProps, mLogger, sites);
        mBag.add(PegasusBag.SITE_STORE, store);
        mLogger.logEventCompletion();
    }

    /** Test of the Flat Output Mapper. */
    @Test
    public void test() {

        int set = 1;
        // test with no deep storage structure enabled
        mLogger.logEventStart("test.output.mapper.Replica", "set", Integer.toString(set++));
        mProps.setProperty(OutputMapperFactory.PROPERTY_KEY, "Replica");
        OutputMapper mapper = OutputMapperFactory.loadInstance(new ADag(), mBag);

        for (FileServer.OPERATION operation : FileServer.OPERATION.values()) {
            // replica mapper maps all operations to the same pfn
            String lfn = "f.a1";
            String expected1 = "gsiftp://corbusier.isi.edu/Volumes/data/output/nonregex/" + lfn;
            String expected2 = "gsiftp://corbusier.isi.edu/Volumes/data/output/" + lfn;
            String pfn = mapper.map(lfn, "local", operation).getValue();
            assertEquals(lfn + " not mapped to right location ", expected1, pfn);
            NameValue[] expectedPFNS = new NameValue[2];
            expectedPFNS[0] = new NameValue("local", expected1);
            expectedPFNS[1] = new NameValue("local", expected2);
            List<NameValue> pfns = mapper.mapAll(lfn, "local", operation);
            assertArrayEquals(expectedPFNS, pfns.toArray());
        }
        mLogger.logEventCompletion();

        // test to make sure that PFN constructed from regex works fine
        mLogger.logEventStart("test.output.mapper.Replica", "set", Integer.toString(set++));
        for (int i = 2; i <= 10; i++) {
            String lfn = "f.a" + i;
            for (FileServer.OPERATION operation : FileServer.OPERATION.values()) {
                // replica mapper maps all operations to the same pfn
                String expected = "gsiftp://corbusier.isi.edu/Volumes/data/output/" + lfn;
                String pfn = mapper.map(lfn, "local", operation).getValue();
                assertEquals(lfn + " not mapped to right location ", expected, pfn);
                NameValue[] expectedPFNS = new NameValue[1];
                expectedPFNS[0] = new NameValue("local", expected);
                List<NameValue> pfns = mapper.mapAll(lfn, "local", operation);
                assertArrayEquals(expectedPFNS, pfns.toArray());
            }
        }
        mLogger.logEventCompletion();
    }

    @After
    public void tearDown() {
        mLogger = null;
        mProps = null;
        mBag = null;
        mTestSetup = null;
    }

    /**
     * Returns the list of property keys that should be sanitized
     *
     * @return List<String>
     */
    protected List<String> getPropertyKeysForSanitization() {
        List<String> keys = new LinkedList();
        keys.add("pegasus.dir.storage.mapper.replica.file");
        keys.add(PegasusProperties.PEGASUS_SITE_CATALOG_FILE_PROPERTY);
        return keys;
    }
}
