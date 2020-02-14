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
import edu.isi.pegasus.planner.catalog.site.classes.FileServerType;
import edu.isi.pegasus.planner.classes.ADag;
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
 * A JUnit Test to test the output mapper interface.
 *
 * @author Karan Vahi
 */
public class FixedOutputMapperTest {

    /** The properties used for this test. */
    private static final String PROPERTIES_BASENAME = "fixed.properties";

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

        mLogger.logEventCompletion();
    }

    /** Test of the Flat Output Mapper. */
    @Test
    public void test() {

        int set = 1;
        // test with no deep storage structure enabled
        mLogger.logEventStart("test.output.mapper.Fixed", "set", Integer.toString(set++));
        mProps.setProperty(OutputMapperFactory.PROPERTY_KEY, "Fixed");
        OutputMapper mapper = OutputMapperFactory.loadInstance(new ADag(), mBag);

        String lfn = "f.a";
        String pfn = mapper.map(lfn, "local", FileServerType.OPERATION.put);
        assertEquals(
                lfn + " not mapped to right location ",
                "gsiftp://outputs.isi.edu/shared/outputs/f.a",
                pfn);

        pfn = mapper.map(lfn, "local", FileServerType.OPERATION.get);
        assertEquals(
                lfn + " not mapped to right location ",
                "gsiftp://outputs.isi.edu/shared/outputs/f.a",
                pfn);

        List<String> pfns = mapper.mapAll(lfn, "local", FileServerType.OPERATION.get);
        String[] expected = {"gsiftp://outputs.isi.edu/shared/outputs/f.a"};
        assertArrayEquals(expected, pfns.toArray());
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
        keys.add(PegasusProperties.PEGASUS_SITE_CATALOG_FILE_PROPERTY);
        return keys;
    }
}
