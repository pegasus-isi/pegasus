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
package edu.isi.pegasus.planner.mapper.submit;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.classes.PlannerOptions;
import edu.isi.pegasus.planner.common.PegasusProperties;
import edu.isi.pegasus.planner.mapper.MapperException;
import edu.isi.pegasus.planner.mapper.SubmitMapper;
import edu.isi.pegasus.planner.mapper.SubmitMapperFactory;
import edu.isi.pegasus.planner.test.DefaultTestSetup;
import edu.isi.pegasus.planner.test.TestSetup;
import java.io.File;
import java.util.LinkedList;
import java.util.List;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * A JUnit Test to test the Named Submit Mapper mapper interface.
 *
 * @author Karan Vahi
 */
public class NamedSubmitMapperTest {

    /** The properties used for this test. */
    private static final String PROPERTIES_BASENAME = "fixed.properties";

    private PegasusBag mBag;

    private LogManager mLogger;

    private static TestSetup mTestSetup;

    private static File mBaseTestSubmitDir;

    private static int mTestNum = 1;

    @BeforeAll
    public static void setUpClass() {
        mTestSetup = new DefaultTestSetup();
        mTestSetup.setInputDirectory(NamedSubmitMapperTest.class);
        mBaseTestSubmitDir = new File(new File(mTestSetup.getInputDirectory()).getParent(), "test");
        System.out.println("Test Dir is " + mBaseTestSubmitDir);
        if (!mBaseTestSubmitDir.mkdirs()) {
            throw new RuntimeException("Unable to create testing directory " + mBaseTestSubmitDir);
        }
    }

    @AfterAll
    public static void tearDownClass() {
        if (mBaseTestSubmitDir != null) {
            mBaseTestSubmitDir.delete();
        }
    }

    /** Setup the logger and properties that all test functions require */
    @BeforeEach
    public final void setUp() {

        mBag = new PegasusBag();
        PegasusProperties props = PegasusProperties.nonSingletonInstance();
        props.setProperty(SubmitMapper.PROPERTY_PREFIX, "Named");
        mBag.add(PegasusBag.PEGASUS_PROPERTIES, props);
        PlannerOptions options = new PlannerOptions();
        options.setBaseSubmitDirectory(mBaseTestSubmitDir.getAbsolutePath());
        mBag.add(PegasusBag.PLANNER_OPTIONS, options);
        mLogger = mTestSetup.loadLogger(props);
        mLogger.logEventStart("test.submit.mapper", "setup", "0");
        mBag.add(PegasusBag.PEGASUS_LOGMANAGER, mLogger);

        mLogger.logEventCompletion();
    }

    @Test
    public void testForNonCompute() {
        // test with no deep storage structure enabled
        mLogger.logEventStart("test.submit.mapper.Named", "test", Integer.toString(mTestNum++));
        SubmitMapper mapper = SubmitMapperFactory.loadInstance(mBag, mBaseTestSubmitDir);

        Job j = new Job();
        j.setJobType(Job.CLEANUP_JOB);
        j.setTXName("test-tr");
        this.test(j, mBaseTestSubmitDir, ".", mapper.getRelativeDir(j));

        mLogger.logEventCompletion();
    }

    @Test
    public void testForNonCosmpute() {
        // test with no deep storage structure enabled
        mLogger.logEventStart("test.submit.mapper.Named", "test", Integer.toString(mTestNum++));
        SubmitMapper mapper = SubmitMapperFactory.loadInstance(mBag, mBaseTestSubmitDir);

        Job j = new Job();
        j.setJobType(Job.CLEANUP_JOB);
        j.setTXName("test-tr");
        this.test(j, mBaseTestSubmitDir, ".", mapper.getRelativeDir(j));

        mLogger.logEventCompletion();
    }

    @Test
    public void testEmptyJob() {

        // test with no deep storage structure enabled
        mLogger.logEventStart("test.submit.mapper.Named", "test", Integer.toString(mTestNum++));
        SubmitMapper mapper = SubmitMapperFactory.loadInstance(mBag, mBaseTestSubmitDir);

        Job j = new Job();
        j.setName("empty");
        this.test(j, mBaseTestSubmitDir, ".", mapper.getRelativeDir(j));

        mLogger.logEventCompletion();
    }

    @Test
    public void testEmptyComputeJob() {

        // test with no deep storage structure enabled
        mLogger.logEventStart("test.submit.mapper.Named", "test", Integer.toString(mTestNum++));
        SubmitMapper mapper = SubmitMapperFactory.loadInstance(mBag, mBaseTestSubmitDir);

        Job j = new Job();
        j.setName("empty-compute");
        j.setJobType(Job.COMPUTE_JOB);

        assertThrows(
                MapperException.class,
                () -> this.test(j, mBaseTestSubmitDir, ".", mapper.getRelativeDir(j)));

        mLogger.logEventCompletion();
    }

    @Test
    public void testDefaultToTX() {

        // test with no deep storage structure enabled
        mLogger.logEventStart("test.submit.mapper.Named", "test", Integer.toString(mTestNum++));
        SubmitMapper mapper = SubmitMapperFactory.loadInstance(mBag, mBaseTestSubmitDir);

        Job j = new Job();
        j.setJobType(Job.COMPUTE_JOB);
        j.setTXName("test-tr");
        this.test(j, mBaseTestSubmitDir, "test-tr", mapper.getRelativeDir(j));

        mLogger.logEventCompletion();
    }

    private void test(Job j, File baseDir, String expectedDir, File actualDir) {
        try {
            assertEquals(
                    expectedDir,
                    actualDir.getPath(),
                    "Job " + j.getName() + " not mapped to right location ");
        } finally {
            if (actualDir != null && !actualDir.getPath().equals(".")) {
                // cleanup any relative directory that was created
                File dir = new File(baseDir, actualDir.getPath());
                dir.delete();
            }
        }
    }

    @AfterEach
    public void tearDown() {
        mLogger = null;
        mBag = null;
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
