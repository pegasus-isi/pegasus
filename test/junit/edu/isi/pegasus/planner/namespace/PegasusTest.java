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
package edu.isi.pegasus.planner.namespace;

import static org.junit.Assert.*;

import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.planner.test.DefaultTestSetup;
import edu.isi.pegasus.planner.test.TestSetup;
import java.util.LinkedList;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test class to test the Pegasus namespace and check if it accepts the right keys
 *
 * @author Karan Vahi
 */
public class PegasusTest {

    private TestSetup mTestSetup;
    private LogManager mLogger;

    public PegasusTest() {}

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
        mLogger.logEventStart("test.pegasus.namespace.pegasus", "setup", "0");
    }

    @Test
    public void testcheckKey() {

        int set = 1;

        // should print
        mLogger.logEventStart("test.namespace.Pegasus", "set", Integer.toString(set++));
        testKey(Pegasus.BUNDLE_KEY, "dummy", Namespace.VALID_KEY);
        mLogger.logEventCompletion();

        mLogger.logEventStart("test.namespace.Pegasus", "set", Integer.toString(set++));
        testKey(Pegasus.BUNDLE_LOCAL_STAGE_IN_KEY, "dummy", Namespace.VALID_KEY);
        mLogger.logEventCompletion();

        mLogger.logEventStart("test.namespace.Pegasus", "set", Integer.toString(set++));
        testKey(Pegasus.BUNDLE_LOCAL_STAGE_OUT_KEY, "dummy", Namespace.VALID_KEY);
        mLogger.logEventCompletion();

        mLogger.logEventStart("test.namespace.Pegasus", "set", Integer.toString(set++));
        testKey(Pegasus.BUNDLE_REMOTE_STAGE_IN_KEY, "dummy", Namespace.VALID_KEY);
        mLogger.logEventCompletion();

        mLogger.logEventStart("test.namespace.Pegasus", "set", Integer.toString(set++));
        testKey(Pegasus.BUNDLE_REMOTE_STAGE_OUT_KEY, "dummy", Namespace.VALID_KEY);
        mLogger.logEventCompletion();

        mLogger.logEventStart("test.namespace.Pegasus", "set", Integer.toString(set++));
        testKey(Pegasus.BUNDLE_STAGE_IN_KEY, "dummy", Namespace.VALID_KEY);
        mLogger.logEventCompletion();

        mLogger.logEventStart("test.namespace.Pegasus", "set", Integer.toString(set++));
        testKey(Pegasus.BUNDLE_STAGE_OUT_KEY, "dummy", Namespace.VALID_KEY);
        mLogger.logEventCompletion();

        mLogger.logEventStart("test.namespace.Pegasus", "set", Integer.toString(set++));
        testKey(Pegasus.CHAIN_STAGE_IN_KEY, "dummy", Namespace.VALID_KEY);
        mLogger.logEventCompletion();

        mLogger.logEventStart("test.namespace.Pegasus", "set", Integer.toString(set++));
        testKey(Pegasus.CHANGE_DIR_KEY, "dummy", Namespace.VALID_KEY);
        mLogger.logEventCompletion();

        mLogger.logEventStart("test.namespace.Pegasus", "set", Integer.toString(set++));
        testKey(Pegasus.CLUSTER_ARGUMENTS, "dummy", Namespace.VALID_KEY);
        mLogger.logEventCompletion();

        mLogger.logEventStart("test.namespace.Pegasus", "set", Integer.toString(set++));
        testKey(Pegasus.CLUSTER_LOCAL_STAGE_IN_KEY, "dummy", Namespace.VALID_KEY);
        mLogger.logEventCompletion();

        mLogger.logEventStart("test.namespace.Pegasus", "set", Integer.toString(set++));
        testKey(Pegasus.CLUSTER_LOCAL_STAGE_OUT_KEY, "dummy", Namespace.VALID_KEY);
        mLogger.logEventCompletion();

        mLogger.logEventStart("test.namespace.Pegasus", "set", Integer.toString(set++));
        testKey(Pegasus.CLUSTER_REMOTE_STAGE_IN_KEY, "dummy", Namespace.VALID_KEY);
        mLogger.logEventCompletion();

        mLogger.logEventStart("test.namespace.Pegasus", "set", Integer.toString(set++));
        testKey(Pegasus.CLUSTER_REMOTE_STAGE_OUT_KEY, "dummy", Namespace.VALID_KEY);
        mLogger.logEventCompletion();

        mLogger.logEventStart("test.namespace.Pegasus", "set", Integer.toString(set++));
        testKey(Pegasus.CLUSTER_STAGE_IN_KEY, "dummy", Namespace.VALID_KEY);
        mLogger.logEventCompletion();

        mLogger.logEventStart("test.namespace.Pegasus", "set", Integer.toString(set++));
        testKey(Pegasus.CLUSTER_STAGE_OUT_KEY, "dummy", Namespace.VALID_KEY);
        mLogger.logEventCompletion();

        mLogger.logEventStart("test.namespace.Pegasus", "set", Integer.toString(set++));
        testKey(Pegasus.COLLAPSE_KEY, "dummy", Namespace.VALID_KEY);
        mLogger.logEventCompletion();

        mLogger.logEventStart("test.namespace.Pegasus", "set", Integer.toString(set++));
        testKey(Pegasus.CORES_KEY, "dummy", Namespace.VALID_KEY);
        mLogger.logEventCompletion();

        mLogger.logEventStart("test.namespace.Pegasus", "set", Integer.toString(set++));
        testKey(Pegasus.CREATE_AND_CHANGE_DIR_KEY, "dummy", Namespace.VALID_KEY);
        mLogger.logEventCompletion();

        mLogger.logEventStart("test.namespace.Pegasus", "set", Integer.toString(set++));
        testKey(Pegasus.DISKSPACE_KEY, "dummy", Namespace.VALID_KEY);
        mLogger.logEventCompletion();

        mLogger.logEventStart("test.namespace.Pegasus", "set", Integer.toString(set++));
        testKey(Pegasus.DATA_CONFIGURATION_KEY, "dummy", Namespace.VALID_KEY);
        mLogger.logEventCompletion();

        mLogger.logEventStart("test.namespace.Pegasus", "set", Integer.toString(set++));
        testKey(Pegasus.ENABLE_FOR_DATA_REUSE_KEY, "dummy", Namespace.VALID_KEY);
        mLogger.logEventCompletion();

        mLogger.logEventStart("test.namespace.Pegasus", "set", Integer.toString(set++));
        testKey(Pegasus.EXITCODE_FAILURE_MESSAGE, "dummy", Namespace.MERGE_KEY);
        mLogger.logEventCompletion();

        mLogger.logEventStart("test.namespace.Pegasus", "set", Integer.toString(set++));
        testKey(Pegasus.EXITCODE_SUCCESS_MESSAGE, "dummy", Namespace.MERGE_KEY);
        mLogger.logEventCompletion();

        mLogger.logEventStart("test.namespace.Pegasus", "set", Integer.toString(set++));
        testKey(Pegasus.CHECKPOINT_TIME_KEY, "dummy", Namespace.VALID_KEY);
        mLogger.logEventCompletion();

        mLogger.logEventStart("test.namespace.Pegasus", "set", Integer.toString(set++));
        testKey(Pegasus.GRIDSTART_ARGUMENTS_KEY, "dummy", Namespace.VALID_KEY);
        mLogger.logEventCompletion();

        mLogger.logEventStart("test.namespace.Pegasus", "set", Integer.toString(set++));
        testKey(Pegasus.GRIDSTART_KEY, "dummy", Namespace.VALID_KEY);
        mLogger.logEventCompletion();

        mLogger.logEventStart("test.namespace.Pegasus", "set", Integer.toString(set++));
        testKey(Pegasus.GRIDSTART_PATH_KEY, "dummy", Namespace.VALID_KEY);
        mLogger.logEventCompletion();

        mLogger.logEventStart("test.namespace.Pegasus", "set", Integer.toString(set++));
        testKey(Pegasus.GROUP_KEY, "dummy", Namespace.VALID_KEY);
        mLogger.logEventCompletion();

        mLogger.logEventStart("test.namespace.Pegasus", "set", Integer.toString(set++));
        testKey(Pegasus.JOB_AGGREGATOR_KEY, "dummy", Namespace.VALID_KEY);
        mLogger.logEventCompletion();

        mLogger.logEventStart("test.namespace.Pegasus", "set", Integer.toString(set++));
        testKey(Pegasus.NODES_KEY, "dummy", Namespace.VALID_KEY);
        mLogger.logEventCompletion();

        mLogger.logEventStart("test.namespace.Pegasus", "set", Integer.toString(set++));
        testKey(Pegasus.LABEL_KEY, "dummy", Namespace.VALID_KEY);
        mLogger.logEventCompletion();

        mLogger.logEventStart("test.namespace.Pegasus", "set", Integer.toString(set++));
        testKey(Pegasus.MAX_RUN_TIME, "dummy", Namespace.VALID_KEY);
        mLogger.logEventCompletion();

        mLogger.logEventStart("test.namespace.Pegasus", "set", Integer.toString(set++));
        testKey(Pegasus.MAX_WALLTIME, "dummy", Namespace.VALID_KEY);
        mLogger.logEventCompletion();

        mLogger.logEventStart("test.namespace.Pegasus", "set", Integer.toString(set++));
        testKey(Pegasus.MEMORY_KEY, "dummy", Namespace.VALID_KEY);
        mLogger.logEventCompletion();

        mLogger.logEventStart("test.namespace.Pegasus", "set", Integer.toString(set++));
        testKey(Pegasus.PMC_PRIORITY_KEY, "dummy", Namespace.VALID_KEY);
        mLogger.logEventCompletion();

        mLogger.logEventStart("test.namespace.Pegasus", "set", Integer.toString(set++));
        testKey(Pegasus.PMC_REQUEST_CPUS_KEY, "dummy", Namespace.VALID_KEY);
        mLogger.logEventCompletion();

        mLogger.logEventStart("test.namespace.Pegasus", "set", Integer.toString(set++));
        testKey(Pegasus.PMC_REQUEST_MEMORY_KEY, "dummy", Namespace.VALID_KEY);
        mLogger.logEventCompletion();

        mLogger.logEventStart("test.namespace.Pegasus", "set", Integer.toString(set++));
        testKey(Pegasus.PMC_TASK_ARGUMENTS, "dummy", Namespace.VALID_KEY);
        mLogger.logEventCompletion();

        mLogger.logEventStart("test.namespace.Pegasus", "set", Integer.toString(set++));
        testKey(Pegasus.REMOTE_INITIALDIR_KEY, "dummy", Namespace.VALID_KEY);
        mLogger.logEventCompletion();

        mLogger.logEventStart("test.namespace.Pegasus", "set", Integer.toString(set++));
        testKey(Pegasus.RUNTIME_KEY, "dummy", Namespace.VALID_KEY);
        mLogger.logEventCompletion();

        mLogger.logEventStart("test.namespace.Pegasus", "set", Integer.toString(set++));
        testKey(Pegasus.STYLE_KEY, "dummy", Namespace.VALID_KEY);
        mLogger.logEventCompletion();

        mLogger.logEventStart("test.namespace.Pegasus", "set", Integer.toString(set++));
        testKey(Pegasus.TRANSFER_ARGUMENTS_KEY, "dummy", Namespace.VALID_KEY);
        mLogger.logEventCompletion();

        mLogger.logEventStart("test.namespace.Pegasus", "set", Integer.toString(set++));
        testKey(Pegasus.TRANSFER_PROXY_KEY, "dummy", Namespace.VALID_KEY);
        mLogger.logEventCompletion();

        mLogger.logEventStart("test.namespace.Pegasus", "set", Integer.toString(set++));
        testKey(Pegasus.TRANSFER_SLS_ARGUMENTS_KEY, "dummy", Namespace.VALID_KEY);
        mLogger.logEventCompletion();

        mLogger.logEventStart("test.namespace.Pegasus", "set", Integer.toString(set++));
        testKey(Pegasus.TRANSFER_SLS_THREADS_KEY, "dummy", Namespace.VALID_KEY);
        mLogger.logEventCompletion();

        mLogger.logEventStart("test.namespace.Pegasus", "set", Integer.toString(set++));
        testKey(Pegasus.TRANSFER_THREADS_KEY, "dummy", Namespace.VALID_KEY);
        mLogger.logEventCompletion();

        mLogger.logEventStart("test.namespace.Pegasus", "set", Integer.toString(set++));
        testKey(Pegasus.TYPE_KEY, "dummy", Namespace.VALID_KEY);
        mLogger.logEventCompletion();

        mLogger.logEventStart("test.namespace.Pegasus", "set", Integer.toString(set++));
        testKey(Pegasus.WORKER_NODE_DIRECTORY_KEY, "dummy", Namespace.VALID_KEY);
        mLogger.logEventCompletion();

        mLogger.logEventStart("test.namespace.Pegasus", "set", Integer.toString(set++));
        testKey(Pegasus.WORKER_NODE_DIRECTORY_KEY, "dummy", Namespace.VALID_KEY);
        mLogger.logEventCompletion();

        // test resource requirement keys
        mLogger.logEventStart("test.namespace.Pegasus", "set", Integer.toString(set++));
        testKey(Pegasus.RUNTIME_KEY, "dummy", Namespace.VALID_KEY);
        mLogger.logEventCompletion();

        mLogger.logEventStart("test.namespace.Pegasus", "set", Integer.toString(set++));
        testKey(Pegasus.CORES_KEY, "2", Namespace.VALID_KEY);
        mLogger.logEventCompletion();

        mLogger.logEventStart("test.namespace.Pegasus", "set", Integer.toString(set++));
        testKey(Pegasus.DISKSPACE_KEY, "12", Namespace.VALID_KEY);
        mLogger.logEventCompletion();

        mLogger.logEventStart("test.namespace.Pegasus", "set", Integer.toString(set++));
        testKey(Pegasus.NODES_KEY, "3", Namespace.VALID_KEY);
        mLogger.logEventCompletion();

        mLogger.logEventStart("test.namespace.Pegasus", "set", Integer.toString(set++));
        testKey(Pegasus.PPN_KEY, "1", Namespace.VALID_KEY);
        mLogger.logEventCompletion();

        mLogger.logEventStart("test.namespace.Pegasus", "set", Integer.toString(set++));
        testKey(Pegasus.MEMORY_KEY, "100", Namespace.VALID_KEY);
        mLogger.logEventCompletion();

        mLogger.logEventStart("test.namespace.Pegasus", "set", Integer.toString(set++));
        testKey(Pegasus.QUEUE_KEY, "pegasusQ", Namespace.VALID_KEY);
        mLogger.logEventCompletion();

        mLogger.logEventStart("test.namespace.Pegasus", "set", Integer.toString(set++));
        testKey(Pegasus.PROJECT_KEY, "pegasus", Namespace.VALID_KEY);
        mLogger.logEventCompletion();

        // deprecated keys
        mLogger.logEventStart("test.namespace.Pegasus", "set", Integer.toString(set++));
        testKey(Pegasus.COLLAPSER_KEY, "dummy", Namespace.DEPRECATED_KEY);
        mLogger.logEventCompletion();

        mLogger.logEventStart("test.namespace.Pegasus", "set", Integer.toString(set++));
        testKey(Pegasus.DEPRECATED_BUNDLE_STAGE_IN_KEY, "dummy", Namespace.DEPRECATED_KEY);
        mLogger.logEventCompletion();

        mLogger.logEventStart("test.namespace.Pegasus", "set", Integer.toString(set++));
        testKey(Pegasus.DEPRECATED_CHANGE_DIR_KEY, "dummy", Namespace.DEPRECATED_KEY);
        mLogger.logEventCompletion();

        mLogger.logEventStart("test.namespace.Pegasus", "set", Integer.toString(set++));
        testKey(Pegasus.DEPRECATED_CHECKPOINT_TIME_KEY, "dummy", Namespace.VALID_KEY);
        mLogger.logEventCompletion();

        mLogger.logEventStart("test.namespace.Pegasus", "set", Integer.toString(set++));
        testKey(Pegasus.DEPRECATED_RUNTIME_KEY, "dummy", Namespace.DEPRECATED_KEY);
        mLogger.logEventCompletion();

        // other keys
        mLogger.logEventStart("test.namespace.Pegasus", "set", Integer.toString(set++));
        testKey(Pegasus.CONDOR_QUOTE_ARGUMENTS_KEY, "true", Namespace.VALID_KEY);
        mLogger.logEventCompletion();
    }

    @After
    public void tearDown() {
        mLogger = null;
        mTestSetup = null;
    }

    /**
     * convenience method
     *
     * @param key
     * @param expected
     * @param actual
     */
    private void testKey(String key, String value, Object expected) {
        System.out.println("Testing Key " + key);
        Pegasus p = new Pegasus();
        int result = p.checkKey(key, value);
        assertEquals(key, expected, result);
    }
}
