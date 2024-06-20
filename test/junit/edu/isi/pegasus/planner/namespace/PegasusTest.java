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

import static org.junit.jupiter.api.Assertions.*;

import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.planner.test.DefaultTestSetup;
import edu.isi.pegasus.planner.test.TestSetup;
import java.util.LinkedList;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Test class to test the Pegasus namespace and check if it accepts the right keys
 *
 * @author Karan Vahi
 */
public class PegasusTest {

    private TestSetup mTestSetup;
    private LogManager mLogger;

    private static int mTestNum = 1;

    public PegasusTest() {}

    @BeforeAll
    public static void setUpClass() {}

    @AfterAll
    public static void tearDownClass() {}

    @BeforeEach
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
        // should print
        mLogger.logEventStart("test.namespace.Pegasus", "set", Integer.toString(mTestNum++));
        testKey(Pegasus.BUNDLE_KEY, "dummy", Namespace.VALID_KEY);
        mLogger.logEventCompletion();

        mLogger.logEventStart("test.namespace.Pegasus", "set", Integer.toString(mTestNum++));
        testKey(Pegasus.BUNDLE_LOCAL_STAGE_IN_KEY, "dummy", Namespace.VALID_KEY);
        mLogger.logEventCompletion();

        mLogger.logEventStart("test.namespace.Pegasus", "set", Integer.toString(mTestNum++));
        testKey(Pegasus.BUNDLE_LOCAL_STAGE_OUT_KEY, "dummy", Namespace.VALID_KEY);
        mLogger.logEventCompletion();

        mLogger.logEventStart("test.namespace.Pegasus", "set", Integer.toString(mTestNum++));
        testKey(Pegasus.BUNDLE_REMOTE_STAGE_IN_KEY, "dummy", Namespace.VALID_KEY);
        mLogger.logEventCompletion();

        mLogger.logEventStart("test.namespace.Pegasus", "set", Integer.toString(mTestNum++));
        testKey(Pegasus.BUNDLE_REMOTE_STAGE_OUT_KEY, "dummy", Namespace.VALID_KEY);
        mLogger.logEventCompletion();

        mLogger.logEventStart("test.namespace.Pegasus", "set", Integer.toString(mTestNum++));
        testKey(Pegasus.BUNDLE_STAGE_IN_KEY, "dummy", Namespace.VALID_KEY);
        mLogger.logEventCompletion();

        mLogger.logEventStart("test.namespace.Pegasus", "set", Integer.toString(mTestNum++));
        testKey(Pegasus.BUNDLE_STAGE_OUT_KEY, "dummy", Namespace.VALID_KEY);
        mLogger.logEventCompletion();

        mLogger.logEventStart("test.namespace.Pegasus", "set", Integer.toString(mTestNum++));
        testKey(Pegasus.CHAIN_STAGE_IN_KEY, "dummy", Namespace.VALID_KEY);
        mLogger.logEventCompletion();

        mLogger.logEventStart("test.namespace.Pegasus", "set", Integer.toString(mTestNum++));
        testKey(Pegasus.CHANGE_DIR_KEY, "dummy", Namespace.VALID_KEY);
        mLogger.logEventCompletion();

        mLogger.logEventStart("test.namespace.Pegasus", "set", Integer.toString(mTestNum++));
        testKey(Pegasus.CLUSTER_ARGUMENTS, "dummy", Namespace.UNKNOWN_KEY);
        mLogger.logEventCompletion();

        mLogger.logEventStart("test.namespace.Pegasus", "set", Integer.toString(mTestNum++));
        testKey(Pegasus.CLUSTER_LOCAL_STAGE_IN_KEY, "dummy", Namespace.VALID_KEY);
        mLogger.logEventCompletion();

        mLogger.logEventStart("test.namespace.Pegasus", "set", Integer.toString(mTestNum++));
        testKey(Pegasus.CLUSTER_LOCAL_STAGE_OUT_KEY, "dummy", Namespace.VALID_KEY);
        mLogger.logEventCompletion();

        mLogger.logEventStart("test.namespace.Pegasus", "set", Integer.toString(mTestNum++));
        testKey(Pegasus.CLUSTER_REMOTE_STAGE_IN_KEY, "dummy", Namespace.VALID_KEY);
        mLogger.logEventCompletion();

        mLogger.logEventStart("test.namespace.Pegasus", "set", Integer.toString(mTestNum++));
        testKey(Pegasus.CLUSTER_REMOTE_STAGE_OUT_KEY, "dummy", Namespace.VALID_KEY);
        mLogger.logEventCompletion();

        mLogger.logEventStart("test.namespace.Pegasus", "set", Integer.toString(mTestNum++));
        testKey(Pegasus.CLUSTER_STAGE_IN_KEY, "dummy", Namespace.VALID_KEY);
        mLogger.logEventCompletion();

        mLogger.logEventStart("test.namespace.Pegasus", "set", Integer.toString(mTestNum++));
        testKey(Pegasus.CLUSTER_STAGE_OUT_KEY, "dummy", Namespace.VALID_KEY);
        mLogger.logEventCompletion();

        mLogger.logEventStart("test.namespace.Pegasus", "set", Integer.toString(mTestNum++));
        testKey(Pegasus.COLLAPSE_KEY, "dummy", Namespace.VALID_KEY);
        mLogger.logEventCompletion();

        mLogger.logEventStart("test.namespace.Pegasus", "set", Integer.toString(mTestNum++));
        testKey(Pegasus.CORES_KEY, "dummy", Namespace.VALID_KEY);
        mLogger.logEventCompletion();

        mLogger.logEventStart("test.namespace.Pegasus", "set", Integer.toString(mTestNum++));
        testKey(Pegasus.CREATE_AND_CHANGE_DIR_KEY, "dummy", Namespace.VALID_KEY);
        mLogger.logEventCompletion();

        mLogger.logEventStart("test.namespace.Pegasus", "set", Integer.toString(mTestNum++));
        testKey(Pegasus.DISKSPACE_KEY, "dummy", Namespace.VALID_KEY);
        mLogger.logEventCompletion();

        mLogger.logEventStart("test.namespace.Pegasus", "set", Integer.toString(mTestNum++));
        testKey(Pegasus.DATA_CONFIGURATION_KEY, "dummy", Namespace.VALID_KEY);
        mLogger.logEventCompletion();

        mLogger.logEventStart("test.namespace.Pegasus", "set", Integer.toString(mTestNum++));
        testKey(Pegasus.ENABLE_FOR_DATA_REUSE_KEY, "dummy", Namespace.VALID_KEY);
        mLogger.logEventCompletion();

        mLogger.logEventStart("test.namespace.Pegasus", "set", Integer.toString(mTestNum++));
        testKey(Pegasus.EXITCODE_FAILURE_MESSAGE, "dummy", Namespace.MERGE_KEY);
        mLogger.logEventCompletion();

        mLogger.logEventStart("test.namespace.Pegasus", "set", Integer.toString(mTestNum++));
        testKey(Pegasus.EXITCODE_SUCCESS_MESSAGE, "dummy", Namespace.MERGE_KEY);
        mLogger.logEventCompletion();

        mLogger.logEventStart("test.namespace.Pegasus", "set", Integer.toString(mTestNum++));
        testKey(Pegasus.CHECKPOINT_TIME_KEY, "dummy", Namespace.VALID_KEY);
        mLogger.logEventCompletion();

        mLogger.logEventStart("test.namespace.Pegasus", "set", Integer.toString(mTestNum++));
        testKey(Pegasus.GRIDSTART_ARGUMENTS_KEY, "dummy", Namespace.VALID_KEY);
        mLogger.logEventCompletion();

        mLogger.logEventStart("test.namespace.Pegasus", "set", Integer.toString(mTestNum++));
        testKey(Pegasus.GRIDSTART_KEY, "dummy", Namespace.VALID_KEY);
        mLogger.logEventCompletion();

        mLogger.logEventStart("test.namespace.Pegasus", "set", Integer.toString(mTestNum++));
        testKey(Pegasus.GRIDSTART_PATH_KEY, "dummy", Namespace.VALID_KEY);
        mLogger.logEventCompletion();

        mLogger.logEventStart("test.namespace.Pegasus", "set", Integer.toString(mTestNum++));
        testKey(Pegasus.GROUP_KEY, "dummy", Namespace.VALID_KEY);
        mLogger.logEventCompletion();

        mLogger.logEventStart("test.namespace.Pegasus", "set", Integer.toString(mTestNum++));
        testKey(Pegasus.GPUS_KEY, "3", Namespace.VALID_KEY);
        mLogger.logEventCompletion();

        mLogger.logEventStart("test.namespace.Pegasus", "set", Integer.toString(mTestNum++));
        testKey(Pegasus.JOB_AGGREGATOR_KEY, "dummy", Namespace.VALID_KEY);
        mLogger.logEventCompletion();

        mLogger.logEventStart("test.namespace.Pegasus", "set", Integer.toString(mTestNum++));
        testKey(Pegasus.JOB_AGGREGATOR_ARGUMENTS_KEY, "dummy", Namespace.VALID_KEY);
        mLogger.logEventCompletion();

        mLogger.logEventStart("test.namespace.Pegasus", "set", Integer.toString(mTestNum++));
        testKey(Pegasus.NODES_KEY, "dummy", Namespace.VALID_KEY);
        mLogger.logEventCompletion();

        mLogger.logEventStart("test.namespace.Pegasus", "set", Integer.toString(mTestNum++));
        testKey(Pegasus.LABEL_KEY, "dummy", Namespace.VALID_KEY);
        mLogger.logEventCompletion();

        mLogger.logEventStart("test.namespace.Pegasus", "set", Integer.toString(mTestNum++));
        testKey(Pegasus.MAX_RUN_TIME, "dummy", Namespace.VALID_KEY);
        mLogger.logEventCompletion();

        mLogger.logEventStart("test.namespace.Pegasus", "set", Integer.toString(mTestNum++));
        testKey(Pegasus.MAX_WALLTIME, "dummy", Namespace.VALID_KEY);
        mLogger.logEventCompletion();

        mLogger.logEventStart("test.namespace.Pegasus", "set", Integer.toString(mTestNum++));
        testKey(Pegasus.MEMORY_KEY, "dummy", Namespace.VALID_KEY);
        mLogger.logEventCompletion();

        mLogger.logEventStart("test.namespace.Pegasus", "set", Integer.toString(mTestNum++));
        testKey(Pegasus.PMC_PRIORITY_KEY, "dummy", Namespace.VALID_KEY);
        mLogger.logEventCompletion();

        mLogger.logEventStart("test.namespace.Pegasus", "set", Integer.toString(mTestNum++));
        testKey(Pegasus.PMC_REQUEST_CPUS_KEY, "dummy", Namespace.VALID_KEY);
        mLogger.logEventCompletion();

        mLogger.logEventStart("test.namespace.Pegasus", "set", Integer.toString(mTestNum++));
        testKey(Pegasus.PMC_REQUEST_MEMORY_KEY, "dummy", Namespace.VALID_KEY);
        mLogger.logEventCompletion();

        mLogger.logEventStart("test.namespace.Pegasus", "set", Integer.toString(mTestNum++));
        testKey(Pegasus.PMC_TASK_ARGUMENTS, "dummy", Namespace.VALID_KEY);
        mLogger.logEventCompletion();

        mLogger.logEventStart("test.namespace.Pegasus", "set", Integer.toString(mTestNum++));
        testKey(Pegasus.REMOTE_INITIALDIR_KEY, "dummy", Namespace.VALID_KEY);
        mLogger.logEventCompletion();

        mLogger.logEventStart("test.namespace.Pegasus", "set", Integer.toString(mTestNum++));
        testKey(Pegasus.RUNTIME_KEY, "dummy", Namespace.VALID_KEY);
        mLogger.logEventCompletion();

        mLogger.logEventStart("test.namespace.Pegasus", "set", Integer.toString(mTestNum++));
        testKey(Pegasus.STYLE_KEY, "dummy", Namespace.VALID_KEY);
        mLogger.logEventCompletion();

        mLogger.logEventStart("test.namespace.Pegasus", "set", Integer.toString(mTestNum++));
        testKey(Pegasus.TRANSFER_ARGUMENTS_KEY, "dummy", Namespace.VALID_KEY);
        mLogger.logEventCompletion();

        mLogger.logEventStart("test.namespace.Pegasus", "set", Integer.toString(mTestNum++));
        testKey(Pegasus.TRANSFER_PROXY_KEY, "dummy", Namespace.VALID_KEY);
        mLogger.logEventCompletion();

        mLogger.logEventStart("test.namespace.Pegasus", "set", Integer.toString(mTestNum++));
        testKey(Pegasus.TRANSFER_SLS_ARGUMENTS_KEY, "dummy", Namespace.VALID_KEY);
        mLogger.logEventCompletion();

        mLogger.logEventStart("test.namespace.Pegasus", "set", Integer.toString(mTestNum++));
        testKey(Pegasus.TRANSFER_SLS_THREADS_KEY, "dummy", Namespace.VALID_KEY);
        mLogger.logEventCompletion();

        mLogger.logEventStart("test.namespace.Pegasus", "set", Integer.toString(mTestNum++));
        testKey(Pegasus.TRANSFER_THREADS_KEY, "dummy", Namespace.VALID_KEY);
        mLogger.logEventCompletion();

        mLogger.logEventStart("test.namespace.Pegasus", "set", Integer.toString(mTestNum++));
        testKey(Pegasus.TYPE_KEY, "dummy", Namespace.VALID_KEY);
        mLogger.logEventCompletion();

        mLogger.logEventStart("test.namespace.Pegasus", "set", Integer.toString(mTestNum++));
        testKey(Pegasus.WORKER_NODE_DIRECTORY_KEY, "dummy", Namespace.VALID_KEY);
        mLogger.logEventCompletion();

        mLogger.logEventStart("test.namespace.Pegasus", "set", Integer.toString(mTestNum++));
        testKey(Pegasus.WORKER_NODE_DIRECTORY_KEY, "dummy", Namespace.VALID_KEY);
        mLogger.logEventCompletion();

        // test resource requirement keys
        mLogger.logEventStart("test.namespace.Pegasus", "set", Integer.toString(mTestNum++));
        testKey(Pegasus.RUNTIME_KEY, "dummy", Namespace.VALID_KEY);
        mLogger.logEventCompletion();

        mLogger.logEventStart("test.namespace.Pegasus", "set", Integer.toString(mTestNum++));
        testKey(Pegasus.CORES_KEY, "2", Namespace.VALID_KEY);
        mLogger.logEventCompletion();

        mLogger.logEventStart("test.namespace.Pegasus", "set", Integer.toString(mTestNum++));
        testKey(Pegasus.DISKSPACE_KEY, "12", Namespace.VALID_KEY);
        mLogger.logEventCompletion();

        mLogger.logEventStart("test.namespace.Pegasus", "set", Integer.toString(mTestNum++));
        testKey(Pegasus.NODES_KEY, "3", Namespace.VALID_KEY);
        mLogger.logEventCompletion();

        mLogger.logEventStart("test.namespace.Pegasus", "set", Integer.toString(mTestNum++));
        testKey(Pegasus.PPN_KEY, "1", Namespace.VALID_KEY);
        mLogger.logEventCompletion();

        mLogger.logEventStart("test.namespace.Pegasus", "set", Integer.toString(mTestNum++));
        testKey(Pegasus.MEMORY_KEY, "100", Namespace.VALID_KEY);
        mLogger.logEventCompletion();

        mLogger.logEventStart("test.namespace.Pegasus", "set", Integer.toString(mTestNum++));
        testKey(Pegasus.QUEUE_KEY, "pegasusQ", Namespace.VALID_KEY);
        mLogger.logEventCompletion();

        mLogger.logEventStart("test.namespace.Pegasus", "set", Integer.toString(mTestNum++));
        testKey(Pegasus.PROJECT_KEY, "pegasus", Namespace.VALID_KEY);
        mLogger.logEventCompletion();

        mLogger.logEventStart("test.namespace.Pegasus", "set", Integer.toString(mTestNum++));
        testKey(Pegasus.CONTAINER_ARGUMENTS_KEY, "pegasus", Namespace.VALID_KEY);
        mLogger.logEventCompletion();

        // deprecated keys
        mLogger.logEventStart("test.namespace.Pegasus", "set", Integer.toString(mTestNum++));
        testKey(Pegasus.COLLAPSER_KEY, "dummy", Namespace.DEPRECATED_KEY);
        mLogger.logEventCompletion();

        mLogger.logEventStart("test.namespace.Pegasus", "set", Integer.toString(mTestNum++));
        testKey(Pegasus.DEPRECATED_BUNDLE_STAGE_IN_KEY, "dummy", Namespace.DEPRECATED_KEY);
        mLogger.logEventCompletion();

        mLogger.logEventStart("test.namespace.Pegasus", "set", Integer.toString(mTestNum++));
        testKey(Pegasus.DEPRECATED_CHANGE_DIR_KEY, "dummy", Namespace.DEPRECATED_KEY);
        mLogger.logEventCompletion();

        mLogger.logEventStart("test.namespace.Pegasus", "set", Integer.toString(mTestNum++));
        testKey(Pegasus.DEPRECATED_CHECKPOINT_TIME_KEY, "dummy", Namespace.VALID_KEY);
        mLogger.logEventCompletion();

        mLogger.logEventStart("test.namespace.Pegasus", "set", Integer.toString(mTestNum++));
        testKey(Pegasus.DEPRECATED_RUNTIME_KEY, "dummy", Namespace.DEPRECATED_KEY);
        mLogger.logEventCompletion();

        // container related keys
        mLogger.logEventStart("test.namespace.Pegasus", "set", Integer.toString(mTestNum++));
        testKey(Pegasus.CONTAINER_ARGUMENTS_KEY, "pegasus", Namespace.VALID_KEY);
        mLogger.logEventCompletion();

        mLogger.logEventStart("test.namespace.Pegasus", "set", Integer.toString(mTestNum++));
        testKey(Pegasus.CONTAINER_LAUNCHER_KEY, "pegasus", Namespace.VALID_KEY);
        mLogger.logEventCompletion();

        mLogger.logEventStart("test.namespace.Pegasus", "set", Integer.toString(mTestNum++));
        testKey(Pegasus.CONTAINER_LAUNCHER_ARGUMENTS_KEY, "pegasus", Namespace.VALID_KEY);
        mLogger.logEventCompletion();

        // other keys
        mLogger.logEventStart("test.namespace.Pegasus", "set", Integer.toString(mTestNum++));
        testKey(Pegasus.CONDOR_QUOTE_ARGUMENTS_KEY, "true", Namespace.VALID_KEY);
        mLogger.logEventCompletion();
    }

    @Test
    public void testPegasusLiteEnvSourceKey() {
        mLogger.logEventStart("test.namespace.Pegasus", "set", Integer.toString(mTestNum++));
        testKey(Pegasus.PEGASUS_LITE_ENV_SOURCE_KEY, "dummy", Namespace.VALID_KEY);
        mLogger.logEventCompletion();
    }

    @Test
    public void testRelativeSubmitDirKey() {
        mLogger.logEventStart("test.namespace.Pegasus", "set", Integer.toString(mTestNum++));
        testKey(Pegasus.RELATIVE_SUBMIT_DIR_KEY, "true", Namespace.VALID_KEY);
        mLogger.logEventCompletion();
    }

    @AfterEach
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
        assertEquals(expected, result, key);
    }
}
