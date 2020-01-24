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
package edu.isi.pegasus.planner.estimate;

import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.common.util.DefaultStreamGobblerCallback;
import edu.isi.pegasus.common.util.FindExecutable;
import edu.isi.pegasus.common.util.StreamGobbler;
import edu.isi.pegasus.common.util.StreamGobblerCallback;
import edu.isi.pegasus.planner.classes.ADag;
import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.common.PegasusProperties;
import edu.isi.pegasus.planner.namespace.Metadata;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Interface with Aspen to estimate job runtimes.
 *
 * @author Karan Vahi
 */
public class Aspen implements Estimator {

    /** The property key to call out ASPEN with. */
    public static final String ASPEN_BIN_PROPERTY_KEY = "pegasus.estimator.aspen.bin";

    /*
     * The property key to call out ASPEN with.
     */
    public static final String ASPEN_MODELS_PROPERTY_KEY = "pegasus.estimator.aspen.models";

    /** name of the pegasus aspen client */
    public static final String PEGASUS_ASPEN_CLIENT_NAME = "estimate";

    /**
     * the environment variable that tells estimate client where to discover the machine and
     * application models.
     */
    public static final String ASPEN_MODELS_PATH_ENV_VARIABLE = "ASPENPATH";

    private PegasusProperties mProps;

    private File mAspenEstimateClient;

    private LogManager mLogger;

    private String[] mEnvVariables;

    /**
     * Initialization method
     *
     * @param dag the workflow
     * @param bag bag of Pegasus initialization objects.
     */
    public void initialize(ADag dag, PegasusBag bag) {
        mProps = bag.getPegasusProperties();
        mLogger = bag.getLogger();

        String binDir = mProps.getProperty(ASPEN_BIN_PROPERTY_KEY);
        mAspenEstimateClient = FindExecutable.findExec(binDir, PEGASUS_ASPEN_CLIENT_NAME);
        if (mAspenEstimateClient == null && binDir == null) {
            throw new RuntimeException(
                    "Unable to determine path to executable "
                            + PEGASUS_ASPEN_CLIENT_NAME
                            + " . Path to the bin directory for pegasus aspen estimate client not set."
                            + " Please specify the property "
                            + ASPEN_BIN_PROPERTY_KEY);
        }

        String modelEnvVariable = mProps.getProperty(Aspen.ASPEN_MODELS_PROPERTY_KEY);
        if (modelEnvVariable == null) {
            // try to pick up from environment
            modelEnvVariable = System.getenv(ASPEN_MODELS_PATH_ENV_VARIABLE);
        }
        if (modelEnvVariable == null) {
            // complain for hte models directory
            throw new RuntimeException(
                    "Models directory not set. Please set the property "
                            + ASPEN_MODELS_PROPERTY_KEY
                            + " or environment variable "
                            + ASPEN_MODELS_PATH_ENV_VARIABLE);
        }

        // construct the environement variables for invoking client
        // we inherit all in current environment and ASPENPATH
        Map<String, String> envs = System.getenv();
        mEnvVariables = new String[envs.keySet().size() + 1];
        mEnvVariables[0] = ASPEN_MODELS_PATH_ENV_VARIABLE + "=" + modelEnvVariable;
        int i = 1;
        for (Map.Entry<String, String> entry : envs.entrySet()) {
            mEnvVariables[i++] = entry.getKey() + "=" + entry.getValue();
        }
        mLogger.log(
                "Aspen estimate client will be invoked with the following evnironment "
                        + Arrays.toString(mEnvVariables),
                LogManager.DEBUG_MESSAGE_LEVEL);
    }

    /**
     * Returns the estimated Runtime of a job, by querying the estimate client with all the metadata
     * associated with the job
     *
     * @param job the job for which estimation is required
     * @return null;
     */
    public String getRuntime(Job job) {
        Map<String, String> estimates = this.getAllEstimates(job);
        return estimates.get("runtime");
    }

    /**
     * Return the estimated memory requirements of a job
     *
     * @param job the job for which estimation is required
     * @return the memory usage
     */
    public String getMemory(Job job) {
        Map<String, String> estimates = this.getAllEstimates(job);
        return estimates.get("memory");
    }

    /**
     * Returns all estimates for a job
     *
     * @param job
     * @return
     */
    public Map<String, String> getAllEstimates(Job job) {
        return this.executeAspenCommand(assembleArgsFromMetadata(job));
    }

    /**
     * Assembles arguments for aspen client from metadata attributes
     *
     * @param job
     * @return
     */
    private String assembleArgsFromMetadata(Job job) {
        StringBuilder args = new StringBuilder();
        Metadata m = (Metadata) job.getMetadata();
        for (Iterator it = m.getProfileKeyIterator(); it.hasNext(); ) {
            String key = (String) it.next();
            String value = (String) m.get(key);
            // build key=value pairs separated by whitespace
            args.append(key).append("=").append(value).append(" ");
        }
        return args.toString();
    }

    /**
     * Executes the aspen command with the arguments passed, and searches for the key in it's
     * stdout.
     *
     * @param command
     * @return the estimates parsed from the stdout
     */
    private Map<String, String> executeAspenCommand(String args) {

        String command = this.mAspenEstimateClient.getAbsolutePath() + " " + args;
        mLogger.log("Executing  " + command, LogManager.DEBUG_MESSAGE_LEVEL);

        Map<String, String> result = null;
        try {
            // set the callback and run the command
            Runtime r = Runtime.getRuntime();
            Process p = r.exec(command, mEnvVariables);

            AspenStreamGobblerCallback callback =
                    new AspenStreamGobblerCallback(mLogger, LogManager.DEBUG_MESSAGE_LEVEL);

            // spawn off the gobblers with the already initialized default callback
            StreamGobbler ips = new StreamGobbler(p.getInputStream(), callback);
            StreamGobbler eps =
                    new StreamGobbler(
                            p.getErrorStream(),
                            new DefaultStreamGobblerCallback(LogManager.ERROR_MESSAGE_LEVEL));

            ips.start();
            eps.start();

            // wait for the threads to finish off
            ips.join();
            eps.join();

            // get the status
            int status = p.waitFor();

            mLogger.log(
                    mAspenEstimateClient + " exited with status " + status,
                    LogManager.DEBUG_MESSAGE_LEVEL);

            if (status != 0) {
                throw new RuntimeException(
                        mAspenEstimateClient + " failed with non zero exit status " + command);
            }
            result = callback.getEstimates();
        } catch (IOException ioe) {
            mLogger.log(
                    "IOException while executing " + mAspenEstimateClient,
                    ioe,
                    LogManager.ERROR_MESSAGE_LEVEL);
            throw new RuntimeException("IOException while executing " + command, ioe);
        } catch (InterruptedException ie) {
            // ignore
        }
        return result;
    }

    private static class AspenStreamGobblerCallback implements StreamGobblerCallback {

        /** The instance to the logger to log messages. */
        private LogManager mLogger;

        private int mLevel;

        /** */
        private Map<String, String> mEstimates;

        public AspenStreamGobblerCallback(LogManager logger, int level) {
            mLogger = logger;
            mLevel = level;
            mEstimates = new HashMap<String, String>();
        }

        /**
         * work on the stdout lines and store them in an internal map
         *
         * @param line
         */
        public void work(String line) {
            mLogger.log(line, mLevel);
            if (line == null) {
                return;
            }

            line = line.trim();
            String[] kvs = line.split("=");
            if (kvs.length != 2) {
                mLogger.log("Unable to parse aspen output " + line, LogManager.ERROR_MESSAGE_LEVEL);
            }
            mEstimates.put(kvs[0], kvs[1]);
        }

        public Map<String, String> getEstimates() {
            return mEstimates;
        }
    }
}
