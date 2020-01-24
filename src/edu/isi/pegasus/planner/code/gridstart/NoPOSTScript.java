/**
 * Copyright 2007-2008 University Of Southern California
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
package edu.isi.pegasus.planner.code.gridstart;

import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.common.logging.LogManagerFactory;
import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.code.POSTScript;
import edu.isi.pegasus.planner.common.PegasusProperties;
import edu.isi.pegasus.planner.namespace.Dagman;

/**
 * This class refers to having no postscript associated with the job. In addition, it removes from
 * the job postscript specific arguments, and other profiles.
 *
 * <p>The postscript is only constructed if the job already contains the Dagman profile key passed.
 *
 * @author Karan Vahi vahi@isi.edu
 * @version $Revision$
 */
public class NoPOSTScript implements POSTScript {

    /** The SHORTNAME for this implementation. */
    public static final String SHORT_NAME = "none";

    /** The LogManager object which is used to log all the messages. */
    protected LogManager mLogger;

    /** The object holding all the properties pertaining to Pegasus. */
    protected PegasusProperties mProps;

    /** The default constructor. */
    public NoPOSTScript() {
        // mLogger = LogManager.getInstance();
    }

    /**
     * Initialize the POSTScript implementation.
     *
     * @param properties the <code>PegasusProperties</code> object containing all the properties
     *     required by Pegasus.
     * @param path the path to the POSTScript on the submit host.
     * @param submitDir the submit directory where the submit file for the job has to be generated.
     * @param globalLog a global log file to use for logging
     */
    public void initialize(
            PegasusProperties properties, String path, String submitDir, String globalLog) {
        mProps = properties;
        mLogger = LogManagerFactory.loadSingletonInstance(properties);
    }

    /**
     * Returns a short textual description of the implementing class.
     *
     * @return short textual description.
     */
    public String shortDescribe() {
        return this.SHORT_NAME;
    }

    /**
     * Constructs the postscript that has to be invoked on the submit host after the job has
     * executed on the remote end. The postscript works on the stdout of the remote job, that has
     * been transferred back to the submit host by Condor.
     *
     * <p>The postscript is constructed and populated as a profile in the DAGMAN namespace.
     *
     * @param job the <code>Job</code> object containing the job description of the job that has to
     *     be enabled on the grid.
     * @param key the <code>DAGMan</code> profile key that has to be inserted.
     * @return false as postscript is never created for the job.
     */
    public boolean construct(Job job, String key) {

        // mode is none , make sure to remove post key and the arguments
        // Karan Nov 15,2005 VDS BUG FIX 128
        // Always remove POST_SCRIPT_ARGUMENTS
        job.dagmanVariables.removeKey(key);
        job.dagmanVariables.removeKey(Dagman.POST_SCRIPT_ARGUMENTS_KEY);

        return false;
    }
}
