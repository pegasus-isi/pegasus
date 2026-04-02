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
import java.io.File;

/**
 * This postscript invokes the netlogger-exitcode to parse the kickstart output and write out in
 * netlogger format.
 *
 * @author Karan Vahi vahi@isi.edu
 * @version $Revision$
 */
public class NetloggerPostScript implements POSTScript {

    /** The SHORTNAME for this implementation. */
    public static final String SHORT_NAME = "Netlogger";

    /** The property to be set for postscript to pick up workflow id */
    public static final String WORKFLOW_ID_PROPERTY = "pegasus.gridstart.workflow.id";

    /** The LOG4j system configuration property. */
    private static String LOG4J_CONF_PROPERTY = "log4j.configuration";

    /** The LogManager object which is used to log all the messages. */
    protected LogManager mLogger;

    /** The object holding all the properties pertaining to Pegasus. */
    protected PegasusProperties mProps;

    /** The path to the user postscript on the submit host. */
    protected String mPOSTScriptPath;

    /** The path to the properties file created in submit directory. */
    private String mPostScriptProperties;

    /** the workflow id used. */
    private String mWorkflowID;

    /** The log4j system property */
    private String mLog4jConf;

    /** The default constructor. */
    public NetloggerPostScript() {
        // mLogger = LogManager.getInstance();

        mLog4jConf = System.getProperty(NetloggerPostScript.LOG4J_CONF_PROPERTY);
    }

    /**
     * Initialize the POSTScript implementation.
     *
     * @param properties the <code>PegasusProperties</code> object containing all the properties
     *     required by Pegasus.
     * @param path the path to the POSTScript on the submit host.
     * @param submitDir the submit directory where the submit file for the job has to be generated.
     * @param globalLog a global log file to use for logging
     * @throws RuntimeException in case of path being null.
     */
    public void initialize(
            PegasusProperties properties, String path, String submitDir, String globalLog) {

        mProps = properties;
        mPOSTScriptPath = path == null ? this.getNetloggerExitCodePath() : path;
        mLogger = LogManagerFactory.loadSingletonInstance(properties);
        mPostScriptProperties = getPostScriptProperties(properties);
        mWorkflowID = properties.getProperty(this.WORKFLOW_ID_PROPERTY);
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
     * @return boolean true if postscript was generated,else false.
     */
    public boolean construct(Job job, String key) {
        boolean constructed = false;

        // see if any specific postscript was specified for this job
        // get the value user specified for the job
        String postscript = mPOSTScriptPath;

        job.dagmanVariables.construct(
                Dagman.OUTPUT_KEY, (String) job.condorVariables.get("output"));

        /////

        StringBuffer extraOptions = new StringBuffer();

        // put in the postscript properties if any
        extraOptions.append(this.mPostScriptProperties);

        // add the log4j conf option if specified
        if (mLog4jConf != null) {
            extraOptions
                    .append(" -D")
                    .append(NetloggerPostScript.LOG4J_CONF_PROPERTY)
                    .append("=")
                    .append(mLog4jConf);
        }

        // add the -j and -w options
        extraOptions
                .append(" -j ")
                .append(job.getID())
                .append(" -w ")
                .append(mWorkflowID)
                .append(" -f ");

        // put the extra options into the exitcode arguments
        // in the correct order.
        Object args = job.dagmanVariables.get(Dagman.POST_SCRIPT_ARGUMENTS_KEY);
        StringBuffer arguments =
                (args == null)
                        ?
                        // only have extra options
                        extraOptions
                        :
                        // have extra options in addition to existing args
                        new StringBuffer().append(extraOptions).append(" ").append(args);
        job.dagmanVariables.construct(Dagman.POST_SCRIPT_ARGUMENTS_KEY, arguments.toString());

        //////

        constructed = true;
        // put in the postscript
        mLogger.log("Postscript constructed is " + postscript, LogManager.DEBUG_MESSAGE_LEVEL);
        job.dagmanVariables.checkKeyInNS(key, postscript);

        //        else{
        //            //Karan Nov 15,2005 VDS BUG FIX 128
        //            //Always remove POST_SCRIPT_ARGUMENTS
        //            job.dagmanVariables.removeKey(Dagman.POST_SCRIPT_ARGUMENTS_KEY);
        //        }

        return constructed;
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
     * Returns the path to exitcode that is to be used on the kickstart output.
     *
     * @return the path to the exitcode script to be invoked.
     */
    public String getNetloggerExitCodePath() {
        StringBuffer sb = new StringBuffer();
        sb.append(mProps.getBinDir());
        sb.append(File.separator).append("netlogger-exitcode");

        return sb.toString();
    }

    /**
     * Returns the properties that need to be passed to the the postscript invocation in the java
     * format. It is of the form "-Dprop1=value1 -Dprop2=value2 .."
     *
     * @param properties the properties object
     * @return the properties list, else empty string.
     */
    protected String getPostScriptProperties(PegasusProperties properties) {
        StringBuffer sb = new StringBuffer();
        appendProperty(sb, "pegasus.user.properties", properties.getPropertiesInSubmitDirectory());
        return sb.toString();
    }

    /**
     * Appends a property to the StringBuffer, in the java command line format.
     *
     * @param sb the StringBuffer to append the property to.
     * @param key the property.
     * @param value the property value.
     */
    protected void appendProperty(StringBuffer sb, String key, String value) {
        sb.append(" ").append("-D").append(key).append("=").append(value);
    }
}
