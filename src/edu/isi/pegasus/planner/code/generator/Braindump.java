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
package edu.isi.pegasus.planner.code.generator;

import edu.isi.pegasus.common.credential.CredentialHandler;
import edu.isi.pegasus.common.credential.CredentialHandlerFactory;
import edu.isi.pegasus.common.credential.impl.Proxy;
import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.common.util.Version;
import edu.isi.pegasus.planner.classes.ADag;
import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.classes.PlannerOptions;
import edu.isi.pegasus.planner.cluster.aggregator.JobAggregatorFactory;
import edu.isi.pegasus.planner.code.CodeGeneratorException;
import edu.isi.pegasus.planner.common.PegasusProperties;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;
import org.globus.common.CoGProperties;
import org.gridforum.jgss.ExtendedGSSManager;
import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.GSSName;

/**
 * Braindump file code generator that generates a Braindump file for the executable workflow in the
 * submit directory.
 *
 * <p>The following keys are generated in the braindump file.
 *
 * <pre>
 * wf_uuid
 * submit_hostname
 * planner_arguments
 * user
 * grid_dn
 * dax_label
 * timestamp
 * submit_dir
 * planner_version
 * type
 * properties
 * </pre>
 *
 * Additionally, the following duplicate keys exist till pegasus-run is modified.
 *
 * <pre>
 * old keyname -> new keyname
 * =============================
 * label --> dax_label
 * pegasus_wf_time --> timestamp
 * run --> submit_dir
 * pegasus_version --> planner_version
 * </pre>
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public class Braindump {

    /** The basename of the braindump file. */
    public static final String BRAINDUMP_FILE = "braindump.txt";

    /** The Key designating type of Pegasus Code Generator. */
    public static final String GENERATOR_TYPE_KEY = "type";

    /** The user who submitted the workflow. */
    public static final String USER_KEY = "user";

    /** The Grid DN of the user. */
    public static final String GRID_DN_KEY = "grid_dn";

    /** The path to the pegasus properties file */
    public static final String PROPERTIES_KEY = "properties";

    /** The key for the submit hostname. */
    public static final String SUBMIT_HOSTNAME_KEY = "submit_hostname";

    /** The arguments passed to the planner. */
    public static final String PLANNER_ARGUMENTS_KEY = "planner_arguments";

    /** The key for UUID of the workflow. */
    public static final String ROOT_UUID_KEY = "root_wf_uuid";

    /** The key for UUID of the workflow. */
    public static final String UUID_KEY = "wf_uuid";

    /** The DAX label. */
    public static final String DAX_LABEL_KEY = "dax_label";

    /** The dax index */
    public static final String DAX_INDEX_KEY = "dax_index";

    /** The DAX version. */
    public static final String DAX_VERRSION_KEY = "dax_version";

    /** The workflow timestamp. */
    public static final String TIMESTAMP_KEY = "timestamp";

    /** The submit directory for the workflow. */
    public static final String SUBMIT_DIR_KEY = "submit_dir";

    /**
     * The Key for the version id.
     *
     * @see org.griphyn.cPlanner.classes.DagInfo#releaseVersion
     */
    public static final String VERSION_KEY = "pegasus_version";

    /** The Key for the planner version */
    public static final String PLANNER_VERSION_KEY = "planner_version";

    /** The Key for the pegasus build. */
    public static final String BUILD_KEY = "pegasus_build";

    /**
     * The Key for the flow id.
     *
     * @see org.griphyn.cPlanner.classes.DagInfo#flowIDName
     */
    public static final String WF_NAME_KEY = "pegasus_wf_name";

    /**
     * The Key for the timestamp.
     *
     * @see org.griphyn.cPlanner.classes.DagInfo#mFlowTimestamp
     */
    public static final String WF_TIME_KEY = "pegasus_wf_time";

    /**
     * The Key for the timestamp.
     *
     * @see org.griphyn.cPlanner.classes.DagInfo#mFlowTimestamp
     */
    public static final String WF_TIMESTAMP_KEY = "timestamp";

    /** The bag of initialization objects. */
    protected PegasusBag mBag;

    /** The directory where all the submit files are to be generated. */
    protected String mSubmitFileDir;

    /** The object holding all the properties pertaining to Pegasus. */
    protected PegasusProperties mProps;

    /** The object containing the command line options specified to the planner at runtime. */
    protected PlannerOptions mPOptions;

    /** The handle to the logging object. */
    protected LogManager mLogger;

    /**
     * A static method that tells us whether planner used PMC or not.
     *
     * @param bag
     * @return
     */
    public static boolean plannerUsedPMC(PegasusBag bag) {
        // add the entry required by pegasus-statistics
        // PM-639
        boolean usesPMC = bag.plannerUsesPMC();
        PegasusProperties props = bag.getPegasusProperties();
        LogManager logger = bag.getLogger();
        // update from properties if required?
        if (!usesPMC) {
            String jobAggregator = props.getJobAggregator();
            // update the bag to set the flag whether
            // PMC was used or not PM-639
            if (jobAggregator.equalsIgnoreCase(JobAggregatorFactory.MPI_EXEC_CLASS)) {
                usesPMC = true;
                logger.log(
                        "Setting uses_pmc to true in braindump file on basis of properties set ",
                        LogManager.CONFIG_MESSAGE_LEVEL);
            }
        }

        return usesPMC;
    }

    /**
     * Initializes the Code Generator implementation.
     *
     * @param bag the bag of initialization objects.
     * @throws CodeGeneratorException in case of any error occuring code generation.
     */
    public void initialize(PegasusBag bag) throws CodeGeneratorException {
        mBag = bag;
        mProps = bag.getPegasusProperties();
        mPOptions = bag.getPlannerOptions();
        mSubmitFileDir = mPOptions.getSubmitDirectory();
        mLogger = bag.getLogger();
    }

    /**
     * Returns default braindump entries.
     *
     * @return default entries
     */
    public Map<String, String> defaultBrainDumpEntries(ADag workflow)
            throws CodeGeneratorException {

        // to preserve order while writing out
        Map<String, String> entries = new LinkedHashMap();
        File directory = new File(mSubmitFileDir);
        String absPath = directory.getAbsolutePath();

        // user
        String user = mProps.getProperty("user.name");
        if (user == null) {
            user = "unknown";
        }
        entries.put(Braindump.USER_KEY, user);

        // grid dn
        entries.put(Braindump.GRID_DN_KEY, getGridDN());

        // submit hostname
        entries.put(Braindump.SUBMIT_HOSTNAME_KEY, getSubmitHostname());

        entries.put(Braindump.ROOT_UUID_KEY, workflow.getRootWorkflowUUID());
        entries.put(Braindump.UUID_KEY, workflow.getWorkflowUUID());

        // dax and dax label
        entries.put("dax", mPOptions.getDAX());
        entries.put(Braindump.DAX_LABEL_KEY, workflow.getLabel());
        entries.put(Braindump.DAX_INDEX_KEY, workflow.getIndex());
        entries.put(Braindump.DAX_VERRSION_KEY, workflow.getDAXVersion());

        // the workflow name
        entries.put(Braindump.WF_NAME_KEY, workflow.getFlowName());

        // the workflow timestamp
        if (workflow.getMTime() != null) {
            entries.put(Braindump.WF_TIMESTAMP_KEY, workflow.getFlowTimestamp());
        }

        // basedir and submit directory
        entries.put("basedir", mPOptions.getBaseSubmitDirectory());
        // append( "dag " ).append(dagFile).append("\n").
        entries.put(Braindump.SUBMIT_DIR_KEY, absPath);

        // information about the planner
        StringBuffer planner = new StringBuffer();
        planner.append(mProps.getBinDir()).append(File.separator).append("pegasus-plan");
        entries.put("planner", planner.toString());

        // planner version and build
        entries.put(Braindump.PLANNER_VERSION_KEY, Version.instance().toString());
        entries.put(Braindump.BUILD_KEY, Version.instance().determineBuilt());

        // planner arguments
        StringBuffer arguments = new StringBuffer();
        arguments.append("\"").append(mPOptions.getOriginalArgString()).append("\"");
        entries.put(Braindump.PLANNER_ARGUMENTS_KEY, arguments.toString());

        // required by tailstatd
        entries.put("jsd", "jobstate.log");
        entries.put("rundir", directory.getName());
        entries.put("bindir", mProps.getBinDir().getAbsolutePath());
        entries.put("vogroup", mPOptions.getVOGroup());

        // add the entry required by pegasus-statistics
        // PM-639
        boolean usesPMC = Braindump.plannerUsedPMC(mBag);
        entries.put("uses_pmc", Boolean.toString(usesPMC));

        return entries;
    }

    /**
     * Generates the code for the executable workflow in terms of a braindump file that contains
     * workflow metadata useful for monitoring daemons etc.
     *
     * @param dag the concrete workflow.
     * @return the Collection of <code>File</code> objects for the files written out.
     * @throws CodeGeneratorException in case of any error occuring code generation.
     */
    public Collection<File> generateCode(ADag dag) throws CodeGeneratorException {
        try {

            Collection<File> result = new LinkedList();

            Map entries = this.defaultBrainDumpEntries(dag);
            // add the location of the properties file
            entries.put(
                    Braindump.PROPERTIES_KEY,
                    new File(mProps.getPropertiesInSubmitDirectory()).getName());
            result.add(writeOutBraindumpFile(entries));
            return result;
        } catch (IOException ioe) {
            throw new CodeGeneratorException(
                    "IOException while writing out the braindump file", ioe);
        }
    }

    /**
     * Generates the code for the executable workflow in terms of a braindump file that contains
     * workflow metadata useful for monitoring daemons etc.
     *
     * @param dag the concrete workflow.
     * @param additionalEntries additional entries to go in the braindump file, overwriting the
     *     default entries.
     * @return the Collection of <code>File</code> objects for the files written out.
     * @throws CodeGeneratorException in case of any error occuring code generation.
     */
    public Collection<File> generateCode(ADag dag, Map<String, String> additionalEntries)
            throws CodeGeneratorException {
        try {

            Collection<File> result = new LinkedList();
            Map<String, String> entries = this.defaultBrainDumpEntries(dag);
            // add the location of the properties file
            entries.put(
                    Braindump.PROPERTIES_KEY,
                    new File(mProps.getPropertiesInSubmitDirectory()).getName());
            entries.putAll(additionalEntries);
            result.add(writeOutBraindumpFile(entries));
            return result;
        } catch (IOException ioe) {
            throw new CodeGeneratorException(
                    "IOException while writing out the braindump file", ioe);
        }
    }

    /**
     * Method not implemented. Throws an exception.
     *
     * @param dag the workflow
     * @param job the job for which the code is to be generated.
     * @throws edu.isi.pegasus.planner.code.CodeGeneratorException
     */
    public void generateCode(ADag dag, Job job) throws CodeGeneratorException {
        throw new CodeGeneratorException(
                "Braindump generator only generates code for the whole workflow");
    }

    /**
     * Writes out the braindump.txt file for a workflow in the submit directory. The braindump.txt
     * file is used for passing to the tailstatd daemon that monitors the state of execution of the
     * workflow.
     *
     * @param entries the Map containing the entries going into the braindump file.
     * @return the absolute path to the braindump file.txt written in the directory.
     * @throws IOException in case of error while writing out file.
     */
    protected File writeOutBraindumpFile(Map<String, String> entries) throws IOException {

        // create a writer to the braindump.txt in the directory.
        File f = new File(mSubmitFileDir, BRAINDUMP_FILE);
        PrintWriter writer = new PrintWriter(new BufferedWriter(new FileWriter(f)));

        // go through all the keys and write out to the file
        for (Map.Entry<String, String> entry : entries.entrySet()) {
            StringBuffer sb = new StringBuffer();
            sb.append(entry.getKey()).append(" ").append(entry.getValue());
            writer.println(sb.toString());
        }

        writer.close();

        return f;
    }

    /**
     * Returns the submit hostname
     *
     * @return hostname
     * @throws edu.isi.pegasus.planner.code.CodeGeneratorException
     */
    protected String getSubmitHostname() throws CodeGeneratorException {
        try {
            InetAddress localMachine = java.net.InetAddress.getLocalHost();
            return localMachine.getCanonicalHostName();
        } catch (UnknownHostException ex) {
            // With all the different type of VMs floating around today, it is
            // not uncommon that the hostname is undefined. Make sure we do not
            // get hung up on that case, so always return a string.
            return "Unknown";
        }
    }

    /**
     * Returns the distinguished name from the proxy
     *
     * @return the DN else null if proxy file not found.
     */
    protected String getGridDN() {
        String dn = null;
        // load and intialize the CredentialHandler Factory
        CredentialHandlerFactory factory = new CredentialHandlerFactory();
        factory.initialize(mBag);
        CredentialHandler handler = factory.loadInstance(CredentialHandler.TYPE.x509);
        String proxy = handler.getPath("local");
        mLogger.log(
                "Proxy whose DN will be logged in the braindump file " + proxy,
                LogManager.DEBUG_MESSAGE_LEVEL);

        try {
            String defaultProxy = CoGProperties.getDefault().getProxyFile();
            if (!defaultProxy.equalsIgnoreCase(proxy)) {
                // the user specified proxy, somewhere in Pegasus configuration
                // can be properties, site catalog or environment.
                mLogger.log(
                        "X509_USER_PROXY system property is set to " + proxy,
                        LogManager.CONFIG_MESSAGE_LEVEL);
                System.setProperty(Proxy.X509_USER_PROXY_KEY, proxy);
            }
            GSSManager manager = ExtendedGSSManager.getInstance();
            GSSCredential credential = manager.createCredential(GSSCredential.INITIATE_AND_ACCEPT);
            GSSName name = credential.getName();
            if (name != null) {
                dn = name.toString();
            }
        } catch (GSSException gsse) {
            mLogger.log("Unable to determine GRID DN", gsse, LogManager.DEBUG_MESSAGE_LEVEL);
        } catch (Exception e) {
            mLogger.log(
                    "Unknown exception caught while determining the DN",
                    e,
                    LogManager.DEBUG_MESSAGE_LEVEL);
        }
        return dn;
    }
}
