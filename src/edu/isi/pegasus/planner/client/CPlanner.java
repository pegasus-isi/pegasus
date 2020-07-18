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
package edu.isi.pegasus.planner.client;

import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.common.logging.LoggingKeys;
import edu.isi.pegasus.common.util.Boolean;
import edu.isi.pegasus.common.util.DefaultStreamGobblerCallback;
import edu.isi.pegasus.common.util.FactoryException;
import edu.isi.pegasus.common.util.StreamGobbler;
import edu.isi.pegasus.common.util.Version;
import edu.isi.pegasus.planner.catalog.SiteCatalog;
import edu.isi.pegasus.planner.catalog.TransformationCatalog;
import edu.isi.pegasus.planner.catalog.site.SiteCatalogException;
import edu.isi.pegasus.planner.catalog.site.SiteFactory;
import edu.isi.pegasus.planner.catalog.site.SiteFactoryException;
import edu.isi.pegasus.planner.catalog.site.classes.GridGateway;
import edu.isi.pegasus.planner.catalog.site.classes.SiteCatalogEntry;
import edu.isi.pegasus.planner.catalog.site.classes.SiteStore;
import edu.isi.pegasus.planner.catalog.transformation.TransformationFactory;
import edu.isi.pegasus.planner.catalog.transformation.TransformationFactoryException;
import edu.isi.pegasus.planner.catalog.transformation.classes.TransformationStore;
import edu.isi.pegasus.planner.classes.ADag;
import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.classes.NameValue;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.classes.PlannerMetrics;
import edu.isi.pegasus.planner.classes.PlannerOptions;
import edu.isi.pegasus.planner.code.CodeGenerator;
import edu.isi.pegasus.planner.code.CodeGeneratorFactory;
import edu.isi.pegasus.planner.code.GridStartFactory;
import edu.isi.pegasus.planner.code.generator.Braindump;
import edu.isi.pegasus.planner.common.PegasusConfiguration;
import edu.isi.pegasus.planner.common.PegasusDBAdmin;
import edu.isi.pegasus.planner.common.PegasusProperties;
import edu.isi.pegasus.planner.common.RunDirectoryFilenameFilter;
import edu.isi.pegasus.planner.namespace.Dagman;
import edu.isi.pegasus.planner.namespace.Metadata;
import edu.isi.pegasus.planner.namespace.Pegasus;
import edu.isi.pegasus.planner.parser.DAXParserFactory;
import edu.isi.pegasus.planner.parser.dax.Callback;
import edu.isi.pegasus.planner.parser.dax.DAXParser;
import edu.isi.pegasus.planner.parser.dax.DAXParser5;
import edu.isi.pegasus.planner.refiner.MainEngine;
import edu.isi.pegasus.planner.refiner.ReplicaCatalogBridge;
import gnu.getopt.Getopt;
import gnu.getopt.LongOpt;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryUsage;
import java.nio.channels.FileChannel;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * This is the main program for the Pegasus. It parses the options specified by the user and calls
 * out to the appropriate components to parse the abstract plan, concretize it and then write the
 * submit files.
 *
 * @author Gaurang Mehta
 * @author Karan Vahi
 * @version $Revision$
 */
public class CPlanner extends Executable {

    /**
     * The basename of the directory that contains the submit files for the cleanup DAG that for the
     * concrete dag generated for the workflow.
     */
    public static final String CLEANUP_DIR = "cleanup";

    /** The prefix for the NoOP jobs that are created. */
    public static final String NOOP_PREFIX = "noop_";

    /** The name of the property key that determines whether pegasus-run should monitord or not. */
    public static final String PEGASUS_MONITORD_LAUNCH_PROPERTY_KEY = "pegasus.monitord";

    /** default dax file to parse if user does not specify one * */
    public static final String DEFAULT_WORKFLOW_DAX_FILE = "workflow.yml";

    /**
     * The regex used to match against a java property that is set using -Dpropertyname=value in the
     * argument string
     */
    public static final String JAVA_COMMAND_LINE_PROPERTY_REGEX =
            "(env|condor|globus|dagman|pegasus)\\..*=.*";

    /** The final successful message that is to be logged. */
    private static final String EMPTY_FINAL_WORKFLOW_MESSAGE =
            "\n\n\n"
                    + "The executable workflow generated contains only a single NOOP job.\n"
                    + "It seems that the output files are already at the output site. \n"
                    + "To regenerate the output data from scratch specify --force option.\n"
                    + "\n\n\n";

    /** The message to be logged in case of empty executable workflow. */
    private static final String SUCCESS_MESSAGE =
            "\n\n\n"
                    + "I have concretized your abstract workflow. The workflow has been entered \n"
                    + "into the workflow database with a state of \"planned\". The next step is \n"
                    + "to start or execute your workflow. The invocation required is"
                    + "\n\n\n";

    /** The object containing all the options passed to the Concrete Planner. */
    private PlannerOptions mPOptions;

    /** The object containing the bag of pegasus objects */
    private PegasusBag mBag;

    /** The PlannerMetrics object storing the metrics about this planning instance. */
    private PlannerMetrics mPMetrics;

    /** The number formatter to format the run submit dir entries. */
    private NumberFormat mNumFormatter;

    /** The user name of the user running Pegasus. */
    private String mUser;

    /** A boolean indicating whether metrics should be sent to metrics server or not */
    private boolean mSendMetrics;

    /** Default constructor. */
    public CPlanner() {
        this(null);
    }

    /**
     * The overload constructor.
     *
     * @param logger the logger object to use. can be null.
     */
    public CPlanner(LogManager logger) {
        super(logger);
    }

    public void initialize(String[] opts, char confChar) {
        super.initialize(opts, confChar);
        mLogMsg = "";
        mVersion = Version.instance().toString();
        mNumFormatter = new DecimalFormat("0000");

        this.mPOptions = new PlannerOptions();
        mPOptions.setSubmitDirectory(".", null);
        mPOptions.setExecutionSites(new java.util.HashSet());
        mPOptions.addOutputSite("");

        mUser = mProps.getProperty("user.name");
        if (mUser == null) {
            mUser = "user";
        }

        mPMetrics = new PlannerMetrics();
        mPMetrics.setUser(mUser);
        mSendMetrics = true;
        mBag = new PegasusBag();
    }

    /**
     * The main program for the CPlanner.
     *
     * @param args the main arguments passed to the planner.
     */
    public static void main(String[] args) {

        CPlanner cPlanner = new CPlanner();
        int result = 0;
        Date startDate = new Date();
        Date endDate = null;
        double starttime = startDate.getTime();
        double duration = -1;

        Exception plannerException = null;
        try {
            cPlanner.initialize(args, '6');
            cPlanner.mPMetrics.setStartTime(startDate);
            cPlanner.executeCommand();
        } catch (FactoryException fe) {
            plannerException = fe;
            cPlanner.log(fe.convertException(), LogManager.FATAL_MESSAGE_LEVEL);
            result = 2;
        } catch (OutOfMemoryError error) {
            cPlanner.log(
                    "Out of Memory Error " + error.getMessage(), LogManager.FATAL_MESSAGE_LEVEL);
            error.printStackTrace();
            // lets print out some GC stats
            cPlanner.logMemoryUsage();
            result = 4;
        } catch (RuntimeException rte) {
            plannerException = rte;
            // catch all runtime exceptions including our own that
            // are thrown that may have chained causes
            cPlanner.log(
                    convertException(rte, cPlanner.mLogger.getLevel()),
                    LogManager.FATAL_MESSAGE_LEVEL);
            result = 1;
        } catch (Exception e) {
            plannerException = e;
            // unaccounted for exceptions
            cPlanner.log(
                    convertException(e, cPlanner.mLogger.getLevel()),
                    LogManager.FATAL_MESSAGE_LEVEL);
            result = 3;
        } finally {
            endDate = new Date();
        }

        try {
            cPlanner.mPMetrics.setEndTime(endDate);
            double endtime = endDate.getTime();
            duration = (endtime - starttime) / 1000;
            cPlanner.mPMetrics.setDuration(duration);
            cPlanner.mPMetrics.setExitcode(result);

            if (plannerException != null) {
                // we want the stack trace to a String Writer.
                StringWriter sw = new StringWriter();
                plannerException.printStackTrace(new PrintWriter(sw));
                cPlanner.mPMetrics.setMetricsTypeToError();
                cPlanner.mPMetrics.setErrorMessage(sw.toString());
            }
            // lets write out the metrics
            if (cPlanner.mSendMetrics) {
                edu.isi.pegasus.planner.code.generator.Metrics metrics =
                        new edu.isi.pegasus.planner.code.generator.Metrics();
                metrics.initialize(cPlanner.mBag);
                metrics.logMetrics(cPlanner.mPMetrics);
            } else {
                // log
                cPlanner.log(
                        "No metrics logged or sent to the metrics server",
                        LogManager.DEBUG_MESSAGE_LEVEL);
            }

        } catch (Exception e) {
            System.out.println("ERROR while logging metrics " + e.getMessage());
        }

        // 2012-03-06 (jsv): Copy dax file to submit directory. It's
        // MUCH SIMPLER to use the parsed CLI options at this point than
        // drill open the shell wrapper without messing up everything.
        if (result == 0) {
            try {
                File src_file = new File(cPlanner.mPOptions.getDAX());
                File dst_file =
                        new File(cPlanner.mPOptions.getSubmitDirectory(), src_file.getName());
                if (!dst_file.exists()) dst_file.createNewFile();

                FileChannel fc_src = null;
                FileChannel fc_dst = null;
                try {
                    fc_src = new FileInputStream(src_file).getChannel();
                    fc_dst = new FileOutputStream(dst_file).getChannel();
                    fc_dst.transferFrom(fc_src, 0, fc_src.size());
                } finally {
                    if (fc_src != null) fc_src.close();
                    if (fc_dst != null) fc_dst.close();
                }
            } catch (IOException ieo) {
                // ignore -- copy is best effort for now
            } catch (NullPointerException npe) {
                // also ignore
            }
        }

        // warn about non zero exit code
        if (result != 0) {
            cPlanner.log(
                    "Exiting with non-zero exit-code " + result, LogManager.DEBUG_MESSAGE_LEVEL);
        } else {
            // log the time taken to execute
            cPlanner.log(
                    "Time taken to execute is " + duration + " seconds",
                    LogManager.CONSOLE_MESSAGE_LEVEL);
        }

        cPlanner.mLogger.logEventCompletion();
        System.exit(result);
    }

    /** Loads all the properties that are needed by this class. */
    public void loadProperties() {}

    /**
     * Executes the command on the basis of the options specified.
     *
     * @param args the command line options.
     */
    public void executeCommand() {
        executeCommand(parseCommandLineArguments(getCommandLineOptions()));
    }

    /**
     * Executes the command on the basis of the options specified.
     *
     * @param options the command line options.
     * @return the Collection of <code>File</code> objects for the files written out.
     */
    public Collection<File> executeCommand(PlannerOptions options) {
        String message = "";
        mPOptions = options;

        mBag.add(PegasusBag.PEGASUS_PROPERTIES, mProps);
        mBag.add(PegasusBag.PLANNER_OPTIONS, mPOptions);
        mBag.add(PegasusBag.PEGASUS_LOGMANAGER, mLogger);
        // PM-1486 set the planner directory
        mBag.add(PegasusBag.PLANNER_DIRECTORY, new File(System.getProperty("user.dir")));

        Collection result = null;

        // print help if asked for
        if (mPOptions.getHelp()) {
            // PM-816 disable metrics logging
            this.mSendMetrics = false;
            printLongVersion();
            return result;
        }

        // set the logging level only if -v was specified
        if (mPOptions.getLoggingLevel() >= 0) {
            mLogger.setLevel(mPOptions.getLoggingLevel());
        } else {
            // set log level to FATAL only
            mLogger.setLevel(LogManager.FATAL_MESSAGE_LEVEL);
        }

        PegasusConfiguration configurator = new PegasusConfiguration(mLogger);
        configurator.loadConfigurationPropertiesAndOptions(mProps, mPOptions);

        mLogger.log(
                "Planner launched in the following directory " + System.getProperty("user.dir"),
                LogManager.INFO_MESSAGE_LEVEL);
        mLogger.log(
                "Planner invoked with following arguments " + mPOptions.getOriginalArgString(),
                LogManager.INFO_MESSAGE_LEVEL);

        // do sanity check on dax file
        String dax = mPOptions.getDAX();
        String baseDir = mPOptions.getBaseSubmitDirectory();
        dax = (dax == null) ? CPlanner.DEFAULT_WORKFLOW_DAX_FILE : dax;

        // output-map is only supported for hierarchal workflows and an internal option
        if (options.getOutputMap() != null && !options.partOfDeferredRun()) {
            throw new RuntimeException(
                    "output map option is only for internal use for hierarchal worfklows");
        }

        // try to get hold of the vds properties
        // set in the jvm that user specifed at command line
        mPOptions.setVDSProperties(mProps.getMatchingProperties("pegasus.", false));

        List allVDSProps = mProps.getMatchingProperties("pegasus.", false);
        mLogger.log("Pegasus Properties set by the user", LogManager.CONFIG_MESSAGE_LEVEL);
        for (java.util.Iterator it = allVDSProps.iterator(); it.hasNext(); ) {
            NameValue nv = (NameValue) it.next();
            mLogger.log(nv.toString(), LogManager.CONFIG_MESSAGE_LEVEL);
        }

        // load the parser and parse the dax
        ADag orgDag = this.parseDAX(dax, mPOptions, mProps);
        mLogger.log(
                "Parsed DAX with following metrics " + orgDag.getWorkflowMetrics().toJson(),
                LogManager.DEBUG_MESSAGE_LEVEL);

        // check if sites set by user. If user has not specified any sites then
        // load all sites from site catalog.
        Collection<String> eSites = mPOptions.getExecutionSites();
        if (eSites.isEmpty()) {
            mLogger.log(
                    "No sites given by user. Will use sites from the site catalog",
                    LogManager.DEBUG_MESSAGE_LEVEL);
            eSites.add("*");
        }

        // load the site catalog
        SiteStore s = loadSiteStore(orgDag.getSiteStore());
        s.setForPlannerUse(mProps, mPOptions);

        // update the local/output site entry if required
        configurator.updateSiteStoreAndOptions(s, mPOptions);

        if (eSites.contains("*")) {
            // set execution sites to all sites that are loaded into site store
            // only if a user passed * option on command line or did not specify
            eSites.remove("*");
            eSites.addAll(s.list());
            // PM-1018 remove the local site from list of execution sites
            eSites.remove("local");
        }
        mLogger.log("Execution sites are " + eSites, LogManager.DEBUG_MESSAGE_LEVEL);
        for (String site : eSites) {
            if (!s.contains(site)) {
                throw new RuntimeException(
                        "Execution site "
                                + site
                                + " not loaded into site store. Loaded sites are "
                                + s.list());
            }
        }

        mBag.add(PegasusBag.SITE_STORE, s);
        mBag.add(PegasusBag.TRANSFORMATION_CATALOG, loadTransformationCatalog(mBag, orgDag));

        // populate planner metrics
        mPMetrics.setVOGroup(mPOptions.getVOGroup());
        mPMetrics.setBaseSubmitDirectory(mPOptions.getSubmitDirectory());
        mPMetrics.setDAX(mPOptions.getDAX());
        String dataConfiguration =
                mProps.getProperty(PegasusConfiguration.PEGASUS_CONFIGURATION_PROPERTY_KEY);
        dataConfiguration =
                (dataConfiguration == null)
                        ? PegasusConfiguration.DEFAULT_DATA_CONFIGURATION_VALUE
                        : dataConfiguration;
        mPMetrics.setDataConfiguration(dataConfiguration);
        mLogger.log(
                "Data Configuration used for the workflow " + dataConfiguration,
                LogManager.CONFIG_MESSAGE_LEVEL);
        mPMetrics.setPlannerOptions(mPOptions.getOriginalArgString());

        // set some initial workflow metrics
        mPMetrics.setApplicationMetrics(mProps, orgDag.getLabel());
        mPMetrics.setRootWorkflowUUID(orgDag.getRootWorkflowUUID());
        mPMetrics.setWorkflowUUID(orgDag.getWorkflowUUID());
        mPMetrics.setWorkflowMetrics(orgDag.getWorkflowMetrics());
        mPMetrics.setWFAPI(orgDag.getWFAPI());

        // write out a the relevant properties to submit directory
        int state = 0;
        // the submit directory relative to the base specified . etermine on basis of
        // --relative-submit-dir and --relative-dir
        String relativeSubmitDir = mPOptions.getRelativeSubmitDirectory();
        String relativeExecDir = mPOptions.getRelativeDirectory();
        String defaultRelativeDir = null;
        try {
            // create our own relative dir
            defaultRelativeDir =
                    determineRelativeSubmitDirectory(
                            orgDag,
                            baseDir,
                            mUser,
                            mPOptions.getVOGroup(),
                            mProps.useTimestampForDirectoryStructure());
            if (relativeSubmitDir == null) {
                // PM-1113 relative submit directory is the default relative dir
                relativeSubmitDir = defaultRelativeDir;
            }
            if (relativeExecDir == null) {
                // PM-1113 relative submit directory is the default relative dir
                relativeExecDir = defaultRelativeDir;
            }

            mPOptions.setSubmitDirectory(baseDir, relativeSubmitDir);

            if (options.partOfDeferredRun()) {
                // PM-667 log what directory the planner is launched in
                // what the base submit directory is
                String launchDir = System.getProperty("user.dir");
                mLogger.log(
                        "The directory in which the planner was launched " + launchDir,
                        LogManager.CONFIG_MESSAGE_LEVEL);

                if (!mPOptions.getForceReplan()) {
                    // if --force-replan is not set handle
                    // rescue dags
                    boolean rescue = handleRescueDAG(orgDag, mPOptions);
                    if (rescue) {
                        result = new LinkedList();
                        result.add(
                                new File(
                                        mPOptions.getSubmitDirectory(),
                                        this.getDAGFilename(orgDag, mPOptions)));

                        // for rescue dag workflows we don't want any metrics
                        // to be sent to prevent double counting.
                        mSendMetrics = false;
                        mLogger.log(
                                "No metrics will be sent for rescue dag submission",
                                LogManager.DEBUG_MESSAGE_LEVEL);

                        return result;
                    }
                }

                // replanning case. rescues already accounted for earlier.

                // the relativeSubmitDir is to be a symlink to relativeSubmitDir.XXX
                relativeSubmitDir =
                        doBackupAndCreateSymbolicLinkForSubmitDirectory(baseDir, relativeSubmitDir);

                // update the submit directory again.
                mPOptions.setSubmitDirectory(baseDir, relativeSubmitDir);
                mLogger.log(
                        "Setting relative submit dir to " + relativeSubmitDir,
                        LogManager.DEBUG_MESSAGE_LEVEL);
            } else {
                // create the relative submit directory if required
                sanityCheck(new File(baseDir, relativeSubmitDir));
            }

            state++;
            // PM-1535 we cannot write out properties file , as we need to also
            // write out the default paths for catalog files for hierachal workflows
            mProps.setPropertiesFileBackend(mPOptions.getSubmitDirectory());
            // mProps.writeOutProperties(mPOptions.getSubmitDirectory());

            mPMetrics.setRelativeSubmitDirectory(mPOptions.getRelativeSubmitDirectory());

            // also log in the planner metrics where the properties are
            mPMetrics.setProperties(mProps.getPropertiesInSubmitDirectory());
        } catch (IOException ioe) {
            String error =
                    (state == 0)
                            ? "Unable to write to directory "
                            : "Unable to set out properties file backend to directory ";
            throw new RuntimeException(error + mPOptions.getSubmitDirectory(), ioe);
        }

        // we have enough information to pin the metrics file in the submit directory
        mPMetrics.setMetricsFileLocationInSubmitDirectory(
                new File(
                        mPOptions.getSubmitDirectory(),
                        edu.isi.pegasus.planner.code.generator.Abstract.getDAGFilename(
                                mPOptions,
                                orgDag.getLabel(),
                                orgDag.getIndex(),
                                edu.isi.pegasus.planner.code.generator.Metrics
                                        .METRICS_FILE_SUFFIX)));

        mLogger.log(
                "Metrics file will be written out to "
                        + mPMetrics.getMetricsFileLocationInSubmitDirectory(),
                LogManager.CONFIG_MESSAGE_LEVEL);

        // PM-1113 check if a relativeExec dir needs to be updated because of --random-dir option
        if (mPOptions.generateRandomDirectory() && mPOptions.getRandomDir() == null) {
            // user has specified the random dir name but wants
            // to go with default name which is the flow id
            // for the workflow unless a basename is specified.
            relativeExecDir = getRandomDirectory(orgDag);
        } else if (mPOptions.getRandomDir() != null) {
            // keep the name that the user passed
            relativeExecDir = mPOptions.getRandomDir();
        }
        mLogger.log(
                "The base submit directory for the workflow        " + baseDir,
                LogManager.CONFIG_MESSAGE_LEVEL);
        mLogger.log(
                "The relative submit directory for the workflow    " + relativeSubmitDir,
                LogManager.CONFIG_MESSAGE_LEVEL);
        mLogger.log(
                "The relative execution directory for the workflow " + relativeExecDir,
                LogManager.CONFIG_MESSAGE_LEVEL);
        mPOptions.setRandomDir(relativeExecDir);

        // before starting the refinement process load
        // the stampede event generator and generate events for the dax
        generateStampedeEventsForAbstractWorkflow(orgDag, mBag);

        // populate the singleton instance for user options
        // UserOptions opts = UserOptions.getInstance(mPOptions);
        MainEngine cwmain = new MainEngine(orgDag, mBag);

        ADag finalDag = cwmain.runPlanner();

        // store the workflow metrics from the final dag into
        // the planner metrics
        mPMetrics.setWorkflowMetrics(finalDag.getWorkflowMetrics());

        CodeGenerator codeGenerator = null;
        codeGenerator = CodeGeneratorFactory.loadInstance(cwmain.getPegasusBag());

        // before generating the codes for the workflow check
        // for emtpy workflows
        boolean emptyWorkflow = false;
        if (finalDag.isEmpty()) {
            mLogger.log("Adding a noop job to the empty workflow ", LogManager.DEBUG_MESSAGE_LEVEL);
            finalDag.add(this.createNoOPJob(getNOOPJobName(finalDag)));
            emptyWorkflow = true;
        }

        message = "Generating codes for the executable workflow";
        log(message, LogManager.INFO_MESSAGE_LEVEL);

        try {
            mLogger.logEventStart(
                    LoggingKeys.EVENTS_PEGASUS_CODE_GENERATION,
                    LoggingKeys.DAX_ID,
                    finalDag.getAbstractWorkflowName());

            result = codeGenerator.generateCode(finalDag);

        } catch (Exception e) {
            throw new RuntimeException("Unable to generate code", e);
        } finally {
            // close the connection to planner cache
            mBag.getHandleToPlannerCache().close();

            mLogger.logEventCompletion();
        }

        // PM-1003 update metrics with whether pmc was used or not.
        mPMetrics.setUsesPMC(Braindump.plannerUsedPMC(mBag));

        checkMasterDatabaseForVersionCompatibility();

        // PM-1549 create an output replica catalog for the workflow if required
        createJDBCRCReplicaCatalogBackend();

        if (mPOptions.submitToScheduler()) { // submit the jobs
            StringBuffer invocation = new StringBuffer();
            // construct the path to the bin directory
            invocation
                    .append(mProps.getBinDir())
                    .append(File.separator)
                    .append(getPegasusRunInvocation());

            boolean submit = submitWorkflow(invocation.toString());
            if (!submit) {
                throw new RuntimeException("Unable to submit the workflow using pegasus-run");
            }
        } else {
            // log the success message
            this.logSuccessfulCompletion(emptyWorkflow);
        }

        // log some memory usage
        // PM-747
        if (mProps.logMemoryUsage()) {
            this.logMemoryUsage();
        }
        return result;
    }

    /**
     * Returns the name of the noop job.
     *
     * @param dag the workflow
     * @return the name
     */
    public String getNOOPJobName(ADag dag) {
        StringBuffer sb = new StringBuffer();
        sb.append(CPlanner.NOOP_PREFIX).append(dag.getLabel()).append("_").append(dag.getIndex());
        return sb.toString();
    }

    /**
     * It creates a NoOP job that runs on the submit host.
     *
     * @param name the name to be assigned to the noop job
     * @return the noop job.
     */
    protected Job createNoOPJob(String name) {

        Job newJob = new Job();

        // jobname has the dagname and index to indicate different
        // jobs for deferred planning
        newJob.setName(name);
        newJob.setTransformation("pegasus", "noop", "1.0");
        newJob.setDerivation("pegasus", "noop", "1.0");

        //        newJob.setUniverse( "vanilla" );
        newJob.setUniverse(GridGateway.JOB_TYPE.auxillary.toString());

        // the noop job does not get run by condor
        // even if it does, giving it the maximum
        // possible chance
        newJob.executable = "/bin/true";

        // construct noop keys
        newJob.setSiteHandle("local");
        newJob.setJobType(Job.CREATE_DIR_JOB);
        newJob.dagmanVariables.construct(Dagman.NOOP_KEY, "true");
        construct(newJob, "noop_job", "true");
        construct(newJob, "noop_job_exit_code", "0");

        // we do not want the job to be launched
        // by kickstart, as the job is not run actually
        newJob.vdsNS.checkKeyInNS(
                Pegasus.GRIDSTART_KEY,
                GridStartFactory.GRIDSTART_SHORT_NAMES[GridStartFactory.NO_GRIDSTART_INDEX]);

        return newJob;
    }

    /**
     * Constructs a condor variable in the condor profile namespace associated with the job.
     * Overrides any preexisting key values.
     *
     * @param job contains the job description.
     * @param key the key of the profile.
     * @param value the associated value.
     */
    protected void construct(Job job, String key, String value) {
        job.condorVariables.checkKeyInNS(key, value);
    }

    /**
     * Parses the command line arguments using GetOpt and returns a <code>PlannerOptions</code>
     * contains all the options passed by the user at the command line.
     *
     * @param args the arguments passed by the user at command line.
     * @return the options.
     */
    public PlannerOptions parseCommandLineArguments(String[] args) {
        return parseCommandLineArguments(args, true);
    }

    /**
     * Parses the command line arguments using GetOpt and returns a <code>PlannerOptions</code>
     * contains all the options passed by the user at the command line.
     *
     * @param args the arguments passed by the user at command line.
     * @param sanitizePath whether to sanitize path during construction of options
     * @return the options.
     */
    public PlannerOptions parseCommandLineArguments(String[] args, boolean sanitizePath) {
        LongOpt[] longOptions = generateValidOptions();

        // store the args with which planner was invoked

        PlannerOptions options = new PlannerOptions();
        options.setSanitizePath(sanitizePath);
        options.setOriginalArgString(args);
        // we default to inplace cleanup unless overriden on command line
        //        options.setCleanup(PlannerOptions.CLEANUP_OPTIONS.inplace );

        Getopt g =
                new Getopt(
                        "pegasus-plan",
                        args,
                        "vqhfSzVr::D:d:s:o:O:m:c:C:b:2:j:3:F:X:4:5:6:78:9:1:",
                        longOptions,
                        false);
        g.setOpterr(false);

        int option = 0;

        // construct the property matcher regex
        Pattern propertyPattern = Pattern.compile(CPlanner.JAVA_COMMAND_LINE_PROPERTY_REGEX);

        while ((option = g.getopt()) != -1) {
            // System.out.println("Option tag " + (char)option);
            switch (option) {
                case 'z': // deferred
                    options.setPartOfDeferredRun(true);
                    break;

                case 'b': // optional basename
                    options.setBasenamePrefix(g.getOptarg());
                    break;

                case 'c': // cache
                    options.setCacheFiles(g.getOptarg());
                    break;

                case 'C': // cluster
                    options.setClusteringTechnique(g.getOptarg());
                    break;

                case '1': // cleanup
                    options.setCleanup(g.getOptarg());
                    break;

                case '6': // conf
                    // PM-667 we need to track conf file option
                    options.setConfFile(g.getOptarg());
                    break;

                case 'd': // dax
                    options.setDAX(g.getOptarg());
                    mLogger.log(
                            "--dax option is deprecated. The abstract workflow is passed via the last positional argument on the commandline.",
                            LogManager.WARNING_MESSAGE_LEVEL);
                    break;

                case 'D': // -Dpegasus.blah=
                    String optarg = g.getOptarg();
                    // if( optarg.matches(  "pegasus\\..*=.*"  ) ){
                    if (propertyPattern.matcher(optarg).matches()) {
                        options.setProperty(optarg);

                    } else {
                        // JIRA PM-390 dont accept -D for --dir
                        // log warning
                        StringBuffer sb = new StringBuffer();
                        sb.append(
                                        "Submit Directory can only be set by specifying the --dir option now. ")
                                .append("Setting -D to ")
                                .append(optarg)
                                .append(" does not work");
                        mLogger.log(sb.toString(), LogManager.WARNING_MESSAGE_LEVEL);
                    }
                    break;

                case '8': // dir option
                    options.setSubmitDirectory(g.getOptarg(), null);
                    break;

                case '2': // relative-dir
                    options.setRelativeDirectory(g.getOptarg());
                    break;

                case '3': // rescue
                    options.setNumberOfRescueTries(g.getOptarg());
                    break;

                case '4': // relative-submit-dir
                    options.setRelativeSubmitDirectory(g.getOptarg());
                    break;

                case 'f': // force
                    options.setForce(true);
                    break;

                case '7': // force replan
                    options.setForceReplan(true);
                    break;

                case 'F': // forward
                    options.addToForwardOptions(g.getOptarg());
                    break;

                case 'h': // help
                    options.setHelp(true);
                    break;

                case '5': // inherited-rc-files
                    options.setInheritedRCFiles(g.getOptarg());
                    break;

                case 'I': // input-dir
                    options.setInputDirectories(g.getOptarg());
                    break;

                case 'j': // job-prefix
                    options.setJobnamePrefix(g.getOptarg());
                    break;

                case 'o': // output-site
                    options.setOutputSites(g.getOptarg());
                    break;

                case 'O': // output-dir
                    options.setOutputDirectory(g.getOptarg());
                    break;

                case 'm': // output-map
                    options.setOutputMap(g.getOptarg());
                    break;

                case 'q': // quiet
                    options.decrementLogging();
                    break;

                case 'r': // randomdir
                    options.setRandomDir(g.getOptarg());
                    break;

                case 'S': // submit option
                    options.setSubmitToScheduler(true);
                    break;

                case 's': // sites
                    options.setExecutionSites(g.getOptarg());
                    break;

                case '9': // staging-sites
                    options.addToStagingSitesMappings(g.getOptarg());
                    break;

                case 'v': // verbose
                    options.incrementLogging();
                    break;

                case 'V': // version
                    mLogger.log(getGVDSVersion(), LogManager.CONSOLE_MESSAGE_LEVEL);
                    System.exit(0);

                case 'X': // jvm options
                    options.addToNonStandardJavaOptions(g.getOptarg());
                    break;

                default: // same as help
                    printShortVersion();
                    throw new RuntimeException(
                            "Incorrect option or option usage " + (char) g.getOptopt());
            }
        }

        // try and detect if there are any unparsed components of the
        // argument string such as inadvertent white space in values
        int nonOptionArgumentIndex = g.getOptind();
        if (nonOptionArgumentIndex < args.length - 1) {
            // this works as planner does not take any positional arguments
            StringBuilder error = new StringBuilder();
            error.append("Unparsed component \"")
                    .append(args[nonOptionArgumentIndex])
                    .append("\" of the command line argument string: ")
                    .append(" ")
                    .append(options.getOriginalArgString());
            throw new RuntimeException(error.toString());
        } else if (nonOptionArgumentIndex == args.length - 1) {
            // PM-1650 the last positional argument is the workflow file to be used as input
            // sanity check
            String dax = args[nonOptionArgumentIndex];
            if (options.getDAX() != null) {
                StringBuilder error = new StringBuilder();
                error.append("You have specified both --dax \"")
                        .append(options.getDAX())
                        .append("\" and as the last positional argument \"")
                        .append(dax)
                        .append("\"");
                throw new RuntimeException(error.toString());
            }
            options.setDAX(dax);
        }

        return options;
    }

    /**
     * Submits the workflow for execution using pegasus-run, a wrapper around pegasus-submit-dag.
     *
     * @param invocation the pegasus run command
     * @return boolean indicating whether could successfully submit the workflow or not.
     */
    public boolean submitWorkflow(String invocation) {
        boolean result = false;
        try {
            // set the callback and run the pegasus-run command
            Runtime r = Runtime.getRuntime();

            mLogger.log("Executing  " + invocation, LogManager.DEBUG_MESSAGE_LEVEL);
            Process p = r.exec(invocation);

            // spawn off the gobblers with the already initialized default callback
            StreamGobbler ips =
                    new StreamGobbler(
                            p.getInputStream(),
                            new DefaultStreamGobblerCallback(LogManager.CONSOLE_MESSAGE_LEVEL));
            // error stream is also logged to console, as 5.0 pegasus-run always
            // logs to stderr and reserves stdout for it's --json option
            StreamGobbler eps =
                    new StreamGobbler(
                            p.getErrorStream(),
                            new DefaultStreamGobblerCallback(LogManager.CONSOLE_MESSAGE_LEVEL));

            ips.start();
            eps.start();

            // wait for the threads to finish off
            ips.join();
            eps.join();

            // get the status
            int status = p.waitFor();

            mLogger.log(
                    "Submission of workflow exited with status " + status,
                    LogManager.DEBUG_MESSAGE_LEVEL);

            result = (status == 0) ? true : false;
        } catch (IOException ioe) {
            mLogger.log(
                    "IOException while executing pegasus-run ",
                    ioe,
                    LogManager.ERROR_MESSAGE_LEVEL);
        } catch (InterruptedException ie) {
            // ignore
        }
        return result;
    }

    /**
     * Sets the basename of the random directory that is created on the remote sites per workflow.
     * The name is generated by default from teh flow ID, unless a basename prefix is specifed at
     * runtime in the planner options.
     *
     * @param dag the DAG containing the abstract workflow.
     * @return the basename of the random directory.
     */
    protected String getRandomDirectory(ADag dag) {

        // constructing the name of the dagfile
        StringBuffer sb = new StringBuffer();
        String bprefix = mPOptions.getBasenamePrefix();
        if (bprefix != null) {
            // the prefix is not null using it
            sb.append(bprefix);
            sb.append("-");

        } else {
            // use the flow name that contains dax name and index both
            sb.append(dag.getFlowName()).append("-");
        }
        // PM-1324 For 5.0 the --randomdir option isb updated to generate w
        // workflow uuid based names to create the scratch directories on the staging site.
        sb.append(dag.getWorkflowUUID());
        return sb.toString();
    }

    /**
     * Tt generates the LongOpt which contain the valid options that the command will accept.
     *
     * @return array of <code>LongOpt</code> objects , corresponding to the valid options
     */
    public LongOpt[] generateValidOptions() {
        LongOpt[] longopts = new LongOpt[30];

        longopts[0] = new LongOpt("dir", LongOpt.REQUIRED_ARGUMENT, null, '8');
        longopts[1] = new LongOpt("dax", LongOpt.REQUIRED_ARGUMENT, null, 'd');
        longopts[2] = new LongOpt("sites", LongOpt.REQUIRED_ARGUMENT, null, 's');
        longopts[3] = new LongOpt("verbose", LongOpt.NO_ARGUMENT, null, 'v');
        longopts[4] = new LongOpt("help", LongOpt.NO_ARGUMENT, null, 'h');
        longopts[5] = new LongOpt("force", LongOpt.NO_ARGUMENT, null, 'f');
        longopts[6] = new LongOpt("submit", LongOpt.NO_ARGUMENT, null, 'S');
        longopts[7] = new LongOpt("version", LongOpt.NO_ARGUMENT, null, 'V');
        longopts[8] = new LongOpt("randomdir", LongOpt.OPTIONAL_ARGUMENT, null, 'r');
        longopts[9] = new LongOpt("conf", LongOpt.REQUIRED_ARGUMENT, null, '6');
        longopts[10] = new LongOpt("cache", LongOpt.REQUIRED_ARGUMENT, null, 'c');
        // collapsing for mpi
        longopts[11] = new LongOpt("cluster", LongOpt.REQUIRED_ARGUMENT, null, 'C');
        // more deferred planning stuff
        longopts[12] = new LongOpt("basename", LongOpt.REQUIRED_ARGUMENT, null, 'b');
        longopts[13] = new LongOpt("nocleanup", LongOpt.NO_ARGUMENT, null, 'n');
        longopts[14] = new LongOpt("deferred", LongOpt.NO_ARGUMENT, null, 'z');
        longopts[15] = new LongOpt("relative-dir", LongOpt.REQUIRED_ARGUMENT, null, '2');
        longopts[16] = new LongOpt("job-prefix", LongOpt.REQUIRED_ARGUMENT, null, 'j');
        longopts[17] = new LongOpt("rescue", LongOpt.REQUIRED_ARGUMENT, null, '3');
        longopts[18] = new LongOpt("forward", LongOpt.REQUIRED_ARGUMENT, null, 'F');
        longopts[19] = new LongOpt("X", LongOpt.REQUIRED_ARGUMENT, null, 'X');
        longopts[20] = new LongOpt("relative-submit-dir", LongOpt.REQUIRED_ARGUMENT, null, '4');
        longopts[21] = new LongOpt("quiet", LongOpt.NO_ARGUMENT, null, 'q');
        longopts[22] = new LongOpt("inherited-rc-files", LongOpt.REQUIRED_ARGUMENT, null, '5');
        longopts[23] = new LongOpt("force-replan", LongOpt.NO_ARGUMENT, null, '7');
        longopts[24] = new LongOpt("staging-site", LongOpt.REQUIRED_ARGUMENT, null, '9');
        longopts[25] = new LongOpt("input-dir", LongOpt.REQUIRED_ARGUMENT, null, 'I');
        longopts[26] = new LongOpt("output-dir", LongOpt.REQUIRED_ARGUMENT, null, 'O');
        longopts[27] = new LongOpt("output-sites", LongOpt.REQUIRED_ARGUMENT, null, 'o');
        longopts[28] = new LongOpt("output-map", LongOpt.REQUIRED_ARGUMENT, null, 'm');
        longopts[29] = new LongOpt("cleanup", LongOpt.REQUIRED_ARGUMENT, null, '1');
        return longopts;
    }

    /** Prints out a short description of what the command does. */
    public void printShortVersion() {
        String text =
                getGVDSVersion()
                        + "\n"
                        + "Usage : pegasus-plan [-Dprop=value…]] [-b prefix]\n"
                        + "                     [-v] [-q] [-V] [-h]\n"
                        + "                     [--conf propsfile] [-c cachefile[,cachefile…]] [--cleanup cleanup strategy ]\n"
                        + "                     [-C style[,style…]] [--dir dir] [--force] [--force-replan]\n"
                        + "                     [--inherited-rc-files file1[,file2…]] [-j prefix] [-n][-I input-dir1[,input-dir2…]]\n"
                        + "                     [-O output-dir] [-o site1[,site2…]] [-s site1[,site2…]] [--staging-site s1=ss1[,s2=ss2[..]]\n"
                        + "                     [--randomdir[=dirname]] [--relative-dir dir] [--relative-submit-dir dir]\n"
                        + "                     [-X[non standard jvm option]]\n"
                        + "                     abstract-workflow]";

        System.out.println(text);
    }

    /**
     * Prints the long description, displaying in detail what the various options to the command
     * stand for.
     */
    public void printLongVersion() {

        StringBuffer text = new StringBuffer();
        text.append("\n $Id$ ")
                .append("\n " + getGVDSVersion())
                .append(
                        "\n pegasus-plan - The main command line client which is used to run Pegasus")
                .append("\nUsage : pegasus-plan [-Dprop=value…]] [-b prefix]")
                .append("\n                     [-v] [-q] [-V] [-h]")
                .append(
                        "\n                     [--conf propsfile] [-c cachefile[,cachefile…]] [--cleanup cleanup strategy ]")
                .append(
                        "\n                     [-C style[,style…]] [--dir dir] [--force] [--force-replan]")
                .append(
                        "\n                     [--inherited-rc-files file1[,file2…]] [-j prefix] [-n][-I input-dir1[,input-dir2…]]")
                .append(
                        "\n                     [-O output-dir] [-o site1[,site2…]] [-s site1[,site2…]] [--staging-site s1=ss1[,s2=ss2[..]]")
                .append(
                        "\n                     [--randomdir[=dirname]] [--relative-dir dir] [--relative-submit-dir dir]")
                .append("\n                     [-X[non standard jvm option]]")
                .append("\n                     [abstract-workflow]")
                .append("\n Options ")
                .append(
                        "\n -b |--basename        the basename prefix while constructing the per workflow files like .dag etc.")
                .append("\n -c |--cache           comma separated list of replica cache files.")
                .append(
                        "\n --inherited-rc-files  comma separated list of replica files. Locations mentioned in these have a lower priority")
                .append("\n                       than the locations in the DAX file")
                .append(
                        "\n --cleanup             the cleanup strategy to use. Can be none|inplace|leaf|constraint. Defaults to inplace. ")
                .append(
                        "\n -C |--cluster         comma separated list of clustering techniques to be applied to the workflow to ")
                .append(
                        "\n                       to cluster jobs in to larger jobs, to avoid scheduling overheads.")
                .append(
                        "\n --conf                the path to the properties file to use for planning. Defaults to pegasus.properties file")
                .append("\n                       in the current working directory ")
                .append(
                        "\n --dir                 the directory where to generate the executable workflow.")
                .append(
                        "\n --relative-dir        the relative directory to the base directory where to generate the concrete workflow.")
                .append(
                        "\n --relative-submit-dir the relative submit directory where to generate the concrete workflow. Overrides --relative-dir .")
                .append(
                        "\n -f |--force           skip reduction of the workflow, resulting in build style dag.")
                .append(
                        "\n --force-replan        force replanning for sub workflows in case of failure. ")
                .append(
                        "\n -F |--forward         any options that need to be passed ahead to pegasus-run in format option[=value] ")
                .append(
                        "\n                       where value can be optional. e.g -F nogrid will result in --nogrid . The option ")
                .append("\n                       can be repeated multiple times.")
                .append(
                        "\n -j |--job-prefix      the prefix to be applied while construction job submit filenames ")
                .append(
                        "\n -I |--input-dir       comma separated list of optional input directories where the input files reside on submit host")
                .append(
                        "\n -O |--output-dir      an optional output directory where the output files should be transferred to on submit host. ")
                .append(
                        "\n                       the directory specified is asscociated with the local-storage directory for the output site.")
                .append(
                        "\n -o |--output-sites    comma separated list of output sites where the data products during workflow execution are ")
                .append("\n                       transferred to.")
                .append(
                        "\n -s |--sites           comma separated list of executions sites on which to map the workflow.")
                .append(
                        "\n --staging-site        comma separated list of key=value pairs , where the key is the execution site and value is the")
                .append("\n                       staging site for that execution site.")
                .append(
                        "\n -r |--randomdir       create random directories on remote execution sites in which jobs are executed")
                // "\n --rescue           the number of times rescue dag should be submitted for sub
                // workflows before triggering re-planning" +
                .append(
                        "\n                       can optionally specify the basename of the remote directories")
                .append("\n -S |--submit          submit the executable workflow generated")
                .append(
                        "\n --staging-site        comma separated list of key=value pairs, where key is the execution site and value is the")
                .append("\n                       staging site")
                .append(
                        "\n -v |--verbose         increases the verbosity of messages about what is going on")
                .append(
                        "\n -q |--quiet           decreases the verbosity of messages about what is going on")
                .append(
                        "\n -V |--version         displays the version of the Pegasus Workflow Management System")
                .append(
                        "\n -X[non standard java option]  pass to jvm a non standard option . e.g. -Xmx1024m -Xms512m")
                .append("\n -h |--help            generates this help.")
                .append(
                        "\n [abstract-workflow]   the YAML input file that describes an abstract workflow. If not specified")
                .append(
                        "\n                       the planner defaults to file *workflow.yml* in the current working directory. ")
                .append("\n The following exitcodes are produced")
                .append("\n 0 planner was able to generate an executable workflow")
                .append(
                        "\n 1 an error occured. In most cases, the error message logged should give a")
                .append("\n   clear indication as to where  things went wrong.")
                .append(
                        "\n 2 an error occured while loading a specific module implementation at runtime")
                .append("\n 3 an unaccounted java exception occured at runtime")
                .append(
                        "\n 4 encountered an out of memory exception. Most probably ran out of heap memory.")
                .append("\n ");

        System.out.println(text);
        // mLogger.log(text,LogManager.INFO_MESSAGE_LEVEL);
    }

    /**
     * Determines the workflow uuid for a workflow
     *
     * @param dag the workflow
     * @param options the options passed to the planner
     * @param properties the properties passed to the planner
     * @return uuid for the root workflow instance
     */
    private String determineRootWorkflowUUID(
            ADag dag, PlannerOptions options, PegasusProperties properties) {
        // figure out the root workflow uuid to put for pegasus-state
        // JIRA PM-396

        String uuid = null;
        if (options.partOfDeferredRun()) {
            // in recursive workflow we are not on the root , but some level
            // we have to retrive from properties
            uuid = properties.getRootWorkflowUUID();
        } else {
            // the root workflow uuid is the uuid of the workflow
            // being planned right now. We are on the root level of the recursive
            // workflows
            uuid = dag.getWorkflowUUID();
        }
        if (uuid == null) {
            // something amiss
            throw new RuntimeException("Unable to determine Root Workflow UUID");
        }

        return uuid;
    }

    /**
     * Creates the submit directory for the workflow. This is not thread safe.
     *
     * @param dag the workflow being worked upon.
     * @param dir the base directory specified by the user.
     * @param user the username of the user.
     * @param vogroup the vogroup to which the user belongs to.
     * @param timestampBased boolean indicating whether to have a timestamp based dir or not
     * @return the directory name created relative to the base directory passed as input.
     * @throws IOException in case of unable to create submit directory.
     */
    protected String determineRelativeSubmitDirectory(
            ADag dag, String dir, String user, String vogroup, boolean timestampBased)
            throws IOException {

        return determineRelativeSubmitDirectory(dag.getLabel(), dir, user, vogroup, timestampBased);
    }

    /**
     * Creates the submit directory for the workflow. This is not thread safe.
     *
     * @param label the label of the workflow
     * @param dir the base directory specified by the user.
     * @param user the username of the user.
     * @param vogroup the vogroup to which the user belongs to.
     * @param timestampBased boolean indicating whether to have a timestamp based dir or not
     * @return the directory name created relative to the base directory passed as input.
     * @throws IOException in case of unable to create submit directory.
     */
    protected String determineRelativeSubmitDirectory(
            String label, String dir, String user, String vogroup, boolean timestampBased)
            throws IOException {
        File base = new File(dir);
        StringBuffer result = new StringBuffer();

        // do a sanity check on the base
        sanityCheck(base);

        // add the user name if possible
        base = new File(base, user);
        result.append(user).append(File.separator);

        // add the vogroup
        base = new File(base, vogroup);
        sanityCheck(base);
        result.append(vogroup).append(File.separator);

        // add the label of the DAX
        base = new File(base, label);
        sanityCheck(base);
        result.append(label).append(File.separator);

        // create the directory name
        StringBuffer leaf = new StringBuffer();
        if (timestampBased) {
            leaf.append(mPOptions.getDateTime(mProps.useExtendedTimeStamp()));
        } else {
            // get all the files in this directory
            String[] files = base.list(new RunDirectoryFilenameFilter());
            // find the maximum run directory
            int num, max = 1;
            for (int i = 0; i < files.length; i++) {
                num =
                        Integer.parseInt(
                                files[i].substring(
                                        RunDirectoryFilenameFilter.SUBMIT_DIRECTORY_PREFIX
                                                .length()));
                if (num + 1 > max) {
                    max = num + 1;
                }
            }

            // create the directory name
            leaf.append(RunDirectoryFilenameFilter.SUBMIT_DIRECTORY_PREFIX)
                    .append(mNumFormatter.format(max));
        }
        result.append(leaf.toString());
        base = new File(base, leaf.toString());
        mLogger.log(
                "Directory to be created is " + base.getAbsolutePath(),
                LogManager.DEBUG_MESSAGE_LEVEL);

        return result.toString();
    }

    /**
     * Checks the destination location for existence, if it can be created, if it is writable etc.
     *
     * @param dir is the new base directory to optionally create.
     * @throws IOException in case of error while writing out files.
     */
    protected static void sanityCheck(File dir) throws IOException {
        if (dir.exists()) {
            // location exists
            if (dir.isDirectory()) {
                // ok, isa directory
                if (dir.canWrite()) {
                    // can write, all is well
                    return;
                } else {
                    // all is there, but I cannot write to dir
                    throw new IOException("Cannot write to existing directory " + dir.getPath());
                }
            } else {
                // exists but not a directory
                throw new IOException(
                        "Destination "
                                + dir.getPath()
                                + " already "
                                + "exists, but is not a directory.");
            }
        } else {
            // does not exist, try to make it
            if (!dir.mkdirs()) {

                // try to get around JVM bug. JIRA PM-91
                if (dir.getPath().endsWith(".")) {
                    // just try to create the parent directory
                    if (!dir.getParentFile().mkdirs()) {
                        throw new IOException("Unable to create  directory " + dir.getPath());
                    }
                    return;
                }

                throw new IOException("Unable to create  directory " + dir.getPath());
            }
        }
    }

    /**
     * Returns the basename of the dag file
     *
     * @param dag the dag that was parsed.
     * @param options the planner options
     * @return boolean true means submit the rescue false do the planning operation
     */
    protected String getDAGFilename(ADag dag, PlannerOptions options) {
        // determine the name of the .dag file that will be written out.
        // constructing the name of the dagfile
        StringBuffer sb = new StringBuffer();
        String bprefix = options.getBasenamePrefix();
        if (bprefix != null) {
            // the prefix is not null using it
            sb.append(bprefix);
        } else {
            // generate the prefix from the name of the dag
            sb.append(dag.getLabel()).append("-").append(dag.getIndex());
        }
        // append the suffix
        sb.append(".dag");
        return sb.toString();
    }

    /**
     * Checks for rescue dags, and determines whether to plan or not.
     *
     * @param dag the dag that was parsed.
     * @param options the planner options
     * @return boolean true means submit the rescue false do the planning operation
     */
    protected boolean handleRescueDAG(ADag dag, PlannerOptions options) {

        return this.handleRescueDAG(
                getDAGFilename(dag, options),
                options.getSubmitDirectory(),
                options.getNumberOfRescueTries());
    }

    /**
     * Checks for rescue dags, and determines whether to submit a rescue dag or not.
     *
     * @param dag the dag file for the dax
     * @param dir the submit directory.
     * @param numOfRescues the number of rescues to handle.
     * @return true means submit the rescue false do the planning operation
     */
    protected boolean handleRescueDAG(String dag, String dir, int numOfRescues) {
        boolean result = false;
        // sanity check
        if (numOfRescues < 1) {
            return result;
        }

        // check for existence of dag file
        // if it does not exists means we need to plan
        File dagFile = new File(dir, dag);
        mLogger.log(
                "Determining existence of dag file " + dagFile.getAbsolutePath(),
                LogManager.DEBUG_MESSAGE_LEVEL);
        if (!dagFile.exists()) {
            return result;
        }

        /*
        //if it is default max value , then return true always
        if( numOfRescues == PlannerOptions.DEFAULT_NUMBER_OF_RESCUE_TRIES ){
            return true;
        }
         */

        int largestRescue = 0;
        String largestRescueFile = null;
        // check for existence of latest rescue file.
        NumberFormat nf = new DecimalFormat("000");
        for (int i = 1; i <= numOfRescues + 1; i++) {
            String rescue = dag + ".rescue" + nf.format(i);
            File rescueFile = new File(dir, rescue);
            mLogger.log(
                    "Determining existence of rescue file " + rescueFile.getAbsolutePath(),
                    LogManager.DEBUG_MESSAGE_LEVEL);
            if (rescueFile.exists()) {
                largestRescue = i;
                largestRescueFile = rescue;
            } else {
                break;
            }
        }

        if (largestRescue == 0) {
            // no rescue dag. but the dag still exists
            mLogger.log(
                    "No planning attempted. Existing DAG will be submitted " + dagFile,
                    LogManager.CONSOLE_MESSAGE_LEVEL);
            return true;
        }

        if (largestRescue == numOfRescues + 1) {
            // we need to start planning now
            mLogger.log(
                    "Reached user specified limit of rescue retries "
                            + numOfRescues
                            + " .Replanning will be triggered ",
                    LogManager.CONFIG_MESSAGE_LEVEL);
            return false;
        }

        if (largestRescueFile != null) {
            // a rescue file was detected . lets log that
            mLogger.log(
                    "Rescue DAG will be submitted. Largest Rescue File detected was "
                            + largestRescueFile,
                    LogManager.CONSOLE_MESSAGE_LEVEL);
        }
        return true;
    }

    /**
     * This method generates a symlink between two files
     *
     * @param source the file that has to be symlinked
     * @param destination the destination of the symlink
     * @param directory the directory in which to execute the command
     * @param logErrorToDebug whether to log messeage to debug or not
     * @return boolean indicating if creation of symlink was successful or not
     */
    protected boolean createSymbolicLink(
            String source, String destination, File directory, boolean logErrorToDebug) {
        try {
            Runtime rt = Runtime.getRuntime();
            String command = "ln -sf " + source + " " + destination;
            mLogger.log(
                    "Creating symlink between " + source + " " + destination,
                    LogManager.DEBUG_MESSAGE_LEVEL);

            Process p =
                    (directory == null)
                            ? rt.exec(command, null)
                            : // dont specify the directory to execute in
                            rt.exec(command, null, directory);

            // set up to read subprogram output
            InputStream is = p.getInputStream();
            InputStreamReader isr = new InputStreamReader(is);
            BufferedReader br = new BufferedReader(isr);

            // set up to read subprogram error
            InputStream er = p.getErrorStream();
            InputStreamReader err = new InputStreamReader(er);
            BufferedReader ebr = new BufferedReader(err);

            // read output from subprogram
            // and display it

            String s, se = null;
            while (((s = br.readLine()) != null) || ((se = ebr.readLine()) != null)) {
                if (s != null) {
                    mLogger.log(s, LogManager.DEBUG_MESSAGE_LEVEL);
                } else {
                    if (logErrorToDebug) {
                        mLogger.log(se, LogManager.DEBUG_MESSAGE_LEVEL);
                    } else {
                        mLogger.log(se, LogManager.ERROR_MESSAGE_LEVEL);
                    }
                }
            }

            br.close();
            return true;
        } catch (Exception ex) {
            if (logErrorToDebug) {
                mLogger.log(
                        "Unable to create symlink to the log file",
                        ex,
                        LogManager.DEBUG_MESSAGE_LEVEL);
            } else {
                mLogger.log(
                        "Unable to create symlink to the log file",
                        ex,
                        LogManager.ERROR_MESSAGE_LEVEL);
            }
            return false;
        }
    }

    /**
     * Generates events for the abstract workflow.
     *
     * @param workflow the parsed dax
     * @param bag the initialized object bag
     */
    private void generateStampedeEventsForAbstractWorkflow(ADag workflow, PegasusBag bag) {
        CodeGenerator codeGenerator =
                CodeGeneratorFactory.loadInstance(
                        bag, CodeGeneratorFactory.STAMPEDE_EVENT_GENERATOR_CLASS);

        mLogger.logEventStart(
                LoggingKeys.EVENTS_PEGASUS_STAMPEDE_GENERATION,
                LoggingKeys.DAX_ID,
                workflow.getAbstractWorkflowName());

        //        String message = "Generating Stampede Events for Abstract Workflow";
        //        log( message, LogManager.INFO_MESSAGE_LEVEL );

        try {
            Collection result = codeGenerator.generateCode(workflow);
            for (Iterator it = result.iterator(); it.hasNext(); ) {
                mLogger.log(
                        "Written out stampede events for the abstract workflow to " + it.next(),
                        LogManager.DEBUG_MESSAGE_LEVEL);
            }
        } catch (Exception e) {
            throw new RuntimeException(
                    "Unable to generate stampede events for abstract workflow", e);
        }

        mLogger.logEventCompletion();
        //        mLogger.log( message + " -DONE", LogManager.INFO_MESSAGE_LEVEL );

    }

    /**
     * Loads the sites from the site catalog into the site store
     *
     * @param daxSiteStore the site catalog entries from the DAX
     * @return SiteStore object containing the information about the sites.
     */
    private SiteStore loadSiteStore(SiteStore daxSiteStore) {
        SiteStore result = new SiteStore();
        // PM-1515 we prefer entries in the DAX Site Store
        // so load them first
        for (Iterator<SiteCatalogEntry> it = daxSiteStore.entryIterator(); it.hasNext(); ) {
            result.addEntry(it.next());
        }

        SiteCatalog catalog = null;

        /* load the catalog using the factory */
        try {
            catalog = SiteFactory.loadInstance(mBag);

            // PM-1047 we want to save the catalogs all around.
            result.setFileSource(catalog.getFileSource());
        } catch (SiteFactoryException e) {
            // PM-1515 site catalog exceptions to be ignored, as
            // we can have entries in the DAX and also the planner
            // generates default entries
            mLogger.log(
                    "Ignoring exception encountered while loading site catalog "
                            + e.convertException(),
                    LogManager.DEBUG_MESSAGE_LEVEL);
        }

        if (catalog != null) {
            // PM-1515 make sure catalog was instantiated
            Set<String> toLoad = new HashSet<String>();
            mLogger.log(
                    "All sites will be loaded from the site catalog",
                    LogManager.DEBUG_MESSAGE_LEVEL);
            toLoad.add("*");

            /* always load local site */
            toLoad.add("local");

            /* load the sites in site catalog */
            try {
                catalog.load(new LinkedList(toLoad));

                // load into SiteStore from the catalog.
                if (toLoad.contains("*")) {
                    // we need to load all sites into the site store
                    toLoad.addAll(catalog.list());
                }
                for (Iterator<String> it = toLoad.iterator(); it.hasNext(); ) {
                    SiteCatalogEntry s = catalog.lookup(it.next());
                    if (s != null && result.lookup(s.getSiteHandle()) == null) {
                        // PM-1515 prefer entries from DAX SiteStore.
                        // Only load from catalog if not in DAX SiteStore
                        result.addEntry(s);
                    }
                }
            } catch (SiteCatalogException e) {
                throw new RuntimeException("Unable to load from site catalog ", e);
            } finally {
                /* close the connection */
                try {
                    catalog.close();
                } catch (Exception e) {
                }
            }
        }

        /* query for the sites, and print them out */
        mLogger.log("Sites loaded are " + result.list(), LogManager.DEBUG_MESSAGE_LEVEL);
        return result;
    }

    /**
     * Logs the successful completion message.
     *
     * @param emptyWorkflow indicates whether the workflow created was empty or not.
     */
    private void logSuccessfulCompletion(boolean emptyWorkflow) {
        StringBuffer message = new StringBuffer();
        message.append(
                        emptyWorkflow
                                ? CPlanner.EMPTY_FINAL_WORKFLOW_MESSAGE
                                : CPlanner.SUCCESS_MESSAGE)
                .append("")
                .append(getPegasusRunInvocation())
                .append("\n\n");
        mLogger.log(message.toString(), LogManager.CONSOLE_MESSAGE_LEVEL);
    }

    /**
     * Returns the pegasus-run command on the workflow planned.
     *
     * @return the pegasus-run command
     */
    private String getPegasusRunInvocation() {
        StringBuffer result = new StringBuffer();

        result.append("pegasus-run ");

        // check if we need to add any other options to pegasus-run
        for (Iterator<NameValue> it = mPOptions.getForwardOptions().iterator(); it.hasNext(); ) {
            NameValue nv = it.next();
            result.append(" --").append(nv.getKey());
            if (nv.getValue() != null) {
                result.append(" ").append(nv.getValue());
            }
        }

        result.append(" ").append(mPOptions.getSubmitDirectory());

        return result.toString();
    }

    protected String doBackupAndCreateSymbolicLinkForSubmitDirectory(
            String baseDir, String relativeSubmitDir) throws IOException {

        // find the maximum run directory
        // get the parent of the current relativeSubmitDir
        File f = new File(relativeSubmitDir);
        String relativeParentSubmitDir = f.getParent();
        File parent =
                (relativeParentSubmitDir == null)
                        ? new File(baseDir)
                        : new File(baseDir, relativeParentSubmitDir);

        String basename = f.getName();

        int num, max = 0;
        String prefix = basename + ".";

        // check if parent exists. first time around the submit directory for
        // sub workflow may not exist
        if (parent.exists()) {
            String[] files = parent.list(new SubmitDirectoryFilenameFilter(basename));
            for (int i = 0; i < files.length; i++) {
                num = Integer.parseInt(files[i].substring(prefix.length()));
                if (num + 1 > max) {
                    max = num + 1;
                }
            }
        }

        // create the directory name
        NumberFormat formatter = new DecimalFormat("000");

        // prefix is just the basname of relativeSubmitDir.XXX
        prefix = prefix + formatter.format(max);

        String relativeSubmitDirXXX =
                (relativeParentSubmitDir == null)
                        ? new File(prefix).getPath()
                        : new File(relativeParentSubmitDir, prefix).getPath();

        // create the relativeSubmitDirXXX
        File fRelativeSubmitDirXXX = new File(baseDir, relativeSubmitDirXXX);
        sanityCheck(fRelativeSubmitDirXXX);

        // we have to create a symlink between relativeSubmitDir and relativeSubmitDir.xxx
        // and update relativeSubmitDir to be relativeSubmitDir.xxx
        File destination = new File(baseDir, relativeSubmitDir);

        if (destination.exists()) {
            // delete existing file
            // no way in java to detect if a file is a symbolic link
            destination.delete();
        }

        // we want symlinks to be created in parent directory
        // without absolute paths
        createSymbolicLink(fRelativeSubmitDirXXX.getName(), destination.getName(), parent, true);

        return relativeSubmitDirXXX;
    }

    /** Logs memory usage of the JVM */
    private void logMemoryUsage() {
        try {
            String memoryUsage = new String();
            List<MemoryPoolMXBean> pools = ManagementFactory.getMemoryPoolMXBeans();
            double totalUsed = 0; // in bytes
            double totalReserved = 0; // in bytes
            double divisor = 1024 * 1024; // display stats in MB
            for (MemoryPoolMXBean pool : pools) {
                MemoryUsage peak = pool.getPeakUsage();
                totalUsed += peak.getUsed();
                totalReserved += peak.getCommitted();
                memoryUsage +=
                        String.format(
                                "Peak %s memory used    : %.3f MB%n",
                                pool.getName(), peak.getUsed() / divisor);
                memoryUsage +=
                        String.format(
                                "Peak %s memory reserved: %.3f MB%n",
                                pool.getName(), peak.getCommitted() / divisor);
            }

            // we print the result in the console
            mLogger.log(
                    "JVM Memory Usage Breakdown \n" + memoryUsage.toString(),
                    LogManager.INFO_MESSAGE_LEVEL);
            mLogger.log(
                    String.format("Total Peak memory used      : %.3f MB", totalUsed / divisor),
                    LogManager.INFO_MESSAGE_LEVEL);
            mLogger.log(
                    String.format("Total Peak memory reserved  : %.3f MB", totalReserved / divisor),
                    LogManager.INFO_MESSAGE_LEVEL);
        } catch (Throwable t) {
            // not fatal
            mLogger.log(
                    "Error while logging peak memory usage " + t.getMessage(),
                    LogManager.ERROR_MESSAGE_LEVEL);
        }
    }

    /**
     * Parses the DAX and returns the associated ADag object
     *
     * @param dax path to the DAX file.
     * @param options the planner options
     * @param properties the properties file passed
     * @return
     */
    private ADag parseDAX(String dax, PlannerOptions options, PegasusProperties properties) {

        DAXParser p =
                DAXParserFactory.loadDAXParser(mBag, DAXParserFactory.DEFAULT_CALLBACK_CLASS, dax);
        Callback cb = p.getDAXCallback();
        p.parse(dax);
        ADag dag = (ADag) cb.getConstructedObject();
        // generate the flow ids for the classads information
        dag.generateFlowName();
        dag.setFlowTimestamp(options.getDateTime(properties.useExtendedTimeStamp()));
        dag.setDAXMTime(new File(dax));
        dag.generateFlowID();
        dag.setReleaseVersion();

        // set out the root workflow id
        dag.setRootWorkflowUUID(determineRootWorkflowUUID(dag, options, properties));

        // PM-1654 generate a default wf.api metadata key if not present already
        // in the parsed workflow
        String defaultAPI = p instanceof DAXParser5 ? "yaml" : "xml";
        if (dag.getAllMetadata() == null // no metadata in the workflow
                ||
                // metadata exists but does not have either dax.api or wf.api
                !(dag.getAllMetadata().containsKey(Metadata.DAX_API_KEY)
                        || dag.getAllMetadata().containsKey(Metadata.WF_API_KEY))) {
            // we always add wf.api if none is present
            dag.addMetadata(Metadata.WF_API_KEY, defaultAPI);
        }

        return dag;
    }

    /** Calls out to the pegasus-db-admin tool to check for database compatibility. */
    private void checkMasterDatabaseForVersionCompatibility() {
        PegasusDBAdmin dbCheck = new PegasusDBAdmin(mBag.getLogger());
        dbCheck.checkMasterDatabaseForVersionCompatibility(mProps.getPropertiesInSubmitDirectory());
    }

    /**
     * Calls out to the pegasus-db-admin tool to create the JDBCRC backed for output replica catalog
     */
    private void createJDBCRCReplicaCatalogBackend() {
        boolean create =
                Boolean.parse(
                        this.mProps.getProperty(
                                ReplicaCatalogBridge.OUTPUT_REPLICA_CATALOG_PREFIX
                                        + "."
                                        + "db.create"),
                        false);
        if (create) {
            PegasusDBAdmin dbCreate = new PegasusDBAdmin(mBag.getLogger());
            if (dbCreate.createJDBCRC(mProps.getPropertiesInSubmitDirectory())) {
                mLogger.log(
                        "Output replica catalog set to "
                                + this.mProps.getProperty(
                                        ReplicaCatalogBridge.OUTPUT_REPLICA_CATALOG_PREFIX
                                                + "."
                                                + "db.url"),
                        LogManager.CONSOLE_MESSAGE_LEVEL);
            }
        }
    }

    /**
     * Loads the transformation catalog. Throws an exception encountered while loading only if the
     * daxStore is null or empty
     *
     * @param bag
     * @param daxStore
     * @return
     */
    private TransformationCatalog loadTransformationCatalog(PegasusBag bag, ADag dag) {

        TransformationCatalog store = null;
        TransformationStore daxStore = dag.getTransformationStore();
        try {
            store = TransformationFactory.loadInstance(bag);
        } catch (TransformationFactoryException e) {
            if ((daxStore == null || daxStore.isEmpty())
                    && dag.getWorkflowMetrics().getTaskCount(Job.COMPUTE_JOB)
                            != 0) { // pure hierarchal workflows with no compute jobs should not
                // throw error
                throw e;
            }
            // log the error nevertheless
            bag.getLogger()
                    .log(
                            "Ignoring error encountered while loading Transformation Catalog "
                                    + e.convertException(),
                            LogManager.DEBUG_MESSAGE_LEVEL);
        }
        // create a temp file as a TC backend for planning purposes
        if (store == null) {
            File f = null;
            try {
                f = File.createTempFile("tc.", ".txt");
                bag.getLogger()
                        .log(
                                "Created a temporary transformation catalog backend " + f,
                                LogManager.DEBUG_MESSAGE_LEVEL);
            } catch (IOException ex) {
                throw new RuntimeException(
                        "Unable to create a temporary transformation catalog backend " + f, ex);
            }
            PegasusBag b = new PegasusBag();
            b.add(PegasusBag.PEGASUS_LOGMANAGER, bag.getLogger());
            PegasusProperties props = PegasusProperties.nonSingletonInstance();
            props.setProperty(
                    PegasusProperties.PEGASUS_TRANSFORMATION_CATALOG_PROPERTY,
                    TransformationFactory.TEXT_CATALOG_IMPLEMENTOR);
            props.setProperty(
                    PegasusProperties.PEGASUS_TRANSFORMATION_CATALOG_FILE_PROPERTY,
                    f.getAbsolutePath());
            b.add(PegasusBag.PEGASUS_PROPERTIES, props);
            return loadTransformationCatalog(b, dag);
        }
        return store;
    }
}
/**
 * A filename filter for identifying the submit directory
 *
 * @author Karan Vahi vahi@isi.edu
 */
class SubmitDirectoryFilenameFilter implements FilenameFilter {

    /** Store the regular expressions necessary to parse kickstart output files */
    private String mRegexExpression;

    /** Stores compiled patterns at first use, quasi-Singleton. */
    private Pattern mPattern = null;

    /**
     * Overloaded constructor.
     *
     * @param prefix prefix for the submit directory
     */
    public SubmitDirectoryFilenameFilter(String prefix) {
        mRegexExpression = "(" + prefix + ")([\\.][0-9][0-9][0-9])";
        mPattern = Pattern.compile(mRegexExpression);
    }

    /**
     * * Tests if a specified file should be included in a file list.
     *
     * @param dir the directory in which the file was found.
     * @param name - the name of the file.
     * @return true if and only if the name should be included in the file list false otherwise.
     */
    public boolean accept(File dir, String name) {

        return mPattern.matcher(name).matches();
    }
}
