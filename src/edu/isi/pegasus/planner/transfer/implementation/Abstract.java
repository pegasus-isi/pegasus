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
package edu.isi.pegasus.planner.transfer.implementation;

import edu.isi.pegasus.common.credential.impl.Irods;
import edu.isi.pegasus.common.credential.impl.PegasusCredentials;
import edu.isi.pegasus.common.credential.impl.Proxy;
import edu.isi.pegasus.common.credential.impl.S3CFG;
import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.common.util.PegasusURL;
import edu.isi.pegasus.common.util.Separator;
import edu.isi.pegasus.planner.catalog.TransformationCatalog;
import edu.isi.pegasus.planner.catalog.site.classes.GridGateway;
import edu.isi.pegasus.planner.catalog.site.classes.SiteCatalogEntry;
import edu.isi.pegasus.planner.catalog.site.classes.SiteStore;
import edu.isi.pegasus.planner.catalog.transformation.TransformationCatalogEntry;
import edu.isi.pegasus.planner.catalog.transformation.classes.TCType;
import edu.isi.pegasus.planner.classes.FileTransfer;
import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.classes.NameValue;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.classes.PlannerOptions;
import edu.isi.pegasus.planner.classes.TransferJob;
import edu.isi.pegasus.planner.code.GridStartFactory;
import edu.isi.pegasus.planner.common.PegasusConfiguration;
import edu.isi.pegasus.planner.common.PegasusProperties;
import edu.isi.pegasus.planner.mapper.SubmitMapper;
import edu.isi.pegasus.planner.namespace.Condor;
import edu.isi.pegasus.planner.namespace.Dagman;
import edu.isi.pegasus.planner.namespace.ENV;
import edu.isi.pegasus.planner.namespace.Pegasus;
import edu.isi.pegasus.planner.transfer.Implementation;
import edu.isi.pegasus.planner.transfer.Refiner;
import java.io.File;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;

/**
 * An abstract implementation that implements some of the common functions in the Implementation
 * Interface that are required by all the implementations.
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public abstract class Abstract implements Implementation {

    /**
     * The logical name of the transformation that creates directories on the remote execution
     * pools.
     */
    public static final String CHANGE_XBIT_TRANSFORMATION = "chmod";

    /** The basename of the pegasus dirmanager executable. */
    public static final String XBIT_EXECUTABLE_BASENAME = "chmod";

    /** The transformation namespace for the setXBit jobs. */
    public static final String XBIT_TRANSFORMATION_NS = "system";

    /** The version number for the derivations for setXBit jobs. */
    public static final String XBIT_TRANSFORMATION_VERSION = null;

    /** The derivation namespace for the setXBit jobs. */
    public static final String XBIT_DERIVATION_NS = "system";

    /** The version number for the derivations for setXBit jobs. */
    public static final String XBIT_DERIVATION_VERSION = null;

    /** The prefix for the jobs which are added to set X bit for the staged executables. */
    public static final String SET_XBIT_PREFIX = "chmod_";

    /** The prefix for the NoOP jobs that are created. */
    public static final String NOOP_PREFIX = "noop_";

    /** The path to the credentials file on the submit host (local pool). */
    protected String mLocalCredentials;

    /** The basename of the user credentials file */
    protected String mLocalCredentialsBasename;

    /**
     * The path to the user proxy on the submit host (local pool), that is picked up for use in
     * transfer of proxies.
     */
    protected String mLocalUserProxy;

    /** The basename of the user proxy , that is picked up for use in transfer of proxies. */
    protected String mLocalUserProxyBasename;

    /** The path to the s3cfg file on the submit host (local pool). */
    protected String mLocalS3cfg;

    /** The basename of the user s3cfg file */
    protected String mLocalS3cfgBasename;

    /** The path to the irodsEnv file on the submit host (local pool). */
    protected String mLocalIrodsEnv;

    /** The basename of the user irodsEnv file */
    protected String mLocalIrodsEnvBasename;

    /** The handle to the properties object holding the properties relevant to Pegasus. */
    protected PegasusProperties mProps;

    /** Contains the various options to the Planner as passed by the user at runtime. */
    protected PlannerOptions mPOptions;

    /** The handle to the Site Catalog. It is instantiated in this class. */
    //    protected PoolInfoProvider mSCHandle;
    /** The handle to the Pool Info Provider. It is instantiated in this class */
    // protected PoolInfoProvider mPoolHandle;
    protected SiteStore mSiteStore;

    /**
     * The handle to the Transformation Catalog. It must be instantiated in the implementing class
     */
    protected TransformationCatalog mTCHandle;

    /** The handle to the refiner that loaded this implementation. */
    protected Refiner mRefiner;

    /** The logging object which is used to log all the messages. */
    protected LogManager mLogger;

    /**
     * The set of sites for which chmod job creation has to be disabled while doing executable
     * staging.
     */
    protected Set mDisabledChmodSites;

    /** A boolean indicating whether chmod is disabled for all sites. */
    protected boolean mChmodDisabledForAllSites;

    /**
     * A boolean indicating whether chmod jobs should be created that set the xbit in case of
     * executable staging.
     */
    // protected boolean mAddNodesForSettingXBit;

    /** handle to PegasusConfiguration */
    protected PegasusConfiguration mPegasusConfiguration;

    /**
     * Handle to the Submit directory factory, that returns the relative submit directory for a job
     */
    protected SubmitMapper mSubmitDirFactory;

    /**
     * The overloaded constructor, that is called by the Factory to load the class.
     *
     * @param bag the bag of initialization objects.
     */
    public Abstract(PegasusBag bag) {
        mProps = bag.getPegasusProperties();
        mPOptions = bag.getPlannerOptions();
        mLogger = bag.getLogger();
        mSiteStore = bag.getHandleToSiteStore();
        mTCHandle = bag.getHandleToTransformationCatalog();
        mSubmitDirFactory = bag.getSubmitMapper();

        // build up the set of disabled chmod sites
        mDisabledChmodSites = determineDisabledChmodSites(mProps.getChmodDisabledSites());
        mChmodDisabledForAllSites = mDisabledChmodSites.contains("*");

        // from pegasus release 3.2 onwards xbit jobs are not added
        // for worker node execution/Pegasus Lite
        // PM-810 it is now per job instead of global.
        mPegasusConfiguration = new PegasusConfiguration(mLogger);
        // mAddNodesForSettingXBit = !mProps.executeOnWorkerNode();

        // Pegasus credentials
        PegasusCredentials creds = new PegasusCredentials();
        creds.initialize(bag);
        mLocalCredentials = creds.getPath();
        // set the path to credentials file only if the file exists
        if (mLocalCredentials != null && !new File(mLocalCredentials).exists()) {
            mLogger.log(
                    "The Pegasus credentials file does not exist - " + mLocalCredentials,
                    LogManager.ERROR_MESSAGE_LEVEL);
            mLocalCredentials = null;
        }

        mLocalCredentialsBasename =
                (mLocalCredentials == null) ? null : new File(mLocalCredentials).getName();

        Proxy p = new Proxy();
        p.initialize(bag);
        mLocalUserProxy = p.getPath();
        // set the path to user proxy only if the proxy exists
        if (!new File(mLocalUserProxy).exists()) {
            mLogger.log(
                    "The user proxy does not exist - " + mLocalUserProxy,
                    LogManager.DEBUG_MESSAGE_LEVEL);
            mLocalUserProxy = null;
        }

        mLocalUserProxyBasename =
                (mLocalUserProxy == null) ? null : new File(mLocalUserProxy).getName();

        S3CFG s3cfg = new S3CFG();
        s3cfg.initialize(bag);
        mLocalS3cfg = s3cfg.getPath();
        // set the path to s3cfg only if the scfg exists
        if (mLocalS3cfg != null && !new File(mLocalS3cfg).exists()) {
            mLogger.log(
                    "The s3cfg file does not exist - " + mLocalS3cfg,
                    LogManager.DEBUG_MESSAGE_LEVEL);
            mLocalS3cfg = null;
        }

        mLocalS3cfgBasename = (mLocalS3cfg == null) ? null : new File(mLocalS3cfg).getName();

        // irods
        Irods irods = new Irods();
        irods.initialize(bag);
        mLocalIrodsEnv = irods.getPath();
        // set the path to irodsEnv file only if the file exists
        if (mLocalIrodsEnv != null && !new File(mLocalIrodsEnv).exists()) {
            mLogger.log(
                    "The irodsEnv file does not exist - " + mLocalIrodsEnv,
                    LogManager.ERROR_MESSAGE_LEVEL);
            mLocalIrodsEnv = null;
        }

        mLocalIrodsEnvBasename =
                (mLocalIrodsEnv == null) ? null : new File(mLocalIrodsEnv).getName();
    }

    /**
     * Applies priorities to the transfer jobs if a priority is specified in the properties file.
     *
     * @param job the transfer job .
     */
    public void applyPriority(TransferJob job) {
        String priority = this.getPriority(job);
        if (priority != null) {
            job.condorVariables.construct(Condor.PRIORITY_KEY, priority);
        }
    }

    /**
     * Determines if there is a need to transfer the Pegasus credentials for the transfer job or
     * not. If there is a need to transfert the file, then the job is modified to create the correct
     * condor commands to transfer the file. The file is transferred from the submit host (i.e site
     * local).
     *
     * @param job the transfer job .
     * @return boolean true job was modified to transfer the Pegasus credentials file, else false
     *     when job is not modified.
     * @deprecated
     */
    public boolean checkAndTransferPegasusCredentials(TransferJob job) {

        // for remote execution, transfer credentials file
        if (!job.getSiteHandle().equalsIgnoreCase("local")
                && mLocalIrodsEnv != null
                && !job.envVariables.containsKey(PegasusCredentials.CREDENTIALS_FILE)) {
            job.condorVariables.addIPFileForTransfer(mLocalCredentials);
            // just the basename
            job.envVariables.checkKeyInNS(
                    PegasusCredentials.CREDENTIALS_FILE, mLocalCredentialsBasename);
        }

        return true;
    }

    /**
     * Determines if there is a need to transfer proxy for the transfer job or not. If there is a
     * need to transfer proxy, then the job is modified to create the correct condor commands to
     * transfer the proxy. Proxy is usually transferred if the Pegasus profile TRANSFER_PROXY is
     * set, or the job is being run in the condor vanilla universe. The proxy is transferred from
     * the submit host (i.e site local). The location is determined from the value of the
     * X509_USER_PROXY profile key associated in the env namespace.
     *
     * @param job the transfer job .
     * @return boolean true job was modified to transfer the proxy, else false when job is not
     *     modified.
     * @deprecated
     */
    public boolean checkAndTransferProxy(TransferJob job) {
        boolean transfer = false;
        // not handling for third party transfers correctly.

        String style =
                job.vdsNS.containsKey(Pegasus.STYLE_KEY)
                        ? (String) job.vdsNS.get(Pegasus.STYLE_KEY)
                        : Pegasus.GLOBUS_STYLE;
        String universe =
                job.condorVariables.containsKey(Condor.UNIVERSE_KEY)
                        ? (String) job.condorVariables.get(Condor.UNIVERSE_KEY)
                        :
                        // empty
                        "";
        boolean condition1 = job.vdsNS.getBooleanValue(Pegasus.TRANSFER_PROXY_KEY);
        boolean condition2 =
                ((style.equalsIgnoreCase(Pegasus.CONDOR_STYLE))
                        || (style.equalsIgnoreCase(Pegasus.GLIDEIN_STYLE))
                        || (job.executionPool.equalsIgnoreCase("local")
                                && (universe.equalsIgnoreCase(Condor.VANILLA_UNIVERSE)
                                        || universe.equalsIgnoreCase(Condor.STANDARD_UNIVERSE))));

        // condition1 is explicit request for transfer of proxy
        // condition2 is determination of the glide in case
        if (condition1 || condition2) {
            if (mLocalUserProxyBasename != null) {
                // set the transfer of proxy from the submit host
                // to the remote execution pool, using internal
                // condor transfer mechanism

                // add condor key transfer_input_files
                // and other required condor keys
                /*
                job.condorVariables.checkKeyInNS(Condor.TRANSFER_IP_FILES_KEY,
                                                 mLocalUserProxy);
                job.condorVariables.construct("should_transfer_files","YES");
                job.condorVariables.construct("when_to_transfer_output","ON_EXIT");
                */
                job.condorVariables.addIPFileForTransfer(mLocalUserProxy);

                // set the environment variable to basefile name
                // only for transfer jobs that dont execute on the local site
                if (job.getSiteHandle().equalsIgnoreCase("local")) {
                    // the full path
                    job.envVariables.checkKeyInNS(ENV.X509_USER_PROXY_KEY, this.mLocalUserProxy);
                } else {
                    // just the basename
                    job.envVariables.checkKeyInNS(ENV.X509_USER_PROXY_KEY, mLocalUserProxyBasename);
                }

                if (!condition2) {
                    // means the transfer job is not being run in
                    // condor vanilla universe. This means, that in
                    // all probability the proxy is being transferred
                    // by gass_cache, and that does not preserve file
                    // permissions correctly
                    if (job.getSiteHandle().equalsIgnoreCase("local")) {
                        // the full path
                        job.envVariables.checkKeyInNS(
                                ENV.GRIDSTART_PREJOB, "/bin/chmod 600 " + mLocalUserProxy);

                    } else {
                        job.envVariables.checkKeyInNS(
                                ENV.GRIDSTART_PREJOB, "/bin/chmod 600 " + mLocalUserProxyBasename);
                    }
                }
                if (!condition1) {
                    // for glide in jobs also tag we are
                    // transferring proxy
                    job.vdsNS.checkKeyInNS(Pegasus.TRANSFER_PROXY_KEY, "true");
                }
                // we want the transfer job to be run in the
                // directory that Condor or GRAM decided to run
                job.condorVariables.removeKey("remote_initialdir");
                transfer = true;
            }
        }
        return transfer;
    }

    /**
     * Determines if there is a need to transfer the IRODS_ENVIRONMENT_FILE for the transfer job or
     * not. If there is a need to transfert the file, then the job is modified to create the correct
     * condor commands to transfer the file. The file is transferred from the submit host (i.e site
     * local).
     *
     * @param job the transfer job .
     * @return boolean true job was modified to transfer the IRODS_ENVIRONMENT_FILE, else false when
     *     job is not modified.
     * @deprecated
     */
    public boolean checkAndTransferIrodsEnvFile(TransferJob job) {

        // for remote execution, transfer the IRODS_ENVIRONMENT_FILE file
        if (!job.getSiteHandle().equalsIgnoreCase("local")
                && mLocalIrodsEnv != null
                && !job.envVariables.containsKey(Irods.IRODSENVFILE)) {
            job.condorVariables.addIPFileForTransfer(mLocalIrodsEnv);
            // just the basename
            job.envVariables.checkKeyInNS(Irods.IRODSENVFILE, mLocalIrodsEnvBasename);
        }

        return true;
    }

    /**
     * Sets the callback to the refiner, that has loaded this implementation.
     *
     * @param refiner the transfer refiner that loaded the implementation.
     */
    public void setRefiner(Refiner refiner) {
        mRefiner = refiner;
    }

    /**
     * Adds the dirmanager to the workflow, that do a chmod on the files being staged.
     *
     * @param computeJob the computeJob for which the files are being staged.
     * @param txJob the transfer job that is staging the files.
     * @param execFiles the executable files that are being staged.
     * @return boolean indicating whether any XBitJobs were succesfully added or not.
     */
    protected boolean addSetXBitJobs(Job computeJob, Job txJob, Collection execFiles) {

        return this.addSetXBitJobs(computeJob, txJob.getName(), execFiles, txJob.getJobType());
    }

    /**
     * Adds the dirmanager job to the workflow, that do a chmod on the files being staged.
     *
     * @param computeJob the computeJob for which the files are being staged.
     * @param txJobName the name of the transfer job that is staging the files.
     * @param execFiles the executable files that are being staged.
     * @param transferClass the class of transfer job
     * @return boolean indicating whether any XBitJobs were succesfully added or not.
     */
    public boolean addSetXBitJobs(
            Job computeJob, String txJobName, Collection execFiles, int transferClass) {

        boolean added = false;
        String computeJobName = computeJob.getName();
        String site = computeJob.getSiteHandle();

        // sanity check
        if (execFiles == null || execFiles.isEmpty()) {
            return added;
        }
        if (transferClass != Job.STAGE_IN_JOB) {
            // extra check. throw an exception
            throw new RuntimeException(
                    "Invalid Transfer Type ("
                            + txJobName
                            + ","
                            + transferClass
                            + ") for staging executable files ");
        }

        // figure out whether we need to create a chmod or noop
        boolean noop = this.disableChmodJobCreation(site);

        // add setXBit jobs into the workflow
        int counter = 0;
        for (Iterator it = execFiles.iterator(); it.hasNext(); counter++) {
            FileTransfer execFile = (FileTransfer) it.next();

            String xBitJobName =
                    this.getSetXBitJobName(computeJobName, counter); // create a chmod job

            Job xBitJob =
                    noop
                            ? this.createNoOPJob(xBitJobName, computeJob.getSiteHandle())
                            : // create a NOOP job
                            this.createSetXBitJob(execFile, xBitJobName); // create a chmod job

            if (xBitJob == null) {
                // error occured while creating the job
                throw new RuntimeException(
                        "Unable to create setXBitJob "
                                + "corresponding to  compute job "
                                + computeJobName
                                + " and transfer"
                                + " job "
                                + txJobName);

            } else {
                added = true;
                mRefiner.addJob(xBitJob);
                // add the relation txJob->XBitJob->ComputeJob
                mRefiner.addRelation(txJobName, xBitJob.getName(), xBitJob.getSiteHandle(), true);
                mRefiner.addRelation(xBitJob.getName(), computeJobName);
            }
        }

        return added;
    }

    /**
     * Adds the dirmanager job to the workflow, that do a chmod on the files being staged.
     *
     * @param computeJob the computeJob for which the files are being staged.
     * @param txJobName the name of the transfer job that is staging the files.
     * @param execFiles the executable files that are being staged.
     * @param transferClass the class of transfer job
     * @param xbitIndex index to be used for creating the name of XBitJob.
     * @return boolean indicating whether any XBitJobs were succesfully added or not.
     */
    public boolean addSetXBitJobs(
            Job computeJob,
            String txJobName,
            Collection execFiles,
            int transferClass,
            int xbitIndex) {

        boolean added = false;
        String computeJobName = computeJob.getName();
        String site = computeJob.getSiteHandle();

        // sanity check
        if (execFiles == null || execFiles.isEmpty()) {
            return added;
        }
        if (transferClass != Job.STAGE_IN_JOB) {
            // extra check. throw an exception
            throw new RuntimeException(
                    "Invalid Transfer Type ("
                            + txJobName
                            + ","
                            + transferClass
                            + ") for staging executable files ");
        }

        // figure out whether we need to create a chmod or noop
        boolean noop = this.disableChmodJobCreation(site);

        // add setXBit jobs into the workflow
        int counter = 0;
        for (Iterator it = execFiles.iterator(); it.hasNext(); counter++) {
            FileTransfer execFile = (FileTransfer) it.next();

            String xBitJobName =
                    this.getSetXBitJobName(computeJobName, xbitIndex); // create a chmod job

            Job xBitJob =
                    noop
                            ? this.createNoOPJob(xBitJobName, computeJob.getSiteHandle())
                            : // create a NOOP job
                            this.createSetXBitJob(execFile, xBitJobName); // create a chmod job

            if (xBitJob == null) {
                // error occured while creating the job
                throw new RuntimeException(
                        "Unable to create setXBitJob "
                                + "corresponding to  compute job "
                                + computeJobName
                                + " and transfer"
                                + " job "
                                + txJobName);

            } else {
                added = true;
                mRefiner.addJob(xBitJob);
                // add the relation txJob->XBitJob->ComputeJob
                mRefiner.addRelation(txJobName, xBitJob.getName(), xBitJob.getSiteHandle(), true);
                mRefiner.addRelation(xBitJob.getName(), computeJobName);
            }
        }

        return added;
    }

    /**
     * Generates the name of the setXBitJob , that is unique for the given workflow.
     *
     * @param name the name of the compute job
     * @param counter the index for the setXBit job.
     * @return the name of the setXBitJob .
     */
    public String getSetXBitJobName(String name, int counter) {
        StringBuffer sb = new StringBuffer();
        sb.append(this.SET_XBIT_PREFIX).append(name).append("_").append(counter);

        return sb.toString();
    }

    /**
     * Generates the name of the noop job , that is unique for the given workflow.
     *
     * @param name the name of the compute job
     * @param counter the index for the noop job.
     * @return the name of the setXBitJob .
     */
    public String getNOOPJobName(String name, int counter) {
        StringBuffer sb = new StringBuffer();
        sb.append(this.NOOP_PREFIX).append(name).append("_").append(counter);

        return sb.toString();
    }

    /**
     * It creates a NoOP job that runs on the submit host.
     *
     * @param name the name to be assigned to the noop job
     * @param computeSiteHandle the execution site of the associated compute job
     * @return the noop job.
     */
    public Job createNoOPJob(String name, String computeSiteHandle) {

        Job newJob = new Job();
        List entries = null;
        String execPath = null;

        // jobname has the dagname and index to indicate different
        // jobs for deferred planning
        newJob.setName(name);
        newJob.setTransformation("pegasus", "noop", "1.0");
        newJob.setDerivation("pegasus", "noop", "1.0");

        newJob.setUniverse(GridGateway.JOB_TYPE.auxillary.toString());

        // the noop job does not get run by condor
        // even if it does, giving it the maximum
        // possible chance
        newJob.executable = "/bin/true";

        // construct noop keys
        newJob.setSiteHandle("local");

        // PM-833 set the relative submit directory for the transfer
        // job based on the associated file factory
        newJob.setRelativeSubmitDirectory(this.mSubmitDirFactory.getRelativeDir(newJob));

        // PM-845
        // we need to set the staging site handle to compute job execution site
        // this is to ensure dependencies are added correctly when adding
        // create dir jobs
        newJob.setStagingSiteHandle(computeSiteHandle);

        newJob.setJobType(Job.CHMOD_JOB);
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
     * Adds the dirmanager job to the workflow, that do a chmod on the files being staged.
     *
     * @param computeJob the computeJob for which the files are being staged.
     * @param execFiles the executable files that are being staged.
     * @param transferClass the class of transfer job
     * @param xbitIndex index to be used for creating the name of XBitJob.
     * @return the job object for the xBitJob
     */
    public Job createSetXBitJob(
            Job computeJob, Collection<FileTransfer> execFiles, int transferClass, int xbitIndex) {

        String computeJobName = computeJob.getName();
        String site = computeJob.getSiteHandle();

        if (transferClass != Job.STAGE_IN_JOB) {
            // extra check. throw an exception
            throw new RuntimeException(
                    "Invalid Transfer Type ("
                            + transferClass
                            + ") for staging executable files for job "
                            + computeJob.getName());
        }

        // figure out whether we need to create a chmod or noop
        boolean noop = this.disableChmodJobCreation(site);

        // add setXBit jobs into the workflow
        int counter = 0;

        String xBitJobName =
                this.getSetXBitJobName(computeJobName, xbitIndex); // create a chmod job
        Job xBitJob =
                noop
                        ? this.createNoOPJob(xBitJobName, computeJob.getSiteHandle())
                        : // create a NOOP chmod job
                        this.createSetXBitJob(
                                execFiles,
                                xBitJobName,
                                computeJob.getSiteHandle()); // create a chmod job

        if (xBitJob == null) {
            // error occured while creating the job
            throw new RuntimeException(
                    "Unable to create setXBitJob "
                            + "corresponding to  compute job "
                            + computeJobName);
        }
        return xBitJob;
    }

    /**
     * Creates a dirmanager job, that does a chmod on the file being staged. The file being staged
     * should be of type executable. Though no explicit check is made for that. The staged file is
     * the one whose X bit would be set on execution of this job. The site at which job is executed,
     * is determined from the site associated with the destination URL.
     *
     * @param file the <code>FileTransfer</code> containing the file that has to be X Bit Set.
     * @param name the name that has to be assigned to the job.
     * @return the chmod job, else null if it is not able to be created for some reason.
     */
    protected Job createSetXBitJob(FileTransfer file, String name) {
        NameValue destURL = (NameValue) file.getDestURL();
        String eSiteHandle = destURL.getKey();
        Collection<FileTransfer> fts = new LinkedList();
        fts.add(file);
        return this.createSetXBitJob(fts, name, eSiteHandle);
    }

    /**
     * Creates a dirmanager job, that does a chmod on the file being staged. The file being staged
     * should be of type executable. Though no explicit check is made for that. The staged file is
     * the one whose X bit would be set on execution of this job. The site at which job is executed,
     * is determined from the site associated with the destination URL.
     *
     * @param files the collection <code>FileTransfer</code> containing the file that has to be X
     *     Bit Set.
     * @param name the name that has to be assigned to the job.
     * @param site the site at which the job has to be created
     * @return the chmod job, else null if it is not able to be created for some reason.
     */
    protected Job createSetXBitJob(Collection<FileTransfer> files, String name, String site) {
        Job xBitJob = new Job();
        TransformationCatalogEntry entry = null;
        String eSiteHandle = site;

        List entries;
        try {
            entries =
                    mTCHandle.lookup(
                            Abstract.XBIT_TRANSFORMATION_NS,
                            Abstract.CHANGE_XBIT_TRANSFORMATION,
                            Abstract.XBIT_TRANSFORMATION_VERSION,
                            eSiteHandle,
                            TCType.INSTALLED);
        } catch (Exception e) {
            // non sensical catching
            mLogger.log(
                    "Unable to retrieve entries from TC " + e.getMessage(),
                    LogManager.ERROR_MESSAGE_LEVEL);
            return null;
        }

        entry =
                (entries == null)
                        ? this.defaultXBitTCEntry(eSiteHandle)
                        : // try using a default one
                        (TransformationCatalogEntry) entries.get(0);

        if (entry == null) {
            // NOW THROWN AN EXCEPTION

            // should throw a TC specific exception
            StringBuffer error = new StringBuffer();
            error.append("Could not find entry in tc for lfn ")
                    .append(
                            Separator.combine(
                                    Abstract.XBIT_TRANSFORMATION_NS,
                                    Abstract.CHANGE_XBIT_TRANSFORMATION,
                                    Abstract.XBIT_TRANSFORMATION_VERSION))
                    .append(" at site ")
                    .append(eSiteHandle);

            mLogger.log(error.toString(), LogManager.ERROR_MESSAGE_LEVEL);
            throw new RuntimeException(error.toString());
        }

        SiteCatalogEntry eSite = mSiteStore.lookup(eSiteHandle);
        StringBuffer arguments = new StringBuffer();
        arguments.append(" +x ");
        for (FileTransfer file : files) {
            NameValue destURL = (NameValue) file.getDestURL();
            arguments.append(" ");
            arguments.append(new PegasusURL(destURL.getValue()).getPath());
        }

        // PM-833 set the relative submit directory for the transfer
        // job based on the associated file factory
        xBitJob.setRelativeSubmitDirectory(this.mSubmitDirFactory.getRelativeDir(xBitJob));

        xBitJob.jobName = name;
        xBitJob.logicalName = Abstract.CHANGE_XBIT_TRANSFORMATION;
        xBitJob.namespace = Abstract.XBIT_TRANSFORMATION_NS;
        xBitJob.version = Abstract.XBIT_TRANSFORMATION_VERSION;
        xBitJob.dvName = Abstract.CHANGE_XBIT_TRANSFORMATION;
        xBitJob.dvNamespace = Abstract.XBIT_DERIVATION_NS;
        xBitJob.dvVersion = Abstract.XBIT_DERIVATION_VERSION;
        xBitJob.executable = entry.getPhysicalTransformation();
        xBitJob.executionPool = eSiteHandle;
        // PM-845 set staging site handle to same as execution site of compute job
        xBitJob.setStagingSiteHandle(eSiteHandle);
        xBitJob.strargs = arguments.toString();
        xBitJob.jobClass = Job.CHMOD_JOB;
        xBitJob.jobID = name;

        // the profile information from the pool catalog needs to be
        // assimilated into the job.
        xBitJob.updateProfiles(eSite.getProfiles());

        // the profile information from the transformation
        // catalog needs to be assimilated into the job
        // overriding the one from pool catalog.
        xBitJob.updateProfiles(entry);

        // the profile information from the properties file
        // is assimilated overidding the one from transformation
        // catalog.
        xBitJob.updateProfiles(mProps);

        return xBitJob;
    }

    /**
     * Returns a default TC entry to be used in case entry is not found in the transformation
     * catalog.
     *
     * @param site the site for which the default entry is required.
     * @return the default entry.
     */
    private TransformationCatalogEntry defaultXBitTCEntry(String site) {
        TransformationCatalogEntry defaultTCEntry = null;

        // construct the path to it
        StringBuffer path = new StringBuffer();
        path.append(File.separator)
                .append("bin")
                .append(File.separator)
                .append(Abstract.XBIT_EXECUTABLE_BASENAME);

        defaultTCEntry =
                new TransformationCatalogEntry(
                        Abstract.XBIT_TRANSFORMATION_NS,
                        Abstract.CHANGE_XBIT_TRANSFORMATION,
                        Abstract.XBIT_TRANSFORMATION_VERSION);

        defaultTCEntry.setPhysicalTransformation(path.toString());
        defaultTCEntry.setResourceId(site);
        defaultTCEntry.setType(TCType.INSTALLED);
        defaultTCEntry.setSysInfo(this.mSiteStore.lookup(site).getSysInfo());

        // register back into the transformation catalog
        // so that we do not need to worry about creating it again
        try {
            mTCHandle.insert(defaultTCEntry, false);
        } catch (Exception e) {
            // just log as debug. as this is more of a performance improvement
            // than anything else
            mLogger.log(
                    "Unable to register in the TC the default entry "
                            + defaultTCEntry.getLogicalTransformation()
                            + " for site "
                            + site,
                    e,
                    LogManager.DEBUG_MESSAGE_LEVEL);
        }

        return defaultTCEntry;
    }

    /**
     * Builds up a set of disabled chmod sites
     *
     * @param sites comma separated list of sites.
     * @return a Set containing the site names.
     */
    protected Set determineDisabledChmodSites(String sites) {
        Set s = new HashSet();

        // sanity checks
        if (sites == null || sites.length() == 0) {
            return s;
        }

        for (StringTokenizer st = new StringTokenizer(sites); st.hasMoreTokens(); ) {
            s.add(st.nextToken());
        }

        return s;
    }

    /**
     * Returns a boolean indicating whether to disable chmod job creation for a site or not.
     *
     * @param site the name of the site
     * @return boolean
     */
    protected boolean disableChmodJobCreation(String site) {
        return this.mChmodDisabledForAllSites || this.mDisabledChmodSites.contains(site);
    }

    /**
     * Returns the priority for the transfer job as specified in the properties file.
     *
     * @param job the Transfer job.
     * @return the priority of the job as determined from properties, can be null if invalid value
     *     passed or property not set.
     */
    protected String getPriority(TransferJob job) {
        String priority;
        int type = job.jobClass;
        switch (type) {
            case Job.STAGE_IN_JOB:
                priority = mProps.getTransferStageInPriority();
                break;

            case Job.STAGE_OUT_JOB:
                priority = mProps.getTransferStageOutPriority();
                break;

            case Job.INTER_POOL_JOB:
                priority = mProps.getTransferInterPriority();
                break;

            default:
                priority = null;
        }
        return priority;
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
}
