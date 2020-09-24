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
package edu.isi.pegasus.planner.refiner;

import edu.isi.pegasus.common.credential.CredentialHandler;
import edu.isi.pegasus.common.credential.CredentialHandlerFactory;
import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.common.logging.LoggingKeys;
import edu.isi.pegasus.planner.catalog.ReplicaCatalog;
import edu.isi.pegasus.planner.catalog.replica.ReplicaCatalogEntry;
import edu.isi.pegasus.planner.catalog.replica.ReplicaFactory;
import edu.isi.pegasus.planner.catalog.replica.classes.ReplicaStore;
import edu.isi.pegasus.planner.catalog.site.classes.GridGateway;
import edu.isi.pegasus.planner.catalog.transformation.TransformationCatalogEntry;
import edu.isi.pegasus.planner.catalog.transformation.classes.TCType;
import edu.isi.pegasus.planner.classes.ADag;
import edu.isi.pegasus.planner.classes.FileTransfer;
import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.classes.NameValue;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.classes.PlannerOptions;
import edu.isi.pegasus.planner.classes.Profile;
import edu.isi.pegasus.planner.classes.ReplicaLocation;
import edu.isi.pegasus.planner.code.generator.Braindump;
import edu.isi.pegasus.planner.common.PegasusProperties;
import edu.isi.pegasus.planner.namespace.Dagman;
import edu.isi.pegasus.planner.namespace.Metadata;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.StringTokenizer;

/**
 * This coordinates the look up to the Replica Location Service, to determine the logical to
 * physical mappings.
 *
 * @author Karan Vahi
 * @author Gaurang Mehta
 * @version $Revision$
 */
public class ReplicaCatalogBridge extends Engine // for the time being.
{

    /**
     * Prefix for the property subset to set up the planner to use or associate a different output
     * replica catalog
     */
    public static final String OUTPUT_REPLICA_CATALOG_PREFIX = "pegasus.catalog.replica.output";

    /** Default category for registration jobs */
    public static final String DEFAULT_REGISTRATION_CATEGORY_KEY = "registration";

    /** The transformation namespace for the regostration jobs. */
    public static final String RC_TRANSFORMATION_NS = "pegasus";

    /** The logical name of the transformation used. */
    public static final String RC_TRANSFORMATION_NAME = "rc-client";

    /** The logical name of the transformation used. */
    public static final String RC_TRANSFORMATION_VERSION = null;

    /** The derivation namespace for the transfer jobs. */
    public static final String RC_DERIVATION_NS = "pegasus";

    /** The derivation name for the transfer jobs. */
    public static final String RC_DERIVATION_NAME = "rc-client";

    /** The version number for the derivations for registration jobs. */
    public static final String RC_DERIVATION_VERSION = "1.0";

    /** The name of the Replica Catalog Implementer that serves as the source for cache files. */
    public static final String CACHE_REPLICA_CATALOG_IMPLEMENTER = "SimpleFile";

    /** The name of the source key for Replica Catalog Implementer that serves as cache */
    public static final String CACHE_REPLICA_CATALOG_KEY = "file";

    /**
     * The name of the key that disables writing back to the cache file. Designates a static file.
     * i.e. read only
     */
    public static final String CACHE_READ_ONLY_KEY = "read.only";

    /** The name of the Replica Catalog Implementer that serves as the source for cache files. */
    public static final String DIRECTORY_REPLICA_CATALOG_IMPLEMENTER = "Directory";

    /** The name of the source key for Replica Catalog Implementer that serves as cache */
    public static final String DIRECTORY_REPLICA_CATALOG_KEY = "directory";

    /** The name of the URL key for the replica catalog impelementer to be picked up. */
    public static final String REPLICA_CATALOG_URL_KEY = "url";

    /** The handle to the main Replica Catalog. */
    private ReplicaCatalog mReplicaCatalog;

    /**
     * The Vector of <code>String</code> objects containing the logical filenames of the files whose
     * locations are to be searched in the Replica Catalog.
     */
    protected Set mSearchFiles;

    /**
     * A boolean variable to desingnate whether the RLI queried was down or not. By default it is
     * up, unless it is set to true explicitly.
     */
    private boolean mRCDown;

    /**
     * The replica store in which we store all the results that are queried from the main replica
     * catalog.
     */
    private ReplicaStore mReplicaStore;

    /**
     * The replica store in which we store all the results that are queried from the cache replica
     * catalogs.
     */
    private ReplicaStore mCacheStore;

    /**
     * The replica store where we store all the results that are queried from the input directory
     * specified at the command line.
     */
    private ReplicaStore mDirectoryReplicaStore;

    /**
     * The replica store where we store all the results that are queried from the output replica
     * catalogs of previous runs.
     */
    private ReplicaStore mPreviousRunsReplicaStore;

    /** The DAX Replica Store. */
    private ReplicaStore mDAXReplicaStore;

    /** The inherited Replica Store */
    private ReplicaStore mInheritedReplicaStore;

    /**
     * A boolean indicating whether the cache file needs to be treated as a replica catalog or not.
     */
    private boolean mTreatCacheAsRC;

    /**
     * A boolean indicating whether the locations in the DAX file needs to be treated as a replica
     * catalog or not.
     */
    private boolean mDAXLocationsAsRC;

    /** The default tc entry. */
    private TransformationCatalogEntry mDefaultTCRCEntry;

    /**
     * A boolean indicating whether the attempt to create a default tc entry has happened or not.
     */
    private boolean mDefaultTCRCCreated;

    /** The DAG being worked upon. */
    private ADag mDag;

    /** A boolean indicating whether to register a deep LFN or not. */
    private boolean mRegisterDeepLFN;

    /** All the replica file sources detected. */
    private Set<File> mReplicaFileSources;

    /** whether integrity checking is enabled */
    private boolean mIntegrityCheckingEnabled;

    /**
     * The overloaded constructor.
     *
     * @param dag the workflow that is being worked on.
     * @param bag of initialization objects.
     */
    public ReplicaCatalogBridge(ADag dag, PegasusBag bag) {
        super(bag);
        this.initialize(dag, bag);
    }

    /**
     * Intialises the refiner.
     *
     * @param dag the workflow that is being worked on.
     * @param bag the bag of Pegasus initialization objects
     */
    public void initialize(ADag dag, PegasusBag bag) {
        this.mBag = bag;
        this.mDAXReplicaStore = dag.getReplicaStore();
        this.initialize(dag, bag.getPegasusProperties(), bag.getPlannerOptions());
        this.mRegisterDeepLFN = mProps.registerDeepLFN();

        mIntegrityCheckingEnabled =
                this.mProps.getIntegrityDial() != PegasusProperties.INTEGRITY_DIAL.none;
    }

    /**
     * Intialises the refiner.
     *
     * @param dag the workflow that is being worked on.
     * @param properties the properties passed to the planner.
     * @param options the options passed to the planner at runtime.
     */
    @SuppressWarnings("static-access")
    public void initialize(ADag dag, PegasusProperties properties, PlannerOptions options) {
        mDag = dag;
        mProps = properties;
        mPOptions = options;
        mRCDown = false;
        mCacheStore = new ReplicaStore();
        mInheritedReplicaStore = new ReplicaStore();
        mDirectoryReplicaStore = new ReplicaStore();
        mPreviousRunsReplicaStore = new ReplicaStore();
        mTreatCacheAsRC = mProps.treatCacheAsRC();
        mDAXLocationsAsRC = mProps.treatDAXLocationsAsRC();
        mDefaultTCRCCreated = false;

        // converting the Vector into vector of
        // strings just containing the logical
        // filenames
        mSearchFiles = dag.getDAGInfo().getLFNs(options.getForce());

        mReplicaFileSources = new LinkedHashSet<File>();

        try {

            // make sure that RLS can be loaded from local environment
            // Karan May 1 2007
            mReplicaCatalog = null;
            if (mSearchFiles != null && !mSearchFiles.isEmpty()) {

                // need to clone before setting any read only properites
                PegasusProperties props = (PegasusProperties) properties.clone();

                // set the read only property for the file based rc
                // we are connecting via PegasusProperties add the prefix
                String name =
                        ReplicaCatalog.c_prefix + "." + ReplicaCatalogBridge.CACHE_READ_ONLY_KEY;
                props.setProperty(name, "true");

                String proxy = getPathToLocalProxy();
                if (proxy != null) {
                    mLogger.log(
                            "Proxy used for Replica Catalog is " + proxy,
                            LogManager.CONFIG_MESSAGE_LEVEL);
                    props.setProperty(
                            ReplicaCatalog.c_prefix + "." + ReplicaCatalog.PROXY_KEY, proxy);
                }

                PegasusBag bag = new PegasusBag();
                bag.add(PegasusBag.PEGASUS_LOGMANAGER, mLogger);
                bag.add(PegasusBag.PEGASUS_PROPERTIES, props);
                bag.add(PegasusBag.PLANNER_DIRECTORY, mBag.getPlannerDirectory());
                mReplicaCatalog = ReplicaFactory.loadInstance(bag);

                // load all the mappings.
                mReplicaStore = new ReplicaStore(mReplicaCatalog.lookup(mSearchFiles));

                // PM-1535 if connect props has a file property add it back to the
                File catalogFile = mReplicaCatalog.getFileSource();
                if (catalogFile != null && catalogFile.exists()) {
                    this.mBag.add(PegasusBag.REPLICA_CATALOG_FILE_SOURCE, catalogFile);
                    mReplicaFileSources.add(catalogFile);
                }
            }

        } catch (Exception ex) {
            String msg = "Problem while connecting with the Replica Catalog: ";

            // set the flag to denote RLI is down
            mRCDown = true;
            // mReplicaStore = new ReplicaStore();

            // exit if there is no cache overloading specified.
            if (options.getCacheFiles().isEmpty()
                    && // no cache files specified
                    options.getInheritedRCFiles().isEmpty()
                    && // no files locations inherited from outer level DAX
                    this.mDAXReplicaStore.isEmpty()
                    && // no file locations in current DAX
                    options.getInputDirectories() == null
                    && // no input directory specified on the command line
                    dag.getDAGInfo().getLFNs(true).size()
                            > 0 // the number of raw input files is more than 1
            ) {
                mLogger.log(msg + ex.getMessage(), LogManager.ERROR_MESSAGE_LEVEL);
                throw new RuntimeException(msg, ex);
            } else {
                mLogger.log(msg + ex.getMessage(), LogManager.DEBUG_MESSAGE_LEVEL);
            }
        } finally {
            // set replica store to an empty store if required
            mReplicaStore = (mReplicaStore == null) ? new ReplicaStore() : mReplicaStore;
        }

        if (requireDefaultCategoryForRegistrationJobs(this.mReplicaCatalog, this.mProps)) {
            // specify maxjobs to 1 for File based replica catalog
            // JIRA PM-377
            // we set the default category value to 1
            // in the properties
            String key = getDefaultRegistrationMaxJobsPropertyKey();
            if (mProps.getProperty(key) == null) {
                mLogger.log(
                        "Setting property "
                                + key
                                + " to 1 to set max jobs for registrations jobs category",
                        LogManager.DEBUG_MESSAGE_LEVEL);
                mProps.setProperty(key, "1");
            }
        }

        // incorporate all mappings from input directory if specified
        Set<String> dirs = options.getInputDirectories();
        if (!dirs.isEmpty()) {
            mDirectoryReplicaStore = getReplicaStoreFromDirectories(dirs);
        }

        // PM-1681 look at if any data reuse submit directories were specified
        dirs = options.getDataReuseSubmitDirectories();
        if (!dirs.isEmpty()) {
            mPreviousRunsReplicaStore = getReplicaStoreFromSubmitDirectories(dirs);
        }

        // incorporate the caching if any
        if (!options.getCacheFiles().isEmpty()) {
            loadCacheFiles(options.getCacheFiles());
            for (String source : options.getCacheFiles()) {
                mReplicaFileSources.add(new File(source));
            }
        }

        // load inherited replica store
        if (!options.getInheritedRCFiles().isEmpty()) {
            this.loadInheritedReplicaStore(options.getInheritedRCFiles());
            for (String source : options.getInheritedRCFiles()) {
                mReplicaFileSources.add(new File(source));
            }
        }
    }

    /**
     * To close the connection to replica services. This must be defined in the case where one has
     * not done a singleton implementation. In other cases just do an empty implementation of this
     * method.
     */
    public void closeConnection() {
        if (mReplicaCatalog != null) {
            mReplicaCatalog.close();
        }
    }

    /** Closes the connection to the rli. */
    public void finalize() {
        this.closeConnection();
    }

    /**
     * This returns the files for which mappings exist in the Replica Catalog. This should return a
     * subset of the files which are specified in the mSearchFiles, while getting an instance to
     * this.
     *
     * @return a <code>Set</code> of logical file names as String objects, for which logical to
     *     physical mapping exists.
     * @see #mSearchFiles
     */
    public Set getFilesInReplica() {

        // check if any exist in the cache
        Set result = mCacheStore.getLFNs(mSearchFiles);
        mLogger.log(
                result.size() + " entries found in cache of total " + mSearchFiles.size(),
                LogManager.DEBUG_MESSAGE_LEVEL);

        // PM-1681 check in the previous runs
        Set lfns = mPreviousRunsReplicaStore.getLFNs(mSearchFiles);
        mLogger.log(
                lfns.size()
                        + " entries found in previous submit dirs of total "
                        + mSearchFiles.size(),
                LogManager.DEBUG_MESSAGE_LEVEL);
        result.addAll(lfns);

        // check if any exist in input directory
        result.addAll(this.mDirectoryReplicaStore.getLFNs(mSearchFiles));
        mLogger.log(
                result.size() + " entries found in cache of total " + mSearchFiles.size(),
                LogManager.DEBUG_MESSAGE_LEVEL);

        // check in the main replica catalog
        if ((this.mDAXReplicaStore.isEmpty() && mDirectoryReplicaStore.isEmpty())
                && (mRCDown || mReplicaCatalog == null)) {
            mLogger.log(
                    "Replica Catalog is either down or connection to it was never opened ",
                    LogManager.WARNING_MESSAGE_LEVEL);
            return result;
        }

        // lookup from the DAX Replica Store
        result.addAll(this.mDAXReplicaStore.getLFNs());

        // lookup from the inherited Replica Store
        result.addAll(this.mInheritedReplicaStore.getLFNs(mSearchFiles));

        // look up from the the main replica catalog
        result.addAll(mReplicaStore.getLFNs());

        mLogger.log(
                result.size()
                        + " entries found in all replica sources of total "
                        + mSearchFiles.size(),
                LogManager.DEBUG_MESSAGE_LEVEL);

        return result;
    }

    /**
     * Returns all the replica file sources.
     *
     * @return the replica file source.
     */
    public Set<File> getReplicaFileSources() {
        return this.mReplicaFileSources;
    }

    /**
     * Returns all the locations as returned from the Replica Lookup Mechanism.
     *
     * @param lfn The name of the logical file whose PFN mappings are required.
     * @return ReplicaLocation containing all the locations for that LFN
     * @see org.griphyn.cPlanner.classes.ReplicaLocation
     */
    public ReplicaLocation getFileLocs(String lfn) {

        ReplicaLocation cacheEntry = retrieveFromCache(lfn);
        ReplicaLocation result = null;

        // first check from cache
        if (cacheEntry != null && !mTreatCacheAsRC) {
            mLogger.log(
                    "Location of file " + cacheEntry + " retrieved from cache",
                    LogManager.TRACE_MESSAGE_LEVEL);
            return cacheEntry;
        }
        result =
                (cacheEntry == null)
                        ? cacheEntry
                        : new ReplicaLocation(cacheEntry); // result can be null

        // we prefer location in Directory over the DAX entries
        if (this.mDirectoryReplicaStore.containsLFN(lfn)) {
            return this.mDirectoryReplicaStore.getReplicaLocation(lfn);
        }

        // we prefer location in DAX over the inherited replica store
        ReplicaLocation daxEntry = null;
        if (this.mDAXReplicaStore.containsLFN(lfn)) {
            daxEntry = this.mDAXReplicaStore.getReplicaLocation(lfn);
            if (this.mDAXLocationsAsRC) {
                // dax entry is non null
                if (result == null) {
                    result = daxEntry;
                } else {
                    // merge with what we received from the cache
                    result.merge(daxEntry);
                }
            } else {
                return daxEntry;
            }
        }

        // we prefer location in inherited replica store over replica catalog
        // this is for hierarchal workflows, where the parent is the parent workflow
        // in recursive hierarchy
        if (this.mInheritedReplicaStore.containsLFN(lfn)) {
            return this.mInheritedReplicaStore.getReplicaLocation(lfn);
        }

        ReplicaLocation rcEntry = mReplicaStore.getReplicaLocation(lfn);
        if (result == null) {
            result = rcEntry; // can still be null
        } else {
            // merge from entry received from replica catalog
            result.merge(rcEntry);
        }

        return result;
    }

    /**
     * Returns the property key that can be used to set the max jobs for the default category
     * associated with the registration jobs.
     *
     * @return the property key
     */
    public String getDefaultRegistrationMaxJobsPropertyKey() {
        StringBuffer key = new StringBuffer();

        key.append(Dagman.NAMESPACE_NAME)
                .append(".")
                .append(ReplicaCatalogBridge.DEFAULT_REGISTRATION_CATEGORY_KEY)
                .append(".")
                .append(Dagman.MAXJOBS_KEY.toLowerCase());

        return key.toString();
    }

    /**
     * It constructs the Job object for the registration node, which registers the materialized
     * files on the output pool in the RLS.Note that the relations corresponding to this node should
     * already have been added to the concerned <code>DagInfo</code> object.
     *
     * @param regJobName The name of the job which registers the files in the Replica Location
     *     Service.
     * @param job The job whose output files are to be registered in the Replica Location Service.
     * @param files Collection of <code>FileTransfer</code> objects containing the information about
     *     source and destination URLs. The destination URLs would be our PFNs.
     * @param computeJobs associated compute jobs
     * @return Job corresponding to the new registration node.
     */
    public Job makeRCRegNode(
            String regJobName, Job job, Collection files, Collection<Job> computeJobs) {
        // making the files string

        Job newJob = new Job();

        newJob.setName(regJobName);
        newJob.setTransformation(
                ReplicaCatalogBridge.RC_TRANSFORMATION_NS,
                ReplicaCatalogBridge.RC_TRANSFORMATION_NAME,
                ReplicaCatalogBridge.RC_TRANSFORMATION_VERSION);
        newJob.setDerivation(
                ReplicaCatalogBridge.RC_DERIVATION_NS,
                ReplicaCatalogBridge.RC_DERIVATION_NAME,
                ReplicaCatalogBridge.RC_DERIVATION_VERSION);

        // change this function
        List tcentries = null;
        try {
            tcentries =
                    mTCHandle.lookup(
                            newJob.getTXNamespace(),
                            newJob.getTXName(),
                            newJob.getTXVersion(),
                            "local",
                            TCType.INSTALLED);

        } catch (Exception e) {
            mLogger.log(
                    "While retrieving entries from TC " + e.getMessage(),
                    LogManager.ERROR_MESSAGE_LEVEL);
        }

        TransformationCatalogEntry tc;

        if (tcentries == null || tcentries.isEmpty()) {

            mLogger.log(
                    "Unable to find in entry for "
                            + newJob.getCompleteTCName()
                            + " in transformation catalog on site local",
                    LogManager.DEBUG_MESSAGE_LEVEL);
            mLogger.log("Constructing a default entry for it ", LogManager.DEBUG_MESSAGE_LEVEL);
            tc = defaultTCRCEntry();

            if (tc == null) {
                throw new RuntimeException(
                        "Unable to create an entry for "
                                + newJob.getCompleteTCName()
                                + " on site local");
            }
        } else {
            tc = (TransformationCatalogEntry) tcentries.get(0);
        }
        newJob.setRemoteExecutable(tc.getPhysicalTransformation());

        // PM-833 set the relative submit directory for the regustration
        // job based on the associated file factory
        newJob.setRelativeSubmitDirectory(this.mSubmitDirMapper.getRelativeDir(newJob));
        // PM-833 the .in file is written in the same directory
        // where the submit file for the job will be written out
        File dir = new File(mPOptions.getSubmitDirectory(), newJob.getRelativeSubmitDirectory());

        newJob.setArguments(this.generateRepJobArgumentString(dir, regJobName, files, computeJobs));
        newJob.setUniverse(GridGateway.JOB_TYPE.register.toString());
        newJob.setSiteHandle(tc.getResourceId());
        newJob.setJobType(Job.REPLICA_REG_JOB);
        newJob.setVDSSuperNode(job.getName());

        // the profile information from the pool catalog needs to be
        // assimilated into the job.
        newJob.updateProfiles(mSiteStore.lookup(newJob.getSiteHandle()).getProfiles());

        // add any notifications specified in the transformation
        // catalog for the job. JIRA PM-391
        newJob.addNotifications(tc);

        // the profile information from the transformation
        // catalog needs to be assimilated into the job
        // overriding the one from pool catalog.
        newJob.updateProfiles(tc);

        // the profile information from the properties file
        // is assimilated overidding the one from transformation
        // catalog.
        newJob.updateProfiles(mProps);

        // if no category is associated with the job, add a default
        // category
        if (!newJob.dagmanVariables.containsKey(Dagman.CATEGORY_KEY)) {
            newJob.dagmanVariables.construct(
                    Dagman.CATEGORY_KEY, DEFAULT_REGISTRATION_CATEGORY_KEY);
        }

        return newJob;
    }

    /**
     * Returns a default TC entry to be used in case entry is not found in the transformation
     * catalog.
     *
     * @return the default entry.
     */
    private TransformationCatalogEntry defaultTCRCEntry() {
        String site = "local";
        // generate only once.
        if (!mDefaultTCRCCreated) {

            // construct the path to it
            StringBuffer path = new StringBuffer();
            path.append(mProps.getBinDir()).append(File.separator).append("pegasus-rc-client");

            mDefaultTCRCEntry =
                    new TransformationCatalogEntry(
                            this.RC_TRANSFORMATION_NS,
                            this.RC_TRANSFORMATION_NAME,
                            this.RC_TRANSFORMATION_VERSION);

            mDefaultTCRCEntry.setPhysicalTransformation(path.toString());
            mDefaultTCRCEntry.setResourceId(site);

            // set the flag back to true
            mDefaultTCRCCreated = true;
        }
        return mDefaultTCRCEntry;
    }

    /**
     * Returns the classpath for the default rc-client entry.
     *
     * @param home the home directory where we need to check for lib directory.
     * @return the classpath in an environment profile.
     */
    private Profile getClassPath(String home) {
        Profile result = null;

        // create the CLASSPATH from home
        String classpath = mProps.getProperty("java.class.path");
        if (classpath == null || classpath.trim().length() == 0) {
            return result;
        }

        mLogger.log("JAVA CLASSPATH SET IS " + classpath, LogManager.DEBUG_MESSAGE_LEVEL);

        StringBuffer cp = new StringBuffer();
        String prefix = home + File.separator + "lib";
        for (StringTokenizer st = new StringTokenizer(classpath, ":"); st.hasMoreTokens(); ) {
            String token = st.nextToken();
            if (token.startsWith(prefix)) {
                // this is a valid lib jar to put in
                cp.append(token).append(":");
            }
        }

        if (cp.length() == 0) {
            // unable to create a valid classpath
            mLogger.log(
                    "Unable to create a sensible classpath from " + home,
                    LogManager.DEBUG_MESSAGE_LEVEL);
            return result;
        }

        // we have everything now
        result = new Profile(Profile.ENV, "CLASSPATH", cp.toString());

        return result;
    }

    /**
     * Generates the argument string to be given to the replica registration job. At present by
     * default it would be picking up the file containing the mappings.
     *
     * @param dir the directory where the .in file should be written to
     * @param regJob The name of the registration job.
     * @param files Collection of <code>FileTransfer</code> objects containing the information about
     *     source and destURLs. The destination URLs would be our PFNs.
     * @return the argument string.
     */
    private String generateRepJobArgumentString(
            File dir, String regJob, Collection files, Collection<Job> computeJobs) {
        StringBuilder arguments = new StringBuilder();

        // PM-1549 set the the type to be output to indicate registration to output replica catalog
        arguments
                .append("--prefix")
                .append(" ")
                .append(ReplicaCatalogBridge.OUTPUT_REPLICA_CATALOG_PREFIX)
                .append(" ");

        // get any command line properties that may need specifying
        arguments
                .append("--conf")
                .append(" ")
                .append(mProps.getPropertiesInSubmitDirectory())
                .append(" ");

        // single verbose flag
        arguments.append("-v").append(" ");

        // PM-1582 list all the associated meta files of the compute jobs
        // if integrity checking is enabled
        if (!computeJobs.isEmpty() && this.mIntegrityCheckingEnabled) {
            arguments.append("--meta").append(" ");
            StringBuilder sb = new StringBuilder();
            for (Job parent : computeJobs) {
                File parentDir =
                        new File(
                                mPOptions.getSubmitDirectory(),
                                parent.getRelativeSubmitDirectory());
                File metaFile = new File(parentDir, parent.getID() + ".meta");
                sb.append(metaFile.getAbsolutePath()).append(",");
            }
            // remove any trailing ,
            int lastIndex = sb.length() - 1;
            String addOn =
                    (sb.lastIndexOf(",") == lastIndex) ? sb.substring(0, lastIndex) : sb.toString();

            arguments.append(addOn);
            arguments.append(" ");
        }

        // append the insert option
        arguments
                .append("--insert")
                .append(" ")
                .append(this.generateMappingsFile(regJob, dir, files));

        return arguments.toString();
    }

    /**
     * Returns the properties that need to be passed to the the rc-client invocation on the command
     * line . It is of the form "-Dprop1=value1 -Dprop2=value2 .."
     *
     * @param properties the properties object
     * @return the properties list, else empty string.
     */
    protected String getCommandLineProperties(PegasusProperties properties) {
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
        sb.append("-D").append(key).append("=").append(value).append(" ");
    }

    /**
     * Generates the registration mappings in a text file that is generated in the dax directory
     * (the directory where all the condor submit files are generated). The pool value for the
     * mapping is the output pool specified by the user when running Pegasus. The name of the file
     * is regJob+.in
     *
     * @param regJob The name of the registration job.
     * @param dir the directory where .in file should be written to
     * @param files Collection of <code>FileTransfer</code>objects containing the information about
     *     source and destURLs. The destination URLs would be our PFNs.
     * @return String corresponding to the path of the the file containig the mappings in the
     *     appropriate format.
     */
    private String generateMappingsFile(String regJob, File dir, Collection files) {
        String fileName = regJob + ".in";
        File f = null;

        // writing the stdin file
        try {
            f = new File(dir, fileName);
            FileWriter stdIn = new FileWriter(f);

            for (Iterator it = files.iterator(); it.hasNext(); ) {
                FileTransfer ft = (FileTransfer) it.next();
                // checking for transient flag
                if (!ft.getTransientRegFlag()) {
                    stdIn.write(ftToRC(ft, mRegisterDeepLFN));
                    stdIn.flush();
                }
            }

            stdIn.close();

        } catch (Exception e) {
            throw new RuntimeException(
                    "While writing out the registration file for job " + regJob, e);
        }

        return f.getAbsolutePath();
    }

    /**
     * Converts a <code>FileTransfer</code> to a RC compatible string representation.
     *
     * @param ft the <code>FileTransfer</code> object
     * @param registerDeepLFN whether to register the deep LFN or only the basename
     * @return the RC version.
     */
    private String ftToRC(FileTransfer ft, boolean registerDeepLFN) {
        StringBuffer sb = new StringBuffer();
        NameValue destURL = ft.getDestURL();
        String lfn = ft.getLFN();
        lfn = registerDeepLFN ? lfn : new File(lfn).getName();
        sb.append(lfn).append(" ");
        sb.append(ft.getURLForRegistrationOnDestination()).append(" ");
        sb.append("site=\"").append(destURL.getKey()).append("\"");

        // add any metadata attributes associated
        Metadata m = ft.getAllMetadata();
        for (Iterator<String> it = m.getProfileKeyIterator(); it.hasNext(); ) {
            String key = it.next();
            String value = (String) m.get(key);
            sb.append(" ").append(key).append("=\"").append(value).append("\"");
        }

        sb.append("\n");
        return sb.toString();
    }

    /**
     * Retrieves a location from the cache table, that contains the contents of the cache files
     * specified at runtime.
     *
     * @param lfn the logical name of the file.
     * @return <code>ReplicaLocation</code> object corresponding to the entry if found, else null.
     */
    private ReplicaLocation retrieveFromCache(String lfn) {
        return mCacheStore.getReplicaLocation(lfn);
    }

    /**
     * Ends up loading the inherited replica files.
     *
     * @param files set of paths to the inherited replica files.
     */
    private void loadInheritedReplicaStore(Set files) {
        mLogger.log(
                "Loading Inhertied ReplicaFiles files: " + files, LogManager.DEBUG_MESSAGE_LEVEL);
        this.mInheritedReplicaStore = this.getReplicaStoreFromFiles(files);
    }
    /**
     * Ends up loading the cache files so as to enable the lookup for the transient files created by
     * the parent jobs.
     *
     * @param cacheFiles set of paths to the cache files.
     */
    private void loadCacheFiles(Set cacheFiles) {
        mLogger.log("Loading cache files: " + cacheFiles, LogManager.DEBUG_MESSAGE_LEVEL);
        mCacheStore = this.getReplicaStoreFromFiles(cacheFiles);
    }

    /**
     * Ends up loading a Replica Store from replica catalog files
     *
     * @param files set of paths to the cache files.
     */
    private ReplicaStore getReplicaStoreFromFiles(Set files) {
        ReplicaStore store = new ReplicaStore();

        mLogger.logEventStart(
                LoggingKeys.EVENT_PEGASUS_LOAD_TRANSIENT_CACHE,
                LoggingKeys.DAX_ID,
                mDag.getAbstractWorkflowName());

        for (Iterator it = files.iterator(); it.hasNext(); ) {
            // read each of the cache file and load in memory
            String file = (String) it.next();

            // suck in all the entries into the cache replica store.
            Map<String, Collection<ReplicaCatalogEntry>> cacheMap =
                    lookupFromCacheFile(file, mSearchFiles);

            File metaCacheFile = new File(file + ".meta");
            if (metaCacheFile.exists()) {
                // PM-1257 rerieve metatadata from cache.meta file that can include
                // checksum data and merge in cache map
                Map<String, Collection<ReplicaCatalogEntry>> metadataCacheMap =
                        lookupFromCacheFile(file + ".meta", mSearchFiles);
                for (Map.Entry<String, Collection<ReplicaCatalogEntry>> metadataEntry :
                        metadataCacheMap.entrySet()) {
                    String lfn = metadataEntry.getKey();

                    for (ReplicaCatalogEntry metadataRCE : metadataEntry.getValue()) {
                        // check if entry has a checksum value
                        if (metadataRCE.hasAttribute(Metadata.CHECKSUM_VALUE_KEY)) {
                            String checksum =
                                    (String) metadataRCE.getAttribute(Metadata.CHECKSUM_VALUE_KEY);
                            String type =
                                    (String) metadataRCE.getAttribute(Metadata.CHECKSUM_TYPE_KEY);
                            // update entry in the cache map with this
                            Collection<ReplicaCatalogEntry> cacheEntries = cacheMap.get(lfn);
                            if (cacheEntries != null) {
                                for (ReplicaCatalogEntry cacheRCE : cacheEntries) {
                                    cacheRCE.addAttribute(Metadata.CHECKSUM_VALUE_KEY, checksum);
                                    if (type != null) {
                                        cacheRCE.addAttribute(Metadata.CHECKSUM_TYPE_KEY, type);
                                    }
                                }
                            }
                            // update with first checksum value found for lfn
                            break;
                        }
                    }
                }
            }

            store.add(cacheMap);
            mLogger.log(
                    "Loaded " + cacheMap.size() + " entry from file " + file,
                    LogManager.DEBUG_MESSAGE_LEVEL);
        }

        mLogger.logEventCompletion();
        return store;
    }

    /**
     * Retrieves locations of search files from a cache file
     *
     * @param file the cache file
     * @param searchFiles lfn's to search for
     * @return
     */
    private Map<String, Collection<ReplicaCatalogEntry>> lookupFromCacheFile(
            String file, Set<String> searchFiles) {
        Properties cacheProps =
                mProps.getVDSProperties().matchingSubset(ReplicaCatalog.c_prefix, false);
        Map<String, Collection<ReplicaCatalogEntry>> found = new HashMap();
        ReplicaCatalog simpleFile;
        // all cache files are loaded in readonly mode
        cacheProps.setProperty(ReplicaCatalogBridge.CACHE_READ_ONLY_KEY, "true");

        // set the appropriate property to designate path to file
        cacheProps.setProperty(ReplicaCatalogBridge.CACHE_REPLICA_CATALOG_KEY, file);

        mLogger.log("Loading  file: " + file, LogManager.DEBUG_MESSAGE_LEVEL);
        try {
            simpleFile =
                    ReplicaFactory.loadInstance(
                            CACHE_REPLICA_CATALOG_IMPLEMENTER, this.mBag, cacheProps);
        } catch (Exception e) {
            mLogger.log("Unable to load cache file " + file, e, LogManager.ERROR_MESSAGE_LEVEL);
            return found;
        }
        // suck in all the entries into the cache replica store.
        // returns an unmodifiable collection. so merging an issue..
        Map<String, Collection<ReplicaCatalogEntry>> m = simpleFile.lookup(searchFiles);

        // only add entries for which we have a PFN
        for (String lfn : m.keySet()) {
            Collection<ReplicaCatalogEntry> rces = m.get(lfn);
            if (!rces.isEmpty()) {
                found.put(lfn, rces);
            }
        }

        // no wildcards as we only want to load mappings for files that
        // we require
        // mCacheStore.add( simpleFile.lookup( wildcardConstraint ) );

        // close connection
        simpleFile.close();
        return found;
    }

    /**
     * Loads the mappings from the input directory
     *
     * @param directies set of directories to load from
     */
    private ReplicaStore getReplicaStoreFromDirectories(Set<String> directories) {
        ReplicaStore store = new ReplicaStore();
        Properties properties =
                mProps.getVDSProperties().matchingSubset(ReplicaCatalog.c_prefix, false);

        for (String directory : directories) {
            mLogger.logEventStart(
                    LoggingKeys.EVENT_PEGASUS_LOAD_DIRECTORY_CACHE,
                    LoggingKeys.DAX_ID,
                    mDag.getAbstractWorkflowName());

            ReplicaCatalog catalog = null;

            // set the appropriate property to designate path to file
            properties.setProperty(ReplicaCatalogBridge.DIRECTORY_REPLICA_CATALOG_KEY, directory);

            mLogger.log("Loading from directory: " + directory, LogManager.DEBUG_MESSAGE_LEVEL);
            try {
                catalog =
                        ReplicaFactory.loadInstance(
                                DIRECTORY_REPLICA_CATALOG_IMPLEMENTER, this.mBag, properties);

                store.add(catalog.lookup(mSearchFiles));
            } catch (Exception e) {
                mLogger.log(
                        "Unable to load from directory  " + directory,
                        e,
                        LogManager.ERROR_MESSAGE_LEVEL);
            } finally {
                if (catalog != null) {
                    catalog.close();
                }
            }
            mLogger.logEventCompletion();
        }
        return store;
    }

    /**
     * Loads the mappings from output replica catalogs in the submit directories of previous runs
     *
     * @param directories set of directories to load from
     */
    private ReplicaStore getReplicaStoreFromSubmitDirectories(Set<String> directories) {
        ReplicaStore store = new ReplicaStore();

        for (String directory : directories) {
            mLogger.logEventStart(
                    LoggingKeys.EVENT_PEGASUS_LOAD_DIRECTORY_CACHE,
                    LoggingKeys.DAX_ID,
                    mDag.getAbstractWorkflowName());

            ReplicaCatalog catalog = null;
            Map<String, String> braindump = new HashMap();
            try {
                braindump = Braindump.loadFrom(new File(directory));
            } catch (IOException ex) {
                mLogger.log(
                        "Unable to access braindump from dir " + directory,
                        ex,
                        LogManager.ERROR_MESSAGE_LEVEL);
                continue;
            }

            // pick up the properties file from braindump to get hold of output
            // replica catalog
            String pfile = braindump.get(Braindump.PROPERTIES_KEY);
            if (pfile == null || !new File(directory, pfile).exists()) {
                mLogger.log(
                        "Skipping. Unable to access properties file "
                                + pfile
                                + " in directory "
                                + directory,
                        LogManager.ERROR_MESSAGE_LEVEL);
                continue;
            }
            PegasusProperties props =
                    PegasusProperties.getInstance(new File(directory, pfile).getPath());
            String implementor =
                    props.getProperty(ReplicaCatalogBridge.OUTPUT_REPLICA_CATALOG_PREFIX);
            if (implementor == null) {
                mLogger.log(
                        "Skipping. Unable to determine output replica catalog from properties file "
                                + pfile
                                + " in directory "
                                + directory,
                        LogManager.ERROR_MESSAGE_LEVEL);
                continue;
            }
            Properties connectProps =
                    props.matchingSubset(ReplicaCatalogBridge.OUTPUT_REPLICA_CATALOG_PREFIX, false);

            mLogger.log(
                    "Loading from output replica catalog of type "
                            + implementor
                            + " with connection props: "
                            + connectProps,
                    LogManager.DEBUG_MESSAGE_LEVEL);
            try {
                catalog = ReplicaFactory.loadInstance(implementor, this.mBag, connectProps);
                store.add(catalog.lookup(mSearchFiles));
            } catch (Exception e) {
                mLogger.log(
                        "Unable to load from submit directory of previous run  " + directory,
                        e,
                        LogManager.ERROR_MESSAGE_LEVEL);
            } finally {
                if (catalog != null) {
                    catalog.close();
                }
            }
            mLogger.logEventCompletion();
        }
        return store;
    }

    /**
     * Returns path to the local proxy
     *
     * @return path to the local proxy
     */
    private String getPathToLocalProxy() {
        // load and intialize the CredentialHandler Factory
        CredentialHandlerFactory factory = new CredentialHandlerFactory();
        factory.initialize(mBag);
        CredentialHandler handler = factory.loadInstance(CredentialHandler.TYPE.x509);
        return handler.getPath("local");
    }

    /**
     * Returns booleans indicating whether default category is required for registration jobs or not
     *
     * @param rc
     * @param props
     * @return
     */
    private boolean requireDefaultCategoryForRegistrationJobs(
            ReplicaCatalog rc, PegasusProperties props) {
        /*
        boolean require = (rc != null);
        if (require) {
            require =
                    rc instanceof edu.isi.pegasus.planner.catalog.replica.impl.SimpleFile
                            || rc instanceof edu.isi.pegasus.planner.catalog.replica.impl.YAML
                            || Boolean.parse(
                                    props.getProperty(ReplicaCatalog.c_prefix + "." + "db.create"),
                                    false);
        }
        return require;
        */
        return true;
    }
}
