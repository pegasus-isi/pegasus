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
package edu.isi.pegasus.planner.transfer.sls;

import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.common.util.PegasusURL;
import edu.isi.pegasus.common.util.Separator;
import edu.isi.pegasus.planner.catalog.TransformationCatalog;
import edu.isi.pegasus.planner.catalog.replica.ReplicaCatalogEntry;
import edu.isi.pegasus.planner.catalog.site.classes.FileServer;
import edu.isi.pegasus.planner.catalog.site.classes.FileServerType.OPERATION;
import edu.isi.pegasus.planner.catalog.site.classes.SiteStore;
import edu.isi.pegasus.planner.catalog.transformation.TransformationCatalogEntry;
import edu.isi.pegasus.planner.catalog.transformation.classes.Container;
import edu.isi.pegasus.planner.catalog.transformation.classes.TCType;
import edu.isi.pegasus.planner.classes.FileTransfer;
import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.classes.PegasusFile;
import edu.isi.pegasus.planner.classes.PlannerCache;
import edu.isi.pegasus.planner.classes.Profile;
import edu.isi.pegasus.planner.common.PegasusProperties;
import edu.isi.pegasus.planner.mapper.StagingMapper;
import edu.isi.pegasus.planner.namespace.ENV;
import edu.isi.pegasus.planner.namespace.Pegasus;
import edu.isi.pegasus.planner.transfer.SLS;
import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

/**
 * This uses the transfer executable distributed with Pegasus to do the second level staging.
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public class Transfer implements SLS {

    /** The transformation namespace for the transfer job. */
    public static final String TRANSFORMATION_NAMESPACE = "pegasus";

    /**
     * The name of the underlying transformation that is queried for in the Transformation Catalog.
     */
    public static final String TRANSFORMATION_NAME = "transfer";

    /** The version number for the transfer job. */
    public static final String TRANSFORMATION_VERSION = null;

    /** The derivation namespace for for the transfer job. */
    public static final String DERIVATION_NAMESPACE = "pegasus";

    /** The name of the underlying derivation. */
    public static final String DERIVATION_NAME = "transfer";

    /** The derivation version number for the transfer job. */
    public static final String DERIVATION_VERSION = "1.0";

    /** The default number of threads pegasus-transfer uses */
    public static final int DEFAULT_NUMBER_OF_THREADS = 1;

    /** A short description of the transfer implementation. */
    public static final String DESCRIPTION = "Pegasus Transfer Wrapper around GUC";

    /** The executable basename */
    public static final String EXECUTABLE_BASENAME = "pegasus-transfer";

    /** The handle to the site catalog. */
    protected SiteStore mSiteStore;

    /** The handle to the transformation catalog. */
    protected TransformationCatalog mTCHandle;

    /** The handle to the properties. */
    protected PegasusProperties mProps;

    /** The handle to the logging manager. */
    protected LogManager mLogger;

    /** Any extra arguments that need to be passed ahead to the s3 client invocation. */
    protected String mExtraArguments;

    /** Boolean to track whether to stage sls file or not */
    protected boolean mStageSLSFile;

    /**
     * A SimpleFile Replica Catalog, that tracks all the files that are being materialized as part
     * of workflow execution.
     */
    private PlannerCache mPlannerCache;

    /**
     * This member variable if set causes the destination URL for the symlink jobs to have
     * symlink:// url if the pool attributed associated with the pfn is same as a particular jobs
     * execution pool.
     */
    protected boolean mUseSymLinks;

    /** Handle to the staging mapper to determine addon's for LFN's per site. */
    protected StagingMapper mStagingMapper;

    /** The dial for integrity checking */
    protected PegasusProperties.INTEGRITY_DIAL mIntegrityDial;

    /** The default constructor. */
    public Transfer() {}

    /**
     * Initializes the SLS implementation.
     *
     * @param bag the bag of objects. Contains access to catalogs etc.
     */
    public void initialize(PegasusBag bag) {
        mProps = bag.getPegasusProperties();
        mLogger = bag.getLogger();
        mSiteStore = bag.getHandleToSiteStore();
        mTCHandle = bag.getHandleToTransformationCatalog();
        mStagingMapper = bag.getStagingMapper();
        mExtraArguments = mProps.getSLSTransferArguments();
        mStageSLSFile = mProps.stageSLSFilesViaFirstLevelStaging();
        mPlannerCache = bag.getHandleToPlannerCache();
        mUseSymLinks = mProps.getUseOfSymbolicLinks();
        mIntegrityDial = mProps.getIntegrityDial();
    }

    /**
     * Returns a boolean whether the SLS implementation does a condor based modification or not. By
     * condor based modification we mean whether it uses condor specific classads to achieve the
     * second level staging or not.
     *
     * @return false
     */
    public boolean doesCondorModifications() {
        return false;
    }

    /**
     * Constructs a command line invocation for a job, with a given sls file. The SLS maybe null. In
     * the case where SLS impl does not read from a file, it is advised to create a file in
     * generateSLSXXX methods, and then read the file in this function and put it on the command
     * line.
     *
     * @param job the job that is being sls enabled
     * @param slsFile the slsFile can be null
     * @return invocation string
     */
    public String invocationString(Job job, File slsFile) {
        StringBuffer invocation = new StringBuffer();

        TransformationCatalogEntry entry = this.getTransformationCatalogEntry(job.getSiteHandle());
        String executable =
                (entry == null)
                        ? this.getExecutableBasename()
                        : // nothing in the transformation catalog, rely on the executable basenmae
                        entry.getPhysicalTransformation(); // rely on what is in the
        // transformation catalog

        invocation.append(executable);

        // get the value if any that is present in the associated compute job
        // that should take care of the properties also
        int threads = Transfer.DEFAULT_NUMBER_OF_THREADS;
        if (job.vdsNS.containsKey(Pegasus.TRANSFER_THREADS_KEY)) {
            try {
                threads = Integer.parseInt(job.vdsNS.getStringValue(Pegasus.TRANSFER_THREADS_KEY));
            } catch (Exception e) {
                mLogger.log(
                        "Invalid value picked up for Pegasus profile "
                                + Pegasus.TRANSFER_THREADS_KEY
                                + " PegasusLite job "
                                + job.getID(),
                        LogManager.ERROR_MESSAGE_LEVEL);
            }
        }
        invocation.append(" --threads ").append(threads).append(" ");

        // append any extra arguments set by user
        String args = job.vdsNS.getStringValue(Pegasus.TRANSFER_SLS_ARGUMENTS_KEY);
        if (args != null) {
            invocation.append(" ").append(args);
        }

        if (slsFile != null) {
            // add the required arguments to transfer
            invocation.append(" -f ");
            // we add absolute path if the sls files are staged via
            // first level staging
            if (this.mStageSLSFile) {
                invocation.append(slsFile.getAbsolutePath());

            } else {
                // only the basename
                invocation.append(slsFile.getName());
            }
        }
        return invocation.toString();
    }

    /**
     * Returns a boolean indicating whether it will an input file for a job to do the transfers.
     * Transfer reads from stdin the file transfers that it needs to do. Always returns true, as we
     * need to transfer the proxy always.
     *
     * @param job the job being detected.
     * @return true
     */
    public boolean needsSLSInputTransfers(Job job) {
        return true;
    }

    /**
     * Returns a boolean indicating whether it will an output file for a job to do the transfers.
     * Transfer reads from stdin the file transfers that it needs to do.
     *
     * @param job the job being detected.
     * @return true
     */
    public boolean needsSLSOutputTransfers(Job job) {
        Set files = job.getOutputFiles();
        return !(files == null || files.isEmpty());
    }

    /**
     * Returns the LFN of sls input file.
     *
     * @param job Job
     * @return the name of the sls input file.
     */
    public String getSLSInputLFN(Job job) {
        StringBuffer lfn = new StringBuffer();
        lfn.append("sls_").append(job.getName()).append(".in");
        return lfn.toString();
    }

    /**
     * Returns the LFN of sls output file.
     *
     * @param job Job
     * @return the name of the sls input file.
     */
    public String getSLSOutputLFN(Job job) {
        StringBuffer lfn = new StringBuffer();
        lfn.append("sls_").append(job.getName()).append(".out");
        return lfn.toString();
    }

    /**
     * Generates a second level staging file of the input files to the worker node directory.
     *
     * @param job job for which the file is being created
     * @param fileName name of the file that needs to be written out.
     * @param stagingSiteServer the file server on the staging site to be used for retrieval of
     *     files i.e the get operation
     * @param stagingSiteDirectory directory on the head node of the staging site.
     * @param workerNodeDirectory worker node directory
     * @return a Collection of FileTransfer objects listing the transfers that need to be done.
     * @see #needsSLSInputTransfers( Job)
     */
    public Collection<FileTransfer> determineSLSInputTransfers(
            Job job,
            String fileName,
            FileServer stagingSiteServer,
            String stagingSiteDirectory,
            String workerNodeDirectory) {

        // sanity check
        if (!needsSLSInputTransfers(job)) {
            mLogger.log(
                    "Not Writing out a SLS input file for job " + job.getName(),
                    LogManager.DEBUG_MESSAGE_LEVEL);
            return null;
        }

        Set files = job.getInputFiles();

        //      To handle for null conditions?
        //        File sls = null;
        Collection<FileTransfer> result = new LinkedList();
        String destDir = workerNodeDirectory;

        // PM-1197 check to see if a job is to be launched in a container
        Container c = job.getContainer();
        String containerLFN = null;
        if (c != null) {
            containerLFN = c.getLFN();
            if (this.mUseSymLinks && c.getMountPoints().isEmpty()) {
                mLogger.log(
                        "Symlink of data files will be disabled because job "
                                + job.getID()
                                + " is launched in a container and no host mounts specified "
                                + containerLFN,
                        LogManager.DEBUG_MESSAGE_LEVEL);
            }
        }

        // To do. distinguish the sls file from the other input files
        for (Iterator it = files.iterator(); it.hasNext(); ) {
            PegasusFile pf = (PegasusFile) it.next();
            String lfn = pf.getLFN();

            if (lfn.equals(ENV.X509_USER_PROXY_KEY)) {
                // ignore the proxy file for time being
                // as we picking it from the head node directory
                continue;
            }

            FileTransfer ft = new FileTransfer();
            ft.setLFN(pf.getLFN());
            // ensure that right type gets associated, especially
            // whether a file is a checkpoint file or not
            ft.setType(pf.getType());

            // create the default path from the directory
            // on the head node
            StringBuffer url = new StringBuffer();

            Collection<ReplicaCatalogEntry> cacheLocations = null;
            Collection<ReplicaCatalogEntry> sources = new LinkedList();
            boolean symlink = false;
            String computeSite = job.getSiteHandle();
            if (pf.doBypassStaging()) {
                // PM-698
                // we retrieve the URL from the Planner Cache as a get URL
                // bypassed URL's are stored as GET urls in the cache and
                // associated with the compute site

                // PM 1002 first we try and find a tighter match on the compute site
                // and then the loose match
                cacheLocations = mPlannerCache.lookupAllEntries(lfn, computeSite, OPERATION.get);
                if (cacheLocations.isEmpty()) {
                    mLogger.log(
                            "Unable to find location of lfn in planner(get) cache with input staging bypassed "
                                    + lfn
                                    + " for job "
                                    + job.getID(),
                            LogManager.DEBUG_MESSAGE_LEVEL);
                }
            }
            if (cacheLocations == null || cacheLocations.isEmpty()) {
                String stagingSite = job.getStagingSiteHandle();
                // construct the location with respect to the staging site
                if (mUseSymLinks
                        && // specified in configuration
                        stagingSite.equals(
                                computeSite)) { // source URL logically on the same site where job
                    // is to be run
                    // we can symlink . construct the source URL as a file url
                    symlink = true;
                    url.append(PegasusURL.FILE_URL_SCHEME)
                            .append("//")
                            .append(stagingSiteDirectory);
                    if (pf.isExecutable()) {
                        // PM-734 for executable files we can have the source url as file url
                        // but we cannot have the destination URL as symlink://
                        // as we want to do chmod on the local copy on the worker node
                        symlink = false;
                    }
                    if (pf.isCheckpointFile()) {
                        // we never symlink checkpoint files
                        symlink = false;
                    }
                } else {
                    url.append(
                            mSiteStore.getExternalWorkDirectoryURL(stagingSiteServer, stagingSite));
                }
                // PM-833 append the relative staging site add on directory
                url.append(File.separator)
                        .append(mStagingMapper.getRelativeDirectory(stagingSite, lfn));

                url.append(File.separator).append(lfn);

                // ft.addSource( stagingSite, url.toString() );
                sources.add(new ReplicaCatalogEntry(url.toString(), stagingSite));
            } else {
                // construct the URL wrt to the planner cache location
                // PM-1014 go through all the candidates returned from the planner cache
                for (ReplicaCatalogEntry cacheLocation : cacheLocations) {
                    url = new StringBuffer();
                    url.append(cacheLocation.getPFN());

                    // ft.addSource(cacheLocation);
                    sources.add((ReplicaCatalogEntry) cacheLocation.clone());

                    symlink =
                            (mUseSymLinks
                                    && // specified in configuration
                                    !pf.isCheckpointFile()
                                    && // can only do symlinks for data files . not checkpoint files
                                    !pf.isExecutable()
                                    && // can only do symlinks for data files . not executables
                                    cacheLocation.getResourceHandle().equals(job.getSiteHandle())
                                    && // source URL logically on the same site where job is to be
                                    // run
                                    url.toString()
                                            .startsWith(
                                                    PegasusURL.FILE_URL_SCHEME)); // source URL is a
                    // file URL
                }
            }

            if (symlink && containerLFN != null) {
                // PM-1197 special handling for the case where job is to be
                // launched in a container.Normally, we can only symlink the
                // container image.
                if (!pf.getLFN().equals(containerLFN)) {
                    symlink = false;
                    // PM-1298 check if source file directory is mounted
                    for (ReplicaCatalogEntry source : sources) {
                        String sourceURL = source.getPFN();
                        String replacedURL = c.getPathInContainer(sourceURL);
                        if (replacedURL != null) {
                            symlink = true;
                            source.setPFN(replacedURL);
                            // we don't want pegasus-transfer to fail in PegasusLite
                            // on the missing source path that is only visible in the container
                            ft.setVerifySymlinkSource(false);
                            mLogger.log(
                                    "Replaced source URL on host "
                                            + sourceURL
                                            + " with path in the container "
                                            + source.getPFN(),
                                    LogManager.DEBUG_MESSAGE_LEVEL);
                        }
                    }
                }
            }

            // add all the sources
            for (ReplicaCatalogEntry source : sources) {
                ft.addSource(source);
            }

            // if the source URL is already present at the compute site
            // and is a file URL, then the destination URL has to be a symlink
            String destURLScheme =
                    (symlink)
                            ? PegasusURL.SYMLINK_URL_SCHEME
                            : PegasusURL.FILE_URL_SCHEME; // default is file URL

            // PM-1375 check the dial to see if we need to check checksum for this
            // or not, and turn off if nosymlink
            if (symlink && mIntegrityDial == PegasusProperties.INTEGRITY_DIAL.nosymlink) {
                pf.setForIntegrityChecking(false);
            }

            // destination
            url = new StringBuffer();
            url.append(destURLScheme)
                    .append("//")
                    .append(destDir)
                    .append(File.separator)
                    .append(pf.getLFN());
            ft.addDestination(job.getSiteHandle(), url.toString());

            result.add(ft);
        }
        return result;
    }

    /**
     * Generates a second level staging file of the input files to the worker node directory.
     *
     * @param job the job for which the file is being created
     * @param fileName the name of the file that needs to be written out.
     * @param stagingSiteServer the file server on the staging site to be used for retrieval of
     *     files i.e the put operation
     * @param stagingSiteDirectory the directory on the head node of the staging site.
     * @param workerNodeDirectory the worker node directory
     * @return a Collection of FileTransfer objects listing the transfers that need to be done.
     * @see #needsSLSOutputTransfers( Job)
     */
    public Collection<FileTransfer> determineSLSOutputTransfers(
            Job job,
            String fileName,
            FileServer stagingSiteServer,
            String stagingSiteDirectory,
            String workerNodeDirectory) {

        // sanity check
        if (!needsSLSOutputTransfers(job)) {
            mLogger.log(
                    "Not Writing out a SLS output file for job " + job.getName(),
                    LogManager.DEBUG_MESSAGE_LEVEL);
            return null;
        }

        //      To handle for null conditions?
        //        File sls = null;
        Collection<FileTransfer> result = new LinkedList();
        Set files = job.getOutputFiles();

        String sourceDir = workerNodeDirectory;

        PegasusFile pf;
        String stagingSite = job.getStagingSiteHandle();
        String computeSite = job.getSiteHandle();

        // To do. distinguish the sls file from the other input files
        for (Iterator it = files.iterator(); it.hasNext(); ) {
            pf = (PegasusFile) it.next();
            String lfn = pf.getLFN();

            FileTransfer ft = new FileTransfer();
            // ensure that right type gets associated, especially
            // whether a file is a checkpoint file or not
            ft.setType(pf.getType());

            // source
            StringBuffer url = new StringBuffer();
            url.append("file://").append(sourceDir).append(File.separator).append(lfn);
            ft.addSource(job.getSiteHandle(), url.toString());

            // destination
            url = new StringBuffer();

            if (mUseSymLinks && computeSite.equals(stagingSite)) {
                // PM-1108 symlinking is turned on and the compute site for
                // the job is the same the staging site. This means that
                // the shared-scratch directory used on the staging site is locally
                // accessible to the compute nodes. So we can go directly via the
                // filesystem to copy the file
                url.append(PegasusURL.FILE_URL_SCHEME).append("//").append(stagingSiteDirectory);
            } else {
                // PM-1108 we go through the external file server interface, since
                // staging site is remote to the compute site
                // on the head node
                url.append(
                        mSiteStore.getExternalWorkDirectoryURL(
                                stagingSiteServer, job.getStagingSiteHandle()));
            }

            // PM-833 append the relative staging site add on directory
            url.append(File.separator)
                    .append(mStagingMapper.getRelativeDirectory(stagingSite, lfn));
            url.append(File.separator).append(lfn);

            ft.addDestination(stagingSite, url.toString());

            result.add(ft);
        }

        return result;
    }

    /**
     * Modifies a compute job for second level staging. The only modification it does is add the
     * appropriate environment varialbes to the job
     *
     * @param job the job to be modified.
     * @param stagingSiteURLPrefix the url prefix for the server on the staging site
     * @param stagingSitedirectory the directory on the staging site, where the inp
     * @param workerNodeDirectory the directory in the worker node tmp
     * @return boolean indicating whether job was successfully modified or not.
     */
    public boolean modifyJobForWorkerNodeExecution(
            Job job,
            String stagingSiteURLPrefix,
            String stagingSitedirectory,
            String workerNodeDirectory) {

        List envs = this.getEnvironmentVariables(job.getSiteHandle());

        if (envs == null || envs.isEmpty()) {
            // no hard failure.
            mLogger.log(
                    "No special environment set for  "
                            + Separator.combine(
                                    this.TRANSFORMATION_NAMESPACE,
                                    this.TRANSFORMATION_NAME,
                                    this.TRANSFORMATION_VERSION)
                            + " for job "
                            + job.getID(),
                    LogManager.TRACE_MESSAGE_LEVEL);
            return true;
        }

        for (Iterator it = envs.iterator(); it.hasNext(); ) {
            job.envVariables.checkKeyInNS((Profile) it.next());
        }

        return true;
    }

    /**
     * Retrieves the transformation catalog entry for the executable that is being used to transfer
     * the files in the implementation. If an entry is not specified in the Transformation Catalog,
     * then null is returned.
     *
     * @param siteHandle the handle of the site where the transformation is to be searched.
     * @return the transformation catalog entry if found, else null.
     */
    public TransformationCatalogEntry getTransformationCatalogEntry(String siteHandle) {
        List tcentries = null;
        try {
            // namespace and version are null for time being
            tcentries =
                    mTCHandle.lookup(
                            Transfer.TRANSFORMATION_NAMESPACE,
                            Transfer.TRANSFORMATION_NAME,
                            Transfer.TRANSFORMATION_VERSION,
                            siteHandle,
                            TCType.INSTALLED);
        } catch (Exception e) {
            mLogger.log(
                    "Unable to retrieve entry from TC for "
                            + Separator.combine(
                                    Transfer.TRANSFORMATION_NAMESPACE,
                                    Transfer.TRANSFORMATION_NAME,
                                    Transfer.TRANSFORMATION_VERSION)
                            + " Cause:"
                            + e,
                    LogManager.DEBUG_MESSAGE_LEVEL);
        }

        return (tcentries == null) ? null : (TransformationCatalogEntry) tcentries.get(0);
    }

    /**
     * Returns a default TC entry to be used in case entry is not found in the transformation
     * catalog.
     *
     * @param namespace the namespace of the transfer transformation
     * @param name the logical name of the transfer transformation
     * @param version the version of the transfer transformation
     * @param executableBasename the basename of the executable
     * @param site the site for which the default entry is required.
     * @return the default entry.
     */
    protected TransformationCatalogEntry defaultTCEntry(
            String namespace, String name, String version, String executableBasename, String site) {

        TransformationCatalogEntry defaultTCEntry = null;
        // check if PEGASUS_HOME is set
        String home = mSiteStore.getPegasusHome(site);
        // if PEGASUS_HOME is not set, use VDS_HOME
        home = (home == null) ? mSiteStore.getVDSHome(site) : home;

        mLogger.log(
                "Creating a default TC entry for "
                        + Separator.combine(namespace, name, version)
                        + " at site "
                        + site,
                LogManager.DEBUG_MESSAGE_LEVEL);

        // if home is still null
        if (home == null) {
            // cannot create default TC
            mLogger.log(
                    "Unable to create a default entry for "
                            + Separator.combine(namespace, name, version)
                            + " as PEGASUS_HOME or VDS_HOME is not set in Site Catalog",
                    LogManager.DEBUG_MESSAGE_LEVEL);
            // set the flag back to true
            return defaultTCEntry;
        }

        // remove trailing / if specified
        home =
                (home.charAt(home.length() - 1) == File.separatorChar)
                        ? home.substring(0, home.length() - 1)
                        : home;

        // construct the path to it
        StringBuffer path = new StringBuffer();
        path.append(home)
                .append(File.separator)
                .append("bin")
                .append(File.separator)
                .append(Transfer.EXECUTABLE_BASENAME);

        defaultTCEntry = new TransformationCatalogEntry(namespace, name, version);

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
        mLogger.log(
                "Created entry with path " + defaultTCEntry.getPhysicalTransformation(),
                LogManager.DEBUG_MESSAGE_LEVEL);
        return defaultTCEntry;
    }

    /**
     * Returns the environment profiles that are required for the default entry to sensibly work.
     * Tries to retrieve the following variables
     *
     * <pre>
     * PEGASUS_HOME
     * GLOBUS_LOCATION
     * </pre>
     *
     * @param site the site where the job is going to run.
     * @return List of environment variables, else empty list if none are found
     */
    protected List getEnvironmentVariables(String site) {
        List result = new ArrayList(2);

        String pegasusHome = mSiteStore.getEnvironmentVariable(site, "PEGASUS_HOME");
        if (pegasusHome != null) {
            // we have both the environment variables
            result.add(new Profile(Profile.ENV, "PEGASUS_HOME", pegasusHome));
        }

        String globus = mSiteStore.getEnvironmentVariable(site, "GLOBUS_LOCATION");
        if (globus != null) {
            result.add(new Profile(Profile.ENV, "GLOBUS_LOCATION", globus));
        }

        return result;
    }

    /**
     * Return the executable basename for transfer executable used.
     *
     * @return the executable basename.
     */
    protected String getExecutableBasename() {
        return Transfer.EXECUTABLE_BASENAME;
    }

    /**
     * Complains for head node url prefix not specified
     *
     * @param site the site handle
     * @throws RuntimeException when URL Prefix cannot be determined for various reason.
     */
    protected void complainForHeadNodeURLPrefix(String site) {
        this.complainForHeadNodeURLPrefix(null, site);
    }

    /**
     * Complains for head node url prefix not specified
     *
     * @param job the related job if any
     * @param site the site handle
     * @throws RuntimeException when URL Prefix cannot be determined for various reason.
     */
    protected void complainForHeadNodeURLPrefix(Job job, String site) {
        StringBuffer error = new StringBuffer();
        if (job != null) {
            error.append("[SLS Transfer] For job (").append(job.getID()).append(").");
        }
        error.append(
                        "Unable to determine URL Prefix for the FileServer for scratch shared file system on site: ")
                .append(site);
        throw new RuntimeException(error.toString());
    }

    /**
     * Returns a textual description of the transfer mode.
     *
     * @return a short textual description
     */
    public String getDescription() {
        return "SLS backend using pegasus-transfer to worker node";
    }
}
