/**
 * Copyright 2007-2021 University Of Southern California
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
package edu.isi.pegasus.planner.transfer.generator;

import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.common.util.PegasusURL;
import edu.isi.pegasus.planner.catalog.ReplicaCatalog;
import edu.isi.pegasus.planner.catalog.replica.ReplicaCatalogEntry;
import edu.isi.pegasus.planner.catalog.site.classes.FileServerType;
import edu.isi.pegasus.planner.catalog.site.classes.SiteCatalogEntry;
import edu.isi.pegasus.planner.classes.ADag;
import edu.isi.pegasus.planner.classes.DAGJob;
import edu.isi.pegasus.planner.classes.DAXJob;
import edu.isi.pegasus.planner.classes.FileTransfer;
import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.classes.NameValue;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.classes.PegasusFile;
import edu.isi.pegasus.planner.classes.PlannerCache;
import edu.isi.pegasus.planner.classes.ReplicaLocation;
import edu.isi.pegasus.planner.common.PegasusProperties;
import edu.isi.pegasus.planner.namespace.Dagman;
import edu.isi.pegasus.planner.namespace.Pegasus;
import edu.isi.pegasus.planner.refiner.ReplicaCatalogBridge;
import edu.isi.pegasus.planner.selector.ReplicaSelector;
import edu.isi.pegasus.planner.transfer.Refiner;
import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * This class determines where the raw inputs required by a job need to be placed on the staging
 * site. It computes FileTransfer pairs that contain both the source URL from the Replica Catalog
 * and the destination URL on the staging site. Internally the StageOut Placer relies on
 * StagingMapper to find the destination URL on the staging site. It also groups File Transfers as
 * transfers that - need to be managed locally (third party mode) IR - remotely (case where the
 * staging site only has file accessible URLs)
 *
 * @author Karan Vahi
 */
public class StageIn extends Abstract {

    /** The property prefix for retrieving SRM properties. */
    public static final String SRM_PROPERTIES_PREFIX = "pegasus.transfer.srm";

    /** The suffix to retrive the service url for SRM server. */
    public static final String SRM_SERVICE_URL_PROPERTIES_SUFFIX = "service.url";

    /** The suffix to retrive the mount point for SRM server. */
    public static final String SRM_MOUNT_POINT_PROPERTIES_SUFFIX = "mountpoint";

    /** The handle to the replica selector that is to used to select the various replicas. */
    private ReplicaSelector mReplicaSelector;

    /** The bridge to the Replica Catalog. */
    private ReplicaCatalogBridge mRCBridge;

    /** A map that associates the site name with the SRM server url and mount point. */
    private Map<String, NameValue> mSRMServiceURLToMountPointMap;

    /**
     * This member variable if set causes the destination URL for the symlink jobs to have
     * symlink:// url if the pool attributed associated with the pfn is same as a particular jobs
     * execution pool.
     */
    private boolean mUseSymLinks;

    /** A boolean indicating whether to bypass first level staging for inputs */
    private boolean mBypassStagingForInputs;

    /**
     * A SimpleFile Replica Catalog, that tracks all the files that are being materialized as part
     * of workflow executaion.
     */
    private PlannerCache mPlannerCache;

    /**
     * A Replica Catalog, that tracks all the GET URL's for the files on the staging sites. This
     * cache is used to do automatic data management between 2 sub workflow jobs that have a data
     * dependency between them. For a workflow with two sub workflow jobs Sub1 -> Sub2, Pegasus
     * passes the workflow cache file that is created when Sub1 workflow is planned as an argument
     * to the planner when it is invoked to plan the Sub2 job.
     */
    private ReplicaCatalog mWorkflowCache;

    public StageIn() {}

    /**
     * Initializes the Stageout generator
     *
     * @param dag the workflow so far.
     * @param bag bag of initialization objects
     * @param refiner
     */
    public void initalize(ADag dag, PegasusBag bag, Refiner refiner) {
        super.initalize(dag, bag, refiner);
        mUseSymLinks = mProps.getUseOfSymbolicLinks();
        mBypassStagingForInputs = mProps.bypassFirstLevelStagingForInputs();
        mSRMServiceURLToMountPointMap = constructSiteToSRMServerMap(mProps);
    }

    /**
     * Initializes the Stageout generator
     *
     * @param dag the workflow so far.
     * @param bag bag of initialization objects
     * @param refiner
     * @param rcb
     * @param rs
     * @param plannerCache
     * @param workflowCache
     */
    public void initalize(
            ADag dag,
            PegasusBag bag,
            Refiner refiner,
            ReplicaCatalogBridge rcb,
            ReplicaSelector rs,
            PlannerCache plannerCache,
            ReplicaCatalog workflowCache) {
        this.initalize(dag, bag, refiner);
        mRCBridge = rcb;
        mReplicaSelector = rs;

        mPlannerCache = plannerCache;
        mWorkflowCache = workflowCache;
        mUseSymLinks = mProps.getUseOfSymbolicLinks();
        mBypassStagingForInputs = mProps.bypassFirstLevelStagingForInputs();
        mSRMServiceURLToMountPointMap = constructSiteToSRMServerMap(mProps);
    }

    /**
     * Constructs FileTransfer required for staging in raw inputs of a DAGJob. Raw inputs are files
     * whose locations need to be retrieved from the various Replica Catalog backends. The generated
     * FileTransfers are binned according to whether they need to be put in a StageIn job that runs
     * locally on the submit host, or remotely on the staging site.
     *
     * @param job the DAGJob
     * @param searchFiles file that need to be looked in the Replica Catalog.
     * @return array of Collection of <code>FileTransfer</code> objects, with the first Collection
     *     referring to transfers that need to happen on submit node, and the second Collection
     *     referring to transfers that need to happen on staging site
     */
    public Collection<FileTransfer>[] constructFileTX(
            DAGJob job, Collection<PegasusFile> searchFiles) {
        Collection<FileTransfer>[] result = new Collection[2];
        result[0] = new LinkedList(); // local transfers
        result[1] = new LinkedList(); // remote transfers

        // dax appears in adag element
        String dag = null;

        // go through all the job input files
        // and set transfer flag to false
        for (Iterator<PegasusFile> it = job.getInputFiles().iterator(); it.hasNext(); ) {
            PegasusFile pf = it.next();
            // at the moment dax files are not staged in.
            // remove from input set of files
            // part of the reason is about how to handle where
            // to run the DAGJob. We dont have much control over it.
            it.remove();
        }

        String lfn = job.getDAGLFN();
        ReplicaLocation rl = mRCBridge.getFileLocs(lfn);

        if (rl == null) { // flag an error
            throw new RuntimeException(
                    "TransferEngine.java: Can't determine a location to "
                            + "transfer input file for DAG lfn "
                            + lfn
                            + " for job "
                            + job.getName());
        }

        ReplicaCatalogEntry selLoc = mReplicaSelector.selectReplica(rl, job.getSiteHandle(), true);
        String pfn = selLoc.getPFN();
        // some extra checks to ensure paths
        if (pfn.startsWith(File.separator)) {
            dag = pfn;
        } else if (pfn.startsWith(PegasusURL.FILE_URL_SCHEME)) {
            dag = new PegasusURL(pfn).getPath();
        } else {
            throw new RuntimeException(
                    "Invalid URL Specified for DAG Job " + job.getName() + " -> " + pfn);
        }

        job.setDAGFile(dag);

        // set the directory if specified
        job.setDirectory((String) job.dagmanVariables.removeKey(Dagman.DIRECTORY_EXTERNAL_KEY));
        return result;
    }

    /**
     * Constructs FileTransfer required for staging in raw inputs of a DAXJob. Raw inputs are files
     * whose locations need to be retrieved from the various Replica Catalog backends. The generated
     * FileTransfers are binned according to whether they need to be put in a StageIn job that runs
     * locally on the submit host, or remotely on the staging site.
     *
     * @param job the DAXJob
     * @param searchFiles file that need to be looked in the Replica Catalog.
     * @return array of Collection of <code>FileTransfer</code> objects, with the first Collection
     *     referring to transfers that need to happen on submit node, and the second Collection
     *     referring to transfers that need to happen on staging site
     */
    public Collection<FileTransfer>[] constructFileTX(
            DAXJob job, Collection<PegasusFile> searchFiles) {
        // dax appears in adag element
        String dax = null;
        String lfn = job.getDAXLFN();

        PegasusFile daxFile = new PegasusFile(lfn);
        if (!job.getInputFiles().contains(daxFile)) {
            // if the LFN is not specified as an input file in the DAX
            // lets add it PM-667 more of a sanity check.
            daxFile.setTransferFlag(PegasusFile.TRANSFER_MANDATORY);
            job.getInputFiles().add(daxFile);
            searchFiles.add(daxFile);
        }

        // update the dax argument with the direct path to the DAX file
        // if present locally. This is to ensure that SUBDAXGenerator
        // can figure out the path to the dag file that will be created for the
        // job. Else the dax job needs to have a --basename option passed.
        ReplicaLocation rl = mRCBridge.getFileLocs(lfn);

        if (rl != null) {

            ReplicaCatalogEntry selLoc =
                    mReplicaSelector.selectReplica(rl, job.getSiteHandle(), true);
            String pfn = selLoc.getPFN();
            // some extra checks to ensure paths
            if (pfn.startsWith(File.separator)) {
                dax = pfn;
            } else if (pfn.startsWith(PegasusURL.FILE_URL_SCHEME)) {
                dax = new PegasusURL(pfn).getPath();
            }
        }

        if (dax == null) {
            // append the lfn instead of the full path to the dax PM-667
            // the user then needs to have a basename option set for the DAX job
            dax = lfn;
        } else {
            // we also remove the daxFile from the input files for the job.
            // and the searchFiles as we have a local path to the DAX .
            if (job.getInputFiles().contains(daxFile)) {
                boolean removed = job.getInputFiles().remove(daxFile);
                logRemoval(job, daxFile, "Job Input files ", removed);
            }
            if (searchFiles.contains(daxFile)) {
                boolean removed = searchFiles.remove(daxFile);
                logRemoval(job, daxFile, "Job Search Files", removed);
            }
        }

        // add the dax to the argument
        StringBuilder arguments = new StringBuilder();
        arguments.append(job.getArguments()).append(" ").append(dax);
        job.setArguments(arguments.toString());

        mLogger.log(
                "Set arguments for DAX job " + job.getID() + " to " + arguments.toString(),
                LogManager.DEBUG_MESSAGE_LEVEL);

        return this.constructFileTX((Job) job, searchFiles);
    }

    /**
     * Constructs FileTransfer required for staging in raw inputs of a compute Job. Raw inputs are
     * files whose locations need to be retrieved from the various Replica Catalog backends. The
     * generated FileTransfers are binned according to whether they need to be put in a StageIn job
     * that runs locally on the submit host, or remotely on the staging site.
     *
     * @param job the <code>Job</code>object for whose ipfile have to search the Replica Mechanism
     *     for.
     * @param searchFiles Collection containing the PegasusFile objects corresponding to the files
     *     that need to have their mapping looked up from the Replica Mechanism.
     * @return array of Collection of <code>FileTransfer</code> objects, with the first Collection
     *     referring to transfers that need to happen on submit node, and the second Collection
     *     referring to transfers that need to happen on staging site
     */
    public Collection<FileTransfer>[] constructFileTX(
            Job job, Collection<PegasusFile> searchFiles) {
        Collection<FileTransfer>[] result = new Collection[2];
        Collection<FileTransfer> localFileTransfers = new LinkedList();
        Collection<FileTransfer> remoteFileTransfers = new LinkedList();
        result[0] = localFileTransfers; // local transfers
        result[1] = remoteFileTransfers; // remote transfers

        String jobName = job.logicalName;
        String stagingSiteHandle = job.getStagingSiteHandle();
        String executionSiteHandle = job.getSiteHandle();
        // contains the remote_initialdir if specified for the job
        String eRemoteDir = job.vdsNS.getStringValue(Pegasus.REMOTE_INITIALDIR_KEY);

        SiteCatalogEntry stagingSite = mSiteStore.lookup(stagingSiteHandle);
        SiteCatalogEntry executionSite = mSiteStore.lookup(executionSiteHandle);
        // we are using the pull mode for data transfer
        String scheme = "file";
        String containerLFN = null;
        if (job.getContainer() != null) {
            containerLFN = job.getContainer().getLFN();
        }

        // sAbsPath would be just the source directory absolute path
        // dAbsPath would be just the destination directory absolute path

        // sDirURL would be the url to the source directory.
        // dDirPutURL would be the url to the destination directoy
        // and is always a networked url.

        boolean symlinkingEnabledForJob = symlinkingEnabled(job, this.mUseSymLinks);

        for (Iterator it = searchFiles.iterator(); it.hasNext(); ) {
            String sourceURL = null, destPutURL = null, destGetURL = null;
            PegasusFile pf = (PegasusFile) it.next();
            List pfns = null;
            ReplicaLocation rl = null;

            String lfn = pf.getLFN();
            NameValue<String, String> nv = null;

            // PM-833 figure out the addOn component just once per lfn
            File addOn = mStagingMapper.mapToRelativeDirectory(job, stagingSite, lfn);

            destPutURL =
                    this.getURLOnSharedScratch(
                            stagingSite, job, FileServerType.OPERATION.put, addOn, lfn);
            destGetURL =
                    this.getURLOnSharedScratch(
                            stagingSite, job, FileServerType.OPERATION.get, addOn, lfn);
            String sDirURL = null;
            String sAbsPath = null;
            String dAbsPath =
                    mSiteStore.getInternalWorkDirectory(stagingSiteHandle, eRemoteDir)
                            + File.separator
                            + addOn;

            // file dest dir is destination dir accessed as a file URL
            String fileDestDir = scheme + "://" + dAbsPath;

            // check if the execution pool is third party or not
            boolean runTransferOnLocalSite =
                    mTransferJobPlacer.runTransferOnLocalSite(
                            stagingSite, destPutURL, Job.STAGE_IN_JOB);
            String destDir =
                    (runTransferOnLocalSite)
                            ?
                            // use the full networked url to the directory
                            destPutURL
                            :
                            // use the default pull mode
                            fileDestDir;

            // see if the pf is infact an instance of FileTransfer
            if (pf instanceof FileTransfer) {
                // that means we should be having the source url already.
                // nv contains both the source pool and the url.
                // PM-1213 remote the source URL. will be added later back
                nv = ((FileTransfer) pf).removeSourceURL();

                NameValue<String, String> destNV = ((FileTransfer) pf).removeDestURL();

                // PM-833 we have to explicity set the remote executable
                // especially for the staging of executables in sharedfs
                if (lfn.equalsIgnoreCase(job.getStagedExecutableBaseName())) {
                    job.setRemoteExecutable(dAbsPath + File.separator + lfn);
                }

                destPutURL =
                        (mTransferJobPlacer.runTransferOnLocalSite(
                                        stagingSite, destPutURL, Job.STAGE_IN_JOB))
                                ?
                                // the destination URL is already third party
                                // enabled. use as it is
                                destPutURL
                                :
                                // explicitly convert to file URL scheme
                                scheme + "://" + new PegasusURL(destPutURL).getPath();

                // for time being for this case the get url is same as put url
                destGetURL = destPutURL;
            } else {
                // query the replica services and get hold of pfn
                rl = mRCBridge.getFileLocs(lfn);
                pfns = (rl == null) ? null : rl.getPFNList();
            }

            if (pfns == null && nv == null) {
                // check to see if the input file is optional
                if (pf.fileOptional()) {
                    // no need to add a transfer node for it if no location found

                    // remove the PegasusFile object from the list of
                    // input files for the job, only if file is not a checkpoint file
                    if (!pf.isCheckpointFile()) {
                        job.getInputFiles().remove(pf);
                    }

                    continue;
                }

                // flag an error. this is when we don't get any replica location
                // from any source
                throw new RuntimeException(
                        "TransferEngine.java: Can't determine a location to "
                                + "transfer input file for lfn "
                                + lfn
                                + " for job "
                                + job.getName());
            }

            FileTransfer ft =
                    (pf instanceof FileTransfer)
                            ? (FileTransfer) pf
                            : new FileTransfer(lfn, jobName, pf.getFlags());

            // make sure the type information is set in file transfer
            ft.setType(pf.getType());
            ft.setSize(pf.getSize());

            // the transfer mode for the file needs to be
            // propogated for optional transfers.
            ft.setTransferFlag(pf.getTransferFlag());

            ReplicaLocation candidateLocations = null;
            if (nv != null) {
                // we have the replica already selected as a result
                // of executable staging
                List rces = new LinkedList();
                rces.add(new ReplicaCatalogEntry(nv.getValue(), nv.getKey()));
                rl = new ReplicaLocation(lfn, rces);
            }

            // PM-1190 add any retrieved metadata from the replica catalog
            // to the associated PegasusFile that is associated with the compute jobs
            pf.addMetadata(rl.getAllMetadata());

            // PM-1250 if no checksum exists then set pegasus-transfer
            // to generate checksum. Later on a dial might be required here
            if (this.mDoIntegrityChecking && !pf.hasRCCheckSum()) {
                ft.setChecksumComputedInWF(true);
                pf.setChecksumComputedInWF(true);
            }

            // PM-1190 associate metadata with the FileTransfer
            ft.setMetadata(pf.getAllMetadata());

            // select from the various replicas
            candidateLocations =
                    mReplicaSelector.selectAndOrderReplicas(
                            rl, executionSiteHandle, runTransferOnLocalSite);
            if (candidateLocations.getPFNCount() == 0) {
                complainForNoCandidateInput(rl, executionSiteHandle, runTransferOnLocalSite);
            }

            // check if we need to replace url prefix for
            // symbolic linking
            boolean symLinkSelectedLocation = false;
            boolean bypassFirstLevelStagingPossible =
                    false; // PM-1327 tracks whether one of candidate locations can trigger bypass
            // for that file
            int candidateNum = 0;
            // PM-1082 we want to select only one destination put URL
            // with preference for symlinks
            // assign to destPutURL to take care of executable staging
            String preferredDestPutURL = destPutURL;
            for (ReplicaCatalogEntry selLoc : candidateLocations.getPFNList()) {
                candidateNum++;
                boolean bypassFirstLevelStagingForCandidateLocation = false;
                if (symLinkSelectedLocation =
                        (symlinkingEnabledForJob
                                && selLoc.getResourceHandle().equals(job.getStagingSiteHandle())
                                && !pf.isExecutable() // PM-1086 symlink only data files as chmod
                        // fails on symlinked file
                        )) {

                    // resolve any srm url's that are specified
                    selLoc = replaceSourceProtocolFromURL(selLoc);
                }

                if (symLinkSelectedLocation) {
                    // PM-1197 we can symlink only if no container is associated with the job
                    // or the file in question is the container file itself.
                    if (!(containerLFN == null || containerLFN.equals(lfn))) {
                        symLinkSelectedLocation = false;
                    }
                }

                if (symLinkSelectedLocation) {
                    // PM-1375 for symlink files check if integrity checking should
                    // be turned off. So make sure we don't trigger computing of checksums
                    // for this file
                    if (mIntegrityDial == PegasusProperties.INTEGRITY_DIAL.nosymlink) {
                        ft.setForIntegrityChecking(false);
                        pf.setForIntegrityChecking(false);
                        ft.setChecksumComputedInWF(false);
                        pf.setChecksumComputedInWF(false);
                    }
                }

                // get the file to the job's execution pool
                // this is assuming that there are no directory paths
                // in the pfn!!!
                sDirURL = selLoc.getPFN().substring(0, selLoc.getPFN().lastIndexOf(File.separator));

                // try to get the directory absolute path
                // yes i know that we sending the url to directory
                // not the file.
                sAbsPath = new PegasusURL(sDirURL).getPath();

                // the final source and destination url's to the file
                sourceURL = selLoc.getPFN();

                if (destPutURL == null
                        || symLinkSelectedLocation) { // PM-1082 if a destination has to be
                    // symlinked always recompute

                    if (symLinkSelectedLocation) {
                        // we use the file URL location to dest dir
                        // in case we are symlinking
                        // destPFN.append( fileDestDir );
                        destPutURL = this.replaceProtocolFromURL(destPutURL);
                    }
                    // ensures symlinked location gets picked up
                    preferredDestPutURL = destPutURL;
                }

                // we have all the chopped up combos of the urls.
                // do some funky matching on the basis of the fact
                // that each pool has one shared filesystem

                // match the source and dest 3rd party urls or
                // match the directory url knowing that lfn and
                // (source and dest pool) are same
                try {
                    // PM-833if(sourceURL.equalsIgnoreCase(dDirPutURL + File.separator + lfn)||
                    if (sourceURL.equalsIgnoreCase(destPutURL)
                            || (selLoc.getResourceHandle().equalsIgnoreCase(stagingSiteHandle)
                                    && lfn.equals(
                                            sourceURL.substring(
                                                    sourceURL.lastIndexOf(File.separator) + 1))
                                    &&
                                    // sAbsPath.equals( dAbsPath )
                                    new File(sAbsPath)
                                            .getCanonicalPath()
                                            .equals(new File(dAbsPath).getCanonicalPath()))) {
                        // do not need to add any transfer node
                        StringBuffer message = new StringBuffer();

                        message.append(sAbsPath).append(" same as ").append(dAbsPath);
                        mLogger.log(message.toString(), LogManager.DEBUG_MESSAGE_LEVEL);
                        message = new StringBuffer();
                        message.append(" Not transferring ip file as ")
                                .append(lfn)
                                .append(" for job ")
                                .append(job.jobName)
                                .append(" to site ")
                                .append(stagingSiteHandle);

                        mLogger.log(message.toString(), LogManager.DEBUG_MESSAGE_LEVEL);
                        continue;
                    }
                } catch (IOException ioe) {
                    /*ignore */
                }

                // add locations of input data on the remote site to the transient RC
                bypassFirstLevelStagingForCandidateLocation =
                        this.bypassStaging(selLoc, pf, job, executionSite);
                if (bypassFirstLevelStagingForCandidateLocation) {
                    // PM-1250 if no checksum exists in RC
                    // then make sure checksum computation is set to false
                    // for bypassed inputs we have no way to compute
                    // checksums in the workflow
                    if (!pf.hasRCCheckSum()) {
                        ft.setChecksumComputedInWF(false);
                        pf.setChecksumComputedInWF(false);
                    }

                    // only the files for which we bypass first level staging , we
                    // store them in the planner cache as a GET URL and associate with the compute
                    // site
                    // PM-698 . we have to clone since original site attribute will be different
                    ReplicaCatalogEntry rce = (ReplicaCatalogEntry) selLoc.clone();
                    rce.setResourceHandle(executionSiteHandle);
                    trackInPlannerCache(lfn, rce, FileServerType.OPERATION.get);

                    if (candidateNum == 1) {
                        // PM-1014 we only track the first candidate in the workflow cache
                        // i.e the cache file written out in the submit directory
                        trackInWorkflowCache(lfn, sourceURL, selLoc.getResourceHandle());
                    }
                    // ensure the input file does not get cleaned up by the
                    // InPlace cleanup algorithm
                    pf.setForCleanup(false);
                    bypassFirstLevelStagingPossible = true;
                    continue;
                } else {
                    // track the location where the data is staged as
                    // part of the first level staging
                    // we always store the thirdparty url
                    // trackInCaches( lfn, destPutURL, job.getSiteHandle() );
                    trackInPlannerCache(lfn, destPutURL, job.getStagingSiteHandle());

                    if (candidateNum == 1) {
                        // PM-1014 we only track the first candidate in the workflow cache
                        // i.e the cache file written out in the submit directory

                        trackInWorkflowCache(lfn, destGetURL, job.getStagingSiteHandle());
                    }
                }

                // PM-1014 we want to track all candidate locations
                ft.addSource(selLoc);
            } // end of traversal of all candidate locations

            // PM-1082 we want to add only one destination URL
            // with preference for symlink destination URL
            if (preferredDestPutURL == null) {
                throw new RuntimeException(
                        "Unable to determine a destination put URL on staging site "
                                + stagingSiteHandle
                                + " for file "
                                + lfn
                                + " for job "
                                + job.getID());
            } else {
                ft.addDestination(stagingSiteHandle, preferredDestPutURL);
            }

            // PM-1386 explicitly now set per file level the bypass flag
            // whether a file is set for bypass staging or not
            pf.setForBypassStaging(bypassFirstLevelStagingPossible);

            if (!bypassFirstLevelStagingPossible) {
                // no bypass of input file staging. we need to add
                // data stage in nodes for the lfn
                if (symLinkSelectedLocation
                        || // symlinks can run only on staging site
                        !runTransferOnLocalSite
                        || runTransferRemotely(
                                job,
                                stagingSite,
                                ft)) { // check on the basis of constructed source URL whether to
                    // run remotely

                    if (removeFileURLFromSource(job, ft, stagingSiteHandle)) {
                        // PM-1082 remote transfers ft can still have file url's
                        // not matching the staging site
                        // sanity check
                        if (ft.getSourceURLCount() == 0) {
                            throw new RuntimeException(
                                    "No source URL's available for stage-in( remote ) transfers for file "
                                            + ft
                                            + " for job "
                                            + job.getID());
                        }
                    }
                    // all symlink transfers and user specified remote transfers
                    remoteFileTransfers.add(ft);
                } else {
                    localFileTransfers.add(ft);
                }
            }

            // we need to set destPutURL to null
            destPutURL = null;
        }

        return result;
    }

    /**
     * Returns a boolean indicating whether to bypass first level staging for a file or not
     *
     * @param entry a ReplicaCatalogEntry matching the selected replica location.
     * @param file the corresponding Pegasus File object
     * @param job the associated job
     * @param computeSiteEntry the compute site
     * @return boolean indicating whether we need to enable bypass or not
     */
    public boolean bypassStaging(
            ReplicaCatalogEntry entry,
            PegasusFile file,
            Job job,
            SiteCatalogEntry computeSiteEntry) {
        boolean bypass = false;
        String computeSite = job.getSiteHandle();
        // check if user has it configured for bypassing the staging or user has bypass flag set
        // and we are in pegasus lite mode
        if ((this.mBypassStagingForInputs || file.doBypassStaging())
                && mPegasusConfiguration.jobSetupForWorkerNodeExecution(job)) {
            boolean isFileURL = entry.getPFN().startsWith(PegasusURL.FILE_URL_SCHEME);
            String fileSite = entry.getResourceHandle();

            if (mPegasusConfiguration.jobSetupForCondorIO(job, mProps)) {
                // additional check for condor io
                // we need to inspect the URL and it's location
                // only file urls for input files are eligible for bypass
                if (isFileURL) {
                    if (fileSite.equals("local")) {
                        bypass = true;
                    } else if (fileSite.equals(computeSiteEntry.getSiteHandle())) {
                        // PM-1783 allow for compute site file URLs to be bypassed
                        // if the compute site is visible to the submit host
                        bypass = computeSiteEntry.isVisibleToLocalSite() || true;
                    }
                }
            } else {
                // Non Shared FS case: we can bypass all url's safely
                // other than file urls
                if (isFileURL) {
                    // PM-1783 for file url's bypass staging can be triggered only
                    // if file site is same as the compute site OR
                    // auxillary.local  is set to true for the compute site and file site is local
                    bypass = fileSite.equalsIgnoreCase(computeSite);
                    if (!bypass) {
                        // check for auxillary.local for the compute site only if a file
                        // URL is for local site
                        if (fileSite.equalsIgnoreCase("local")) {
                            bypass = computeSiteEntry.isVisibleToLocalSite();
                        }
                    }
                } else {
                    // for non shared fs case
                    bypass = true;
                }
            }
        }

        return bypass;
    }

    /**
     * Determines a particular created transfer pair has to be binned for remote transfer or local.
     *
     * @param job the associated compute job
     * @param ft the file transfer created
     * @param stagingSite the staging site for the job
     * @return
     */
    private boolean runTransferRemotely(Job job, SiteCatalogEntry stagingSite, FileTransfer ft) {
        boolean remote = false;

        NameValue<String, String> destTX = ft.getDestURL();
        for (String sourceSite : ft.getSourceSites()) {
            // traverse through all the URL's on that site
            for (ReplicaCatalogEntry rce : ft.getSourceURLs(sourceSite)) {
                String sourceURL = rce.getPFN();
                // if the source URL is a FILE URL and
                // source site matches the destination site
                // then has to run remotely
                if (sourceURL != null && sourceURL.startsWith(PegasusURL.FILE_URL_SCHEME)) {
                    // sanity check to make sure source site
                    // matches destination site
                    if (sourceSite.equalsIgnoreCase(destTX.getKey())) {

                        if (sourceSite.equalsIgnoreCase(stagingSite.getSiteHandle())
                                && stagingSite.isVisibleToLocalSite()) {
                            // PM-1024 if the source also matches the job staging site
                            // then we do an extra check if the staging site is the same
                            // as the sourceSite, then we consider the auxillary.local attribute
                            // for the staging site
                            remote = false;
                        } else {
                            remote = true;
                            break;
                        }
                    } else if (sourceSite.equals("local")) {
                        remote = false;
                    }
                }
            }
        }
        return remote;
    }

    /**
     * Inserts an entry into the workflow cache that is to be written out to the submit directory.
     *
     * @param lfn the logical name of the file.
     * @param pfn the pfn
     * @param site the site handle
     */
    private void trackInWorkflowCache(String lfn, String pfn, String site) {

        mWorkflowCache.insert(lfn, pfn, site);
    }

    /**
     * Removes file URL's from FT sources that if the site attribute for it does not match site
     * handle passed
     *
     * @param job
     * @param ft
     * @param site
     */
    public boolean removeFileURLFromSource(Job job, FileTransfer ft, String site) {

        boolean remove = false;
        for (String sourceSite : ft.getSourceSites()) {
            // traverse through all the URL's on that site
            for (Iterator<ReplicaCatalogEntry> it = ft.getSourceURLs(sourceSite).iterator();
                    it.hasNext(); ) {
                ReplicaCatalogEntry rce = it.next();
                String sourceURL = rce.getPFN();
                // if the source URL is a FILE URL and
                // source site matches the destination site
                // then has to run remotely
                if (sourceURL != null && sourceURL.startsWith(PegasusURL.FILE_URL_SCHEME)) {

                    if (!sourceSite.equalsIgnoreCase(site)) {
                        // source site associated with file URL does
                        // not match the site attribute. remove the source url
                        mLogger.log(
                                "Removing source url "
                                        + sourceURL
                                        + " associated with site "
                                        + sourceSite
                                        + " for job "
                                        + job.getID(),
                                LogManager.TRACE_MESSAGE_LEVEL);
                        it.remove();
                        remove = true;
                    }
                }
            }
        }
        return remove;
    }

    /**
     * Replaces the SRM URL scheme from the url, and replaces it with the file url scheme and
     * returns in a new object if replacement happens. The original object passed as a parameter
     * still remains the same.
     *
     * @param rce the <code>ReplicaCatalogEntry</code> object whose url need to be replaced.
     * @return the object with the url replaced.
     */
    protected ReplicaCatalogEntry replaceSourceProtocolFromURL(ReplicaCatalogEntry rce) {
        String pfn = rce.getPFN();

        // if the pfn starts with a file url we
        // dont need to replace . a sanity check
        if (pfn.startsWith(PegasusURL.FILE_URL_SCHEME)) {
            return rce;
        }

        /* special handling for SRM urls */
        StringBuffer newPFN = new StringBuffer();
        if (mSRMServiceURLToMountPointMap.containsKey(rce.getResourceHandle())) {
            // try to do replacement of URL with internal mount point
            NameValue<String, String> nv =
                    mSRMServiceURLToMountPointMap.get(rce.getResourceHandle());
            String urlPrefix = nv.getKey();
            if (pfn.startsWith(urlPrefix)) {
                // replace the starting with the mount point
                newPFN.append(PegasusURL.FILE_URL_SCHEME).append("//");
                newPFN.append(nv.getValue());
                newPFN.append(pfn.substring(urlPrefix.length(), pfn.length()));
                mLogger.log(
                        "Replaced pfn " + pfn + " with " + newPFN.toString(),
                        LogManager.TRACE_MESSAGE_LEVEL);
            }
        }
        if (newPFN.length() == 0) {
            // there is no SRM Replacement to do
            // Still do the FILE replacement
            // return the original object

            // we have to the manual replacement
            /*
                        String hostName = Utility.getHostName( pfn );
                        newPFN.append( FILE_URL_SCHEME ).append( "//" );
                        //we want to skip out the hostname
                        newPFN.append( pfn.substring( pfn.indexOf( hostName ) + hostName.length() ) );
            */

            newPFN.append(PegasusURL.FILE_URL_SCHEME).append("//");
            newPFN.append(new PegasusURL(pfn).getPath());
        }

        // we do not need a full clone, just the PFN
        ReplicaCatalogEntry result =
                new ReplicaCatalogEntry(newPFN.toString(), rce.getResourceHandle());

        for (Iterator it = rce.getAttributeIterator(); it.hasNext(); ) {
            String key = (String) it.next();
            result.addAttribute(key, rce.getAttribute(key));
        }

        return result;
    }

    /**
     * Constructs a Properties objects by parsing the relevant SRM pegasus properties.
     *
     * <p>For example, if users have the following specified in properties file
     *
     * <pre>
     * pegasus.transfer.srm.ligo-cit.service.url          srm://osg-se.ligo.caltech.edu:10443/srm/v2/server?SFN=/mnt/hadoop
     * pegasus.transfer.srm.ligo-cit.service.mountpoint   /mnt/hadoop
     * </pre>
     *
     * then, a Map is create the associates ligo-cit with NameValue object containing the service
     * url and mount point ( ).
     *
     * @param props the <code>PegasusProperties</code> object
     * @return Map that maps a site name to a NameValue object that has the URL prefix and the mount
     *     point
     */
    private Map<String, NameValue> constructSiteToSRMServerMap(PegasusProperties props) {
        Map<String, NameValue> m = new HashMap();

        // first strip of prefix from properties and get matching subset
        Properties siteProps = props.matchingSubset(StageIn.SRM_PROPERTIES_PREFIX, false);

        // retrieve all the sites for which SRM servers are specified
        Map<String, String> m1 = new HashMap(); // associates site name to url prefix
        Map<String, String> m2 = new HashMap(); // associates site name to mount point
        for (Iterator it = siteProps.keySet().iterator(); it.hasNext(); ) {
            String key = (String) it.next();
            // determine the site name
            String site = key.substring(0, key.indexOf("."));

            if (key.endsWith(StageIn.SRM_SERVICE_URL_PROPERTIES_SUFFIX)) {
                m1.put(site, siteProps.getProperty(key));
            } else if (key.endsWith(StageIn.SRM_MOUNT_POINT_PROPERTIES_SUFFIX)) {
                m2.put(site, siteProps.getProperty(key));
            }
        }

        // now merge the information into m and return
        for (Iterator<Map.Entry<String, String>> it = m1.entrySet().iterator(); it.hasNext(); ) {
            Map.Entry<String, String> entry = it.next();
            String site = entry.getKey();
            String url = entry.getValue();
            String mountPoint = m2.get(site);

            if (mountPoint == null) {
                mLogger.log(
                        "Mount Point for SRM server not specified in properties for site " + site,
                        LogManager.WARNING_MESSAGE_LEVEL);
                continue;
            }

            m.put(site, new NameValue(url, mountPoint));
        }

        mLogger.log("SRM Server map is " + m, LogManager.DEBUG_MESSAGE_LEVEL);

        return m;
    }

    /**
     * Replaces the gsiftp URL scheme from the url, and replaces it with the symlink url scheme and
     * returns in a new object. The original object passed as a parameter still remains the same.
     *
     * @param pfn the pfn that needs to be replaced
     * @return the replaced PFN
     */
    protected String replaceProtocolFromURL(String pfn) {
        /* special handling for SRM urls */
        StringBuffer newPFN = new StringBuffer();

        if (pfn.startsWith(PegasusURL.FILE_URL_SCHEME)) {
            // special handling for FILE URL's as
            // utility hostname functions dont hold up
            newPFN.append(PegasusURL.SYMLINK_URL_SCHEME)
                    .append(pfn.substring(PegasusURL.FILE_URL_SCHEME.length()));

            // System.out.println( "Original PFN " + pfn + " \nReplaced PFN " + newPFN.toString() );
            return newPFN.toString();
        }

        newPFN.append(PegasusURL.SYMLINK_URL_SCHEME).append("//");

        // we want to skip out the hostname
        newPFN.append(new PegasusURL(pfn).getPath());

        return newPFN.toString();
    }

    /**
     * Helped method for logging removal message. If removed is true, then logged on debug else
     * logged as warning.
     *
     * @param job the job
     * @param file the file to be removed
     * @param prefix prefix for log message
     * @param removed whether removal was successful or not.
     */
    private void logRemoval(Job job, PegasusFile file, String prefix, boolean removed) {
        StringBuilder message = new StringBuilder();
        message.append(prefix).append(" : ");
        if (removed) {
            message.append("Removed file ")
                    .append(file.getLFN())
                    .append(" for job ")
                    .append(job.getID());

            mLogger.log(message.toString(), LogManager.DEBUG_MESSAGE_LEVEL);
        } else {
            // warn
            message.append("Unable to remove file ")
                    .append(file.getLFN())
                    .append(" for job ")
                    .append(job.getID());

            mLogger.log(message.toString(), LogManager.WARNING_MESSAGE_LEVEL);
        }
    }

    /**
     * Throws an exception with a detailed message as to why replica selection failed
     *
     * @param rl
     * @param destinationSite
     * @param runTransferOnLocalSite
     * @throws RuntimeException
     */
    private void complainForNoCandidateInput(
            ReplicaLocation rl, String destinationSite, boolean runTransferOnLocalSite)
            throws RuntimeException {
        StringBuilder error = new StringBuilder();
        error.append(
                        "Unable to select a Physical Filename (PFN) for file with logical filename (LFN) as ")
                .append(rl.getLFN())
                .append(" for transfer to destination site (")
                .append(destinationSite)
                .append("). runTransferOnLocalSite:")
                .append(runTransferOnLocalSite)
                .append(" amongst ")
                .append(rl.getPFNList());

        // PM-1248 traverse through to check if any file URL's available
        Collection<ReplicaCatalogEntry> localSiteFileRCEs = new LinkedList();
        for (Iterator it = rl.pfnIterator(); it.hasNext(); ) {
            ReplicaCatalogEntry rce = (ReplicaCatalogEntry) it.next();
            String site = rce.getResourceHandle();
            // check if equal to the execution pool
            if (site != null && site.equals("local")) {
                if (rce.getPFN().startsWith(PegasusURL.FILE_URL_SCHEME)) {
                    localSiteFileRCEs.add(rce);
                }
            }
        }
        if (!localSiteFileRCEs.isEmpty()) {
            error.append("\n")
                    .append("If any of the following file URLs are also accessible on the site (")
                    .append(destinationSite)
                    .append(") consider setting the pegasus profile ")
                    .append("\"")
                    .append(Pegasus.LOCAL_VISIBLE_KEY)
                    .append("\"")
                    .append(" to true in the site catalog for site: ")
                    .append(destinationSite)
                    .append("\n")
                    .append(localSiteFileRCEs);
        }
        throw new RuntimeException(error.toString());
    }

    /**
     * Inserts an entry into the planner cache as a put URL.
     *
     * @param lfn the logical name of the file.
     * @param pfn the pfn
     * @param site the site handle
     */
    private void trackInPlannerCache(String lfn, String pfn, String site) {

        trackInPlannerCache(lfn, pfn, site, FileServerType.OPERATION.put);
    }

    /**
     * Inserts an entry into the planner cache as a put URL.
     *
     * @param lfn the logical name of the file.
     * @param rce replica catalog entry
     * @param type the type of url
     */
    private void trackInPlannerCache(
            String lfn, ReplicaCatalogEntry rce, FileServerType.OPERATION type) {

        mPlannerCache.insert(lfn, rce, type);
    }

    /**
     * Inserts an entry into the planner cache as a put URL.
     *
     * @param lfn the logical name of the file.
     * @param pfn the pfn
     * @param site the site handle
     * @param type the type of url
     */
    private void trackInPlannerCache(
            String lfn, String pfn, String site, FileServerType.OPERATION type) {

        mPlannerCache.insert(lfn, pfn, site, type);
    }

    /**
     * A convenience method that indicates whether to enable symlinking for a job or not
     *
     * @param job the job for which symlinking needs to be enabled
     * @param workflowSymlinking whether the user has turned on symlinking for workflow or not
     * @return
     */
    protected boolean symlinkingEnabled(Job job, boolean workflowSymlinking) {
        if (!workflowSymlinking) {
            // user does not have symlinking enabled for the workflow
            return false;
        }

        // the profile value can turn symlinking off
        return !job.vdsNS.getBooleanValue(Pegasus.NO_SYMLINK_KEY);
    }
}
