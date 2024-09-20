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

import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.common.util.FactoryException;
import edu.isi.pegasus.planner.catalog.ReplicaCatalog;
import edu.isi.pegasus.planner.catalog.replica.ReplicaFactory;
import edu.isi.pegasus.planner.catalog.site.classes.Directory;
import edu.isi.pegasus.planner.catalog.site.classes.FileServer;
import edu.isi.pegasus.planner.catalog.site.classes.FileServerType.OPERATION;
import edu.isi.pegasus.planner.catalog.site.classes.SiteCatalogEntry;
import edu.isi.pegasus.planner.classes.ADag;
import edu.isi.pegasus.planner.classes.DAGJob;
import edu.isi.pegasus.planner.classes.DAXJob;
import edu.isi.pegasus.planner.classes.FileTransfer;
import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.classes.PegasusFile;
import edu.isi.pegasus.planner.classes.PlannerCache;
import edu.isi.pegasus.planner.common.PegasusConfiguration;
import edu.isi.pegasus.planner.common.PegasusProperties;
import edu.isi.pegasus.planner.mapper.StagingMapper;
import edu.isi.pegasus.planner.mapper.StagingMapperFactory;
import edu.isi.pegasus.planner.mapper.SubmitMapperFactory;
import edu.isi.pegasus.planner.namespace.Pegasus;
import edu.isi.pegasus.planner.partitioner.graph.Graph;
import edu.isi.pegasus.planner.partitioner.graph.GraphNode;
import edu.isi.pegasus.planner.selector.ReplicaSelector;
import edu.isi.pegasus.planner.selector.replica.ReplicaSelectorFactory;
import edu.isi.pegasus.planner.transfer.JobPlacer;
import edu.isi.pegasus.planner.transfer.Refiner;
import edu.isi.pegasus.planner.transfer.generator.StageIn;
import edu.isi.pegasus.planner.transfer.generator.StageOut;
import edu.isi.pegasus.planner.transfer.refiner.RefinerFactory;
import java.io.File;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

/**
 * The transfer engine, which on the basis of the pools on which the jobs are to run, adds nodes to
 * transfer the data products.
 *
 * @author Karan Vahi
 * @author Gaurang Mehta
 * @version $Revision$
 */
public class TransferEngine extends Engine {

    /**
     * The MAX level is assigned as the level for deleted jobs. We can put it to Integer.MAX_VALUE,
     * but it is rare that number of levels in a workflows exceed 1000.
     */
    public static final int DELETED_JOBS_LEVEL = 1000;

    /**
     * The name of the Replica Catalog Implementer that is used to write out the workflow cache file
     * in the submit directory.
     */
    public static final String WORKFLOW_CACHE_FILE_IMPLEMENTOR = "FlushedCache";

    /** The name of the source key for Replica Catalog Implementer that serves as cache */
    public static final String WORKFLOW_CACHE_REPLICA_CATALOG_KEY = "file";

    /** The name of the refiner for purposes of error logging */
    public static final String REFINER_NAME = "TranferEngine";

    /**
     * The DAG object to which the transfer nodes are to be added. This is the reduced Dag, which is
     * got from the Reduction Engine.
     */
    private ADag mDag;

    /** The bridge to the Replica Catalog. */
    private ReplicaCatalogBridge mRCBridge;

    /** The handle to the replica selector that is to used to select the various replicas. */
    private ReplicaSelector mReplicaSelector;

    /** The handle to the transfer refiner that adds the transfer nodes into the workflow. */
    private Refiner mTXRefiner;

    /** Holds all the jobs deleted by the reduction algorithm. */
    private List<Job> mDeletedJobs;

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

    /**
     * Handle to an Staging Mapper that tells where to place the files on the shared scratch space
     * on the staging site.
     */
    private StagingMapper mStagingMapper;

    /**
     * The working directory relative to the mount point of the execution pool. It is populated from
     * the pegasus.dir.exec property from the properties file. If not specified then it work_dir is
     * supposed to be the exec mount point of the execution pool.
     */
    protected String mWorkDir;

    /** A boolean to track whether condor file io is used for the workflow or not. */
    // private final boolean mSetupForCondorIO;
    protected PegasusConfiguration mPegasusConfiguration;

    /** The output site where files need to be staged to. */
    protected final Set<String> mOutputSites;

    /** The dial for integrity checking */
    protected PegasusProperties.INTEGRITY_DIAL mIntegrityDial;

    /** Whether to do any integrity checking or not. */
    protected boolean mDoIntegrityChecking;

    /**
     * Handle to the placer that determines whether a file transfer needs to be handled locally or
     * remotely on the staging site.
     */
    protected JobPlacer mTransferJobPlacer;

    /**
     * The stage-out generator that generates the File Transfer pairs required to place outputs of a
     * job to the various user defined locations
     */
    protected StageIn mStageInFileTransferGenerator;

    /**
     * The stage-out generator that generates the File Transfer pairs required to place outputs of a
     * job to the various user defined locations
     */
    protected StageOut mStageOutFileTransferGenerator;

    /**
     * Overloaded constructor.
     *
     * @param reducedDag the reduced workflow.
     * @param bag bag of initialization objects
     * @param deletedJobs list of all jobs deleted by reduction algorithm.
     * @param deletedLeafJobs list of deleted leaf jobs by reduction algorithm.
     */
    public TransferEngine(
            ADag reducedDag, PegasusBag bag, List<Job> deletedJobs, List<Job> deletedLeafJobs) {
        super(bag);

        mSubmitDirMapper =
                SubmitMapperFactory.loadInstance(bag, new File(mPOptions.getSubmitDirectory()));
        bag.add(PegasusBag.PEGASUS_SUBMIT_MAPPER, mSubmitDirMapper);

        mStagingMapper = StagingMapperFactory.loadInstance(bag);
        bag.add(PegasusBag.PEGASUS_STAGING_MAPPER, mStagingMapper);

        // PM-1375 we check if we need to do any integriy checking or not
        mIntegrityDial = mProps.getIntegrityDial();
        mDoIntegrityChecking = mProps.doIntegrityChecking();

        mDag = reducedDag;
        mDeletedJobs = deletedJobs;

        mPegasusConfiguration = new PegasusConfiguration(bag.getLogger());

        try {
            mTXRefiner = RefinerFactory.loadInstance(reducedDag, bag);
            mTransferJobPlacer = new JobPlacer(mTXRefiner);
            mReplicaSelector = ReplicaSelectorFactory.loadInstance(mProps);
        } catch (Exception e) {
            // wrap all the exceptions into a factory exception
            throw new FactoryException("Transfer Engine ", e);
        }

        mOutputSites = (Set<String>) mPOptions.getOutputSites();
        mStageOutFileTransferGenerator = new StageOut();
        mStageOutFileTransferGenerator.initalize(reducedDag, bag, mTXRefiner);

        mWorkflowCache = this.initializeWorkflowCacheFile(reducedDag);

        // log some configuration messages
        mLogger.log(
                "Transfer Refiner loaded is           [" + mTXRefiner.getDescription() + "]",
                LogManager.CONFIG_MESSAGE_LEVEL);
        mLogger.log(
                "ReplicaSelector loaded is            [" + mReplicaSelector.description() + "]",
                LogManager.CONFIG_MESSAGE_LEVEL);
        mLogger.log(
                "Submit Directory Mapper loaded is    [" + mSubmitDirMapper.description() + "]",
                LogManager.CONFIG_MESSAGE_LEVEL);
        mLogger.log(
                "Staging Mapper loaded is             [" + mStagingMapper.description() + "]",
                LogManager.CONFIG_MESSAGE_LEVEL);
    }

    /**
     * Adds the transfer nodes to the workflow.
     *
     * @param rcb the bridge to the ReplicaCatalog.
     * @param plannerCache an instance of the replica catalog that will store the locations of the
     *     files on the remote sites.
     */
    public void addTransferNodes(ReplicaCatalogBridge rcb, PlannerCache plannerCache) {
        mRCBridge = rcb;
        mRCBridge.mSubmitDirMapper = this.mSubmitDirMapper;
        mPlannerCache = plannerCache;

        mStageInFileTransferGenerator = new StageIn();
        mStageInFileTransferGenerator.initalize(
                mDag, mBag, mTXRefiner, mRCBridge, mReplicaSelector, mPlannerCache, mWorkflowCache);

        Job currentJob;
        String currentJobName;
        String msg;

        // convert the dax to a graph representation and walk it
        // in a top down manner
        // PM-747 no need for conversion as ADag now implements Graph interface
        Graph workflow = mDag;
        boolean stageOut = this.mPOptions.doStageOut();

        for (Iterator it = workflow.iterator(); it.hasNext(); ) {
            GraphNode node = (GraphNode) it.next();
            currentJob = (Job) node.getContent();

            // PM-833 associate a directory with the job
            // that is used to determine relative submit directory
            currentJob.setRelativeSubmitDirectory(getRelativeSubmitDirectory(currentJob));

            // set the node depth as the level
            currentJob.setLevel(node.getDepth());
            currentJobName = currentJob.getName();

            mLogger.log("", LogManager.DEBUG_MESSAGE_LEVEL);
            msg = "Job being traversed is " + currentJobName;
            mLogger.log(msg, LogManager.DEBUG_MESSAGE_LEVEL);
            msg = "To be run at " + currentJob.executionPool;
            mLogger.log(msg, LogManager.DEBUG_MESSAGE_LEVEL);

            // getting the parents of that node
            Collection<GraphNode> parents = node.getParents();
            mLogger.log("Parents of job:" + node.parentsToString(), LogManager.DEBUG_MESSAGE_LEVEL);
            processParents(currentJob, parents);

            // transfer the nodes output files
            // to the output sites
            if (stageOut) {
                Collection<FileTransfer> localTransfersToOutputSites = new LinkedList();
                Collection<FileTransfer> remoteTransfersToOutputSites = new LinkedList();
                Set<String> outputSites = new HashSet();
                outputSites.addAll(this.mOutputSites);

                if (this.mPOptions.getOutputMap() != null) {
                    // PM-1608 special null site notation to indicate that mapper should return
                    // locations of files without matching on site name
                    outputSites.add(null);
                }
                for (String outputSite : outputSites) {
                    Collection<FileTransfer>[] fileTransfers =
                            mStageOutFileTransferGenerator.constructFileTX(currentJob, outputSite);
                    localTransfersToOutputSites.addAll(fileTransfers[0]);
                    remoteTransfersToOutputSites.addAll(fileTransfers[1]);
                }
                mTXRefiner.addStageOutXFERNodes(
                        currentJob, localTransfersToOutputSites, remoteTransfersToOutputSites, rcb);
            }

            // PM-1765 even if staging outputs, locations for generated
            // output files are tracked in this one function below uniformly

            // create the cache file always
            // Pegasus Bug PM-32 and PM-356
            trackInCaches(currentJob);
        }

        // we are done with the traversal.
        // mTXRefiner.done();

        // get the deleted leaf jobs o/p files to output sites
        if (stageOut && !mDeletedJobs.isEmpty()) {

            mLogger.log(
                    "Adding stage out jobs for jobs deleted from the workflow",
                    LogManager.INFO_MESSAGE_LEVEL);

            for (Iterator it = this.mDeletedJobs.iterator(); it.hasNext(); ) {
                currentJob = (Job) it.next();
                currentJob.setLevel(TransferEngine.DELETED_JOBS_LEVEL);

                // for a deleted node, to transfer it's output
                // the execution pool should be set to local i.e submit host
                currentJob.setSiteHandle("local");
                // PM-936 set the staging site for the deleted job
                // to local site
                currentJob.setStagingSiteHandle("local");

                // for jobs deleted during data reuse we dont
                // go through the staging site. they are transferred
                // directly to the output sites
                Collection<FileTransfer> deletedFileTransfers = new LinkedList();
                for (String outputSite : this.mOutputSites) {
                    deletedFileTransfers.addAll(
                            this.mStageOutFileTransferGenerator.constructDeletedFileTX(
                                    mRCBridge, mReplicaSelector, currentJob, outputSite));
                }
                if (!deletedFileTransfers.isEmpty()) {
                    // the job is deleted anyways. The files exist somewhere
                    // as mentioned in the Replica Catalog. We assume it is
                    // URL remotely accessible
                    boolean localTransfer = true;
                    mTXRefiner.addStageOutXFERNodes(
                            currentJob, deletedFileTransfers, rcb, localTransfer, true);
                }
            }
        }

        // we are done with the traversal.
        mTXRefiner.done();

        // close the handle to the workflow cache file if it is written
        // not the planner cache file
        this.mWorkflowCache.close();
    }

    /**
     * Returns the staging site to be used for a job. If a staging site is not determined from the
     * options it is set to be the execution site for the job
     *
     * @param job the job for which to determine the staging site
     * @return the staging site
     */
    public String getStagingSite(Job job) {
        /*
        String ss =  this.mPOptions.getStagingSite( job.getSiteHandle() );
        return (ss == null) ? job.getSiteHandle(): ss;
        */
        return job.getStagingSiteHandle();
    }

    /**
     * It processes a nodes parents and determines if nodes are to be added or not. All the input
     * files for the job are searched in the output files of the parent nodes and the Replica
     * Mechanism.
     *
     * @param job the <code>Job</code> object containing all the details of the job.
     * @param parents list <code>GraphNode</code> ojbects corresponding to the parent jobs of the
     *     job.
     */
    private void processParents(Job job, Collection<GraphNode> parents) {

        Set nodeIpFiles = job.getInputFiles();
        Vector vRCSearchFiles = new Vector(); // vector of PegasusFile

        // getAll the output Files of the parents
        Set<PegasusFile> parentsOutFiles = getOutputFiles(parents);

        // interpool transfer of the nodes parents
        // output files
        Collection[] interSiteFileTX = this.getInterpoolFileTX(job, parents);
        Collection localInterSiteTX = interSiteFileTX[0];
        Collection remoteInterSiteTX = interSiteFileTX[1];

        // only add if there are files to transfer
        if (!localInterSiteTX.isEmpty()) {
            mTXRefiner.addInterSiteTXNodes(job, localInterSiteTX, true);
        }
        if (!remoteInterSiteTX.isEmpty()) {
            mTXRefiner.addInterSiteTXNodes(job, remoteInterSiteTX, false);
        }

        // check if node ip files are in the parents out files
        // if files are not, then these are to be got
        // from the RC based on the transiency characteristic
        for (Iterator it = nodeIpFiles.iterator(); it.hasNext(); ) {
            PegasusFile pf = (PegasusFile) it.next();
            if (!parentsOutFiles.contains(pf)) {
                // PM-976 all input files that are not generated
                // by parent jobs should be looked up in the replica catalog
                // we don't consider the value of the transfer flag
                vRCSearchFiles.addElement(pf);

                // PM-1250 any file fetched from RC is a raw input file
                pf.setRawInput(true);
            }
        }

        Collection<FileTransfer>[] fileTransfers = new Collection[2];
        fileTransfers[0] = new LinkedList();
        fileTransfers[1] = new LinkedList();

        if (job instanceof DAXJob) {
            // for the DAX jobs we should always call the method
            // as DAX may just be referred as the LFN
            fileTransfers =
                    mStageInFileTransferGenerator.constructFileTX((DAXJob) job, vRCSearchFiles);
        } else if (!vRCSearchFiles.isEmpty()) {
            if (job instanceof DAGJob) {
                fileTransfers =
                        mStageInFileTransferGenerator.constructFileTX((DAGJob) job, vRCSearchFiles);
            } else {
                // get the locations from the RC
                fileTransfers = mStageInFileTransferGenerator.constructFileTX(job, vRCSearchFiles);
            }
        }

        Collection<FileTransfer> localFileTransfersToStagingSite = fileTransfers[0];
        Collection<FileTransfer> remoteFileTransfersToStagingSite = fileTransfers[1];

        // add the stage in transfer nodes if required
        if (!(localFileTransfersToStagingSite.isEmpty()
                && remoteFileTransfersToStagingSite.isEmpty())) {
            mTXRefiner.addStageInXFERNodes(
                    job, localFileTransfersToStagingSite, remoteFileTransfersToStagingSite);
        }
    }

    /**
     * This generates a error message for pool not found in the pool config file.
     *
     * @param poolName the name of pool that is not found.
     * @param universe the condor universe
     * @return the message.
     */
    private String poolNotFoundMsg(String poolName, String universe) {
        String st =
                "Error: No matching entry to pool = "
                        + poolName
                        + " ,universe = "
                        + universe
                        + "\n found in the pool configuration file ";
        return st;
    }

    /**
     * This gets the Vector of FileTransfer objects for all the files which have to be transferred
     * to the destination pool in case of Interpool transfers. Each FileTransfer object has the
     * source and the destination URLs. the source URI is determined from the pool on which the jobs
     * are executed.
     *
     * @param job the job with reference to which interpool file transfers need to be determined.
     * @param parents Collection of <code>GraphNode</code> ojbects corresponding to the parent jobs
     *     of the job.
     * @return array of Collection of <code>FileTransfer</code> objects
     */
    private Collection<FileTransfer>[] getInterpoolFileTX(Job job, Collection<GraphNode> parents) {
        String destSiteHandle = job.getStagingSiteHandle();
        // contains the remote_initialdir if specified for the job
        String destRemoteDir = job.vdsNS.getStringValue(Pegasus.REMOTE_INITIALDIR_KEY);

        SiteCatalogEntry destSite = mSiteStore.lookup(destSiteHandle);
        SiteCatalogEntry sourceSite;

        Collection[] result = new Collection[2];
        Collection<FileTransfer> localTransfers = new LinkedList();
        Collection<FileTransfer> remoteTransfers = new LinkedList();

        ReplicaCatalog jobInputCache =
                (job instanceof DAXJob) ? this.getWorkflowCache((DAXJob) job) : null;

        // tracks whether we can consider short circuit the interpool transfer
        // for a job or not . we cannot shortcircuit for two cases
        //       | Job      | Parent Job | Consider Short Circuit|
        // Case 1| Compute  | Compute    |  Yes                  |
        // Case 2| Compute  | DAX        |  No                   |
        // Case 3| DAX      | Compute    |  No                   |
        // Case 4| DAX      | DAX        |  Yes                  |

        // Case 2 // PM-1676 if parent job is a sub workflow job, then we cannot
        // short circuit the inter pool transfer, as the sub workflow job
        // outputs need to be placed explicitly using the output map for
        // the compute job to pick up
        // Case 3 PM-1766 DAX job has some input files that are produced by a parent compute job
        // Case 4 let the cache file between exchanged with parent dax job provide the info
        boolean considerShortCircuit = job.getJobType() == Job.COMPUTE_JOB;

        // PM-1602 tracks input files for which to disable integrity
        Set<PegasusFile> integrityDisabledFiles = new HashSet();
        for (GraphNode parent : parents) {
            // get the parent job
            Job pJob = (Job) parent.getContent();
            sourceSite = mSiteStore.lookup(pJob.getStagingSiteHandle());

            considerShortCircuit =
                    (considerShortCircuit && pJob.getJobType() == Job.COMPUTE_JOB)
                            ||
                            // both the job and parent as pegasusWorkflow/DAXJobs
                            (job instanceof DAXJob && pJob instanceof DAXJob);

            if (considerShortCircuit
                    && sourceSite.getSiteHandle().equalsIgnoreCase(destSiteHandle)) {
                // no need to add transfers, as the parent job and child
                // job are run in the same directory on the pool
                continue;
            }

            String sourceURI = null;
            for (Iterator fileIt = pJob.getOutputFiles().iterator(); fileIt.hasNext(); ) {
                PegasusFile pf = (PegasusFile) fileIt.next();
                String outFile = pf.getLFN();

                if (job.getInputFiles().contains(pf)) {
                    // PM-833 figure out the addOn component just once per lfn
                    String lfn = pf.getLFN();
                    File addOn = mStagingMapper.mapToRelativeDirectory(job, destSite, lfn);
                    String thirdPartyDestPutURL =
                            this.getURLOnSharedScratch(destSite, job, OPERATION.put, addOn, lfn);

                    // PM-1977 relative dir on the staging site where the parent job ran
                    File parentAddon =
                            mStagingMapper.getRelativeDirectory(pJob.getStagingSiteHandle(), lfn);

                    // definite inconsitency as url prefix and mount point
                    // are not picked up from the same server
                    boolean localTransfer =
                            mTransferJobPlacer.runTransferOnLocalSite(
                                    destSite, thirdPartyDestPutURL, Job.INTER_POOL_JOB);
                    String destURL =
                            localTransfer
                                    ?
                                    // construct for third party transfer
                                    thirdPartyDestPutURL
                                    :
                                    // construct for normal transfer
                                    "file://"
                                            + mSiteStore.getInternalWorkDirectory(
                                                    destSiteHandle, destRemoteDir)
                                            + File.separator
                                            + addOn
                                            + File.separator
                                            + lfn;

                    String sourceURL = null;
                    /* PM-833 String destURL       = destURI + File.separator + outFile;
                    String thirdPartyDestURL = thirdPartyDestPutURI + File.separator +
                                           outFile;
                    */
                    FileTransfer ft = new FileTransfer(outFile, pJob.jobName);
                    ft.setSize(pf.getSize());
                    ft.addDestination(destSiteHandle, destURL);

                    // for intersite transfers we need to track in transient rc
                    // for the cleanup algorithm
                    // only the destination is tracked as source will have been
                    // tracked for the parent jobs
                    trackInPlannerCache(outFile, thirdPartyDestPutURL, destSiteHandle);

                    if (pJob instanceof DAXJob) {
                        // PM-1608 we don't create inter site transfers instead we need
                        // to create an output map for the sub workflow referred to by the dax job
                        // the output map should transfer files to the staging site of the compute
                        // job in question. we log in the output map file for the DAX job
                        mLogger.log(
                                "Parent DAX job "
                                        + pJob.getID()
                                        + " will transfer output file to "
                                        + ft.getDestURL()
                                        + " which is required by "
                                        + job.getID(),
                                LogManager.DEBUG_MESSAGE_LEVEL);
                        ((DAXJob) pJob).addOutputFileLocation(mBag, ft);

                        // PM-1608 explicitly disable integrity checking as we don't
                        // know which job in the sub workflow referred to by the parent DAX job pJob
                        // generates the file. we are adding parent output file
                        integrityDisabledFiles.add(pf);
                        continue;
                    }

                    // in the workflow cache we track the get URL for the outfile
                    String thirdPartyDestGetURL =
                            this.getURLOnSharedScratch(
                                    destSite, job, OPERATION.get, addOn, outFile);
                    trackInWorkflowCache(outFile, thirdPartyDestGetURL, destSiteHandle);

                    // add all the possible source urls iterating through
                    // the list of grid ftp servers associated with the dest pool.
                    boolean first = true;

                    Directory parentScratchDir =
                            mSiteStore
                                    .lookup(pJob.getStagingSiteHandle())
                                    .getDirectory(Directory.TYPE.shared_scratch);
                    if (parentScratchDir == null) {
                        throw new RuntimeException(
                                "Unable to determine the scratch dir for site "
                                        + pJob.getStagingSiteHandle());
                    }
                    // retrive all the file servers matching the get operations
                    for (FileServer.OPERATION op : FileServer.OPERATION.operationsForGET()) {
                        for (Iterator it1 = parentScratchDir.getFileServersIterator(op);
                                it1.hasNext(); ) {

                            FileServer server = (FileServer) it1.next();
                            // definite inconsitency as url prefix and mount point
                            // are not picked up from the same server
                            sourceURI = server.getURLPrefix();

                            // sourceURI += server.getMountPoint();
                            sourceURI +=
                                    mSiteStore.getExternalWorkDirectory(
                                            server, pJob.getSiteHandle());

                            // PM-1977 the relative add on path (like 00/23 etc) needs to
                            // be the one retrieved from the parent job that generated
                            // the source file
                            sourceURL =
                                    sourceURI
                                            + File.separator
                                            + parentAddon
                                            + File.separator
                                            + outFile;

                            if (job instanceof DAXJob) {
                                // Case 3 PM-1766 DAX job has some input files that are produced by
                                // a parent compute job. track this URL in the cache file for
                                // the job
                                jobInputCache.insert(
                                        outFile, sourceURL, pJob.getStagingSiteHandle());
                            } else if (!(sourceURL.equalsIgnoreCase(thirdPartyDestPutURL))) {
                                // add the source url only if it does not match to
                                // the third party destination url
                                ft.addSource(pJob.getStagingSiteHandle(), sourceURL);
                            }
                            first = false;
                        }
                    }
                    if (ft.isValid()) {
                        if (localTransfer) {
                            localTransfers.add(ft);
                        } else {
                            remoteTransfers.add(ft);
                        }
                    }
                }
            }
        }

        // PM-1608 disable integrity for some of job input files that are outputs
        // of parent dax jobs
        for (PegasusFile ip : job.getInputFiles()) {
            if (integrityDisabledFiles.contains(ip)) {
                ip.setForIntegrityChecking(false);
                mLogger.log(
                        "Disabled file "
                                + ip.getLFN()
                                + " for job "
                                + job.getID()
                                + " for integrity checking",
                        LogManager.TRACE_MESSAGE_LEVEL);
            }
        }

        // close the job input cache if opened
        if (jobInputCache != null) {
            jobInputCache.close();
        }

        result[0] = localTransfers;
        result[1] = remoteTransfers;
        return result;
    }

    /**
     * It gets the output files for all the nodes which are specified in the nodes passed.
     *
     * @param nodes List<GraphNode> containing the jobs
     * @return Set of PegasusFile objects
     */
    private Set<PegasusFile> getOutputFiles(Collection<GraphNode> nodes) {

        Set<PegasusFile> files = new HashSet();

        for (GraphNode n : nodes) {
            Job job = (Job) n.getContent();
            files.addAll(job.getOutputFiles());
        }

        return files;
    }

    /**
     * Tracks the files created by a job in the both the planner and workflow cache The planner
     * cache stores the put URL's and the GET URL is stored in the workflow cache.
     *
     * @param job the job whose input files need to be tracked.
     */
    private void trackInCaches(Job job) {

        // check if there is a remote initialdir set
        String path = job.vdsNS.getStringValue(Pegasus.REMOTE_INITIALDIR_KEY);

        SiteCatalogEntry stagingSiteEntry = mSiteStore.lookup(job.getStagingSiteHandle());
        if (stagingSiteEntry == null) {
            this.poolNotFoundMsg(job.getStagingSiteHandle(), "vanilla");
            mLogger.log(mLogMsg, LogManager.ERROR_MESSAGE_LEVEL);
            throw new RuntimeException(mLogMsg);
        }

        for (Iterator it = job.getOutputFiles().iterator(); it.hasNext(); ) {
            PegasusFile pf = (PegasusFile) it.next();
            String lfn = pf.getLFN();

            // PM-833 figure out the addOn component just once per lfn
            File addOn = mStagingMapper.mapToRelativeDirectory(job, stagingSiteEntry, lfn);

            // construct the URL to track in planner cache
            String stagingSitePutURL =
                    this.getURLOnSharedScratch(stagingSiteEntry, job, OPERATION.put, addOn, lfn);
            trackInPlannerCache(lfn, stagingSitePutURL, stagingSiteEntry.getSiteHandle());

            String stagingSiteGetURL =
                    this.getURLOnSharedScratch(stagingSiteEntry, job, OPERATION.get, addOn, lfn);
            trackInWorkflowCache(lfn, stagingSiteGetURL, stagingSiteEntry.getSiteHandle());
        }
    }

    /**
     * Inserts an entry into the planner cache as a put URL.
     *
     * @param lfn the logical name of the file.
     * @param pfn the pfn
     * @param site the site handle
     */
    private void trackInPlannerCache(String lfn, String pfn, String site) {

        trackInPlannerCache(lfn, pfn, site, OPERATION.put);
    }

    /**
     * Inserts an entry into the planner cache as a put URL.
     *
     * @param lfn the logical name of the file.
     * @param pfn the pfn
     * @param site the site handle
     * @param type the type of url
     */
    private void trackInPlannerCache(String lfn, String pfn, String site, OPERATION type) {

        mPlannerCache.insert(lfn, pfn, site, type);
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
     * Returns a URL on the shared scratch of the staging site
     *
     * @param entry the SiteCatalogEntry for the associated stagingsite
     * @param job the job
     * @param operation the FileServer operation for which we need the URL
     * @param lfn the LFN can be null to get the path to the directory
     * @return the URL
     */
    private String getURLOnSharedScratch(
            SiteCatalogEntry entry,
            Job job,
            FileServer.OPERATION operation,
            File addOn,
            String lfn) {
        return mStagingMapper.map(job, addOn, entry, operation, lfn);
    }

    /**
     * Complains for a missing head node file server on a site for a job
     *
     * @param job the job
     * @param operation the operation
     * @param site the site
     */
    private void complainForScratchFileServer(
            Job job, FileServer.OPERATION operation, String site) {
        this.complainForScratchFileServer(job.getID(), operation, site);
    }

    /**
     * Complains for a missing head node file server on a site for a job
     *
     * @param jobname the name of the job
     * @param operation the file server operation
     * @param site the site
     */
    private void complainForScratchFileServer(
            String jobname, FileServer.OPERATION operation, String site) {
        StringBuffer error = new StringBuffer();
        error.append("[").append(REFINER_NAME).append("] ");
        if (jobname != null) {
            error.append("For job (").append(jobname).append(").");
        }
        error.append(" File Server not specified for shared-scratch filesystem for site: ")
                .append(site);
        throw new RuntimeException(error.toString());
    }

    /**
     * Initializes a Replica Catalog Instance that is used to store the GET URL's for all files on
     * the staging site ( inputs staged and outputs created ).
     *
     * @param dag the workflow being planned
     * @return handle to transient catalog
     */
    private ReplicaCatalog initializeWorkflowCacheFile(ADag dag) {
        ReplicaCatalog rc = null;
        mLogger.log(
                "Initialising Workflow Cache File in the Submit Directory",
                LogManager.DEBUG_MESSAGE_LEVEL);

        Properties cacheProps =
                mProps.getVDSProperties().matchingSubset(ReplicaCatalog.c_prefix, false);
        String file = mPOptions.getSubmitDirectory() + File.separatorChar + getCacheFileName(dag);

        // set the appropriate property to designate path to file
        cacheProps.setProperty(WORKFLOW_CACHE_REPLICA_CATALOG_KEY, file);

        try {
            rc = ReplicaFactory.loadInstance(WORKFLOW_CACHE_FILE_IMPLEMENTOR, mBag, cacheProps);
        } catch (Exception e) {
            throw new RuntimeException(
                    "Unable to initialize Workflow Cache File in the Submit Directory  " + file, e);
        }
        return rc;
    }

    /**
     * Constructs the basename to the cache file that is to be used to log the transient files. The
     * basename is dependant on whether the basename prefix has been specified at runtime or not.
     *
     * @param adag the ADag object containing the workflow that is being concretized.
     * @return the name of the cache file
     */
    private String getCacheFileName(ADag adag) {
        StringBuffer sb = new StringBuffer();
        String bprefix = mPOptions.getBasenamePrefix();

        if (bprefix != null) {
            // the prefix is not null using it
            sb.append(bprefix);
        } else {
            // generate the prefix from the name of the dag
            sb.append(adag.getLabel()).append("-").append(adag.getIndex());
        }
        // append the suffix
        sb.append(".cache");

        return sb.toString();
    }

    /**
     * Returns the relative submit directory for the job from the top level submit directory where
     * workflow files are written.
     *
     * @param job
     * @return
     */
    protected String getRelativeSubmitDirectory(Job job) {

        String relative = null;
        try {
            File f = mSubmitDirMapper.getRelativeDir(job);
            mLogger.log(
                    "Directory for job " + job.getID() + " is " + f,
                    LogManager.DEBUG_MESSAGE_LEVEL);
            relative = f.getPath();
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Error while determining relative submit dir for job " + job.getID(), ex);
        }
        return relative;
    }

    /**
     * Initializes a Replica Catalog Instance that is used to store the the GET url's of files
     * generated in the current workflow (by parent jobs of this DAXJob) that are required when the
     * sub workflow represented by this DAXJob executes.
     *
     * @param dag the workflow being planned
     * @return handle to transient catalog
     */
    private ReplicaCatalog getWorkflowCache(DAXJob job) {
        ReplicaCatalog rc = null;
        mLogger.log(
                "Initialising Workflow Cache File for job " + job.getID(),
                LogManager.DEBUG_MESSAGE_LEVEL);

        Properties cacheProps =
                mProps.getVDSProperties().matchingSubset(ReplicaCatalog.c_prefix, false);

        StringBuilder file = new StringBuilder();
        file.append(mPOptions.getSubmitDirectory())
                .append(File.separator)
                .append(job.getRelativeSubmitDirectory())
                .append(File.separator)
                .append(job.getID())
                .append(".input.cache");

        // PM-1916 set the path to input cache file for sub workflow explicilty
        job.setInputWorkflowCacheFile(file.toString());

        // set the appropriate property to designate path to file
        cacheProps.setProperty(WORKFLOW_CACHE_REPLICA_CATALOG_KEY, file.toString());

        try {
            rc = ReplicaFactory.loadInstance(WORKFLOW_CACHE_FILE_IMPLEMENTOR, mBag, cacheProps);
        } catch (Exception e) {
            throw new RuntimeException(
                    "Unable to initialize Workflow Cache File in the Submit Directory  " + file, e);
        }
        return rc;
    }
}
