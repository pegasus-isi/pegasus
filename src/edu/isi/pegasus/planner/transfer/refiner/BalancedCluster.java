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
package edu.isi.pegasus.planner.transfer.refiner;

import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.planner.catalog.classes.Profiles.NAMESPACES;
import edu.isi.pegasus.planner.catalog.site.classes.SiteCatalogEntry;
import edu.isi.pegasus.planner.catalog.site.classes.SiteStore;
import edu.isi.pegasus.planner.classes.ADag;
import edu.isi.pegasus.planner.classes.FileTransfer;
import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.common.PegasusConfiguration;
import edu.isi.pegasus.planner.namespace.Condor;
import edu.isi.pegasus.planner.namespace.Pegasus;
import edu.isi.pegasus.planner.partitioner.graph.GraphNode;
import edu.isi.pegasus.planner.refiner.ReplicaCatalogBridge;
import edu.isi.pegasus.planner.refiner.TransferEngine;
import edu.isi.pegasus.planner.transfer.Implementation;
import edu.isi.pegasus.planner.transfer.Refiner;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

/**
 * An extension of the default refiner, that allows the user to specify the number of transfer nodes
 * per execution site for stagein and stageout. The files are distributed in a round robin manner
 * across the stagein and stageout jobs. Currently it is per workflow for the stage-in while for
 * stageout it is per level of the workflow.
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public class BalancedCluster extends Basic {

    /** A short description of the transfer refinement. */
    public static final String DESCRIPTION =
            "Balanced Cluster Transfer Refiner( round robin distribution at file level)";

    public static final String SCALING_MESSAGE_PREFIX =
            "Pegasus now has a strategy for scaling transfer jobs based on size of workflow.";

    public static final String SCALING_MESSAGE_PROPERTY_PREFIX = "Consider removing the property";

    public static final String SCALING_MESSAGE_PROFILE_PREFIX =
            "Consider removing the pegasus profile";

    /** number of compute jobs to be associated with a single job */
    public static final float NUM_COMPUTE_JOBS_PER_TRANSFER_JOB = 10;

    /** Default value for deleted jobs level */
    public static final int DEFAULT_TX_JOBS_FOR_DELETED_JOBS = 10;

    /** If no transfer profile is specified the value, for the parameters */
    private static final int NO_PROFILE_VALUE = -1;

    /**
     * The map containing the list of stage in transfer jobs that are being created for the workflow
     * indexed by the execution poolname.
     */
    private Map<String, PoolTransfer> mStageInLocalMapPerLevel;

    /**
     * The map containing the list of stage in transfer jobs that are being created for the workflow
     * indexed by the execution poolname.
     */
    private Map<String, PoolTransfer> mStageInRemoteMapPerLevel;

    /**
     * The map indexed by compute jobnames that contains the list of stagin job names that are being
     * added during the traversal of the workflow. This is used to construct the relations that need
     * to be added to workflow, once the traversal is done.
     */
    private Map mRelationsParentMap;

    /** The ClusterValue that evaluates for local stage in jobs. */
    protected ClusterValue mStageinLocalBundleValue;

    /** The ClusterValue that evaluates for remote stage-in jobs. */
    protected ClusterValue mStageInRemoteBundleValue;

    /** The ClusterValue that evaluates for local stage out jobs. */
    protected ClusterValue mStageOutLocalBundleValue;

    /** The ClusterValue that evaluates for remote stage out jobs. */
    protected ClusterValue mStageOutRemoteBundleValue;

    /**
     * The map indexed by staged executable logical name. Each entry is the name of the
     * corresponding setup job, that changes the XBit on the staged file.
     */
    protected Map mSetupMap;

    /**
     * A map indexed by site name, that contains the pointer to the local stage out PoolTransfer
     * objects for that site. This is per level of the workflow.
     */
    private Map<String, PoolTransfer> mStageOutLocalMapPerLevel;

    /**
     * A map indexed by site name, that contains the pointer to the remote stage out PoolTransfer
     * objects for that site. This is per level of the workflow.
     */
    private Map<String, PoolTransfer> mStageOutRemoteMapPerLevel;

    /** The current level of the jobs being traversed. */
    private int mCurrentSOLevel;

    /** The handle to the replica catalog bridge. */
    private ReplicaCatalogBridge mRCB;

    /** The job prefix that needs to be applied to the job file basenames. */
    protected String mJobPrefix;

    /** Pegasus Profiles specified in the properties. */
    protected Pegasus mPegasusProfilesInProperties;

    /** Handle to the SiteStore */
    protected SiteStore mSiteStore;

    /**
     * A boolean indicating whether chmod jobs should be created that set the xbit in case of
     * executable staging.
     */
    // protected boolean mAddNodesForSettingXBit;
    /** The current level of the jobs being traversed. */
    private int mCurrentSILevel;

    /** handle to PegasusConfiguration */
    protected PegasusConfiguration mPegasusConfiguration;

    /** Tracks number of jobs at each level of the workflow. */
    private Map<Integer, Integer> mTXJobsPerLevelMap;

    /** List of scaling messages to log */
    private Set<String> mScalingMessages;

    /**
     * The overloaded constructor.
     *
     * @param dag the workflow to which transfer nodes need to be added.
     * @param bag the bag of initialization objects
     */
    public BalancedCluster(ADag dag, PegasusBag bag) {
        super(dag, bag);

        // from pegasus release 3.2 onwards xbit jobs are not added
        // for worker node execution/Pegasus Lite
        // PM-810 it is now per job instead of global.
        mPegasusConfiguration = new PegasusConfiguration(mLogger);
        // mAddNodesForSettingXBit = !mProps.executeOnWorkerNode();

        mStageInLocalMapPerLevel = new HashMap(mPOptions.getExecutionSites().size());
        mStageInRemoteMapPerLevel = new HashMap(mPOptions.getExecutionSites().size());

        mRelationsParentMap = new HashMap();
        mSetupMap = new HashMap();
        mCurrentSOLevel = -1;
        mCurrentSILevel = -1;
        mJobPrefix = mPOptions.getJobnamePrefix();

        mSiteStore = bag.getHandleToSiteStore();
        mScalingMessages = new LinkedHashSet();
        mPegasusProfilesInProperties = (Pegasus) mProps.getProfiles(NAMESPACES.pegasus);
        initializeClusterValues();
    }

    /**
     * Initializes the bundle value variables, that are responsible determining the bundle values.
     */
    protected void initializeClusterValues() {
        // PM-1212 we want to get an idea
        mTXJobsPerLevelMap =
                buildDefaultTXJobsPerLevelMap(BalancedCluster.NUM_COMPUTE_JOBS_PER_TRANSFER_JOB);

        mStageinLocalBundleValue = new ClusterValue();
        mStageinLocalBundleValue.initialize(
                Pegasus.CLUSTER_LOCAL_STAGE_IN_KEY,
                Pegasus.CLUSTER_STAGE_IN_KEY,
                getDefaultClusterValueFromProperties(
                        Pegasus.CLUSTER_LOCAL_STAGE_IN_KEY,
                        Pegasus.CLUSTER_STAGE_IN_KEY,
                        BalancedCluster.NO_PROFILE_VALUE));

        mStageInRemoteBundleValue = new ClusterValue();
        mStageInRemoteBundleValue.initialize(
                Pegasus.CLUSTER_REMOTE_STAGE_IN_KEY,
                Pegasus.CLUSTER_STAGE_IN_KEY,
                getDefaultClusterValueFromProperties(
                        Pegasus.CLUSTER_LOCAL_STAGE_IN_KEY,
                        Pegasus.CLUSTER_STAGE_IN_KEY,
                        BalancedCluster.NO_PROFILE_VALUE));

        mStageOutLocalBundleValue = new ClusterValue();
        mStageOutLocalBundleValue.initialize(
                Pegasus.CLUSTER_LOCAL_STAGE_OUT_KEY,
                Pegasus.CLUSTER_STAGE_OUT_KEY,
                getDefaultClusterValueFromProperties(
                        Pegasus.CLUSTER_LOCAL_STAGE_OUT_KEY,
                        Pegasus.CLUSTER_STAGE_OUT_KEY,
                        BalancedCluster.NO_PROFILE_VALUE));

        mStageOutRemoteBundleValue = new ClusterValue();
        mStageOutRemoteBundleValue.initialize(
                Pegasus.CLUSTER_REMOTE_STAGE_OUT_KEY,
                Pegasus.CLUSTER_STAGE_OUT_KEY,
                getDefaultClusterValueFromProperties(
                        Pegasus.CLUSTER_REMOTE_STAGE_OUT_KEY,
                        Pegasus.CLUSTER_STAGE_OUT_KEY,
                        BalancedCluster.NO_PROFILE_VALUE));
    }

    /**
     * Returns the default value for the clustering/bundling of jobs to be used.
     *
     * <p>The factor is computed by looking up the pegasus profiles in the properties.
     *
     * <pre>
     *    return value of pegasus profile key if it exists,
     *    else return value of pegasus profile defaultKey if it exists,
     *    else the defaultValue
     * </pre>
     *
     * @param key the pegasus profile key
     * @param defaultKey the default pegasus profile key
     * @param defaultValue the default value.
     * @return the value
     */
    protected int getDefaultClusterValueFromProperties(
            String key, String defaultKey, int defaultValue) {

        String result = mPegasusProfilesInProperties.getStringValue(key);

        if (result == null) {
            // rely on defaultKey value
            result = mPegasusProfilesInProperties.getStringValue(defaultKey);

            if (result == null) {
                // none of the keys are mentioned in properties
                // use the default value
                return defaultValue;
            } else {
                logDefferedScalingPropertyMessage(defaultKey);
            }
        } else {
            // PM-1212 log about our scaling thing
            logDefferedScalingPropertyMessage(key);
        }

        return Integer.parseInt(result);
    }

    /**
     * Adds the stage in transfer nodes which transfer the input files for a job, from the location
     * returned from the replica catalog to the job's execution pool.
     *
     * @param job <code>Job</code> object corresponding to the node to which the files are to be
     *     transferred to.
     * @param files Collection of <code>FileTransfer</code> objects containing the information about
     *     source and destURL's.
     * @param symlinkFiles Collection of <code>FileTransfer</code> objects containing source and
     *     destination file url's for symbolic linking on compute site.
     */
    public void addStageInXFERNodes(
            Job job, Collection<FileTransfer> files, Collection<FileTransfer> symlinkFiles) {

        addStageInXFERNodes(
                job,
                true,
                files,
                Job.STAGE_IN_JOB,
                this.mStageInLocalMapPerLevel,
                this.mStageinLocalBundleValue,
                this.mTXStageInImplementation);

        addStageInXFERNodes(
                job,
                false,
                symlinkFiles,
                Job.STAGE_IN_JOB,
                this.mStageInRemoteMapPerLevel,
                this.mStageInRemoteBundleValue,
                this.mTXStageInImplementation);
    }

    /**
     * Adds the stage in transfer nodes which transfer the input files for a job, from the location
     * returned from the replica catalog to the job's execution pool.
     *
     * @param job <code>Job</code> object corresponding to the node to which the files are to be
     *     transferred to.
     * @param localTransfer boolean indicating whether transfer has to happen on local site.
     * @param files Collection of <code>FileTransfer</code> objects containing the information about
     *     source and destURL's.
     * @param type the type of transfer job being created
     * @param stageInMap Map indexed by site name that gives all the transfers for that site.
     * @param bundleValue used to determine the bundling factor to employ for a job.
     * @param implementation the transfer implementation to use.
     */
    public void addStageInXFERNodes(
            Job job,
            boolean localTransfer,
            Collection files,
            int type,
            Map<String, PoolTransfer> stageInMap,
            ClusterValue bundleValue,
            Implementation implementation) {

        String jobName = job.getName();
        String siteHandle = job.getStagingSiteHandle();
        int bundle = -1;
        int priority = getJobPriority(job);

        int level = job.getLevel();
        if (level != mCurrentSILevel) {
            mCurrentSILevel = level;
            // we are starting on a new level of the workflow.
            // reinitialize stuff
            this.resetStageInMaps();
            // the stagein map needs to point to the correct reinitialized one
            if (localTransfer) {
                stageInMap = this.mStageInLocalMapPerLevel;
            } else {
                stageInMap = this.mStageInRemoteMapPerLevel;
            }
        }

        // to prevent duplicate dependencies
        Set tempSet = new HashSet();
        int staged = 0;
        Collection stagedExecutableFiles = new LinkedList();
        Collection<String> stageInExecJobs =
                new LinkedList(); // store list of jobs that are transferring the stage file
        for (Iterator it = files.iterator(); it.hasNext(); ) {
            FileTransfer ft = (FileTransfer) it.next();
            String lfn = ft.getLFN();

            // set the priority associated with the
            // compute job PM-622
            ft.setPriority(priority);

            // get the key for this lfn and pool
            // if the key already in the table
            // then remove the entry from
            // the Vector and add a dependency
            // in the graph
            String key = this.constructFileKey(lfn, siteHandle);
            String par = (String) mFileTable.get(key);
            // System.out.println("lfn " + lfn + " par " + par);
            if (par != null) {
                it.remove();
                // check if tempSet does not contain the parent
                tempSet.add(par);

                // PM-810 worker node exeucution is per job level now
                boolean addNodeForSettingXBit =
                        !mPegasusConfiguration.jobSetupForWorkerNodeExecution(job);

                if (ft.isTransferringExecutableFile() && addNodeForSettingXBit) {
                    // currently we have only one file to be staged per
                    // compute job . Taking a short cut in determining
                    // the name of setXBit job
                    String xBitJobName = (String) mSetupMap.get(key);
                    if (key == null) {
                        throw new RuntimeException(
                                "Internal Pegasus Error while "
                                        + "constructing balanced cluster stagein jobs");
                    }
                    // add relation xbitjob->computejob
                    this.addRelation(xBitJobName, jobName);
                }

            } else {
                // get the name of the transfer job
                boolean contains = stageInMap.containsKey(siteHandle);
                // following pieces need rearragnement!
                if (!contains) {
                    bundle =
                            bundleValue.determine(
                                    implementation, job, mTXJobsPerLevelMap.get(job.getLevel()));
                }
                PoolTransfer pt =
                        (contains)
                                ? (PoolTransfer) stageInMap.get(siteHandle)
                                : new PoolTransfer(siteHandle, localTransfer, bundle);
                if (!contains) {
                    stageInMap.put(siteHandle, pt);
                }
                // add the FT to the appropriate transfer job.
                TransferContainer siTC = pt.addTransfer(ft, level, type);
                String newJobName = siTC.getTXName();

                if (ft.isTransferringExecutableFile()) {
                    // add both the name of the stagein job and the executable file
                    stageInExecJobs.add(newJobName);
                    stagedExecutableFiles.add(ft);

                    mLogger.log(
                            "Entered "
                                    + key
                                    + "->"
                                    + implementation.getSetXBitJobName(job.getName(), staged),
                            LogManager.DEBUG_MESSAGE_LEVEL);
                    mSetupMap.put(key, implementation.getSetXBitJobName(job.getName(), staged));
                    // all executables for a job are chmod with a single node
                    // staged++;
                }

                // make a new entry into the table
                mFileTable.put(key, newJobName);
                // add the newJobName to the tempSet so that even
                // if the job has duplicate input files only one instance
                // of transfer is scheduled. This came up during collapsing
                // June 15th, 2004
                tempSet.add(newJobName);
            }
        }

        // if there were any staged files
        // add the setXBitJobs for them
        int index = 0;

        // stageInExecJobs has corresponding list of transfer
        // jobs that transfer the files
        // PM-810 worker node exeucution is per job level now
        boolean addNodeForSettingXBit = !mPegasusConfiguration.jobSetupForWorkerNodeExecution(job);

        if (!stagedExecutableFiles.isEmpty() && addNodeForSettingXBit) {
            Job xBitJob =
                    implementation.createSetXBitJob(
                            job, stagedExecutableFiles, Job.STAGE_IN_JOB, index);

            this.addJob(xBitJob);

            // add the relation txJob->XBitJob->ComputeJob
            Set edgesAdded = new HashSet();
            for (String txJobName : stageInExecJobs) {

                // adding relation txJob->XBitJob
                if (edgesAdded.contains(txJobName)) {
                    // do nothing
                    mLogger.log(
                            "Not adding edge " + txJobName + " -> " + xBitJob.getName(),
                            LogManager.DEBUG_MESSAGE_LEVEL);
                } else {
                    this.addRelation(txJobName, xBitJob.getName(), xBitJob.getSiteHandle(), true);
                    edgesAdded.add(txJobName);
                }
            }
            this.addRelation(xBitJob.getName(), job.getName());
        }

        // add the temp set to the relations
        // relations are added to the workflow in the end.
        if (mRelationsParentMap.containsKey(jobName)) {
            // the map already has some relations for the job
            // add those to temp set to
            tempSet.addAll((Set) mRelationsParentMap.get(jobName));
        }
        mRelationsParentMap.put(jobName, tempSet);
    }

    /**
     * Adds the stageout transfer nodes, that stage data to an output site specified by the user.
     *
     * @param job <code>Job</code> object corresponding to the node to which the files are to be
     *     transferred to.
     * @param files Collection of <code>FileTransfer</code> objects containing the information about
     *     source and destURL's.
     * @param rcb bridge to the Replica Catalog. Used for creating registration nodes in the
     *     workflow.
     * @param localTransfer whether the transfer should be on local site or not.
     * @param deletedLeaf to specify whether the node is being added for a deleted node by the
     *     reduction engine or not. default: false
     */
    public void addStageOutXFERNodes(
            Job job,
            Collection files,
            ReplicaCatalogBridge rcb,
            boolean localTransfer,
            boolean deletedLeaf) {

        // initializing rcb till the change in function signature happens
        // needs to be passed during refiner initialization
        mRCB = rcb;

        // sanity check
        if (files.isEmpty()) {
            return;
        }

        String jobName = job.getName();
        ClusterValue bundleValue =
                (localTransfer) ? this.mStageOutLocalBundleValue : this.mStageOutRemoteBundleValue;

        mLogMsg = "Adding stageout nodes for job " + jobName;

        // PM-622
        int priority = getJobPriority(job);
        int level = job.getLevel();
        String site = job.getStagingSiteHandle();

        int bundle =
                bundleValue.determine(
                        this.mTXStageOutImplementation,
                        job,
                        mTXJobsPerLevelMap.get(job.getLevel()));

        if (level != mCurrentSOLevel) {
            mCurrentSOLevel = level;
            // we are starting on a new level of the workflow.
            // reinitialize stuff
            this.resetStageOutMaps();
        }

        // track stageout and registration jobs that maybe associated with the compute job
        Set<String> children = new HashSet();
        for (Iterator it = files.iterator(); it.hasNext(); ) {
            boolean makeTNode = false;
            boolean makeRNode = false;
            FileTransfer ft = (FileTransfer) it.next();

            ft.setPriority(priority);

            if (!ft.getTransientTransferFlag()) {
                makeTNode = true;
            }
            if (mCreateRegistrationJobs && ft.getRegisterFlag()) {
                makeRNode = true;
            }

            TransferContainer soTC = null;
            if (makeTNode) {
                // a stageout job needs to be associated
                // get the appropriate pool transfer object for the site
                PoolTransfer pt = this.getStageOutPoolTransfer(site, localTransfer, bundle);
                // we add the appropriate file transfer
                soTC = pt.addTransfer(ft, level, Job.STAGE_OUT_JOB);
                String soJob = soTC.getTXName();

                if (!deletedLeaf) {
                    // need to add a relation between a compute and stage-out
                    // job only if the compute job was not reduced.
                    children.add(soJob);
                }
            } else if (makeRNode) {
                // add an empty file transfer
                // get the appropriate pool transfer object for the site
                PoolTransfer pt = this.getStageOutPoolTransfer(site, localTransfer, bundle);
                // we add all the file transfers to the pool transfer
                soTC = pt.addTransfer(new Vector(), level, Job.STAGE_OUT_JOB);
                // direct link between compute job and registration job
                children.add(soTC.getRegName());
            }
            if (makeRNode) {
                soTC.addRegistrationFiles(ft);
            }
        }
        // add any relation that are required between the compute
        // job and stageout|registration jobs
        for (String child : children) {
            addRelation(jobName, child);
        }
    }

    /**
     * Returns the bundle value associated with a compute job as a String.
     *
     * @param job
     * @return value as String or NULL
     */
    protected String getComputeJobBundleValue(Job job) {
        return job.vdsNS.getStringValue(Pegasus.CLUSTER_STAGE_OUT_KEY);
    }

    /**
     * Signals that the traversal of the workflow is done. At this point the transfer nodes are
     * actually constructed traversing through the transfer containers and the stdin of the transfer
     * jobs written.
     */
    public void done() {

        // increment the level counter
        this.mCurrentSILevel++;
        this.resetStageInMaps();

        // adding relations that tie in the stagin
        // jobs to the compute jobs.
        /*        for (Iterator it = mRelationsParentMap.entrySet().iterator(); it.hasNext(); ) {
                    Map.Entry entry = (Map.Entry) it.next();
                    String key = (String) entry.getKey();
                    mLogger.log("Adding relations for job " + key, LogManager.DEBUG_MESSAGE_LEVEL);
                    for (Iterator pIt = ((Collection) entry.getValue()).iterator(); pIt.hasNext(); ) {
                        String value = (String) pIt.next();

                        mLogger.log("Adding Edge " + value + " -> " + key, LogManager.DEBUG_MESSAGE_LEVEL);
                        this.mDAG.addEdge(value, key);
                    }
                }
        */
        // reset the stageout map too
        this.resetStageOutMaps();

        // PM-747 add the edges in the very end
        super.done();

        // PM-1212 log any scaling informational messages
        for (String message : this.mScalingMessages) {
            mLogger.log(message, LogManager.INFO_MESSAGE_LEVEL);
        }
    }

    /** Resets the local and remote stage out maps. */
    protected void resetStageInMaps() {
        List localTXJobs =
                addStageinJobs(
                        this.mStageInLocalMapPerLevel,
                        this.mTXStageInImplementation,
                        Job.STAGE_IN_JOB,
                        true);
        mStageInLocalMapPerLevel = new HashMap<String, PoolTransfer>();

        List remoteTXJobs =
                addStageinJobs(
                        this.mStageInRemoteMapPerLevel,
                        this.mTXStageInImplementation,
                        Job.STAGE_IN_JOB,
                        false);
        mStageInRemoteMapPerLevel = new HashMap<String, PoolTransfer>();

        // adding relations that tie in the stagin
        // jobs to the compute jobs.
        for (Iterator it = mRelationsParentMap.entrySet().iterator(); it.hasNext(); ) {
            Map.Entry entry = (Map.Entry) it.next();
            String key = (String) entry.getKey();
            mLogger.log("Adding relations for job " + key, LogManager.DEBUG_MESSAGE_LEVEL);
            for (Iterator pIt = ((Collection) entry.getValue()).iterator(); pIt.hasNext(); ) {
                String value = (String) pIt.next();

                mLogger.log("Adding Edge " + value + " -> " + key, LogManager.DEBUG_MESSAGE_LEVEL);
                this.mDAG.addEdge(value, key);
            }
        }
        mRelationsParentMap = new HashMap();

        // PM-1385 assign priorties for the transfer job
        assignPriority(localTXJobs);
        assignPriority(remoteTXJobs);
    }

    /**
     * Signals that the traversal of the workflow is done or a new level in the workflow is started.
     * At this point the transfer nodes are actually constructed traversing through the transfer
     * containers and the stdin of the transfer jobs written.
     *
     * @param stageInMap maps site names to PoolTransfer
     * @param implementation the transfer implementation to use
     * @param stageInJobType whether a stagein or symlink stagein job
     * @param localTransfer indicates whether transfer job needs to run on local site or not.
     * @return List of jobs added to the workflow
     */
    public List<Job> addStageinJobs(
            Map<String, PoolTransfer> stageInMap,
            Implementation implementation,
            int stageInJobType,
            boolean localTransfer) {
        List<Job> txJobs = new LinkedList();
        if (stageInMap != null) {
            // traverse through the stagein map and
            // add transfer nodes per pool
            String key;
            String value;
            PoolTransfer pt;
            TransferContainer tc;
            Map.Entry entry;
            Job job = new Job();

            for (Iterator it = stageInMap.entrySet().iterator(); it.hasNext(); ) {
                entry = (Map.Entry) it.next();
                key = (String) entry.getKey();
                pt = (PoolTransfer) entry.getValue();
                mLogger.log(
                        "Adding stage in transfer nodes for site " + key,
                        LogManager.DEBUG_MESSAGE_LEVEL);

                for (Iterator pIt = pt.getTransferContainerIterator(); pIt.hasNext(); ) {
                    tc = (TransferContainer) pIt.next();
                    if (tc == null) {
                        // break out
                        break;
                    }
                    mLogger.log(
                            "Adding stagein transfer node " + tc.getTXName(),
                            LogManager.DEBUG_MESSAGE_LEVEL);
                    // added in make transfer node
                    // mDag.addNewJob(tc.getName());
                    // we just need the execution pool in the job object
                    job.executionPool = key;
                    job.setStagingSiteHandle(key);

                    String site = localTransfer ? "local" : job.getSiteHandle();

                    Job tJob =
                            implementation.createTransferJob(
                                    job,
                                    site,
                                    tc.getFileTransfers(),
                                    null,
                                    tc.getTXName(),
                                    stageInJobType);
                    // always set the type to stagein after it is created
                    tJob.setJobType(stageInJobType);
                    addJob(tJob);
                    txJobs.add(tJob);
                }
            }
        }
        return txJobs;
    }

    /**
     * Assigns priority to the jobs based on the fanout (number of child jobs) a job has
     *
     * @param txJobs
     */
    protected void assignPriority(List<Job> txJobs) {
        txJobs.sort(
                new Comparator<Job>() {

                    private int getNumberOfChildren(Job j1) {
                        GraphNode node = j1.getGraphNodeReference();
                        return node.getChildren().size();
                    }

                    @Override
                    public int compare(Job j1, Job j2) {
                        // want the largest number of children first
                        return getNumberOfChildren(j2) - getNumberOfChildren(j1);
                    }
                });

        int priority = 0;
        for (Job j : txJobs) {
            // negative priorities here as we will later add them to a base priority - the idea
            // being that the most important job will have base priority, the next one
            // base priority - 1, and so on
            j.condorVariables.checkKeyInNS(Condor.PRIORITY_KEY, Integer.toString(priority--));
            GraphNode node = j.getGraphNodeReference();
            int children = node.getChildren().size();
            mLogger.log(
                    "Assigned priority adjustment of "
                            + priority
                            + " to transfer job "
                            + j.getID()
                            + " with children - "
                            + children,
                    LogManager.DEBUG_MESSAGE_LEVEL);
        }
    }

    /**
     * Returns a textual description of the transfer mode.
     *
     * @return a short textual description
     */
    public String getDescription() {
        return BalancedCluster.DESCRIPTION;
    }

    /**
     * Returns the appropriate pool transfer for a particular site.
     *
     * @param site the site for which the PT is reqd.
     * @param localTransfer whethe the associated transfer job runs on local site or remote.
     * @param num the number of Stageout jobs required for that Pool.
     * @return the PoolTransfer
     */
    public PoolTransfer getStageOutPoolTransfer(String site, boolean localTransfer, int num) {

        // pick up appropriate map
        Map map = localTransfer ? this.mStageOutLocalMapPerLevel : this.mStageOutRemoteMapPerLevel;

        if (map.containsKey(site)) {
            return (PoolTransfer) map.get(site);
        } else {
            PoolTransfer pt = new PoolTransfer(site, localTransfer, num);
            map.put(site, pt);
            return pt;
        }
    }

    /** Resets the local and remote stage out maps. */
    protected void resetStageOutMaps() {
        mStageOutLocalMapPerLevel = this.resetStageOutMap(mStageOutLocalMapPerLevel, true);
        mStageOutRemoteMapPerLevel = this.resetStageOutMap(mStageOutRemoteMapPerLevel, false);
    }

    /**
     * Resets a single map
     *
     * @param map the map to be reset
     * @param localTransfer whether the transfer jobs need to run on local site or not
     * @return the reset map
     */
    protected Map resetStageOutMap(Map<String, PoolTransfer> map, boolean localTransfer) {
        if (map != null) {
            // before flushing add the stageout nodes to the workflow
            Job job = new Job();

            for (Iterator it = map.values().iterator(); it.hasNext(); ) {
                PoolTransfer pt = (PoolTransfer) it.next();
                job.setSiteHandle(pt.mPool);
                job.setStagingSiteHandle(pt.mPool);

                // site is where transfer job runs
                String site = localTransfer ? "local" : job.getSiteHandle();

                mLogger.log(
                        "Adding jobs for staging out data from site " + pt.mPool,
                        LogManager.DEBUG_MESSAGE_LEVEL);

                // traverse through all the TransferContainers
                for (Iterator tcIt = pt.getTransferContainerIterator(); tcIt.hasNext(); ) {
                    TransferContainer tc = (TransferContainer) tcIt.next();
                    if (tc == null) {
                        // break out
                        break;
                    }

                    // add the stageout job if required
                    Job soJob = null;
                    if (!tc.getFileTransfers().isEmpty()) {
                        mLogger.log(
                                "Adding stage-out job " + tc.getTXName(),
                                LogManager.DEBUG_MESSAGE_LEVEL);
                        soJob =
                                mTXStageOutImplementation.createTransferJob(
                                        job,
                                        site,
                                        tc.getFileTransfers(),
                                        null,
                                        tc.getTXName(),
                                        Job.STAGE_OUT_JOB);
                        addJob(soJob);
                    }

                    // add registration job if required
                    if (!tc.getRegistrationFiles().isEmpty()) {

                        // add relation to stage out if the stageout job was created
                        if (soJob != null) {
                            // make the stageout job the super node for the registration job
                            job.setName(soJob.getName());
                            addRelation(tc.getTXName(), tc.getRegName());
                        }

                        mLogger.log(
                                "Adding registration job " + tc.getRegName(),
                                LogManager.DEBUG_MESSAGE_LEVEL);
                        addJob(
                                createRegistrationJob(
                                        tc.getRegName(), job, tc.getRegistrationFiles(), mRCB));
                    }
                }
            }
        }

        map = new HashMap();

        return map;
    }

    /**
     * Builds a map that maps for each level the number of default transfer jobs to be created
     *
     * @param divisor
     * @return
     */
    private Map<Integer, Integer> buildDefaultTXJobsPerLevelMap(float divisor) {
        // PM-1212
        Map<Integer, Integer> m = new HashMap();
        int count = 0;
        int previous = -1;
        int cluster = -1;
        int level = 0;
        for (Iterator it = this.mDAG.iterator(); it.hasNext(); ) {
            GraphNode node = (GraphNode) it.next();
            level = node.getDepth();
            if (level != previous) {
                cluster = (int) Math.ceil(count / divisor);
                mLogger.log(
                        "Number of transfer jobs for " + previous + " are " + cluster,
                        LogManager.DEBUG_MESSAGE_LEVEL);
                m.put(previous, cluster);
                count = 0;
            }
            count++;
            previous = level;
        }

        cluster = (int) Math.ceil(count / divisor);
        m.put(level, cluster);
        mLogger.log(
                "Number of transfer jobs for " + level + " are " + cluster,
                LogManager.DEBUG_MESSAGE_LEVEL);

        // add a value for the level associated with deleted jobs
        m.put(TransferEngine.DELETED_JOBS_LEVEL, BalancedCluster.DEFAULT_TX_JOBS_FOR_DELETED_JOBS);

        return m;
    }

    /**
     * Builds up a message for logging a scaling message indicating user to remove a property
     *
     * @param key
     */
    protected void logDefferedScalingPropertyMessage(String key) {
        StringBuilder message = new StringBuilder();
        message.append(SCALING_MESSAGE_PREFIX)
                .append(" ")
                .append(SCALING_MESSAGE_PROPERTY_PREFIX)
                .append(" ")
                .append("pegasus.")
                .append(key)
                .append(" ")
                .append(" from properties file");
        this.mScalingMessages.add(message.toString());
    }

    /**
     * Builds up a message for logging a scaling message indicating user to remove a profile
     *
     * @param key
     */
    protected void logDefferedScalingProfileMessage(String key, String site) {
        StringBuilder message = new StringBuilder();
        message.append(SCALING_MESSAGE_PREFIX)
                .append(" ")
                .append(SCALING_MESSAGE_PROFILE_PREFIX)
                .append(" ")
                .append(key)
                .append(" ")
                .append("from site")
                .append(" ")
                .append(site)
                .append(" ")
                .append("in site catalog");
        this.mScalingMessages.add(message.toString());
    }

    /**
     * A container class for storing the name of the transfer job, the list of file transfers that
     * the job is responsible for.
     */
    protected class TransferContainer {

        /** The name of the transfer job. */
        private String mTXName;

        /** The name of the registration job. */
        private String mRegName;

        /**
         * The collection of <code>FileTransfer</code> objects containing the transfers the job is
         * responsible for.
         */
        private Collection<FileTransfer> mFileTXList;

        /**
         * The collection of <code>FileTransfer</code> objects containing the files that need to be
         * registered.
         */
        private Collection<FileTransfer> mRegFiles;

        /** The type of the transfers the job is responsible for. */
        private int mTransferType;

        /** The default constructor. */
        public TransferContainer() {
            mTXName = null;
            mRegName = null;
            mFileTXList = new Vector();
            mRegFiles = new Vector();
            mTransferType = Job.STAGE_IN_JOB;
        }

        /**
         * Sets the name of the transfer job.
         *
         * @param name the name of the transfer job.
         */
        public void setTXName(String name) {
            mTXName = name;
        }

        /**
         * Sets the name of the registration job.
         *
         * @param name the name of the transfer job.
         */
        public void setRegName(String name) {
            mRegName = name;
        }

        /**
         * Adds a file transfer to the underlying collection.
         *
         * @param transfer the <code>FileTransfer</code> containing the information about a single
         *     transfer.
         */
        public void addTransfer(FileTransfer transfer) {
            mFileTXList.add(transfer);
        }

        /**
         * Adds a file transfer to the underlying collection.
         *
         * @param files collection of <code>FileTransfer</code>.
         */
        public void addTransfer(Collection files) {
            mFileTXList.addAll(files);
        }

        /**
         * Adds a single file for registration.
         *
         * @param files collection of <code>FileTransfer</code>.
         */
        public void addRegistrationFiles(FileTransfer file) {
            mRegFiles.add(file);
        }

        /**
         * Adds a Collection of File transfer to the underlying collection of files to be
         * registered.
         *
         * @param files collection of <code>FileTransfer</code>.
         */
        public void addRegistrationFiles(Collection files) {
            mRegFiles.addAll(files);
        }

        /**
         * Sets the transfer type for the transfers associated.
         *
         * @param type type of transfer.
         */
        public void setTransferType(int type) {
            mTransferType = type;
        }

        /**
         * Returns the name of the transfer job.
         *
         * @return name of the transfer job.
         */
        public String getTXName() {
            return mTXName;
        }

        /**
         * Returns the name of the registration job.
         *
         * @return name of the registration job.
         */
        public String getRegName() {
            return mRegName;
        }

        /**
         * Returns the collection of transfers associated with this transfer container.
         *
         * @return a collection of <code>FileTransfer</code> objects.
         */
        public Collection getFileTransfers() {
            return mFileTXList;
        }

        /**
         * Returns the collection of registration files associated with this transfer container.
         *
         * @return a collection of <code>FileTransfer</code> objects.
         */
        public Collection getRegistrationFiles() {
            return mRegFiles;
        }
    }

    /**
     * A container to store the transfers that need to be done per staging site The transfers are
     * stored over a collection of Transfer Containers with each transfer container responsible for
     * one transfer job.
     */
    protected class PoolTransfer {

        /** The maximum number of transfer jobs that are allowed for this particular pool. */
        private int mCapacity;

        /** The index of the job to which the next transfer for the pool would be scheduled. */
        private int mNext;

        /** The staging site for which these transfers are grouped. */
        private String mPool;

        /** The list of <code>TransferContainer</code> that correspond to each transfer job. */
        private List mTXContainers;

        /** boolean indicating whether the transfer job needs to run on local site */
        private boolean mLocalTransfer;

        /** The default constructor. */
        public PoolTransfer() {
            mCapacity = 0;
            mNext = -1;
            mPool = null;
            mTXContainers = null;
            mLocalTransfer = true;
        }

        /**
         * Convenience constructor.
         *
         * @param pool the pool name for which transfers are being grouped.
         * @param localTransfer whether the transfers need to be run on local site
         * @param number the number of transfer jobs that are going to be created for the pool.
         */
        public PoolTransfer(String pool, boolean localTransfer, int number) {
            mLocalTransfer = localTransfer;
            mCapacity = number;
            mNext = 0;
            mPool = pool;
            mTXContainers = new ArrayList(number);
            // intialize to null
            for (int i = 0; i < number; i++) {
                mTXContainers.add(null);
            }
        }

        /**
         * Adds a a collection of <code>FileTransfer</code> objects to the appropriate
         * TransferContainer. The collection is added to a single TransferContainer, and the pointer
         * is then updated to the next container.
         *
         * @param files the collection <code>FileTransfer</code> to be added.
         * @param level the level of the workflow
         * @param type the type of transfer job
         * @return the Transfer Container to which the job file transfers were added.
         */
        public TransferContainer addTransfer(Collection<FileTransfer> files, int level, int type) {
            // we add the transfer to the container pointed
            // by next
            Object obj = mTXContainers.get(mNext);
            TransferContainer tc = null;
            if (obj == null) {
                // on demand add a new transfer container to the end
                // is there a scope for gaps??
                tc = new TransferContainer();
                tc.setTXName(getTXJobName(mNext, type, level));
                // add the name for the registration job that maybe associated
                tc.setRegName(getRegJobName(mNext, level));
                mTXContainers.set(mNext, tc);
            } else {
                tc = (TransferContainer) obj;
            }
            tc.addTransfer(files);

            // update the next pointer to maintain
            // round robin status
            mNext = (mNext < (mCapacity - 1)) ? mNext + 1 : 0;

            return tc;
        }

        /**
         * Adds a single <code>FileTransfer</code> objects to the appropriate TransferContainer. The
         * collection is added to a single TransferContainer, and the pointer is then updated to the
         * next container.
         *
         * @param file
         * @param level the level of the workflow
         * @param type the type of transfer job
         * @return the Transfer Container to which the job file transfers were added.
         */
        public TransferContainer addTransfer(FileTransfer file, int level, int type) {
            // we add the transfer to the container pointed
            // by next
            Object obj = mTXContainers.get(mNext);
            TransferContainer tc = null;
            if (obj == null) {
                // on demand add a new transfer container to the end
                // is there a scope for gaps??
                tc = new TransferContainer();
                tc.setTXName(getTXJobName(mNext, type, level));
                // add the name for the registration job that maybe associated
                tc.setRegName(getRegJobName(mNext, level));
                mTXContainers.set(mNext, tc);
            } else {
                tc = (TransferContainer) obj;
            }
            tc.addTransfer(file);

            // update the next pointer to maintain
            // round robin status
            mNext = (mNext < (mCapacity - 1)) ? mNext + 1 : 0;

            return tc;
        }

        /**
         * Returns the iterator to the list of transfer containers.
         *
         * @return the iterator.
         */
        public Iterator getTransferContainerIterator() {
            return mTXContainers.iterator();
        }

        /**
         * Generates the name of the transfer job, that is unique for the given workflow.
         *
         * @param counter the index for the registration job.
         * @param level the level of the workflow.
         * @return the name of the transfer job.
         */
        private String getRegJobName(int counter, int level) {
            StringBuffer sb = new StringBuffer();
            sb.append(Refiner.REGISTER_PREFIX);

            // append the job prefix if specified in options at runtime
            if (mJobPrefix != null) {
                sb.append(mJobPrefix);
            }

            sb.append(mPool).append("_").append(level).append("_").append(counter);

            return sb.toString();
        }

        /**
         * Return the pool for which the transfers are grouped
         *
         * @return name of pool.
         */
        public String getPoolName() {
            return this.mPool;
        }

        /**
         * Generates the name of the transfer job, that is unique for the given workflow.
         *
         * @param counter the index for the transfer job.
         * @param type the type of transfer job.
         * @param level the level of the workflow.
         * @return the name of the transfer job.
         */
        private String getTXJobName(int counter, int type, int level) {
            StringBuffer sb = new StringBuffer();
            switch (type) {
                case Job.STAGE_IN_JOB:
                    sb.append(Refiner.STAGE_IN_PREFIX);
                    break;

                case Job.STAGE_OUT_JOB:
                    sb.append(Refiner.STAGE_OUT_PREFIX);
                    break;

                default:
                    throw new RuntimeException("Wrong type specified " + type);
            }

            if (mLocalTransfer) {
                sb.append(Refiner.LOCAL_PREFIX);
            } else {
                sb.append(Refiner.REMOTE_PREFIX);
            }
            // append the job prefix if specified in options at runtime
            if (mJobPrefix != null) {
                sb.append(mJobPrefix);
            }

            sb.append(mPool).append("_").append(level).append("_").append(counter);

            return sb.toString();
        }
    }

    protected class ClusterValue {

        /** The pegasus profile key to use for lookup */
        private String mProfileKey;

        /** The default bundle value to use. */
        private int mDefaultBundleValue;

        /** The Default Pegasus profile key to use for lookup */
        private String mDefaultProfileKey;

        /** The default constructor. */
        public ClusterValue() {}

        /**
         * Initializes the implementation
         *
         * @param key the Pegasus Profile key to be used for lookup of bundle values.
         * @param defaultKey the default Profile Key to be used if key is not found.
         * @param defaultValue the default value to be associated if no key is found.
         */
        public void initialize(String key, String defaultKey, int defaultValue) {
            mProfileKey = key;
            mDefaultProfileKey = defaultKey;
            mDefaultBundleValue = defaultValue;
        }

        /**
         * Determines the bundle factor for a particular site on the basis of the stage in bundle
         * value associcated with the underlying transfer transformation in the transformation
         * catalog. If the key is not found, then the default value is returned. In case of the
         * default value being null the global default is returned.
         *
         * <p>The value is tored internally to ensure that a subsequent call to get(String site)
         * returns the value determined.
         *
         * @param implementation the transfer implementation being used
         * @param job the compute job for which the bundle factor needs to be determined.
         * @return the bundle factor.
         */
        public int determine(Implementation implementation, Job job) {
            return this.determine(implementation, job, mDefaultBundleValue);
        }

        /**
         * Determines the bundle factor for a particular site on the basis of the stage in bundle
         * value associcated with the underlying transfer transformation in the transformation
         * catalog. If the key is not found, then the default value is returned. In case of the
         * default value being null the global default is returned.
         *
         * <p>The value is stored internally to ensure that a subsequent call to get(String site)
         * returns the value determined.
         *
         * @param implementation the transfer implementation being used
         * @param job the compute job for which the bundle factor needs to be determined.
         * @param defaultValue the default value to use
         * @return the bundle factor.
         */
        public int determine(Implementation implementation, Job job, int defaultValue) {
            String site = job.getStagingSiteHandle();

            // look up the value in SiteCatalogEntry for the store
            SiteCatalogEntry entry = BalancedCluster.this.mSiteStore.lookup(site);

            // check if a profile are set in site catalog entry
            String profileValue = null;
            if (entry != null) {
                // check for Pegasus Profile mProfileKey in the site entry
                Pegasus profiles = (Pegasus) entry.getProfiles().get(NAMESPACES.pegasus);
                profileValue = profiles.getStringValue(mProfileKey);
                if (profileValue == null) {
                    // try to look up value of default key
                    profileValue = profiles.getStringValue(mDefaultProfileKey);
                    if (profileValue == null) {
                        BalancedCluster.this.logDefferedScalingProfileMessage(
                                mDefaultProfileKey, site);
                    }
                } else {
                    BalancedCluster.this.logDefferedScalingProfileMessage(mProfileKey, site);
                }
            }

            // if value is still null, grab the default value
            // when initialized from properties
            return (profileValue == null && mDefaultBundleValue != BalancedCluster.NO_PROFILE_VALUE)
                    ? mDefaultBundleValue
                    : // the value used in the properties file
                    defaultValue;
        }
    }
}
