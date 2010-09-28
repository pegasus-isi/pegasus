/**
 *  Copyright 2007-2008 University Of Southern California
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.griphyn.cPlanner.engine;

import edu.isi.pegasus.planner.catalog.site.classes.FileServer;
import edu.isi.pegasus.planner.catalog.site.classes.SiteCatalogEntry;

import edu.isi.pegasus.common.logging.LogManager;

import org.griphyn.cPlanner.classes.ADag;
import org.griphyn.cPlanner.classes.FileTransfer;
import org.griphyn.cPlanner.classes.NameValue;
import org.griphyn.cPlanner.classes.PegasusFile;
import org.griphyn.cPlanner.classes.ReplicaLocation;
import org.griphyn.cPlanner.classes.SubInfo;
import org.griphyn.cPlanner.classes.PegasusBag;


import org.griphyn.cPlanner.common.PegasusProperties;
import org.griphyn.cPlanner.common.Utility;

import org.griphyn.cPlanner.engine.createdir.S3;

import org.griphyn.cPlanner.namespace.VDS;

import org.griphyn.cPlanner.partitioner.graph.GraphNode;
import org.griphyn.cPlanner.partitioner.graph.Graph;
import org.griphyn.cPlanner.partitioner.graph.Adapter;

import edu.isi.pegasus.planner.selector.ReplicaSelector;
import edu.isi.pegasus.planner.selector.replica.ReplicaSelectorFactory;

import edu.isi.pegasus.planner.transfer.Refiner;
import edu.isi.pegasus.planner.transfer.refiner.RefinerFactory;

import org.griphyn.cPlanner.engine.createdir.Implementation;

import edu.isi.pegasus.planner.catalog.ReplicaCatalog;
import edu.isi.pegasus.planner.catalog.replica.ReplicaCatalogEntry;

import edu.isi.pegasus.common.util.FactoryException;

import org.griphyn.vdl.euryale.FileFactory;
import org.griphyn.vdl.euryale.VirtualDecimalHashedFileFactory;
import org.griphyn.vdl.euryale.VirtualFlatFileFactory;

import java.io.File;
import java.io.IOException;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;
import java.util.Properties;



/**
 * The transfer engine, which on the basis of the pools on which the jobs are to
 * run, adds nodes to transfer the data products.
 *
 * @author Karan Vahi
 * @author Gaurang Mehta
 * @version $Revision$
 *
 */
public class TransferEngine extends Engine {

    /**
     * The MAX level is assigned as the level for deleted jobs.
     * We can put it to Integer.MAX_VALUE, but it is rare that number of levels
     * in a workflows exceed 1000.
     */
    public static final int DELETED_JOBS_LEVEL = 1000;

    /**
     * The scheme name for file url.
     */
    public static final String FILE_URL_SCHEME = "file:";

    /**
     * The scheme name for file url.
     */
    public static final String SYMLINK_URL_SCHEME = "symlink:";

    /**
     * The property prefix for retrieving SRM properties.
     */
    public static final String SRM_PROPERTIES_PREFIX = "pegasus.transfer.srm";
    
    /**
     * The suffix to retrive the service url for SRM server.
     */
    public static final String SRM_SERVICE_URL_PROPERTIES_SUFFIX = "service.url";
    
    /**
     * The suffix to retrive the mount point for SRM server.
     */
    public static final String SRM_MOUNT_POINT_PROPERTIES_SUFFIX = "mountpoint";
    
    /**
     * A map that associates the site name with the SRM server url and mount point. 
     */ 
    private Map<String, NameValue> mSRMServiceURLToMountPointMap;

    /**
     * The classname of the class that stores the file transfer information for
     * a transfer object.
     *
     * @see org.griphyn.cPlanner.classes.FileTransfer
     */
    private static final String FILE_TX_CLASS_NAME =
        "org.griphyn.cPlanner.classes.FileTransfer";

    /**
     * The DAG object to which the transfer nodes are to be added. This is the
     * reduced Dag, which is got from the Reduction Engine.
     */
    private ADag mDag;

    /**
     * The bridge to the Replica Catalog.
     */
    private ReplicaCatalogBridge mRCBridge;

    /**
     * The handle to the replica selector that is to used to select the various
     * replicas.
     */
    private ReplicaSelector mReplicaSelector;

    /**
     * The handle to the transfer refiner that adds the transfer nodes into the
     * workflow.
     */
    private Refiner mTXRefiner;

    
    /**
     * Holds all the jobs deleted by the reduction algorithm.
     */
    private List mDeletedJobs;
    
    /**
     * Holds the jobs from the original dags which are deleted by the reduction
     * algorithm.
     */
    private List mDeletedLeafJobs;


    /**
     * A SimpleFile Replica Catalog, that tracks all the files that are being
     * materialized as part of workflow executaion.
     */
    private ReplicaCatalog mTransientRC;


    /**
     * The handle to the file factory, that is  used to create the top level
     * directories for each of the partitions.
     */
    private FileFactory mFactory;

    /**
     * The base path for the stageout directory on the output site where all
     * the files are staged out.
     */
    private String mStageOutBaseDirectory;
    
    /**
     * The working directory relative to the mount point of the execution pool.
     * It is populated from the pegasus.dir.exec property from the properties file.
     * If not specified then it work_dir is supposed to be the exec mount point
     * of the execution pool.
     */
    protected String mWorkDir;

    /**
     * This contains the storage directory relative to the se mount point of the
     * pool. It is populated from the pegasus.dir.storage property from the properties
     * file. If not specified then the storage directory is the se mount point
     * from the pool.config file.
     */
    protected String mStorageDir;


    /**
     * A boolean indicating whether to have a deep directory structure for
     * the storage directory or not.
     */
    protected boolean mDeepStorageStructure;
    
    /**
     * This member variable if set causes the source url for the pull nodes from
     * the RLS to have file:// url if the pool attributed associated with the pfn
     * is same as a particular jobs execution pool.
     */
    protected boolean mUseSymLinks;
    
    /**
     * A boolean indicating whether we are doing worker node execution or not.
     */
    private boolean mWorkerNodeExecution;

            
    /**
     * A Boolean indicating whether S3 is being used for backend storage.
     */
    private boolean mS3BucketUsedForStorage;
    
    /**
     * An instance to the Create Direcotry Implementation being used in Pegasus.
     */
    private Implementation mCreateDirImpl;
    

    /**
     * Overloaded constructor.
     *
     * @param reducedDag  the reduced workflow.
     * @param bag         bag of initialization objects
     * @param deletedJobs     list of all jobs deleted by reduction algorithm.
     * @param deletedLeafJobs list of deleted leaf jobs by reduction algorithm.
     */
    public TransferEngine( ADag reducedDag,
                           PegasusBag bag,
                           List<SubInfo> deletedJobs ,
                           List<SubInfo> deletedLeafJobs){
        super( bag );

        
        mWorkDir = mProps.getExecDirectory();
        mStorageDir = mProps.getStorageDirectory();
        mDeepStorageStructure = mProps.useDeepStorageDirectoryStructure();
        mUseSymLinks = mProps.getUseOfSymbolicLinks();
        mWorkerNodeExecution = mProps.executeOnWorkerNode();
        
        mSRMServiceURLToMountPointMap = constructSiteToSRMServerMap( mProps );
        
        mDag = reducedDag;
        mDeletedJobs     = deletedJobs;
        mDeletedLeafJobs = deletedLeafJobs;

        //mS3BucketURL = getS3BucketURL( bag );
        //figure out if we are creating any s3 buckets or not
        mCreateDirImpl = 
                CreateDirectory.loadCreateDirectoryImplementationInstance(bag);
        mS3BucketUsedForStorage =  mCreateDirImpl instanceof org.griphyn.cPlanner.engine.createdir.S3 ?
                                   true:
                                   false;
           
        
        
        try{
            mTXRefiner = RefinerFactory.loadInstance( reducedDag,
                                                      bag );
            mReplicaSelector = ReplicaSelectorFactory.loadInstance(mProps);
        }
        catch(Exception e){
            //wrap all the exceptions into a factory exception
            throw new FactoryException("Transfer Engine ", e);
        }

        this.initializeStageOutSiteDirectoryFactory( reducedDag );

        //log some configuration messages
        mLogger.log("Transfer Refiner loaded is [" + mTXRefiner.getDescription() +
                            "]",LogManager.CONFIG_MESSAGE_LEVEL);
        mLogger.log("ReplicaSelector loaded is [" + mReplicaSelector.description() +
                    "]",LogManager.CONFIG_MESSAGE_LEVEL);
    }

    /**
     * Returns whether to run a transfer job on local site or not.
     *
     *
     * @param site  the site handle associated with the destination URL.
     * @param destURL the destination URL
     * @param type  the type of transfer job for which the URL is being constructed.
     *
     * @return true indicating if the associated transfer job should run on local
     *              site or not.
     */
    public boolean runTransferOnLocalSite( String site, String destinationURL, int type) {
        //check if user has specified any preference in config
        boolean result = true;

        //short cut for local site
        if( site.equals( "local" ) ){
            //transfer to run on local site
            return result;
        }

        
        if( mTXRefiner.refinerPreferenceForTransferJobLocation() ){
            //refiner is advertising a preference for where transfer job
            //should be run. Use that.
            return mTXRefiner.refinerPreferenceForLocalTransferJobs( type );
        }
        
        if( result = mTXRefiner.runTransferRemotely( site, type )){
            //always use user preference
            return !result;
        }
        //check to see if destination URL is a file url
        else if( destinationURL != null && destinationURL.startsWith( TransferEngine.FILE_URL_SCHEME ) ){
           result = false;
            
        }

        return result;
    }

    /**
     * Returns the SubInfo object for the job specified.
     *
     * @param jobName  the name of the job
     *
     * @return  the SubInfo object for a job.
     */
    private SubInfo getSubInfo(String jobName) {
        return mDag.getSubInfo(jobName);
    }

    /**
     * Adds the transfer nodes to the workflow.
     *
     * @param rcb                the bridge to the ReplicaCatalog.
     * @param transientCatalog   an instance of the replica catalog that will
     *                           store the locations of the files on the remote
     *                           sites.
     */
    public void addTransferNodes( ReplicaCatalogBridge rcb, ReplicaCatalog transientCatalog ) {
        mRCBridge = rcb;
        mTransientRC = transientCatalog;

        SubInfo currentJob;
        String currentJobName;
        Vector vOutPoolTX;
//        int noOfJobs = mDag.getNoOfJobs();
//        int counter = 0;
        String msg;
        String outputSite = mPOptions.getOutputSite();


        //convert the dag to a graph representation and walk it
        //in a top down manner
        Graph workflow = Adapter.convert( mDag );

        //go through each job in turn

        boolean stageOut = (( outputSite != null ) && ( outputSite.trim().length() > 0 ));

        for( Iterator it = workflow.iterator(); it.hasNext(); ){
            GraphNode node = ( GraphNode )it.next();
            currentJob = (SubInfo)node.getContent();
            //set the node depth as the level
            currentJob.setLevel( node.getDepth() );
            currentJobName = currentJob.getName();

            mLogger.log("",LogManager.DEBUG_MESSAGE_LEVEL);
            msg = "Job being traversed is " + currentJobName;
            mLogger.log(msg, LogManager.DEBUG_MESSAGE_LEVEL);
            msg = "To be run at " + currentJob.executionPool;
            mLogger.log(msg, LogManager.DEBUG_MESSAGE_LEVEL);

            //getting the parents of that node
            Vector vParents = mDag.getParents(currentJobName);
            mLogger.log(vectorToString("Parents of job:", vParents),
                        LogManager.DEBUG_MESSAGE_LEVEL);
            processParents(currentJob, vParents);

            //transfer the nodes output files
            //to the output pool
            if ( stageOut ) {
                boolean localTransfer = runTransferOnLocalSite( currentJob.getSiteHandle(), null, SubInfo.STAGE_OUT_JOB);
                vOutPoolTX = getFileTX(outputSite, currentJob, localTransfer );
                mTXRefiner.addStageOutXFERNodes( currentJob, vOutPoolTX, rcb, localTransfer );
            }

            if( mPOptions.partOfDeferredRun() && !stageOut ){
                //create the cache file always for deferred runs
                //Pegasus Bug 34
                trackInTransientRC( currentJob );
            }
        }

        //we are done with the traversal.
        //mTXRefiner.done();

        //get the deleted leaf jobs o/p files to output pool
        //only if output pool is specified
        //should be moved upwards in the pool. redundancy at present
        if (outputSite != null &&
            outputSite.trim().length() > 0
            && !mDeletedJobs.isEmpty() ) {

            mLogger.log( "Adding stage out jobs for jobs deleted from the workflow", LogManager.INFO_MESSAGE_LEVEL );

            for( Iterator it = this.mDeletedJobs.iterator(); it.hasNext() ;) {
                currentJob = (SubInfo)it.next();
                currentJob.setLevel(  TransferEngine.DELETED_JOBS_LEVEL );
                
                //for a deleted node, to transfer it's output
                //the execution pool should be set to local i.e submit host
                currentJob.executionPool = "local";

                vOutPoolTX = getDeletedFileTX(outputSite, currentJob);
                if( !vOutPoolTX.isEmpty() ){
                    mTXRefiner.addStageOutXFERNodes( currentJob, vOutPoolTX, rcb, true );
                }

            }
        }

        //we are done with the traversal.
        mTXRefiner.done();


        //close the handle to the cache file if it is written
        //closeTransientRC();
    }

    /**
     * This gets the file transfer objects corresponding to the location of files
     * found in the replica mechanism, and transfers it to the output pool asked
     * by the user. If the output pool path and the one returned by the replica
     * mechanism match then that object is not transferred.
     *
     * @param pool    this the output pool which the user specifies at runtime.
     * @param job     The SubInfo object corresponding to the leaf job which was
     *                deleted by the Reduction algorithm
     *
     * @return        Vector of <code>FileTransfer</code> objects
     */
    private Vector getDeletedFileTX( String pool, SubInfo job ) {
        Vector vFileTX = new Vector();
        SiteCatalogEntry p = mSiteStore.lookup(pool);//getPoolEntry( pool, "vanilla" );

        for( Iterator it = job.getOutputFiles().iterator(); it.hasNext(); ){
            PegasusFile pf = (PegasusFile)it.next();
            String  lfn = pf.getLFN();

            //we only have to get a deleted file that user wants to be transferred
            if( pf.getTransientTransferFlag() ){
                continue;
            }
            
            ReplicaLocation rl = mRCBridge.getFileLocs( lfn );
            //sanity check
            if( rl == null ){
                throw new RuntimeException( "Unable to find a location in the Replica Catalog for output file "  + lfn );
            }



            //definite inconsitency as url prefix and mount point
            //are not picked up from the same server
            String destURL = //p.getURLPrefix(true) +
                                 p.getHeadNodeFS().selectScratchSharedFileServer().getURLPrefix() +
                                 this.getPathOnStageoutSite( lfn );

            //selLocs are all the locations found in ReplicaMechanism corr
            //to the pool pool
            ReplicaLocation selLocs = mReplicaSelector.selectReplicas( rl,
                                                                       pool,
                                                                       this.runTransferOnLocalSite( pool,destURL, SubInfo.STAGE_OUT_JOB ));


            boolean flag = false;

            FileTransfer ft = null;
            //checking through all the pfn's returned on the pool
            for ( Iterator selIt = selLocs.pfnIterator(); selIt.hasNext(); ) {
                ReplicaCatalogEntry selLoc = ( ReplicaCatalogEntry ) selIt.next();
                String sourceURL = selLoc.getPFN();

                //check if the URL's match
                if (sourceURL.trim().equalsIgnoreCase(destURL.trim())){
                    String msg = "The leaf file " + lfn +
                        " is already at the output pool " + pool;
                    mLogger.log(msg,LogManager.INFO_MESSAGE_LEVEL);
                    flag = true;
                    break;
                }


                ft = new FileTransfer( lfn, job.getName() );
                ft.addSource( selLoc.getResourceHandle() , sourceURL );
                ft.addDestination( pool, destURL );

                //System.out.println("Deleted Leaf Job File transfer object " + ft);

            }
            if (!flag) { //  adding the last pfn
                vFileTX.addElement(ft);
            }
        }
        return vFileTX;
    }

    /**
     * It processes a nodes parents and determines if nodes are to be added
     * or not. All the input files for the job are searched in the output files of
     * the parent nodes and the Replica Mechanism.
     *
     * @param job       the <code>SubInfo</code> object containing all the
     *                  details of the job.
     * @param vParents  Vector of String objects corresponding to the Parents
     *                  of the node.
     */
    private void processParents(SubInfo job, Vector vParents) {

        Set nodeIpFiles = job.getInputFiles();
        Vector vRCSearchFiles = new Vector(); //vector of PegasusFile
        Vector vIPTxFiles = new Vector();
        Vector vParentSubs = new Vector();

        //getAll the output Files of the parents
        Set parentsOutFiles = getOutputFiles(vParents, vParentSubs);


        //interpool transfer of the nodes parents
        //output files
        Collection[] interSiteFileTX = this.getInterpoolFileTX(job, vParentSubs);
        Collection localInterSiteTX = interSiteFileTX[0];
        Collection remoteInterSiteTX = interSiteFileTX[1];

        //only add if there are files to transfer
        if( !localInterSiteTX.isEmpty()){
            mTXRefiner.addInterSiteTXNodes(job, localInterSiteTX, true );
        }
        if( !remoteInterSiteTX.isEmpty() ){
            mTXRefiner.addInterSiteTXNodes(job, remoteInterSiteTX, false );
        }

        //check if node ip files are in the parents out files
        //if files are not, then these are to be got
        //from the RC based on the transiency characteristic
        for( Iterator it = nodeIpFiles.iterator(); it.hasNext(); ){
            PegasusFile pf = (PegasusFile) it.next();
            if( !parentsOutFiles.contains( pf ) ){
                if (!pf.getTransientTransferFlag()) {
                    vRCSearchFiles.addElement(pf);
                }
            }
        }

        if (!vRCSearchFiles.isEmpty()) {
            //get the locations from the RC
            getFilesFromRC(job, vRCSearchFiles);
        }
    }

    /**
     * This gets the Vector of FileTransfer objects for the files which have to
     * be transferred to an one destination pool. It checks for the transient
     * flags for files. If the transfer transient flag is set, it means the file
     * does not have to be transferred to the destination pool.
     *
     * @param destPool The pool to which the files are to be transferred to.
     * @param job      The <code>SubInfo</code>object of the job whose output files
     *                 are needed at the destination pool.
     * @param localTransfer  boolean indicating that associated transfer job will run
     *                       on local site.
     *
     * @return        Vector of <code>FileTransfer</code> objects
     */
    private Vector getFileTX(String destPool, SubInfo job, boolean localTransfer ) {
        Vector vFileTX = new Vector();

        //check if there is a remote initialdir set
        String path  = job.vdsNS.getStringValue(
                                                 VDS.REMOTE_INITIALDIR_KEY );

        for( Iterator it = job.getOutputFiles().iterator(); it.hasNext(); ){
            PegasusFile pf = (PegasusFile) it.next();
            String file = pf.getLFN();


            FileTransfer ft = this.constructFileTX( pf, job.executionPool,
                                                    destPool, job.logicalName,
                                                    path,
                                                    localTransfer );
            if (ft != null) {
                vFileTX.add(ft);

            }

        }

        return vFileTX;

    }


    
    /**
     * Constructs the FileTransfer object on the basis of the transiency
     * information. If the transient flag for transfer is set, the destURL for the
     * FileTransfer object would be the execution directory, as this is the entry
     * that has to be registered in the ReplicaMechanism
     *
     * @param pf          the PegasusFile for which the transfer has to be done.
     * @param execPool    the pool on which the file is created.
     * @param destPool    the output pool where the job should be transferred
     * @param job         the name of the associated job.
     * @param path        the path that a user specifies in the profile for key
     *                    remote_initialdir that results in the workdir being
     *                    changed for a job on a execution pool.
     * @param localTransfer  boolean indicating that associated transfer job will run
     *                       on local site.

     *
     * @return   the corresponding FileTransfer object
     */
    private FileTransfer constructFileTX( PegasusFile pf, String execPool,
                                          String destPool, String job, String path,
                                          boolean localTransfer ) {

        String lfn = pf.getLFN();
        FileTransfer ft = null;

        SiteCatalogEntry ePool = mSiteStore.lookup( execPool );
        SiteCatalogEntry dPool = mSiteStore.lookup( destPool );
        if (ePool == null || dPool == null) {
            mLogMsg = (ePool == null) ?
                this.poolNotFoundMsg(execPool, "vanilla") :
                this.poolNotFoundMsg(destPool, "vanilla");
            mLogger.log( mLogMsg, LogManager.ERROR_MESSAGE_LEVEL );
            throw new RuntimeException( mLogMsg );
        }

        //definite inconsitency as url prefix and mount point
        //are not picked up from the same server
        String execURL = ePool.getHeadNodeFS().selectScratchSharedFileServer().getURLPrefix() +
            mSiteStore.getWorkDirectory(execPool,path) + File.separatorChar +
            lfn;

        //write out the exec url to the cache file
        trackInTransientRC(lfn,execURL,execPool);

        //if both transfer and registration
        //are transient return null
        if (pf.getTransientRegFlag() && pf.getTransientTransferFlag()) {
            return null;
        }

        //if only transient transfer flag
        //means destURL and sourceURL
        //are same and are equal to
        //execution directory on execPool
        if (pf.getTransientTransferFlag()) {

            ft = new FileTransfer(lfn,job,pf.getFlags());
            //set the transfer mode
            ft.setTransferFlag(pf.getTransferFlag());
            ft.addSource(execPool,execURL);
            ft.addDestination(execPool,execURL);
        }
        //the source dir is the exec dir
        //on exec pool and dest dir
        //would be on the output pool
        else {
            //construct the source url depending on whether third party tx
//            String sourceURL = runTransferOnLocalSite( execPool, null, SubInfo.STAGE_OUT_JOB) ?
            String sourceURL = localTransfer ?
                                execURL :
                                "file://" + mSiteStore.getWorkDirectory(execPool,path) +
                                File.separator + lfn;

            ft = new FileTransfer(lfn,job,pf.getFlags());
            //set the transfer mode
            ft.setTransferFlag(pf.getTransferFlag());

            ft.addSource(execPool,sourceURL);

            //if the PegasusFile is already an instance of
            //FileTransfer the user has specified the destination
            //that they want to use in the DAX 3.0 
            if( pf instanceof FileTransfer ){
                ft.addDestination( ((FileTransfer)pf).removeDestURL() );
                return ft;
            }

            //add all the possible destination urls iterating through
            //the list of grid ftp servers associated with the dest pool.
            Iterator it = mSiteStore.lookup( destPool ).getHeadNodeFS().getStorage().getSharedDirectory().getFileServersIterator();
            //sanity check
            if( !it.hasNext() ){
                // no file servers were returned
                throw new RuntimeException( " No File Servers specified for Shared Storage on Headnode for site " + destPool );
            }
            
            String destURL = null;
            boolean first = true;
            while(it.hasNext()){
                /*
                destURL = (first)?
                          //the first entry has to be the one in the Pool object
                          dPool.getURLPrefix(false):
                          //get it from the list
                          ((GridFTPServer)it.next()).getInfo(GridFTPServer.GRIDFTP_URL);
                 */
                FileServer fs = (FileServer)it.next();
                destURL = fs.getURLPrefix() ;

                /*
                if(!first && destURL.equals(dPool.getURLPrefix(false))){
                    //ensures no duplicate entries. The gridftp server in the pool
                    //object is one of the servers in the list of gridftp servers.
                    continue;
                }
                 */


                //assumption of same se mount point for each gridftp server
                destURL += this.getPathOnStageoutSite( lfn );//  + File.separator + lfn;


                //if the paths match of dest URI
                //and execDirURL we return null
                if (execURL.equalsIgnoreCase(destURL)) {
                    /*ft = new FileTransfer(file, job);
                    ft.addSource(execPool, execURL);*/
                    ft.addDestination(execPool, execURL);
                    //make the transfer transient?
                    ft.setTransferFlag(PegasusFile.TRANSFER_NOT);
                    return ft;
                }

                ft.addDestination(destPool, destURL);
                first = false;
            }

        }

        return ft;
    }

    /**
     * This generates a error message for pool not found in the pool
     * config file.
     *
     * @param poolName  the name of pool that is not found.
     * @param universe  the condor universe
     *
     * @return the message.
     */
    private String poolNotFoundMsg(String poolName, String universe) {
        String st = "Error: No matching entry to pool = " + poolName +
            " ,universe = " + universe +
            "\n found in the pool configuration file ";
        return st;

    }

    /**
     * This gets the Vector of FileTransfer objects for all the files which have
     * to be transferred to the destination pool in case of Interpool transfers.
     * Each FileTransfer object has the source and the destination URLs. the
     * source URI is determined from the pool on which the jobs are executed.
     *
     * @param job     the job with reference to which interpool file transfers
     *                need to be determined.
     * @param nodes   Vector of <code> SubInfo</code> objects for the nodes, whose
     *                outputfiles are to be transferred to the dest pool.
     *
     * @return        Vector of <code>FileTransfer</code> objects
     */
    private Collection<FileTransfer>[] getInterpoolFileTX(SubInfo job, Vector nodes) {
        String destPool = job.executionPool;
        //contains the remote_initialdir if specified for the job
        String destRemoteDir = job.vdsNS.getStringValue(
                                                 VDS.REMOTE_INITIALDIR_KEY);

        SiteCatalogEntry desPool = mSiteStore.lookup( destPool );
        SiteCatalogEntry sourcePool;

        Collection[] result = new Collection[2];
        Collection<FileTransfer> localTransfers  = new LinkedList();
        Collection<FileTransfer> remoteTransfers = new LinkedList();

        for (Iterator it = nodes.iterator();it.hasNext();) {
            //get the parent job
            SubInfo pJob = (SubInfo)it.next();
            sourcePool = mSiteStore.lookup( pJob.getSiteHandle() );

            if( sourcePool.getSiteHandle().equalsIgnoreCase(destPool) ){
                //no need to add transfers, as the parent job and child
                //job are run in the same directory on the pool
                continue;
            }

            String sourceURI = null;
            String thirdPartyDestURI = desPool.getHeadNodeFS().selectScratchSharedFileServer().getURLPrefix() +
                                        mSiteStore.getWorkDirectory( destPool, destRemoteDir );

            //definite inconsitency as url prefix and mount point
            //are not picked up from the same server
            boolean localTransfer = runTransferOnLocalSite( destPool, thirdPartyDestURI, SubInfo.INTER_POOL_JOB );
            String destURI = localTransfer ?
                //construct for third party transfer
                thirdPartyDestURI :
                //construct for normal transfer
                "file://" + mSiteStore.getWorkDirectory( destPool, destRemoteDir );


            for (Iterator fileIt = pJob.getOutputFiles().iterator(); fileIt.hasNext(); ){
                PegasusFile pf = (PegasusFile) fileIt.next();
                String outFile = pf.getLFN();

               if( job.getInputFiles().contains( pf ) ){
                    String sourceURL     = null;
                    String destURL       = destURI + File.separator + outFile;
                    String thirdPartyDestURL = thirdPartyDestURI + File.separator +
                                           outFile;
                    FileTransfer ft      = new FileTransfer(outFile,pJob.jobName);
                    ft.addDestination(destPool,destURL);

                    //add all the possible source urls iterating through
                    //the list of grid ftp servers associated with the dest pool.
                    boolean first = true;
                    for( Iterator it1 = mSiteStore.lookup( pJob.getSiteHandle() ).getHeadNodeFS().getScratch().getSharedDirectory().getFileServersIterator(); 
                                      it1.hasNext();){
                        FileServer server = ( FileServer)it1.next();
                        //definite inconsitency as url prefix and mount point
                        //are not picked up from the same server
                        sourceURI = server.getURLPrefix();
                                                                          
                        sourceURI += mSiteStore.getWorkDirectory(  pJob.executionPool,
                                                                   pJob.vdsNS.getStringValue(VDS.REMOTE_INITIALDIR_KEY));
                        
                        sourceURL = sourceURI + File.separator + outFile;

                        if(!(sourceURL.equalsIgnoreCase(thirdPartyDestURL))){
                            //add the source url only if it does not match to
                            //the third party destination url
                            ft.addSource(pJob.executionPool, sourceURL);
                        }
                        first = false;
                    }
                    if( ft.isValid() ){
                        if( localTransfer ){
                            localTransfers.add(ft);
                        }
                        else{
                            remoteTransfers.add(ft);
                        }
                    }
                }
                
            }


        }

        result[0] = localTransfers;
        result[1] = remoteTransfers;
        return result;

    }

    /**
     * It looks up the RCEngine Hashtable to lookup the locations for the
     * files and add nodes to transfer them. If a file is not found to be in
     * the Replica Catalog the Transfer Engine flags an error and exits
     *
     * @param job           the <code>SubInfo</code>object for whose ipfile have
     *                      to search the Replica Mechanism for.
     * @param searchFiles   Vector containing the PegasusFile objects corresponding
     *                      to the files that need to have their mapping looked
     *                      up from the Replica Mechanism.
     */
    private void getFilesFromRC( SubInfo job, Vector searchFiles ) {
        //Vector vFileTX = new Vector();
        //Collection<FileTransfer> symLinkFileTransfers = new LinkedList();
        Collection<FileTransfer> localFileTransfers = new LinkedList();
        Collection<FileTransfer> remoteFileTransfers = new LinkedList();


        String jobName = job.logicalName;
        String ePool   = job.executionPool;
        //contains the remote_initialdir if specified for the job
        String eRemoteDir = job.vdsNS.getStringValue(
                                                 VDS.REMOTE_INITIALDIR_KEY);
        
        SiteCatalogEntry ep        = mSiteStore.lookup( ePool );
        //we are using the pull mode for data transfer
        String scheme  = "file";

        //sAbsPath would be just the source directory absolute path
        //dAbsPath would be just the destination directory absolute path
        String dAbsPath = mSiteStore.getWorkDirectory( ePool, eRemoteDir );
        String sAbsPath = null;

        //sDirURL would be the url to the source directory.
        //dDirURL would be the url to the destination directoy
        //and is always a networked url.
        //definite inconsitency as url prefix and mount point
        //are not picked up from the same server
        String dDirURL = ep.getHeadNodeFS().selectScratchSharedFileServer( ).getURLPrefix() + dAbsPath;
        String sDirURL = null;
        
        
        //file dest dir is destination dir accessed as a file URL
        String fileDestDir = scheme + "://" + mSiteStore.getWorkDirectory( ePool, eRemoteDir );
                
        //check if the execution pool is third party or not
        boolean runTransferOnLocalSite = runTransferOnLocalSite( ePool, dDirURL, SubInfo.STAGE_IN_JOB);
        String destDir = ( runTransferOnLocalSite ) ?
            //use the full networked url to the directory
            dDirURL
            :
            //use the default pull mode
            fileDestDir;


        for( Iterator it = searchFiles.iterator(); it.hasNext(); ){
            String sourceURL,destURL=null;
            PegasusFile pf = (PegasusFile) it.next();
            List  pfns   = null;
            ReplicaLocation rl = null;

            String lfn     = pf.getLFN();
            NameValue nv   = null;

            //see if the pf is infact an instance of FileTransfer
            if( pf instanceof FileTransfer ){
                //that means we should be having the source url already.
                //nv contains both the source pool and the url.
                //This happens in case of AI Planner or transfer of executables
                nv = ((FileTransfer)pf).getSourceURL();
                
                NameValue destNV = ((FileTransfer)pf).removeDestURL();
                if( destNV == null ){
                    //the source URL was specified in the DAX
                    //no transfer of executables case
                }
                else{//staging of executables case
                    destURL = destNV.getValue();
                    destURL = (runTransferOnLocalSite(ePool, destURL, SubInfo.STAGE_IN_JOB))?
                               //the destination URL is already third party
                               //enabled. use as it is
                               destURL:
                               //explicitly convert to file URL scheme
                               scheme + "://" + Utility.getAbsolutePath(destURL);
                }
            }
            else{
                //query the replica services and get hold of pfn
                rl = mRCBridge.getFileLocs( lfn );
                pfns = (rl == null) ? null: rl.getPFNList();
            }

            if ( pfns == null && nv == null ) {
                //check to see if the input file is optional
                if(pf.fileOptional()){
                    //no need to add a transfer node for it if no location found

                    //remove the PegasusFile object from the list of
                    //input files for the job
                    boolean removed = job.getInputFiles().remove( pf );
                    //System.out.println( "Removed " + pf.getLFN() + " " + removed );

                    continue;
                }

                //flag an error
                throw new RuntimeException(
                           "TransferEngine.java: Can't determine a location to " +
                           "transfer input file for lfn " + lfn + " for job " +
                           job.getName());
            }

            
            ReplicaCatalogEntry selLoc = (nv == null)?
                                        //select from the various replicas
                                        mReplicaSelector.selectReplica( rl, 
                                                                        job.getSiteHandle(),
                                                                        runTransferOnLocalSite ):
                                        //we have the replica already selected
                                        new ReplicaCatalogEntry( nv.getValue(), nv.getKey() );

            //check if we need to replace url prefix for 
            //symbolic linking
            boolean symLinkSelectedLocation;
            
            if ( symLinkSelectedLocation = 
                    (mUseSymLinks && selLoc.getResourceHandle().equals( job.getSiteHandle() )) ) {
                //create symbolic links instead of going through gridftp server
                selLoc = replaceProtocolFromURL( selLoc );
            }
                                        
            //get the file to the job's execution pool
            //this is assuming that there are no directory paths
            //in the pfn!!!
            sDirURL  = selLoc.getPFN().substring( 0, selLoc.getPFN().lastIndexOf(File.separator) );

            //try to get the directory absolute path
            //yes i know that we sending the url to directory
            //not the file.
            sAbsPath = Utility.getAbsolutePath(sDirURL);

            //the final source and destination url's to the file
            sourceURL = selLoc.getPFN();
            
            if( destURL == null ){
                //no staging of executables case. 
                //we construct destination URL to file.
                StringBuffer destPFN = new StringBuffer();
                if( symLinkSelectedLocation ){
                    //we use the file URL location to dest dir
                    //in case we are symlinking
                    destPFN.append( fileDestDir );
                }
                else{
                    //we use whatever destDir was set to earlier
                    destPFN.append( destDir );
                }
                destPFN.append( File.separator).append( lfn );
                destURL = destPFN.toString();
            }
            

            //we have all the chopped up combos of the urls.
            //do some funky matching on the basis of the fact
            //that each pool has one shared filesystem


            //match the source and dest 3rd party urls or
            //match the directory url knowing that lfn and
            //(source and dest pool) are same
            try{
                if(sourceURL.equalsIgnoreCase(dDirURL + File.separator + lfn)||
                     ( selLoc.getResourceHandle().equalsIgnoreCase( ePool ) &&
                       lfn.equals( sourceURL.substring(sourceURL.lastIndexOf(File.separator) + 1)) &&
                       //sAbsPath.equals( dAbsPath )
                       new File( sAbsPath ).getCanonicalPath().equals(  new File( dAbsPath).getCanonicalPath())
                     )
                 ){
                    //do not need to add any transfer node
                    StringBuffer message = new StringBuffer( );
                    
                    message.append( sAbsPath ).append( " same as " ).append( dAbsPath );
                    mLogger.log( message.toString() , LogManager.DEBUG_MESSAGE_LEVEL );
                    message = new StringBuffer();
                    message.append( " Not transferring ip file as ").append( lfn ).
                            append( " for job " ).append( job.jobName ).append( " to site " ).append( ePool);
                    
                    mLogger.log( message.toString() , LogManager.DEBUG_MESSAGE_LEVEL );
                    continue;
                }
            }catch( IOException ioe ){
                /*ignore */
            }
                
            //add locations of input data on the remote site to the transient RC
            if( mWorkerNodeExecution && selLoc.getResourceHandle().equals( job.getSiteHandle() ) ){
                //the selected replica already exists on
                //the compute site.  we can bypass first level
                //staging of the data
                //we add into transient RC the source URL without any modifications
                trackInTransientRC( lfn, sourceURL, job.getSiteHandle(), false );
                continue;
            }
            else{
                //track the location where the data is staged as 
                //part of the first level staging
                //we always store the thirdparty url
                //trackInTransientRC( lfn, destURL, job.getSiteHandle() );
                trackInTransientRC( lfn, 
                                    dDirURL + File.separator + lfn, 
                                    job.getSiteHandle());
            }
            
            //construct the file transfer object
            FileTransfer ft = (pf instanceof FileTransfer) ?
                               (FileTransfer)pf:
                               new FileTransfer( lfn, jobName );
            
            //make sure the type information is set in file transfer
            ft.setType( pf.getType() );
            
            //the transfer mode for the file needs to be
            //propogated for optional transfers.
            ft.setTransferFlag(pf.getTransferFlag());

            //to prevent duplicate source urls
            if(ft.getSourceURL() == null){
                ft.addSource( selLoc.getResourceHandle(), sourceURL);
            }

            //to prevent duplicate destination urls
            if(ft.getDestURL() == null)
                ft.addDestination(ePool,destURL);
            
            if( symLinkSelectedLocation || !runTransferOnLocalSite ){
                //all symlink transfers and user specified remote transfers
                remoteFileTransfers.add(ft);
            }
            else{
                localFileTransfers.add(ft);
            }
            
            //we need to set destURL to null
            destURL = null;
        }

        //call addTransferNode
        if (!localFileTransfers.isEmpty() || !remoteFileTransfers.isEmpty()) {
            mTXRefiner.addStageInXFERNodes(job, localFileTransfers, remoteFileTransfers );

        }
    }

    /**
     * Replaces the gsiftp URL scheme from the url, and replaces it with the
     * file url scheme and returns in a new object. The original object
     * passed as a parameter still remains the same.
     *
     * @param rce  the <code>ReplicaCatalogEntry</code> object whose url need to be
     *             replaced.
     *
     * @return  the object with the url replaced.
     */
    protected ReplicaCatalogEntry replaceProtocolFromURL( ReplicaCatalogEntry rce ) {
        String pfn = rce.getPFN();
        
        //if the pfn starts with a file url we 
        //dont need to replace . a sanity check
        if( pfn.startsWith( FILE_URL_SCHEME ) ){
            return rce;
        }
        
        /* special handling for SRM urls */
        StringBuffer newPFN = new StringBuffer();
        if( mSRMServiceURLToMountPointMap.containsKey( rce.getResourceHandle() ) ){
            //try to do replacement of URL with internal mount point
            NameValue nv = mSRMServiceURLToMountPointMap.get( rce.getResourceHandle() );
            String urlPrefix = nv.getKey();
            if( pfn.startsWith( urlPrefix ) ){
                //replace the starting with the mount point
                newPFN.append( FILE_URL_SCHEME ).append( "//" );
                newPFN.append( nv.getValue() );
                newPFN.append( pfn.substring( urlPrefix.length(), pfn.length() ));
                
            }
        }
        
        if( newPFN.length() == 0 ){
            //we have to the manual replacement 
            String hostName = Utility.getHostName( pfn );

            newPFN.append( FILE_URL_SCHEME ).append( "//" );
       
            //we want to skip out the hostname
            newPFN.append( pfn.substring( pfn.indexOf( hostName ) + hostName.length() ) );
        }

        //we do not need a full clone, just the PFN
        ReplicaCatalogEntry result = new ReplicaCatalogEntry( newPFN.toString(),
                                                              rce.getResourceHandle() );
        String key;
        for( Iterator it = rce.getAttributeIterator(); it.hasNext();){
            key = (String)it.next();
            result.addAttribute( key, rce.getAttribute( key ) );
        }

        return result;
    }
    
    /**
     * Constructs a Properties objects by parsing the relevant SRM 
     * pegasus properties. 
     * 
     * For example, if users have the following specified in properties file
     * <pre>
     * pegasus.transfer.srm.ligo-cit.service.url          srm://osg-se.ligo.caltech.edu:10443/srm/v2/server?SFN=/mnt/hadoop
     * pegasus.transfer.srm.ligo-cit.service.mountpoint   /mnt/hadoop
     * </pre>
     * 
     * then, a Map is create the associates ligo-cit with NameValue object 
     * containing the service url and mount point ( ).
     * 
     * @param props   the <code>PegasusProperties</code> object
     * 
     * @return  Map that maps a site name to a NameValue object that has the
     *          URL prefix and the mount point
     */
    private Map<String, NameValue> constructSiteToSRMServerMap( PegasusProperties props ) {
        Map<String, NameValue> m = new HashMap();
        
        //first strip of prefix from properties and get matching subset
        Properties siteProps = props.matchingSubset( TransferEngine.SRM_PROPERTIES_PREFIX, false );
        
        //retrieve all the sites for which SRM servers are specified
        Map<String, String> m1 = new HashMap(); //associates site name to url prefix
        Map<String, String> m2 = new HashMap(); //associates site name to mount point
        for( Iterator it = siteProps.keySet().iterator(); it.hasNext(); ){
            String key = (String) it.next();
            //determine the site name
            String site = key.substring( 0, key.indexOf( "." ) );
            
            if( key.endsWith( TransferEngine.SRM_SERVICE_URL_PROPERTIES_SUFFIX ) ){
                m1.put( site, siteProps.getProperty( key ) );
            }
            else if( key.endsWith( TransferEngine.SRM_MOUNT_POINT_PROPERTIES_SUFFIX ) ){
                m2.put( site, siteProps.getProperty( key ) );
            }
        }
        
        //now merge the information into m and return
        for( Iterator <Map.Entry<String, String>>it = m1.entrySet().iterator(); it.hasNext(); ){
            Map.Entry<String, String> entry = it.next();
            String site = entry.getKey();
            String url = entry.getValue();
            String mountPoint = m2.get( site );
            
            if( mountPoint == null ){
                mLogger.log( "Mount Point for SRM server not specified in properties for site " + site,
                             LogManager.WARNING_MESSAGE_LEVEL );
                continue;
            }
            
            m.put( site, new NameValue( url, mountPoint ) );
        }
        
        mLogger.log( "SRM Server map is " + m,
                     LogManager.DEBUG_MESSAGE_LEVEL );
        
        return m;
    }
    
    /**
     * It gets the output files for all the nodes which are specified in
     * the Vector nodes passed.
     *
     * @param nodes   Vector of nodes job names whose output files are required.
     *
     * @param parentSubs  Vector of <code>SubInfo</code> objects. One passes an
     *                    empty vector as a parameter. And this populated with
     *                    SubInfo objects, of the nodes when output files are
     *                    being determined.
     *
     * @return   Set of PegasusFile objects
     */
    private Set getOutputFiles(Vector nodes, Vector parentSubs) {

        Set files = new HashSet();

        for( Iterator it = nodes.iterator(); it.hasNext(); ){
            String jobName = (String) it.next();
            SubInfo sub = getSubInfo(jobName);
            parentSubs.addElement(sub);
            files.addAll( sub.getOutputFiles() );
        }

        return files;
    }


    /**
     * Returns the full path on remote output site, where the lfn will reside.
     * Each call to this function could trigger a change in the directory
     * returned depending upon the file factory being used.
     *
     * @param lfn   the logical filename of the file.
     *
     * @return the storage mount point.
     */
    protected String getPathOnStageoutSite( String lfn ){
        String file;
        try{
            file = mFactory.createFile( lfn ).toString();
         }
         catch( IOException e ){
             throw new RuntimeException( "IOException " , e );
         }
         return file;
    }

    /**
     * Initialize the Stageout Site Directory factory.
     * The factory is used to returns the relative directory that a particular
     * file needs to be staged to on the output site.
     *
     * @param workflow  the workflow to which the transfer nodes need to be
     *                  added.
     *
     */
    protected void initializeStageOutSiteDirectoryFactory( ADag workflow ){
        String outputSite = mPOptions.getOutputSite();
        boolean stageOut = (( outputSite != null ) && ( outputSite.trim().length() > 0 ));

        if (!stageOut ){
            //no initialization and return
            mLogger.log( "No initialization of StageOut Site Directory Factory",
                         LogManager.DEBUG_MESSAGE_LEVEL );
            return;
        }

        // create files in the directory, unless anything else is known.
//        mStageOutBaseDirectory = mPoolHandle.getSeMountPoint( mPoolHandle.getPoolEntry( outputSite, "vanilla") );
        //mStageOutBaseDirectory = mSiteStore.lookup( outputSite ).selectStorageFileServerForStageout().getMountPoint() ;
        mStageOutBaseDirectory = mSiteStore.getStorageDirectory( outputSite );

        if( mProps.useDeepStorageDirectoryStructure() ){
            // create hashed, and levelled directories
            try {
                VirtualDecimalHashedFileFactory temp = null;

                //get the total number of files that need to be stageout
                int totalFiles = 0;
                for ( Iterator it = workflow.jobIterator(); it.hasNext(); ){
                    SubInfo job = ( SubInfo )it.next();

                    //traverse through all the job output files
                    for( Iterator opIt = job.getOutputFiles().iterator(); opIt.hasNext(); ){
                        if( !((PegasusFile)opIt.next()).getTransientTransferFlag() ){
                            //means we have to stage to output site
                            totalFiles++;
                        }
                    }
                }

                temp = new VirtualDecimalHashedFileFactory( mStageOutBaseDirectory, totalFiles );

                //each stageout file  has only 1 file associated with it
                temp.setMultiplicator( 1 );
                mFactory = temp;
            }
            catch (IOException e) {
                //wrap into runtime and throw
                throw new RuntimeException( "While initializing HashedFileFactory", e );
            }
        }
        else{
            try {
                //Create a flat file factory
                mFactory = new VirtualFlatFileFactory( mStageOutBaseDirectory ); // minimum default
            } catch ( IOException ioe ) {
                throw new RuntimeException( "Unable to generate files in the submit directory " ,
                                            ioe );
            }
        }

    }
    
    
    /**
     * Tracks the files created by a job in the Transient Replica Catalog.
     *
     * @param job  the job whose input files need to be tracked.
     */
    protected void trackInTransientRC( SubInfo job ){


        //check if there is a remote initialdir set
        String path  = job.vdsNS.getStringValue(
                                                 VDS.REMOTE_INITIALDIR_KEY );

//        SiteInfo ePool = mPoolHandle.getPoolEntry( job.getSiteHandle(), "vanilla" );
        SiteCatalogEntry ePool = mSiteStore.lookup( job.getSiteHandle() );
        if ( ePool == null ) {
            this.poolNotFoundMsg( job.getSiteHandle(), "vanilla" ) ;
            mLogger.log( mLogMsg, LogManager.ERROR_MESSAGE_LEVEL );
            throw new RuntimeException( mLogMsg );
        }


        for( Iterator it = job.getOutputFiles().iterator(); it.hasNext(); ){
            PegasusFile pf = (PegasusFile) it.next();
            String lfn = pf.getLFN();

            //definite inconsitency as url prefix and mount point
            //are not picked up from the same server
            StringBuffer execURL = new StringBuffer();
            FileServer server = ePool.getHeadNodeFS().selectScratchSharedFileServer();
            execURL.append( server.getURLPrefix() ).
                    append( mSiteStore.getWorkDirectory( job.getSiteHandle(), path ) ).
                    append( File.separatorChar ).append( lfn );

            
            trackInTransientRC( lfn, execURL.toString(), job.getSiteHandle() );

        }
    }



    
    /**
     * Inserts an entry into the Transient RC. It modifies the PFN if the
     * workflow is running on the cloud and S3 is being used for storage..
     *
     * @param lfn  the logical name of the file.
     * @param pfn  the pfn
     * @param site the site handle
     */
    private void trackInTransientRC( String lfn, 
                                     String pfn,
                                     String site ){

        this.trackInTransientRC( lfn, pfn, site, true );
    }
    
    /**
     * Inserts an entry into the Transient RC. It modifies the PFN if the
     * workflow is running on the cloud and S3 is being used for storage, 
     * dependant on the modifyURL parameter passed.
     *
     * @param lfn  the logical name of the file.
     * @param pfn  the pfn
     * @param site the site handle
     * @param modifyURL whether to modify URL in case of S3 or not.
     */
    private void trackInTransientRC( String lfn, 
                                     String pfn,
                                     String site, 
                                     boolean modifyURL ){

        if( this.mS3BucketUsedForStorage ){
            //modify the PFN only for non raw input files.
            //This takes care of the case, where
            //the data already might be on the cloud , and first level 
            //staging is bypassed.
            if( modifyURL ){
                StringBuffer execURL = new StringBuffer();
                execURL.append( ((S3)mCreateDirImpl).getBucketNameURL( site ) ).append( File.separatorChar ).append( lfn );
                pfn = execURL.toString();
                System.out.println( execURL.toString() );
            }
        }
        
        mTransientRC.insert( lfn, pfn, site );
    }



   

    /**
     * Return the storage mount point for a particular pool.
     *
     * @param site  SiteInfo object of the site for which you want the
     *              storage-mount-point.
     *
     * @return    String corresponding to the mount point if the pool is found.
     *            null if pool entry is not found.
     *//*
    public String getStorageMountPoint( SiteCatalogEntry site ) {
        String storageDir = mStorageDir;
        String mount_point = storageDir;
        FileServer server = null;
        if ( storageDir.length() == 0 || storageDir.charAt( 0 ) != '/' ) {
            server = site.getHeadNodeFS().selectStorageLocalFileServer();
            mount_point = server.getMountPoint();

            //removing the trailing slash if there
            int length = mount_point.length();
            if ( length > 1 && mount_point.charAt( length - 1 ) == '/' ) {
                mount_point = mount_point.substring( 0, length - 1 );
            }

            //append the Storage Dir
            File f = new File( mount_point, storageDir );
            mount_point = f.getAbsolutePath();

        }

        //check if we need to replicate the submit directory
        //structure on the storage directory
        if( mDeepStorageStructure ){
            String leaf = ( this.mPOptions.partOfDeferredRun() )?
                             //if a deferred run then pick up the relative random directory
                             //this.mUserOpts.getOptions().getRandomDir():
                             this.mPOptions.getRelativeDirectory():
                             //for a normal run add the relative submit directory
                             this.mPOptions.getRelativeDirectory();
            File f = new File( mount_point, leaf );
            mount_point = f.getAbsolutePath();
        }


        return mount_point;

    }*/


}
