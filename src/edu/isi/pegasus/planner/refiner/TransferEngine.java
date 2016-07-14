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

package edu.isi.pegasus.planner.refiner;

import edu.isi.pegasus.planner.catalog.site.classes.FileServer;
import edu.isi.pegasus.planner.catalog.site.classes.SiteCatalogEntry;

import edu.isi.pegasus.common.logging.LogManager;

import edu.isi.pegasus.planner.classes.ADag;
import edu.isi.pegasus.planner.classes.FileTransfer;
import edu.isi.pegasus.planner.classes.NameValue;
import edu.isi.pegasus.planner.classes.PegasusFile;
import edu.isi.pegasus.planner.classes.ReplicaLocation;
import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.classes.PegasusBag;


import edu.isi.pegasus.planner.common.PegasusProperties;

import edu.isi.pegasus.planner.namespace.Pegasus;

import edu.isi.pegasus.planner.partitioner.graph.GraphNode;
import edu.isi.pegasus.planner.partitioner.graph.Graph;

import edu.isi.pegasus.planner.selector.ReplicaSelector;
import edu.isi.pegasus.planner.selector.replica.ReplicaSelectorFactory;

import edu.isi.pegasus.planner.transfer.Refiner;
import edu.isi.pegasus.planner.transfer.refiner.RefinerFactory;


import edu.isi.pegasus.planner.catalog.ReplicaCatalog;
import edu.isi.pegasus.planner.catalog.replica.ReplicaCatalogEntry;

import edu.isi.pegasus.common.util.FactoryException;

import edu.isi.pegasus.common.util.PegasusURL;

import edu.isi.pegasus.planner.catalog.replica.ReplicaFactory;
import edu.isi.pegasus.planner.catalog.site.classes.Directory;
import edu.isi.pegasus.planner.catalog.site.classes.FileServerType.OPERATION;
import edu.isi.pegasus.planner.classes.DAGJob;
import edu.isi.pegasus.planner.classes.DAXJob;
import edu.isi.pegasus.planner.classes.PlannerCache;

import edu.isi.pegasus.planner.mapper.SubmitMapper;

import edu.isi.pegasus.planner.common.PegasusConfiguration;
import edu.isi.pegasus.planner.mapper.SubmitMapperFactory;

import edu.isi.pegasus.planner.namespace.Dagman;
import edu.isi.pegasus.planner.mapper.OutputMapper;
import edu.isi.pegasus.planner.mapper.OutputMapperFactory;
import edu.isi.pegasus.planner.mapper.StagingMapper;
import edu.isi.pegasus.planner.mapper.StagingMapperFactory;
import edu.isi.pegasus.planner.mapper.output.Hashed;


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
     * The name of the Replica Catalog Implementer that is used to write out
     * the workflow cache file in the submit directory.
     */
    public static final String WORKFLOW_CACHE_FILE_IMPLEMENTOR = "FlushedCache";


    /**
     * The name of the source key for Replica Catalog Implementer that serves as
     * cache
     */
    public static final String WORKFLOW_CACHE_REPLICA_CATALOG_KEY = "file";

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
     * The name of the refiner for purposes of error logging
     */
    public static final String REFINER_NAME = "TranferEngine";
    
    /**
     * A map that associates the site name with the SRM server url and mount point. 
     */ 
    private Map<String, NameValue> mSRMServiceURLToMountPointMap;

   

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
    private List<Job> mDeletedJobs;
    
    
    /**
     * A SimpleFile Replica Catalog, that tracks all the files that are being
     * materialized as part of workflow executaion.
     */
    private PlannerCache mPlannerCache;

    /**
     * A  Replica Catalog, that tracks all the GET URL's for the files on the
     * staging sites.
     */
    private ReplicaCatalog mWorkflowCache;

    /**
     * Handle to an OutputMapper that tells where to place the files on the
     * output site.
     */
    private OutputMapper mOutputMapper;
    
    /**
     * Handle to an Staging Mapper that tells where to place the files on the
     * shared scratch space on the staging site.
     */
    private StagingMapper mStagingMapper;
    
    /**
     * The working directory relative to the mount point of the execution pool.
     * It is populated from the pegasus.dir.exec property from the properties file.
     * If not specified then it work_dir is supposed to be the exec mount point
     * of the execution pool.
     */
    protected String mWorkDir;
    
    /**
     * This member variable if set causes the destination URL for the symlink jobs
     * to have symlink:// url if the pool attributed associated with the pfn
     * is same as a particular jobs execution pool. 
     */
    protected boolean mUseSymLinks;
    
    /**
     * A boolean indicating whether we are doing worker node execution or not.
     */
    //private boolean mWorkerNodeExecution;

    /**
     * A boolean indicating whether to bypass first level staging for inputs
     */
    private boolean mBypassStagingForInputs;

    /**
     * A boolean to track whether condor file io is used for the workflow or not.
     */
    //private final boolean mSetupForCondorIO;
    private PegasusConfiguration mPegasusConfiguration;
    
    /**
     * The output site where files need to be staged to.
     */
    private final String mOutputSite;
    

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
                           List<Job> deletedJobs ,
                           List<Job> deletedLeafJobs){
        super( bag );

        mSubmitDirMapper =  SubmitMapperFactory.loadInstance( bag,  new File(mPOptions.getSubmitDirectory()));
        bag.add(PegasusBag.PEGASUS_SUBMIT_DIR_FACTORY, mSubmitDirMapper );
        
        mUseSymLinks = mProps.getUseOfSymbolicLinks();
        mSRMServiceURLToMountPointMap = constructSiteToSRMServerMap( mProps );
        
        mDag = reducedDag;
        mDeletedJobs     = deletedJobs;

        mBypassStagingForInputs = mProps.bypassFirstLevelStagingForInputs();

        mPegasusConfiguration = new PegasusConfiguration( bag.getLogger() );   
         
        try{
            mTXRefiner = RefinerFactory.loadInstance( reducedDag,
                                                      bag );
            mReplicaSelector = ReplicaSelectorFactory.loadInstance(mProps);
        }
        catch(Exception e){
            //wrap all the exceptions into a factory exception
            throw new FactoryException("Transfer Engine ", e);
        }

        mOutputSite    = mPOptions.getOutputSite(); 
        mOutputMapper  = OutputMapperFactory.loadInstance( reducedDag, bag);
        mStagingMapper = StagingMapperFactory.loadInstance(bag);

        mWorkflowCache = this.initializeWorkflowCacheFile( reducedDag );

        
                
                
        //log some configuration messages
        mLogger.log("Transfer Refiner loaded is [" + mTXRefiner.getDescription() +
                            "]",LogManager.CONFIG_MESSAGE_LEVEL);
        mLogger.log("ReplicaSelector loaded is  [" + mReplicaSelector.description() +
                    "]",LogManager.CONFIG_MESSAGE_LEVEL);
        mLogger.log("Submit Directory Mapper loaded is    [" + mSubmitDirMapper.description() +
                    "]",LogManager.CONFIG_MESSAGE_LEVEL);
        mLogger.log("Output Mapper loaded is    [" + mOutputMapper.description() +
                    "]",LogManager.CONFIG_MESSAGE_LEVEL);
    }
    
    
    /**
     * Determines a particular created transfer pair has to be binned
     * for remote transfer or local.
     * 
     * @param job the associated compute job
     * @param ft  the file transfer created
     * @return 
     */
    private boolean runTransferRemotely(Job job , SiteCatalogEntry stagingSite, FileTransfer ft) {
        boolean remote = false;
        
        NameValue destTX = ft.getDestURL();
        for( String sourceSite: ft.getSourceSites() ){
                //traverse through all the URL's on that site
                for( ReplicaCatalogEntry rce : ft.getSourceURLs(sourceSite) ){
                    String sourceURL = rce.getPFN();
                    //if the source URL is a FILE URL and 
                    //source site matches the destination site
                    //then has to run remotely
                    if( sourceURL != null && sourceURL.startsWith( PegasusURL.FILE_URL_SCHEME ) ){
                        //sanity check to make sure source site 
                        //matches destination site
                        if( sourceSite.equalsIgnoreCase( destTX.getKey()) ){

                            if( sourceSite.equalsIgnoreCase( stagingSite.getSiteHandle() ) && stagingSite.isVisibleToLocalSite() ){
                                //PM-1024 if the source also matches the job staging site
                                //then we do an extra check if the staging site is the same
                                //as the sourceSite, then we consider the auxillary.local attribute
                                //for the staging site
                                remote = false;
                            }
                            else{
                                remote = true;
                                break;
                            }
                        }
                        else if( sourceSite.equals( "local") ){
                            remote = false;
                        }
                    }
                }
        }
        return remote;
    }
    
    /**
     * Returns whether to run a transfer job on local site or not.
     *
     *
     * @param site   the site entry associated with the destination URL.
     * @param destPutURL the destination URL
     * @param type  the type of transfer job for which the URL is being constructed.
     *
     * @return true indicating if the associated transfer job should run on local
     *              site or not.
     */
    public boolean runTransferOnLocalSite( SiteCatalogEntry site, String destinationURL, int type) {
        //check if user has specified any preference in config
        boolean result = true;
        String siteHandle = site.getSiteHandle();
        
        //short cut for local site
        if( siteHandle.equals( "local" ) ){
            //transfer to run on local site
            return result;
        }

        //PM-1024 check if the filesystem on site visible to the local site
        if( site.isVisibleToLocalSite() ){
            return true;
        }
        
        if( mTXRefiner.refinerPreferenceForTransferJobLocation() ){
            //refiner is advertising a preference for where transfer job
            //should be run. Use that.
            return mTXRefiner.refinerPreferenceForLocalTransferJobs( type );
        }
        
        if( mTXRefiner.runTransferRemotely(siteHandle, type )){
            //always use user preference
            return !result;
        }
        //check to see if destination URL is a file url
        else if( destinationURL != null && destinationURL.startsWith( PegasusURL.FILE_URL_SCHEME ) ){
           result = false;
            
        }

        return result;
    }

    
    /**
     * Adds the transfer nodes to the workflow.
     *
     * @param rcb                the bridge to the ReplicaCatalog.
     * @param plannerCache   an instance of the replica catalog that will
     *                           store the locations of the files on the remote
     *                           sites.
     */
    public void addTransferNodes( ReplicaCatalogBridge rcb, PlannerCache plannerCache ) {
        mRCBridge = rcb;
        mRCBridge.mSubmitDirMapper = this.mSubmitDirMapper;
        mPlannerCache = plannerCache;

        Job currentJob;
        String currentJobName;
        Vector vOutPoolTX;
        String msg;
        String outputSite = mPOptions.getOutputSite();


        //convert the dax to a graph representation and walk it
        //in a top down manner
        //PM-747 no need for conversion as ADag now implements Graph interface
        Graph workflow =  mDag;

        //go through each job in turn

        boolean stageOut = (( outputSite != null ) && ( outputSite.trim().length() > 0 ));

        for( Iterator it = workflow.iterator(); it.hasNext(); ){
            GraphNode node = ( GraphNode )it.next();
            currentJob = (Job)node.getContent();

            //PM-833 associate a directory with the job
            //that is used to determine relative submit directory
            currentJob.setRelativeSubmitDirectory( getRelativeSubmitDirectory( currentJob ) );

            //set the node depth as the level
            currentJob.setLevel( node.getDepth() );
            currentJobName = currentJob.getName();

            mLogger.log("",LogManager.DEBUG_MESSAGE_LEVEL);
            msg = "Job being traversed is " + currentJobName;
            mLogger.log(msg, LogManager.DEBUG_MESSAGE_LEVEL);
            msg = "To be run at " + currentJob.executionPool;
            mLogger.log(msg, LogManager.DEBUG_MESSAGE_LEVEL);

            //getting the parents of that node
            Collection<GraphNode> parents = node.getParents();
            mLogger.log("Parents of job:" + node.parentsToString(),
                        LogManager.DEBUG_MESSAGE_LEVEL);
            processParents(currentJob, parents);

            //transfer the nodes output files
            //to the output pool
            if ( stageOut ) {
                SiteCatalogEntry stagingSite = mSiteStore.lookup( currentJob.getStagingSiteHandle() );
                if (stagingSite == null ) {
                    mLogMsg = this.poolNotFoundMsg(  currentJob.getSiteHandle(), "vanilla");
                    mLogger.log( mLogMsg, LogManager.ERROR_MESSAGE_LEVEL );
                    throw new RuntimeException( mLogMsg );
                }

                //PM-590 Stricter checks
                String stagingSiteURLPrefix = stagingSite.selectHeadNodeScratchSharedFileServerURLPrefix( FileServer.OPERATION.put );
                if( stagingSiteURLPrefix == null ){
                    this.complainForHeadNodeURLPrefix( REFINER_NAME, stagingSite.getSiteHandle(), FileServer.OPERATION.put, currentJob );
                }
                boolean localTransfer = runTransferOnLocalSite( 
                                            stagingSite, 
                                            stagingSiteURLPrefix,
                                            Job.STAGE_OUT_JOB);
                vOutPoolTX = getFileTX(outputSite, currentJob, localTransfer );
                mTXRefiner.addStageOutXFERNodes( currentJob, vOutPoolTX, rcb, localTransfer );
            }
            else{
                //create the cache file always
                //Pegasus Bug PM-32 and PM-356
                trackInCaches( currentJob );
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
                currentJob = (Job)it.next();
                currentJob.setLevel(  TransferEngine.DELETED_JOBS_LEVEL );
                
                //for a deleted node, to transfer it's output
                //the execution pool should be set to local i.e submit host
                currentJob.setSiteHandle( "local" );
                //PM-936 set the staging site for the deleted job
                //to local site
                currentJob.setStagingSiteHandle( "local" );

                //for jobs deleted during data reuse we dont
                //go through the staging site. they are transferred
                //directly to the output site.
                vOutPoolTX = getDeletedFileTX(outputSite, currentJob);
                if( !vOutPoolTX.isEmpty() ){
                    //the job is deleted anyways. The files exist somewhere
                    //as mentioned in the Replica Catalog. We assume it is 
                    //URL remotely accessible
                    boolean localTransfer = true;
                    mTXRefiner.addStageOutXFERNodes( currentJob, vOutPoolTX, rcb, localTransfer, true );
                }

            }
        }

        //we are done with the traversal.
        mTXRefiner.done();


        //close the handle to the workflow cache file if it is written
        //not the planner cache file
        this.mWorkflowCache.close();
    }


    /**
     * Returns the staging site to be used for a job. If a staging site is not
     * determined from the options it is set to be the execution site for the job
     *
     * @param job  the job for which to determine the staging site
     *
     * @return the staging site
     */
    public String getStagingSite( Job job ){
        /*
        String ss =  this.mPOptions.getStagingSite( job.getSiteHandle() );
        return (ss == null) ? job.getSiteHandle(): ss;
        */
        return job.getStagingSiteHandle();
    }

    /**
     * This gets the file transfer objects corresponding to the location of files
     * found in the replica mechanism, and transfers it to the output pool asked
     * by the user. If the output pool path and the one returned by the replica
     * mechanism match then that object is not transferred.
     *
     * @param destSite    this the output pool which the user specifies at runtime.
     * @param job     The Job object corresponding to the leaf job which was
     *                deleted by the Reduction algorithm
     *
     * @return        Vector of <code>FileTransfer</code> objects
     */
    private Vector getDeletedFileTX( String destSite, Job job ) {
        Vector vFileTX = new Vector();
        SiteCatalogEntry outputSite = mSiteStore.lookup(destSite);
        for( Iterator it = job.getOutputFiles().iterator(); it.hasNext(); ){
            PegasusFile pf = (PegasusFile)it.next();
            String  lfn = pf.getLFN();
            
            //PM-739 all output files for deleted jobs should have their
            //cleanup flag set to false. these output files are not 
            //generated during the workflow, but are retrieved from a
            //location specified in the replica catalog.
            pf.setForCleanup( false );

            //we only have to get a deleted file that user wants to be transferred
            if( pf.getTransientTransferFlag() ){
                continue;
            }
            
            ReplicaLocation rl = mRCBridge.getFileLocs( lfn );
            //sanity check
            if( rl == null ){
                throw new RuntimeException( "Unable to find a physical filename (PFN) in the Replica Catalog for output file with logical filename (LFN) as "  + lfn );
            }

            String putDestURL = mOutputMapper.map( lfn, destSite,  FileServer.OPERATION.put );
            String getDestURL = mOutputMapper.map( lfn, destSite,  FileServer.OPERATION.get );
            
            
            //selLocs are all the locations found in ReplicaMechanism corr
            //to the pool pool
            ReplicaLocation selLocs = mReplicaSelector.selectAndOrderReplicas(rl,
                                                                       destSite,
                                                                       this.runTransferOnLocalSite( outputSite,putDestURL, Job.STAGE_OUT_JOB  ));


            boolean flag = false;

            FileTransfer ft = null;
            //checking through all the pfn's returned on the pool
            for ( Iterator selIt = selLocs.pfnIterator(); selIt.hasNext(); ) {
                ReplicaCatalogEntry selLoc = ( ReplicaCatalogEntry ) selIt.next();
                String sourceURL = selLoc.getPFN();

                //check if the URL's match
                if (sourceURL.trim().equalsIgnoreCase(putDestURL.trim())){
                    String msg = "The leaf file " + lfn +
                        " is already at the output pool " + destSite;
                    mLogger.log(msg,LogManager.INFO_MESSAGE_LEVEL);
                    flag = true;
                    break;
                }


                ft = new FileTransfer( lfn, job.getName() );
                ft.addSource( selLoc.getResourceHandle() , sourceURL );
                ft.addDestination(destSite, putDestURL  );
                ft.setURLForRegistrationOnDestination( getDestURL );
                ft.setSize( pf.getSize() );
                ft.setForCleanup( false );//PM-739

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
     * @param job       the <code>Job</code> object containing all the
     *                  details of the job.
     * @param parents   list <code>GraphNode</code> ojbects corresponding to the parent jobs
     *                  of the job.
     */
    private void processParents(Job job, Collection<GraphNode> parents) {

        Set nodeIpFiles = job.getInputFiles();
        Vector vRCSearchFiles = new Vector(); //vector of PegasusFile


        //getAll the output Files of the parents
        Set<PegasusFile> parentsOutFiles = getOutputFiles( parents );


        //interpool transfer of the nodes parents
        //output files
        Collection[] interSiteFileTX = this.getInterpoolFileTX(job, parents);
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
                //PM-976 all input files that are not generated
                //by parent jobs should be looked up in the replica catalog
                //we don't consider the value of the transfer flag
                vRCSearchFiles.addElement(pf);
            }
        }

        if( job instanceof DAXJob ){
            //for the DAX jobs we should always call the method
            //as DAX may just be referred as the LFN
            getFilesFromRC( (DAXJob)job, vRCSearchFiles);
        }
        else if (!vRCSearchFiles.isEmpty()) {
            if( job instanceof DAGJob ){
                getFilesFromRC( (DAGJob)job, vRCSearchFiles);
            }
            else{
                //get the locations from the RC
                getFilesFromRC(job, vRCSearchFiles);
            }
        }
    }

    /**
     * This gets the Vector of FileTransfer objects for the files which have to
     * be transferred to an one destination pool. It checks for the transient
     * flags for files. If the transfer transient flag is set, it means the file
     * does not have to be transferred to the destination pool.
     *
     * @param destSiteHandle The pool to which the files are to be transferred to.
     * @param job      The <code>Job</code>object of the job whose output files
     *                 are needed at the destination pool.
     * @param localTransfer  boolean indicating that associated transfer job will run
     *                       on local site.
     *
     * @return        Vector of <code>FileTransfer</code> objects
     */
    private Vector getFileTX(String destPool, Job job, boolean localTransfer ) {
        Vector vFileTX = new Vector();

        //check if there is a remote initialdir set
        String path  = job.vdsNS.getStringValue(
                                                 Pegasus.REMOTE_INITIALDIR_KEY );

        for( Iterator it = job.getOutputFiles().iterator(); it.hasNext(); ){
            PegasusFile pf = (PegasusFile) it.next();
            String file = pf.getLFN();


            FileTransfer ft = this.constructFileTX( pf,
                                                    job,
                                                    destPool,
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
     * information. If the transient flag for transfer is set, the destPutURL for the
     * FileTransfer object would be the execution directory, as this is the entry
     * that has to be registered in the ReplicaMechanism
     *
     * @param pf          the PegasusFile for which the transfer has to be done.
     * @param stagingSiteHandle    the staging site at which file is placed after execution.
     * @param destSiteHandle    the output pool where the job should be transferred
     * @param job         the name of the associated job.
     * @param path        the path that a user specifies in the profile for key
     *                    remote_initialdir that results in the workdir being
     *                    changed for a job on a execution pool.
     * @param localTransfer  boolean indicating that associated transfer job will run
     *                       on local site.

     *
     * @return   the corresponding FileTransfer object
     */
    private FileTransfer constructFileTX( PegasusFile pf,
                                          Job job,
                                          String destSiteHandle,
                                          String path,
                                          boolean localTransfer ) {

        String stagingSiteHandle = job.getStagingSiteHandle();
        String lfn = pf.getLFN();
        FileTransfer ft = null;

        SiteCatalogEntry stagingSite = mSiteStore.lookup( stagingSiteHandle );
        SiteCatalogEntry destinationSite = mSiteStore.lookup( destSiteHandle );
        if (stagingSite == null || destinationSite == null) {
            mLogMsg = (stagingSite == null) ?
                this.poolNotFoundMsg(stagingSiteHandle, "vanilla") :
                this.poolNotFoundMsg(destSiteHandle, "vanilla");
            mLogger.log( mLogMsg, LogManager.ERROR_MESSAGE_LEVEL );
            throw new RuntimeException( mLogMsg );
        }

        //PM-833 figure out the addOn component just once per lfn
        File addOn = mStagingMapper.mapToRelativeDirectory(job, stagingSite, lfn);
            
        //the get
        String sharedScratchGetURL = this.getURLOnSharedScratch( stagingSite, job, OPERATION.get, addOn, lfn );
        String sharedScratchPutURL = this.getURLOnSharedScratch( stagingSite, job, OPERATION.put, addOn, lfn );

        //in the planner cache we track the output files put url on staging site
        trackInPlannerCache( lfn, sharedScratchPutURL, stagingSiteHandle );
        //in the workflow cache we track the output files put url on staging site
        trackInWorkflowCache( lfn, sharedScratchGetURL, stagingSiteHandle );

        //if both transfer and registration
        //are transient return null
        if (pf.getTransientRegFlag() && pf.getTransientTransferFlag()) {
            return null;
        }

        //if only transient transfer flag
        //means destPutURL and sourceURL
        //are same and are equal to
        //execution directory on stagingSiteHandle
        if (pf.getTransientTransferFlag()) {

            ft = new FileTransfer(lfn,job.getID(),pf.getFlags());
            //set the transfer mode
            ft.setSize( pf.getSize() );
            ft.setTransferFlag(pf.getTransferFlag());
            ft.addSource(stagingSiteHandle,sharedScratchGetURL);
            ft.addDestination(stagingSiteHandle,sharedScratchGetURL);
            ft.setURLForRegistrationOnDestination( sharedScratchGetURL );
            ft.setMetadata( pf.getAllMetadata() );
        }
        //the source dir is the exec dir
        //on exec pool and dest dir
        //would be on the output pool
        else {
            //construct the source url depending on whether third party tx
           String sourceURL = localTransfer ?
                                sharedScratchGetURL :
                                "file://" + mSiteStore.getInternalWorkDirectory(stagingSiteHandle,path) +
                                File.separator + lfn;

            ft = new FileTransfer(lfn, job.getID(), pf.getFlags());
            ft.setSize( pf.getSize() );
            //set the transfer mode
            ft.setTransferFlag(pf.getTransferFlag());

            ft.addSource(stagingSiteHandle,sourceURL);

            //if the PegasusFile is already an instance of
            //FileTransfer the user has specified the destination
            //that they want to use in the DAX 3.0 
            if( pf instanceof FileTransfer ){
                //not really supported in DAX 3.3?
                ft.addDestination( ((FileTransfer)pf).removeDestURL() );
                return ft;
            }
            ft.setMetadata( pf.getAllMetadata() );

            //add all the possible destination urls iterating through
            //the list of grid ftp servers associated with the dest pool.
/*
            Directory storageDirectory = mSiteStore.lookup( destSiteHandle ).getHeadNodeStorageDirectory();
            if( storageDirectory == null ){
                throw new RuntimeException( "No Storage directory specified for site " + destSiteHandle );
            }

            //sanity check
            if( !storageDirectory.hasFileServerForPUTOperations() ){
                //no file servers for put operations
                throw new RuntimeException( " No File Servers specified for PUT Operation on Shared Storage on Headnode for site " + destSiteHandle );
            }
*/
            for( String destURL : mOutputMapper.mapAll( lfn, destSiteHandle, OPERATION.put )){
                //if the paths match of dest URI
                //and execDirURL we return null
                if (sharedScratchGetURL.equalsIgnoreCase(destURL)) {
                    /*ft = new FileTransfer(file, job);
                    ft.addSource(stagingSiteHandle, sharedScratchGetURL);*/
                    ft.addDestination(stagingSiteHandle, sharedScratchGetURL);
                    ft.setURLForRegistrationOnDestination( sharedScratchGetURL );
                    //make the transfer transient?
                    ft.setTransferFlag(PegasusFile.TRANSFER_NOT);
                    return ft;
                }
                ft.addDestination( destSiteHandle, destURL );
            }

            //construct a registration URL
            ft.setURLForRegistrationOnDestination( mOutputMapper.map( lfn, destSiteHandle, FileServer.OPERATION.get , true ) );
        }

        return ft;
    }

     
    /**
     * Constructs a Registration URL for a LFN
     *
     * @param site       the site handle
     * @param lfn        the LFN for which the URL needs to be constructed
     *
     * @return  the URL
     */
    private String constructRegistrationURL( String site, String lfn ){
        //assumption of same external mount point for each storage
        //file server on output site
//                url = this.getURLOnStageoutSite( fs, lfn );
        return mOutputMapper.map( lfn, site, FileServer.OPERATION.get );
    }

 
    /**
     * Constructs a Registration URL for a LFN
     *
     * @param directory  the storage directory
     * @param site       the site handle
     * @param lfn        the LFN for which the URL needs to be constructed
     *
     * @return  the URL
     */
/*
    private String constructRegistrationURL(  Directory directory , String site, String lfn ){
        //sanity check
        if( !directory.hasFileServerForGETOperations() ){
            //no file servers for GET operations
            throw new RuntimeException( " No File Servers specified for GET Operation on Shared Storage for site " + site );
        }

        String url = null;
        for( FileServer.OPERATION op : FileServer.OPERATION.operationsForGET() ){
            for( Iterator it = directory.getFileServersIterator(op); it.hasNext();){
                FileServer fs = (FileServer)it.next();
                

                //assumption of same external mount point for each storage
                //file server on output site
//                url = this.getURLOnStageoutSite( fs, lfn );
                url = mOutputMapper.map( lfn, site, FileServer.OPERATION.get );

                return url;
            }

        }//end of different get operations
        return url;
    }
*/
    
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
     * @param parents   Collection of <code>GraphNode</code> ojbects corresponding to the
     *                  parent jobs of the job.
     *
     * @return    array of Collection of  <code>FileTransfer</code> objects
     */
    private Collection<FileTransfer>[] getInterpoolFileTX(Job job, Collection<GraphNode>parents ) {
        String destSiteHandle = job.getStagingSiteHandle();
        //contains the remote_initialdir if specified for the job
        String destRemoteDir = job.vdsNS.getStringValue(
                                                 Pegasus.REMOTE_INITIALDIR_KEY);

        SiteCatalogEntry destSite = mSiteStore.lookup( destSiteHandle );
        SiteCatalogEntry sourceSite;

        Collection[] result = new Collection[2];
        Collection<FileTransfer> localTransfers  = new LinkedList();
        Collection<FileTransfer> remoteTransfers = new LinkedList();

        for ( GraphNode parent: parents ) {
            //get the parent job
            Job pJob = (Job)parent.getContent();
            sourceSite = mSiteStore.lookup( pJob.getStagingSiteHandle() );

            if( sourceSite.getSiteHandle().equalsIgnoreCase( destSiteHandle ) ){
                //no need to add transfers, as the parent job and child
                //job are run in the same directory on the pool
                continue;
            }

            String sourceURI = null;
            
            //PM-590 Stricter checks
            /* PM-833
            String thirdPartyDestPutURI = this.getURLOnSharedScratch( destSite, job, OPERATION.put, null );


            //definite inconsitency as url prefix and mount point
            //are not picked up from the same server
            boolean localTransfer = runTransferOnLocalSite( destSite, thirdPartyDestPutURI, Job.INTER_POOL_JOB );
            String destURI = localTransfer ?
                //construct for third party transfer
                thirdPartyDestPutURI :
                //construct for normal transfer
                "file://" + mSiteStore.getInternalWorkDirectory( destSiteHandle, destRemoteDir );
            */

            for (Iterator fileIt = pJob.getOutputFiles().iterator(); fileIt.hasNext(); ){
                PegasusFile pf = (PegasusFile) fileIt.next();
                String outFile = pf.getLFN();

               if( job.getInputFiles().contains( pf ) ){

                   //PM-833 figure out the addOn component just once per lfn
                   String lfn = pf.getLFN();
                   File addOn = mStagingMapper.mapToRelativeDirectory(job, destSite, lfn);
                   String thirdPartyDestPutURL = this.getURLOnSharedScratch(destSite, job, OPERATION.put, addOn, lfn);


                   //definite inconsitency as url prefix and mount point
                   //are not picked up from the same server
                   boolean localTransfer = runTransferOnLocalSite( destSite, thirdPartyDestPutURL, Job.INTER_POOL_JOB );
                   String destURL = localTransfer ?
                                            //construct for third party transfer
                                            thirdPartyDestPutURL :
                                            //construct for normal transfer
                                            "file://" + mSiteStore.getInternalWorkDirectory( destSiteHandle, destRemoteDir ) + File.separator + addOn;

                   
                    String sourceURL     = null;
                    /* PM-833 String destURL       = destURI + File.separator + outFile;
                    String thirdPartyDestURL = thirdPartyDestPutURI + File.separator +
                                           outFile;
                    */
                    FileTransfer ft      = new FileTransfer(outFile,pJob.jobName);
                    ft.setSize( pf.getSize() );
                    ft.addDestination(destSiteHandle,destURL);

                    //for intersite transfers we need to track in transient rc
                    //for the cleanup algorithm
                    //only the destination is tracked as source will have been
                    //tracked for the parent jobs
                    trackInPlannerCache( outFile, thirdPartyDestPutURL, destSiteHandle );

                    //in the workflow cache we track the get URL for the outfile
                    String thirdPartyDestGetURL = this.getURLOnSharedScratch( destSite, job, OPERATION.get, addOn, outFile );
                    trackInWorkflowCache( outFile, thirdPartyDestGetURL, destSiteHandle );

                    //add all the possible source urls iterating through
                    //the list of grid ftp servers associated with the dest pool.
                    boolean first = true;

                    Directory parentScratchDir = mSiteStore.lookup( pJob.getStagingSiteHandle() ).getDirectory( Directory.TYPE.shared_scratch );
                    if( parentScratchDir == null ){
                        throw new RuntimeException( "Unable to determine the scratch dir for site " + pJob.getStagingSiteHandle() );
                    }
                    //retrive all the file servers matching the get operations
                    for( FileServer.OPERATION op : FileServer.OPERATION.operationsForGET() ){
                        for( Iterator it1 = parentScratchDir.getFileServersIterator(op); it1.hasNext(); ){

                            FileServer server = ( FileServer)it1.next();
                            //definite inconsitency as url prefix and mount point
                            //are not picked up from the same server
                            sourceURI = server.getURLPrefix();
                                                                          
                            //sourceURI += server.getMountPoint();
                            sourceURI += mSiteStore.getExternalWorkDirectory(server, pJob.getSiteHandle());
                        
                            sourceURL = sourceURI + File.separator + outFile;

                            if(!(sourceURL.equalsIgnoreCase(thirdPartyDestPutURL))){
                                //add the source url only if it does not match to
                                //the third party destination url
                                ft.addSource(pJob.getStagingSiteHandle(), sourceURL);
                            }
                            first = false;
                        }
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
     * Special Handling for a DAGJob for retrieving files from the Replica Catalog.
     *
     * @param job           the DAGJob
     * @param searchFiles   file that need to be looked in the Replica Catalog.
     */
    private void getFilesFromRC( DAGJob job, Collection searchFiles ){
        //dax appears in adag element
        String dag = null;

        //go through all the job input files
        //and set transfer flag to false
        for (Iterator<PegasusFile> it = job.getInputFiles().iterator(); it.hasNext();) {
            PegasusFile pf = it.next();
            //at the moment dax files are not staged in.
            //remove from input set of files
            //part of the reason is about how to handle where
            //to run the DAGJob. We dont have much control over it.
            it.remove();
        }

        String lfn = job.getDAGLFN();
        ReplicaLocation rl = mRCBridge.getFileLocs( lfn );

        if (rl == null) { //flag an error
            throw new RuntimeException(
                    "TransferEngine.java: Can't determine a location to " +
                    "transfer input file for DAG lfn " + lfn + " for job " +
                    job.getName());
        }

        ReplicaCatalogEntry selLoc = mReplicaSelector.selectReplica( rl,
                                                                     job.getSiteHandle(),
                                                                     true );
        String pfn = selLoc.getPFN();
        //some extra checks to ensure paths
        if( pfn.startsWith( File.separator ) ){
            dag = pfn;
        }
        else if( pfn.startsWith( PegasusURL.FILE_URL_SCHEME ) ){
            dag = new PegasusURL( pfn ).getPath();
        }
        else{
            throw new RuntimeException( "Invalid URL Specified for DAG Job " + job.getName() + " -> " + pfn );
        }
        
        job.setDAGFile(dag);
        
        //set the directory if specified
        job.setDirectory((String) job.dagmanVariables.removeKey(Dagman.DIRECTORY_EXTERNAL_KEY));



    }

    /**
     * Special Handling for a DAXJob for retrieving files from the Replica Catalog.
     *
     * @param job           the DAXJob
     * @param searchFiles   file that need to be looked in the Replica Catalog.
     */
    private void getFilesFromRC( DAXJob job, Collection searchFiles ){
        //dax appears in adag element
        String dax = null;
        String lfn = job.getDAXLFN();
        
        PegasusFile daxFile = new PegasusFile( lfn );
        if( !job.getInputFiles().contains( daxFile )){
            //if the LFN is not specified as an input file in the DAX
            //lets add it PM-667 more of a sanity check.
            daxFile.setTransferFlag( PegasusFile.TRANSFER_MANDATORY );
            job.getInputFiles().add( daxFile );
            searchFiles.add( daxFile );
        }
        

        //update the dax argument with the direct path to the DAX file
        //if present locally. This is to ensure that SUBDAXGenerator
        //can figure out the path to the dag file that will be created for the 
        //job. Else the dax job needs to have a --basename option passed.
        ReplicaLocation rl = mRCBridge.getFileLocs( lfn );

        if (rl != null) { 
         
	    ReplicaCatalogEntry selLoc = mReplicaSelector.selectReplica( rl,
									 job.getSiteHandle(),
									 true );
	    String pfn = selLoc.getPFN();
	    //some extra checks to ensure paths
	    if( pfn.startsWith( File.separator ) ){
		dax = pfn;
	    }
	    else if( pfn.startsWith( PegasusURL.FILE_URL_SCHEME ) ){
		dax = new PegasusURL( pfn ).getPath();
	    }
	}
        
        if( dax == null ){
            //append the lfn instead of the full path to the dax PM-667
            //the user then needs to have a basename option set for the DAX job
            dax = lfn;
        }
        else{
            //we also remove the daxFile from the input files for the job.
            //and the searchFiles as we have a local path to the DAX . 
            if( job.getInputFiles().contains( daxFile )){
                 boolean removed = job.getInputFiles().remove( daxFile );
                 logRemoval( job, daxFile,  "Job Input files ", removed );
            }
            if( searchFiles.contains( daxFile ) ){
                boolean removed = searchFiles.remove( daxFile );
                logRemoval( job, daxFile,  "Job Search Files", removed );
            }
        }

        //add the dax to the argument
        StringBuilder arguments = new StringBuilder();
        arguments.append(job.getArguments()).
                append(" --dax ").append( dax );
        job.setArguments(arguments.toString());
        
        mLogger.log( "Set arguments for DAX job " + job.getID()+ " to " + arguments.toString(),
                     LogManager.DEBUG_MESSAGE_LEVEL );
        
        this.getFilesFromRC( (Job)job, searchFiles );
    }

    /**
     * It looks up the RCEngine Hashtable to lookup the locations for the
     * files and add nodes to transfer them. If a file is not found to be in
     * the Replica Catalog the Transfer Engine flags an error and exits
     *
     * @param job           the <code>Job</code>object for whose ipfile have
     *                      to search the Replica Mechanism for.
     * @param searchFiles   Vector containing the PegasusFile objects corresponding
     *                      to the files that need to have their mapping looked
     *                      up from the Replica Mechanism.
     */
    private void getFilesFromRC( Job job, Collection searchFiles ) {
        //Vector vFileTX = new Vector();
        //Collection<FileTransfer> symLinkFileTransfers = new LinkedList();
        Collection<FileTransfer> localFileTransfers = new LinkedList();
        Collection<FileTransfer> remoteFileTransfers = new LinkedList();


        String jobName = job.logicalName;
        String stagingSiteHandle   = job.getStagingSiteHandle();
        String executionSiteHandle   = job.getSiteHandle();
        //contains the remote_initialdir if specified for the job
        String eRemoteDir = job.vdsNS.getStringValue(
                                                 Pegasus.REMOTE_INITIALDIR_KEY);
        
        SiteCatalogEntry stagingSite        = mSiteStore.lookup( stagingSiteHandle );
        //we are using the pull mode for data transfer
        String scheme  = "file";

        //sAbsPath would be just the source directory absolute path
        //dAbsPath would be just the destination directory absolute path

        //sDirURL would be the url to the source directory.
        //dDirPutURL would be the url to the destination directoy
        //and is always a networked url.
/* for PM-833
        String dDirPutURL = this.getURLOnSharedScratch( stagingSite, job, OPERATION.put, null );
        String dDirGetURL = this.getURLOnSharedScratch( stagingSite, job, OPERATION.get, null );
        String sDirURL = null;
        String sAbsPath = null;
        String dAbsPath = mSiteStore.getInternalWorkDirectory( stagingSiteHandle, eRemoteDir );
        
        
        //file dest dir is destination dir accessed as a file URL
        String fileDestDir = scheme + "://" + dAbsPath;
                
        //check if the execution pool is third party or not
        boolean runTransferOnLocalSite = runTransferOnLocalSite( stagingSite, dDirPutURL, Job.STAGE_IN_JOB);
        String destDir = ( runTransferOnLocalSite ) ?
            //use the full networked url to the directory
            dDirPutURL
            :
            //use the default pull mode
            fileDestDir;
*/

        for( Iterator it = searchFiles.iterator(); it.hasNext(); ){
            String sourceURL = null,destPutURL = null, destGetURL =null;
            PegasusFile pf = (PegasusFile) it.next();
            List  pfns   = null;
            ReplicaLocation rl = null;

            String lfn     = pf.getLFN();
            NameValue nv   = null;
            
            //PM-833 figure out the addOn component just once per lfn
            File addOn = mStagingMapper.mapToRelativeDirectory(job, stagingSite, lfn);
            
            destPutURL = this.getURLOnSharedScratch( stagingSite, job, OPERATION.put, addOn, lfn );
            destGetURL = this.getURLOnSharedScratch( stagingSite, job, OPERATION.get, addOn, lfn );
            String sDirURL = null;
            String sAbsPath = null;
            String dAbsPath = mSiteStore.getInternalWorkDirectory( stagingSiteHandle, eRemoteDir ) + File.separator + addOn;
        
        
            //file dest dir is destination dir accessed as a file URL
            String fileDestDir = scheme + "://" + dAbsPath;
                
            //check if the execution pool is third party or not
            boolean runTransferOnLocalSite = runTransferOnLocalSite( stagingSite, destPutURL, Job.STAGE_IN_JOB);
            String destDir = ( runTransferOnLocalSite ) ?
                //use the full networked url to the directory
                destPutURL
                :
                //use the default pull mode
                fileDestDir;
            

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
                    throw new RuntimeException( "Unreachable code . Signifies error in internal logic " );
                }
                else{
                    //staging of executables case
                    destPutURL = destNV.getValue();
                    destPutURL = (runTransferOnLocalSite( stagingSite, destPutURL, Job.STAGE_IN_JOB))?
                               //the destination URL is already third party
                               //enabled. use as it is
                               destPutURL:
                               //explicitly convert to file URL scheme
                               scheme + "://" + new PegasusURL( destPutURL ).getPath();
                               
                    //for time being for this case the get url is same as put url
                    destGetURL = destPutURL;
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
                    //input files for the job, only if file is not a checkpoint file
                    if ( !pf.isCheckpointFile()){
                         job.getInputFiles().remove( pf );
                    }

                    continue;
                }

                //flag an error. this is when we don't get any replica location
                //from any source
                throw new RuntimeException(
                           "TransferEngine.java: Can't determine a location to " +
                           "transfer input file for lfn " + lfn + " for job " +
                           job.getName());
            }

            
            FileTransfer ft = (pf instanceof FileTransfer) ?
                                   (FileTransfer)pf:
                                   new FileTransfer( lfn, jobName, pf.getFlags() );

            //make sure the type information is set in file transfer
            ft.setType( pf.getType() );
            ft.setSize( pf.getSize() );

            //the transfer mode for the file needs to be
            //propogated for optional transfers.
            ft.setTransferFlag(pf.getTransferFlag());

            ReplicaLocation candidateLocations = null;
            if( nv == null ){
                //select from the various replicas
                candidateLocations =  mReplicaSelector.selectAndOrderReplicas( rl, 
                                                                        executionSiteHandle,
                                                                        runTransferOnLocalSite );
                if( candidateLocations.getPFNCount() == 0 ){
                    StringBuilder error = new StringBuilder();
                    error.append( "Unable to select a Physical Filename (PFN) for file with logical filename (LFN) as ").
                          append( rl.getLFN() ).append( " for preferred site " ).append( executionSiteHandle ).
                          append( "with runTransferOnLocalSite - ").append( runTransferOnLocalSite ).
                          append( " amongst ").append( rl.getPFNList() );
                    throw new RuntimeException( error.toString() );
                }
            }
            else{
                //we have the replica already selected
                List rces = new LinkedList();
                rces.add(  new ReplicaCatalogEntry( nv.getValue(), nv.getKey() ));
                candidateLocations  = new ReplicaLocation( lfn, rces );
            }
            
            //check if we need to replace url prefix for 
            //symbolic linking
            boolean symLinkSelectedLocation = false;
            //is set to false later, on basis of property value
            boolean bypassFirstLevelStaging = true;

            int candidateNum = 0; 
            for( ReplicaCatalogEntry selLoc : candidateLocations.getPFNList()){
                candidateNum++;
                
                if ( symLinkSelectedLocation = 
                        (mUseSymLinks && selLoc.getResourceHandle().equals( job.getStagingSiteHandle() )) ) {

                    //resolve any srm url's that are specified
                    selLoc = replaceSourceProtocolFromURL( selLoc );
                }
            
                                        
                //get the file to the job's execution pool
                //this is assuming that there are no directory paths
                //in the pfn!!!
                sDirURL  = selLoc.getPFN().substring( 0, selLoc.getPFN().lastIndexOf(File.separator) );

                //try to get the directory absolute path
                //yes i know that we sending the url to directory
                //not the file.
                sAbsPath = new PegasusURL(  sDirURL ).getPath();

                //the final source and destination url's to the file
                sourceURL = selLoc.getPFN();

                if( destPutURL == null ){
                    //no staging of executables case. 
                    //we construct destination URL to file.
                    /* PM-833
                    StringBuffer destPFN = new StringBuffer();
                    if( symLinkSelectedLocation ){
                        //we use the file URL location to dest dir
                        //in case we are symlinking
                        //destPFN.append( fileDestDir );
                        destPFN.append( this.replaceProtocolFromURL( destDir ) );
                    }
                    else{
                        //we use whatever destDir was set to earlier
                        destPFN.append( destDir );
                    }
                    destPFN.append( File.separator).append( lfn );
                    destPutURL = destPFN.toString();
                    destGetURL = dDirGetURL + File.separator + lfn;
                    */
                    if( symLinkSelectedLocation ){
                        //we use the file URL location to dest dir
                        //in case we are symlinking
                        //destPFN.append( fileDestDir );
                        destPutURL =  this.replaceProtocolFromURL( destPutURL );
                    }
                }
            

                //we have all the chopped up combos of the urls.
                //do some funky matching on the basis of the fact
                //that each pool has one shared filesystem


                //match the source and dest 3rd party urls or
                //match the directory url knowing that lfn and
                //(source and dest pool) are same
                try{
                    //PM-833if(sourceURL.equalsIgnoreCase(dDirPutURL + File.separator + lfn)||
                    if(sourceURL.equalsIgnoreCase( destPutURL )||
                         ( selLoc.getResourceHandle().equalsIgnoreCase( stagingSiteHandle ) &&
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
                                append( " for job " ).append( job.jobName ).append( " to site " ).append( stagingSiteHandle );

                        mLogger.log( message.toString() , LogManager.DEBUG_MESSAGE_LEVEL );
                        continue;
                    }
                }catch( IOException ioe ){
                    /*ignore */
                }
                
                //add locations of input data on the remote site to the transient RC
                bypassFirstLevelStaging = this.bypassStagingForInputFile( selLoc , pf , job  );
                if( bypassFirstLevelStaging ){
                    //only the files for which we bypass first level staging , we
                    //store them in the planner cache as a GET URL and associate with the compute site
                    //PM-698 . we have to clone since original site attribute will be different
                    ReplicaCatalogEntry rce = (ReplicaCatalogEntry) selLoc.clone();
                    rce.setResourceHandle( executionSiteHandle );
                    trackInPlannerCache( lfn, rce, OPERATION.get );
                    
                    if( candidateNum == 1 ){
                        //PM-1014 we only track the first candidate in the workflow cache
                        //i.e the cache file written out in the submit directory
                        trackInWorkflowCache( lfn, sourceURL, selLoc.getResourceHandle() );
                    }
                    //ensure the input file does not get cleaned up by the
                    //InPlace cleanup algorithm
                    pf.setForCleanup( false );
                    continue;
                }
                else{
                    //track the location where the data is staged as 
                    //part of the first level staging
                    //we always store the thirdparty url
                    //trackInCaches( lfn, destPutURL, job.getSiteHandle() );
                    trackInPlannerCache( lfn,
                                        destPutURL,
                                        job.getStagingSiteHandle());

                    if( candidateNum == 1 ){
                        //PM-1014 we only track the first candidate in the workflow cache
                        //i.e the cache file written out in the submit directory
                     
                        trackInWorkflowCache( lfn,
                                        destGetURL,
                                        job.getStagingSiteHandle());
                    }
                }
                
                //PM-1014 we want to track all candidate locations
                ft.addSource( selLoc);

                //to prevent duplicate destination urls
                //and have only a single destination.
                if(ft.getDestURL() == null)
                    ft.addDestination(stagingSiteHandle,destPutURL);
           
            } //end of traversal of all candidate locations
                
            if ( !bypassFirstLevelStaging ) {
                //no bypass of input file staging. we need to add
                //data stage in nodes for the lfn
                if(  symLinkSelectedLocation || //symlinks can run only on staging site
                     !runTransferOnLocalSite ||
                     runTransferRemotely( job, stagingSite, ft ) ){ //check on the basis of constructed source URL whether to run remotely

                    //all symlink transfers and user specified remote transfers
                    remoteFileTransfers.add(ft);
                }
                else{
                    localFileTransfers.add(ft);
                }
            }
            
            //we need to set destPutURL to null
            destPutURL = null;
        }

        
        //call addTransferNode
        if (!localFileTransfers.isEmpty() || !remoteFileTransfers.isEmpty()) {
            mTXRefiner.addStageInXFERNodes(job, localFileTransfers, remoteFileTransfers );

        }
    }

    /**
     * Replaces the SRM URL scheme from the url, and replaces it with the
     * file url scheme and returns in a new object if replacement happens.
     * The original object passed as a parameter still remains the same.
     *
     * @param rce  the <code>ReplicaCatalogEntry</code> object whose url need to be
     *             replaced.
     *
     * @return  the object with the url replaced.
     */
    protected ReplicaCatalogEntry replaceSourceProtocolFromURL( ReplicaCatalogEntry rce ) {
        String pfn = rce.getPFN();

         //if the pfn starts with a file url we
        //dont need to replace . a sanity check
        if( pfn.startsWith( PegasusURL.FILE_URL_SCHEME ) ){
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
                newPFN.append( PegasusURL.FILE_URL_SCHEME ).append( "//" );
                newPFN.append( nv.getValue() );
                newPFN.append( pfn.substring( urlPrefix.length(), pfn.length() ));
                mLogger.log( "Replaced pfn " + pfn + " with " + newPFN.toString() ,
                             LogManager.TRACE_MESSAGE_LEVEL );
            }
        }
        if( newPFN.length() == 0 ){
            //there is no SRM Replacement to do
            //Still do the FILE replacement
            //return the original object

            //we have to the manual replacement
/*
            String hostName = Utility.getHostName( pfn );
            newPFN.append( FILE_URL_SCHEME ).append( "//" );
            //we want to skip out the hostname
            newPFN.append( pfn.substring( pfn.indexOf( hostName ) + hostName.length() ) );
*/

            newPFN.append( PegasusURL.FILE_URL_SCHEME ).append( "//" );
            newPFN.append( new PegasusURL( pfn ).getPath() );
        }
        
        //we do not need a full clone, just the PFN
        ReplicaCatalogEntry result = new ReplicaCatalogEntry( newPFN.toString(),
                                                              rce.getResourceHandle() );
        
        for( Iterator it = rce.getAttributeIterator(); it.hasNext();){
            String key = (String)it.next();
            result.addAttribute( key, rce.getAttribute( key ) );
        }

        return result;
    }
    
    /**
     * Replaces the gsiftp URL scheme from the url, and replaces it with the
     * symlink url scheme and returns in a new object. The original object
     * passed as a parameter still remains the same.
     *
     * @param pfn  the pfn that needs to be replaced
     *
     * @return  the replaced PFN
     */
    protected String replaceProtocolFromURL( String pfn ) {
        /* special handling for SRM urls */
        StringBuffer newPFN = new StringBuffer();
        
        if( pfn.startsWith(PegasusURL.FILE_URL_SCHEME) ){
            //special handling for FILE URL's as 
            //utility hostname functions dont hold up
            newPFN.append( PegasusURL.SYMLINK_URL_SCHEME ).
                   append( pfn.substring( PegasusURL.FILE_URL_SCHEME.length() ) );
            
            //System.out.println( "Original PFN " + pfn + " \nReplaced PFN " + newPFN.toString() );
            return newPFN.toString();
        }
        
 
        newPFN.append( PegasusURL.SYMLINK_URL_SCHEME ).append( "//" );
       
        //we want to skip out the hostname        
        newPFN.append( new PegasusURL( pfn ).getPath()  );
        
        return newPFN.toString();
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
     * the  nodes passed.
     *
     * @param nodes   List<GraphNode> containing the jobs
     * 
     *
     * @return   Set of PegasusFile objects
     */
    private Set<PegasusFile> getOutputFiles( Collection<GraphNode> nodes ) {

        Set<PegasusFile> files = new HashSet();

        for( GraphNode n : nodes ){
            Job job = (Job)n.getContent();
            files.addAll( job.getOutputFiles() );
        }

        return files;
    }



        
    
    /**
     * Tracks the files created by a job in the both the planner and workflow cache
     * The planner cache stores the put URL's and the GET URL is stored in the
     * workflow cache.
     *
     * @param job  the job whose input files need to be tracked.
     */
    private void trackInCaches( Job job ){


        //check if there is a remote initialdir set
        String path  = job.vdsNS.getStringValue(
                                                 Pegasus.REMOTE_INITIALDIR_KEY );

        SiteCatalogEntry stagingSiteEntry = mSiteStore.lookup( job.getStagingSiteHandle() );
        if ( stagingSiteEntry == null ) {
            this.poolNotFoundMsg( job.getStagingSiteHandle(), "vanilla" ) ;
            mLogger.log( mLogMsg, LogManager.ERROR_MESSAGE_LEVEL );
            throw new RuntimeException( mLogMsg );
        }


        for( Iterator it = job.getOutputFiles().iterator(); it.hasNext(); ){
            PegasusFile pf = (PegasusFile) it.next();
            String lfn = pf.getLFN();
            
            //PM-833 figure out the addOn component just once per lfn
            File addOn = mStagingMapper.mapToRelativeDirectory(job, stagingSiteEntry, lfn);

            //construct the URL to track in planner cache
            String stagingSitePutURL = this.getURLOnSharedScratch( stagingSiteEntry, job, OPERATION.put, addOn, lfn);
            trackInPlannerCache( lfn, stagingSitePutURL, stagingSiteEntry.getSiteHandle() );

            String stagingSiteGetURL = this.getURLOnSharedScratch( stagingSiteEntry, job, OPERATION.get, addOn, lfn);
            trackInWorkflowCache( lfn, stagingSiteGetURL, stagingSiteEntry.getSiteHandle() );

        }
    }





    
    /**
     * Inserts an entry into the planner cache as a put URL.
     *
     *
     * @param lfn  the logical name of the file.
     * @param pfn  the pfn
     * @param site the site handle
     */
    private void trackInPlannerCache( String lfn,
                                     String pfn,
                                     String site ){

         trackInPlannerCache( lfn, pfn, site, OPERATION.put );
    }

    /**
     * Inserts an entry into the planner cache as a put URL.
     *
     *
     * @param lfn  the logical name of the file.
     * @param rce  replica catalog entry
     * @param type the type of url
     */
    private void trackInPlannerCache( String lfn,
                                      ReplicaCatalogEntry rce,
                                      OPERATION type ){

         mPlannerCache.insert( lfn, rce, type );
    }

    /**
     * Inserts an entry into the planner cache as a put URL.
     *
     *
     * @param lfn  the logical name of the file.
     * @param pfn  the pfn
     * @param site the site handle
     * @param type the type of url
     */
    private void trackInPlannerCache( String lfn,
                                     String pfn,
                                     String site,
                                     OPERATION type ){

         mPlannerCache.insert( lfn, pfn, site, type );
    }
    
    /**
     * Inserts an entry into the workflow cache that is to be written out to the
     * submit directory.
     *
     * @param lfn  the logical name of the file.
     * @param pfn  the pfn
     * @param site the site handle
     */
    private void trackInWorkflowCache( String lfn,
                                     String pfn,
                                     String site ){

         mWorkflowCache.insert( lfn, pfn, site );
    }

    /**
     * Returns a URL on the shared scratch of the staging site
     *
     * @param entry         the SiteCatalogEntry for the associated stagingsite
     * @param job           the job
     * @param operation     the FileServer operation for which we need the URL
     * @param lfn           the LFN can be null to get the path to the directory
     *
     * @return  the URL
     */
    private String getURLOnSharedScratch( SiteCatalogEntry entry ,
                                          Job job,
                                          FileServer.OPERATION operation ,
                                          File addOn,
                                          String lfn ){
        return mStagingMapper.map(job, addOn, entry,  operation, lfn );
        
    }
    /**
     * Returns a URL on the shared scratch of the staging site
     *
     * @param entry         the SiteCatalogEntry for the associated stagingsite
     * @param job           the job
     * @param operation     the FileServer operation for which we need the URL
     * @param lfn           the LFN can be null to get the path to the directory
     *
     * @return  the URL
     */
    private String getURLOnSharedScratchOriginal( SiteCatalogEntry entry ,
                                          Job job,
                                          FileServer.OPERATION operation ,
                                          String lfn ){


        StringBuffer url = new StringBuffer();

        FileServer getServer = entry.selectHeadNodeScratchSharedFileServer( operation );

        if( getServer == null ){
            this.complainForScratchFileServer(job, operation, entry.getSiteHandle());
        }

        url.append( getServer.getURLPrefix() ).
            append( mSiteStore.getExternalWorkDirectory(getServer, entry.getSiteHandle() ));

        if( lfn != null ){
            url.append( File.separatorChar ).append( lfn );
        }

        return url.toString();
    }

    /**
     * Complains for a missing head node file server on a site for a job
     *
     * @param job       the job
     * @param operation the operation
     * @param site      the site
     */
    private void complainForScratchFileServer( Job job,
                                                FileServer.OPERATION operation,
                                                String site) {
        this.complainForScratchFileServer( job.getID(), operation, site);
    }

    /**
     * Complains for a missing head node file server on a site for a job
     *
     * @param jobname  the name of the job
     * @param operation the file server operation
     * @param site     the site
     */
    private void complainForScratchFileServer( String jobname,
                                                FileServer.OPERATION operation,
                                                String site) {
        StringBuffer error = new StringBuffer();
        error.append( "[" ).append( REFINER_NAME ).append( "] ");
        if( jobname != null ){
            error.append( "For job (" ).append( jobname).append( ")." );
        }
        error.append( " File Server not specified for shared-scratch filesystem for site: ").
              append( site );
        throw new RuntimeException( error.toString() );

    }

    
    /**
     * Initializes a Replica Catalog Instance that is used to store
     * the GET URL's for all files on the staging site ( inputs staged and outputs
     * created ).
     *
     * @param dag  the workflow being planned
     *
     * @return handle to transient catalog
     */
    private ReplicaCatalog initializeWorkflowCacheFile( ADag dag ){
        ReplicaCatalog rc = null;
        mLogger.log("Initialising Workflow Cache File in the Submit Directory",
                    LogManager.DEBUG_MESSAGE_LEVEL );


        Properties cacheProps = mProps.getVDSProperties().matchingSubset(
                                                              ReplicaCatalog.c_prefix,
                                                              false );
        String file = mPOptions.getSubmitDirectory() + File.separatorChar +
                          getCacheFileName( dag );

        //set the appropriate property to designate path to file
        cacheProps.setProperty( WORKFLOW_CACHE_REPLICA_CATALOG_KEY, file );

        try{
            rc = ReplicaFactory.loadInstance(
                                          WORKFLOW_CACHE_FILE_IMPLEMENTOR,
                                          cacheProps);
        }
        catch( Exception e ){
            throw new RuntimeException( "Unable to initialize Workflow Cache File in the Submit Directory  " + file,
                                         e );

        }
        return rc;
    }

     /**
     * Constructs the basename to the cache file that is to be used
     * to log the transient files. The basename is dependant on whether the
     * basename prefix has been specified at runtime or not.
     *
     * @param adag  the ADag object containing the workflow that is being
     *              concretized.
     *
     * @return the name of the cache file
     */
    private String getCacheFileName(ADag adag){
        StringBuffer sb = new StringBuffer();
        String bprefix = mPOptions.getBasenamePrefix();

        if(bprefix != null){
            //the prefix is not null using it
            sb.append(bprefix);
        }
        else{
            //generate the prefix from the name of the dag
            sb.append(adag.getLabel()).append("-").
           append(adag.getIndex());
        }
        //append the suffix
        sb.append(".cache");

        return sb.toString();

    }

    /**
     * Returns a boolean indicating whether to bypass first level staging for a
     * file or not
     *
     * @param entry        a ReplicaCatalogEntry matching the selected replica location.
     * @param file         the corresponding Pegasus File object
     * @param job          the associated job
     *
     * @return  boolean indicating whether we need to enable bypass or not
     */
    private boolean bypassStagingForInputFile( ReplicaCatalogEntry entry , PegasusFile file, Job job ) {
        boolean bypass = false;
        String computeSite = job.getSiteHandle();
        //check if user has it configured for bypassing the staging and
        //we are in pegasus lite mode
        if( this.mBypassStagingForInputs && mPegasusConfiguration.jobSetupForWorkerNodeExecution(job) ){
            boolean isFileURL = entry.getPFN().startsWith( PegasusURL.FILE_URL_SCHEME);
            String fileSite = entry.getResourceHandle();

            if( mPegasusConfiguration.jobSetupForCondorIO(job, mProps) ){
                //additional check for condor io
                //we need to inspect the URL and it's location
                //only file urls for input files are eligible for bypass
                if( isFileURL &&
                    fileSite.equals( "local" ) ){
                    //in condor io  we cannot remap the destination URL
                    //we need to make sure the PFN ends with lfn to enable bypass
                    bypass = entry.getPFN().endsWith( file.getLFN() );
                }
            }
            else{
                //for non shared fs case we can bypass all url's safely
                //other than file urls
                bypass = isFileURL ?
                         fileSite.equalsIgnoreCase( computeSite )://file site is same as the compute site
                         true;
            }
            
        }

        return bypass;
    }

    /**
     * Helped method for logging removal message. If removed is true, then logged on
     * debug else logged as warning.
     * 
     * @param job       the job 
     * @param file      the file to be removed
     * @param prefix    prefix for log message
     * @param removed   whether removal was successful or not.
     * 
     */
    private void logRemoval( Job job, PegasusFile file, String prefix, boolean removed) {
        StringBuilder message = new StringBuilder();
        message.append( prefix ).append( " : " );
        if( removed ){
            message.append( "Removed file " ).append( file.getLFN() ).append( " for job " ).
                    append( job.getID() );
                
            mLogger.log( message.toString() , LogManager.DEBUG_MESSAGE_LEVEL );
        }
        else{
            //warn
            message.append( "Unable to remove file " ).append( file.getLFN() ).append( " for job " ).
                    append( job.getID() );
                
            mLogger.log( message.toString() , LogManager.WARNING_MESSAGE_LEVEL );
        }
    }

    /**
     * Returns the relative submit directory for the job from the top level
     * submit directory where workflow files are written.
     * 
     * @param job
     * @return 
     */
    protected String getRelativeSubmitDirectory(Job job) {
        
        String relative = null;
        try {
            File f =  mSubmitDirMapper.getRelativeDir(job);
            mLogger.log("Directory for job " + job.getID() + " is " + f,
                         LogManager.DEBUG_MESSAGE_LEVEL );
            relative = f.getPath();
        } catch ( Exception ex) {
            throw new RuntimeException( "Error while determining relative submit dir for job " + job.getID() , ex);
        }
        return relative;
        
    }

    


}
