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


package org.griphyn.cPlanner.transfer.refiner;


import org.griphyn.cPlanner.classes.ADag;
import org.griphyn.cPlanner.classes.PlannerOptions;
import org.griphyn.cPlanner.classes.SubInfo;
import org.griphyn.cPlanner.classes.FileTransfer;

import org.griphyn.cPlanner.common.PegasusProperties;
import org.griphyn.cPlanner.common.LogManager;

import org.griphyn.cPlanner.namespace.VDS;

import org.griphyn.common.catalog.TransformationCatalogEntry;

import org.griphyn.cPlanner.transfer.Refiner;

import java.util.Collection;
import java.util.List;
import java.util.ArrayList;
import java.util.Vector;
import java.util.Iterator;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.HashSet;
import org.griphyn.cPlanner.classes.PegasusBag;
import org.griphyn.cPlanner.engine.ReplicaCatalogBridge;

/**
 * An extension of the default refiner, that allows the user to specify
 * the number of transfer nodes per execution site for stagein and stageout.
 *
 * @author Karan Vahi
 * @version $Revision$
 */

public class Bundle extends Default {

    /**
     * A short description of the transfer refinement.
     */
    public static final String DESCRIPTION =
                      "Bundle Mode (stagein files distributed amongst bundles)";


    /**
     * The default bundling factor that identifies the number of transfer jobs
     * that are being created per execution pool for stageing in data for
     * the workflow.
     */
    public static final String DEFAULT_STAGE_IN_BUNDLE_FACTOR = "1";

    /**
     * The default bundling factor that identifies the number of transfer jobs
     * that are being created per execution pool while stageing data out.
     */
    public static final String DEFAULT_STAGE_OUT_BUNDLE_FACTOR = "1";


    /**
     * The map containing the list of stage in transfer jobs that are being
     * created for the workflow indexed by the execution poolname.
     */
    private Map mStageInMap;

    /**
     * The map indexed by compute jobnames that contains the list of stagin job
     * names that are being added during the traversal of the workflow. This is
     * used to construct the relations that need to be added to workflow, once
     * the traversal is done.
     */
    private Map mRelationsMap;

    /**
     * The map containing the stage in bundle values indexed by the name of the
     * pool. If the bundle value is not specified, then null is stored.
     */
    private Map mSIBundleMap;

    /**
     * The map indexed by staged executable logical name. Each entry is the
     * name of the corresponding setup job, that changes the XBit on the staged
     * file.
     */
    protected Map mSetupMap;


    /**
     * A map indexed by site name, that contains the pointer to the stage out
     * PoolTransfer objects for that site. This is per level of the workflow.
     */
    private Map mStageOutMapPerLevel;


    /**
     * The current level of the jobs being traversed.
     */
    private int mCurrentSOLevel;

    /**
     * The handle to the replica catalog bridge.
     */
    private ReplicaCatalogBridge mRCB;

    /**
     * The job prefix that needs to be applied to the job file basenames.
     */
    protected String mJobPrefix;


    /**
     * The overloaded constructor.
     *
     * @param dag        the workflow to which transfer nodes need to be added.
     * @param bag        the bag of initialization objects
     *
     */
    public Bundle( ADag dag, PegasusBag bag ){
        super( dag, bag );
        mStageInMap   = new HashMap( mPOptions.getExecutionSites().size());
        mSIBundleMap  = new HashMap();
        mRelationsMap = new HashMap();
        mSetupMap     = new HashMap();
        mCurrentSOLevel = -1;
        mJobPrefix    = mPOptions.getJobnamePrefix();
    }

    /**
     * Adds the stage in transfer nodes which transfer the input files for a job,
     * from the location returned from the replica catalog to the job's execution
     * pool.
     *
     * @param job   <code>SubInfo</code> object corresponding to the node to
     *              which the files are to be transferred to.
     * @param files Collection of <code>FileTransfer</code> objects containing the
     *              information about source and destURL's.
     */
    public  void addStageInXFERNodes(SubInfo job,
                                     Collection files){
        String jobName    = job.getName();
        String siteHandle = job.getSiteHandle();
        String key = null;
        String par = null;
        int bundle = -1;

        //to prevent duplicate dependencies
        Set tempSet = new HashSet();

        int staged = 0;
        Collection stagedFiles = new ArrayList();
        Collection stageInExecJobs = new ArrayList();//store list of jobs that are transferring the stage file
        for(Iterator it = files.iterator();it.hasNext();) {
            FileTransfer ft = (FileTransfer) it.next();
            String lfn = ft.getLFN();

            //get the key for this lfn and pool
            //if the key already in the table
            //then remove the entry from
            //the Vector and add a dependency
            //in the graph
            key = this.constructFileKey(lfn, siteHandle);
            par = (String) mFileTable.get(key);
            //System.out.println("lfn " + lfn + " par " + par);
            if (par != null) {
                it.remove();

                //check if tempSet does not contain the parent
                //fix for sonal's bug
                tempSet.add(par);

                if(ft.isTransferringExecutableFile()){
                    //currently we have only one file to be staged per
                    //compute job . Taking a short cut in determining
                    //the name of setXBit job
                    String xBitJobName = (String)mSetupMap.get(key);
                    if(key == null){
                        throw new RuntimeException("Internal Pegasus Error while " +
                                                   "constructing bundled stagein jobs");
                    }
                    //add relation xbitjob->computejob
                    this.addRelation(xBitJobName,jobName);
                }

            } else {
                //get the name of the transfer job
                boolean contains = mStageInMap.containsKey(siteHandle);
                //following pieces need rearragnement!
                if(!contains){
                    bundle = getSISiteBundleValue(siteHandle,
                                                job.vdsNS.getStringValue(VDS.BUNDLE_STAGE_IN_KEY));
                    mSIBundleMap.put(siteHandle,Integer.toString(bundle));
                }
                PoolTransfer pt = (contains)?
                                  (PoolTransfer)mStageInMap.get(siteHandle):
                                  new PoolTransfer(siteHandle,bundle);
                if(!contains){
                    mStageInMap.put(siteHandle,pt);
                }
                //add the FT to the appropriate transfer job.
                String newJobName = pt.addTransfer(ft);

                if(ft.isTransferringExecutableFile()){
                    //currently we have only one file to be staged per
                    //compute job
//                    Collection execFiles = new ArrayList(1);
//                    execFiles.add(ft);
                    //add both the name of the stagein job and the executable file
                    stageInExecJobs.add( newJobName );
                    stagedFiles.add( ft );

//                    mTXStageInImplementation.addSetXBitJobs(job, newJobName,
//                                                            execFiles,
//                                                            SubInfo.STAGE_IN_JOB);
                    mLogger.log("Entered " + key + "->" +
                                mTXStageInImplementation.getSetXBitJobName(job.getName(),staged),
                                LogManager.DEBUG_MESSAGE_LEVEL);
                    mSetupMap.put(key,
                                  mTXStageInImplementation.getSetXBitJobName(job.getName(),staged));
                    staged++;
                }

                //make a new entry into the table
                mFileTable.put(key, newJobName);
                //add the newJobName to the tempSet so that even
                //if the job has duplicate input files only one instance
                //of transfer is scheduled. This came up during collapsing
                //June 15th, 2004
                tempSet.add(newJobName);


            }
        }

        //if there were any staged files
        //add the setXBitJobs for them
        int index = 0;
        Iterator jobIt= stageInExecJobs.iterator();
        for( Iterator it = stagedFiles.iterator(); it.hasNext(); index++){
            Collection execFiles = new ArrayList(1);
            execFiles.add( it.next() );
            mTXStageInImplementation.addSetXBitJobs(job, (String)jobIt.next(),
                                                         execFiles,
                                                         SubInfo.STAGE_IN_JOB,
                                                         index);

        }


        //add the temp set to the relations
        //relations are added to the workflow in the end.
        mRelationsMap.put(jobName,tempSet);


    }


    /**
     * Adds the stageout transfer nodes, that stage data to an output site
     * specified by the user.
     *
     * @param job   <code>SubInfo</code> object corresponding to the node to
     *              which the files are to be transferred to.
     * @param files Collection of <code>FileTransfer</code> objects containing the
     *              information about source and destURL's.
     * @param rcb   bridge to the Replica Catalog. Used for creating registration
     *              nodes in the workflow.
     * @param deletedLeaf to specify whether the node is being added for
     *                      a deleted node by the reduction engine or not.
     *                      default: false
     */
    public  void addStageOutXFERNodes(SubInfo job,
                                      Collection files,
                                      ReplicaCatalogBridge rcb,
                                      boolean deletedLeaf){

        //initializing rcb till the change in function signature happens
        //needs to be passed during refiner initialization
        mRCB = rcb;

        //sanity check
        if( files.isEmpty() ){
            return;
        }

        String jobName = job.getName();
//        String regJob = this.REGISTER_PREFIX + jobName;

        mLogMsg = "Adding output pool nodes for job " + jobName;

        //separate the files for transfer
        //and for registration
        List txFiles = new ArrayList();
        List regFiles = new ArrayList();
        for (Iterator it = files.iterator(); it.hasNext(); ) {
            FileTransfer ft = (FileTransfer) it.next();
            if (!ft.getTransientTransferFlag()) {
                txFiles.add(ft);
            }
            if (!ft.getTransientRegFlag()) {
                regFiles.add(ft);
            }
        }

        boolean makeTNode = !txFiles.isEmpty();
        boolean makeRNode = !regFiles.isEmpty();

        int level   = job.getLevel();
        String site = job.getSiteHandle();
        int bundleValue = getSOSiteBundleValue( site,
                                                getComputeJobBundleValue( job ) );

        if ( level != mCurrentSOLevel ){
            mCurrentSOLevel = level;
            //we are starting on a new level of the workflow.
            //reinitialize stuff
            this.resetStageOutMap();
        }


        TransferContainer soTC = null;
        if (makeTNode) {

            //get the appropriate pool transfer object for the site
            PoolTransfer pt = this.getStageOutPoolTransfer(  site, bundleValue );
            //we add all the file transfers to the pool transfer
            soTC = pt.addTransfer( txFiles, level, SubInfo.STAGE_OUT_JOB );
            String soJob = soTC.getTXName();

            if (!deletedLeaf) {
                //need to add a relation between a compute and stage-out
                //job only if the compute job was not reduced.
                addRelation( jobName, soJob );
            }
            //moved to the resetStageOut method
//            if (makeRNode) {
//                addRelation( soJob, soTC.getRegName() );
//            }
        }
        else if ( makeRNode ) {
            //add an empty file transfer
            //get the appropriate pool transfer object for the site
            PoolTransfer pt = this.getStageOutPoolTransfer(  site, bundleValue );
            //we add all the file transfers to the pool transfer
            soTC = pt.addTransfer( new Vector(), level, SubInfo.STAGE_OUT_JOB );


            //direct link between compute job and registration job
            addRelation( jobName, soTC.getRegName() );

        }
        if ( makeRNode ) {
            soTC.addRegistrationFiles( regFiles );
            //call to make the reg subinfo
            //added in make registration node
 //           addJob(createRegistrationJob(regJob, job, regFiles, rcb));
        }


    }

    /**
     * Returns the bundle value associated with a compute job as a String.
     * 
     * @param job
     * 
     * @return value as String or NULL
     */
    protected String getComputeJobBundleValue( SubInfo job ){
        return  job.vdsNS.getStringValue( VDS.BUNDLE_STAGE_OUT_KEY );
    }
    
    /**
     * Signals that the traversal of the workflow is done. At this point the
     * transfer nodes are actually constructed traversing through the transfer
     * containers and the stdin of the transfer jobs written.
     */
    public void done(){
        //traverse through the stagein map and
        //add transfer nodes per pool
        String key; String value;
        PoolTransfer pt;
        TransferContainer tc;
        Map.Entry entry;
        SubInfo job = new SubInfo();

        for(Iterator it = mStageInMap.entrySet().iterator();it.hasNext();){
            entry = (Map.Entry)it.next();
            key = (String)entry.getKey();
            pt   = (PoolTransfer)entry.getValue();
            mLogger.log("Adding stage in transfer nodes for pool " + key,
                        LogManager.DEBUG_MESSAGE_LEVEL);

            for(Iterator pIt = pt.getTransferContainerIterator();pIt.hasNext();){
                tc = (TransferContainer)pIt.next();
                if(tc == null){
                    //break out
                    break;
                }
                mLogger.log("Adding stagein transfer node " + tc.getTXName(),
                            LogManager.DEBUG_MESSAGE_LEVEL);
                //added in make transfer node
                //mDag.addNewJob(tc.getName());
                //we just need the execution pool in the job object
                job.executionPool = key;
                addJob(mTXStageInImplementation.createTransferJob(job,tc.getFileTransfers(),
                                                           null,tc.getTXName(),
                                                           SubInfo.STAGE_IN_JOB));

            }
        }

        //adding relations that tie in the stagin
        //jobs to the compute jobs.
        for(Iterator it = mRelationsMap.entrySet().iterator();it.hasNext();){
            entry = (Map.Entry)it.next();
            key   = (String)entry.getKey();
            mLogger.log("Adding relations for job " + key,
                        LogManager.DEBUG_MESSAGE_LEVEL);
            for(Iterator pIt = ((Collection)entry.getValue()).iterator();
                                                              pIt.hasNext();){
                value = (String)pIt.next();
                mLogMsg = "Adding relation " + value + " -> " + key;
                mLogger.log(mLogMsg, LogManager.DEBUG_MESSAGE_LEVEL);
//                mDag.addNewRelation(value,key);
                addRelation(value,key);
            }
        }

        //reset the stageout map too
        this.resetStageOutMap();
    }

    /**
     * Returns a textual description of the transfer mode.
     *
     * @return a short textual description
     */
    public  String getDescription(){
        return this.DESCRIPTION;
    }


    /**
     * Determines the bundle factor for a particular site on the basis of the
     * stage in bundle value associcated with the underlying transfer
     * transformation in the transformation catalog. If the key is not found,
     * then the default value is returned. In case of the default value being
     * null the global default is returned.
     *
     * @param site    the site at which the value is desired.
     * @param deflt   the default value.
     *
     * @return the bundle factor.
     *
     * @see #DEFAULT_BUNDLE_STAGE_IN_FACTOR
     */
    protected int getSISiteBundleValue(String site,  String deflt){
        //this should be parameterised Karan Dec 20,2005
        TransformationCatalogEntry entry  =
            mTXStageInImplementation.getTransformationCatalogEntry(site);
        SubInfo sub = new SubInfo();
        String value = (deflt == null)?
                        this.DEFAULT_STAGE_IN_BUNDLE_FACTOR:
                        deflt;

        if(entry != null){
            sub.updateProfiles(entry);
            value = (sub.vdsNS.containsKey( VDS.BUNDLE_STAGE_IN_KEY ))?
                     sub.vdsNS.getStringValue( VDS.BUNDLE_STAGE_IN_KEY ):
                     value;
        }

        return Integer.parseInt(value);
    }


    /**
     * Determines the bundle factor for a particular site on the basis of the
     * stage out bundle value associcated with the underlying transfer
     * transformation in the transformation catalog. If the key is not found,
     * then the default value is returned. In case of the default value being
     * null the global default is returned.
     *
     * @param site    the site at which the value is desired.
     * @param deflt   the default value.
     *
     * @return the bundle factor.
     *
     * @see #DEFAULT_STAGE_OUT_BUNDLE_FACTOR
     */
    protected int getSOSiteBundleValue( String site,  String deflt ){
        //this should be parameterised Karan Dec 20,2005
        TransformationCatalogEntry entry  =
            mTXStageInImplementation.getTransformationCatalogEntry(site);
        SubInfo sub = new SubInfo();
        String value = (deflt == null)?
                        this.DEFAULT_STAGE_OUT_BUNDLE_FACTOR:
                        deflt;

        if(entry != null){
            sub.updateProfiles(entry);
            value = (sub.vdsNS.containsKey( VDS.BUNDLE_STAGE_OUT_KEY ))?
                     sub.vdsNS.getStringValue( VDS.BUNDLE_STAGE_OUT_KEY ):
                     value;
        }

        return Integer.parseInt(value);
    }


    /**
     * Returns the appropriate pool transfer for a particular site.
     *
     * @param site  the site for which the PT is reqd.
     * @param num   the number of Stageout jobs required for that Pool.
     *
     * @return the PoolTransfer
     */
    public PoolTransfer getStageOutPoolTransfer( String site, int num  ){

        if ( this.mStageOutMapPerLevel.containsKey( site ) ){
            return ( PoolTransfer ) this.mStageOutMapPerLevel.get( site );
        }
        else{
            PoolTransfer pt = new PoolTransfer( site, num );
            this.mStageOutMapPerLevel.put( site, pt );
            return pt;
        }
    }

    /**
     * Resets the stage out map.
     */
    protected void resetStageOutMap(){
        if ( this.mStageOutMapPerLevel != null ){
            //before flushing add the stageout nodes to the workflow
            SubInfo job = new SubInfo();

            for( Iterator it = mStageOutMapPerLevel.values().iterator(); it.hasNext(); ){
                PoolTransfer pt = ( PoolTransfer ) it.next();
                job.setSiteHandle( pt.mPool );

                mLogger.log( "Adding jobs for staging out data from site " + pt.mPool,
                             LogManager.DEBUG_MESSAGE_LEVEL );

                //traverse through all the TransferContainers
                for( Iterator tcIt = pt.getTransferContainerIterator(); tcIt.hasNext(); ){
                    TransferContainer tc = ( TransferContainer ) tcIt.next();
                    if(tc == null){
                        //break out
                        break;
                    }

                    //add the stageout job if required
                    SubInfo soJob = null;
                    if( !tc.getFileTransfers().isEmpty() ){
                        mLogger.log( "Adding stage-out job " + tc.getTXName(),
                                     LogManager.DEBUG_MESSAGE_LEVEL);
                        soJob = mTXStageOutImplementation.createTransferJob(
                                                             job, tc.getFileTransfers(), null,
                                                             tc.getTXName(), SubInfo.STAGE_OUT_JOB );
                        addJob( soJob );
                    }

                    //add registration job if required
                    if( !tc.getRegistrationFiles().isEmpty() ){

                        //add relation to stage out if the stageout job was created
                        if( soJob != null ){
                            //make the stageout job the super node for the registration job
                            job.setName( soJob.getName() );
                            addRelation( tc.getTXName(), tc.getRegName() );
                        }

                        mLogger.log( "Adding registration job " + tc.getRegName(),
                                     LogManager.DEBUG_MESSAGE_LEVEL );
                        addJob(createRegistrationJob( tc.getRegName(), job, tc.getRegistrationFiles(), mRCB));

                    }

                }
            }
        }

        mStageOutMapPerLevel = new HashMap();
    }

    /**
     * A container class for storing the name of the transfer job, the list of
     * file transfers that the job is responsible for.
     */
    protected class TransferContainer{

        /**
         * The name of the transfer job.
         */
        private String mTXName;

        /**
         * The name of the registration job.
         */
        private String mRegName;


        /**
         * The collection of <code>FileTransfer</code> objects containing the
         * transfers the job is responsible for.
         */
        private Collection mFileTXList;

        /**
         * The collection of <code>FileTransfer</code> objects containing the
         * files that need to be registered.
         */
        private Collection mRegFiles;


        /**
         * The type of the transfers the job is responsible for.
         */
        private int mTransferType;


        /**
         * The default constructor.
         */
        public TransferContainer(){
            mTXName       = null;
            mRegName      = null;
            mFileTXList   = new Vector();
            mRegFiles     = new Vector();
            mTransferType = SubInfo.STAGE_IN_JOB;
        }

        /**
         * Sets the name of the transfer job.
         *
         * @param name  the name of the transfer job.
         */
        public void setTXName(String name){
            mTXName = name;
        }

        /**
         * Sets the name of the registration job.
         *
         * @param name  the name of the transfer job.
         */
        public void setRegName(String name){
            mRegName = name;
        }


        /**
         * Adds a file transfer to the underlying collection.
         *
         * @param transfer  the <code>FileTransfer</code> containing the
         *                  information about a single transfer.
         */
        public void addTransfer(FileTransfer transfer){
            mFileTXList.add(transfer);
        }

        /**
         * Adds a file transfer to the underlying collection.
         *
         * @param files   collection of <code>FileTransfer</code>.
         */
        public void addTransfer( Collection files ){
            mFileTXList.addAll( files );
        }

        /**
         * Adds a Collection of File transfer to the underlying collection of
         * files to be registered.
         *
         * @param files   collection of <code>FileTransfer</code>.
         */
        public void addRegistrationFiles( Collection files ){
            mRegFiles.addAll( files );
        }



        /**
         * Sets the transfer type for the transfers associated.
         *
         * @param type  type of transfer.
         */
        public void setTransferType(int type){
            mTransferType = type;
        }

        /**
         * Returns the name of the transfer job.
         *
         * @return name of the transfer job.
         */
        public String getTXName(){
            return mTXName;
        }

        /**
         * Returns the name of the registration job.
         *
         * @return name of the registration job.
         */
        public String getRegName(){
            return mRegName;
        }



        /**
         * Returns the collection of transfers associated with this transfer
         * container.
         *
         * @return a collection of <code>FileTransfer</code> objects.
         */
        public Collection getFileTransfers(){
            return mFileTXList;
        }

        /**
         * Returns the collection of registration files associated with this transfer
         * container.
         *
         * @return a collection of <code>FileTransfer</code> objects.
         */
        public Collection getRegistrationFiles(){
            return mRegFiles;
        }

    }

    /**
     * A container to store the transfers that need to be done on a single pool.
     * The transfers are stored over a collection of Transfer Containers with
     * each transfer container responsible for one transfer job.
     */
    protected class PoolTransfer{

        /**
         * The maximum number of transfer jobs that are allowed for this
         * particular pool.
         */
        private int mCapacity;

        /**
         * The index of the job to which the next transfer for the pool would
         * be scheduled.
         */
        private int mNext;

        /**
         * The pool for which these transfers are grouped.
         */
        private String mPool;

        /**
         * The list of <code>TransferContainer</code> that correspond to
         * each transfer job.
         */
        private List mTXContainers;

        /**
         * The default constructor.
         */
        public PoolTransfer(){
            mCapacity = 0;
            mNext     = -1;
            mPool     = null;
            mTXContainers = null;
        }

        /**
         * Convenience constructor.
         *
         * @param pool    the pool name for which transfers are being grouped.
         * @param number  the number of transfer jobs that are going to be created
         *                for the pool.
         */
        public PoolTransfer(String pool, int number){
            mCapacity = number;
            mNext     = 0;
            mPool     = pool;
            mTXContainers = new ArrayList(number);
            //intialize to null
            for(int i = 0; i < number; i++){
                mTXContainers.add(null);
            }
        }

      /**
        * Adds a a collection of <code>FileTransfer</code> objects to the
        * appropriate TransferContainer. The collection is added to a single
        * TransferContainer, and the pointer is then updated to the next container.
        *
        * @param files  the collection <code>FileTransfer</code> to be added.
        * @param level  the level of the workflow
        * @param type   the type of transfer job
        *
        * @return  the Transfer Container to which the job file transfers were added.
        */
       public TransferContainer addTransfer( Collection files, int level, int type ){
           //we add the transfer to the container pointed
           //by next
           Object obj = mTXContainers.get(mNext);
           TransferContainer tc = null;
           if(obj == null){
               //on demand add a new transfer container to the end
               //is there a scope for gaps??
               tc = new TransferContainer();
               tc.setTXName( getTXJobName(  mNext,  type, level ) );
               //add the name for the registration job that maybe associated
               tc.setRegName( getRegJobName( mNext, level) );
               mTXContainers.set(mNext,tc);
           }
           else{
               tc = (TransferContainer)obj;
           }
           tc.addTransfer( files );

           //update the next pointer to maintain
           //round robin status
           mNext = (mNext < (mCapacity -1))?
                    mNext + 1 :
                    0;

           return tc;
       }



        /**
         * Adds a file transfer to the appropriate TransferContainer.
         * The file transfers are added in a round robin manner underneath.
         *
         * @param transfer  the <code>FileTransfer</code> containing the
         *                  information about a single transfer.
         *
         * @return  the name of the transfer job to which the transfer is added.
         */
        public String addTransfer(FileTransfer transfer){
            //we add the transfer to the container pointed
            //by next
            Object obj = mTXContainers.get(mNext);
            TransferContainer tc = null;
            if(obj == null){
                //on demand add a new transfer container to the end
                //is there a scope for gaps??
                tc = new TransferContainer();
                tc.setTXName( getTXJobName( mNext, SubInfo.STAGE_IN_JOB ) );
                mTXContainers.set(mNext,tc);
            }
            else{
                tc = (TransferContainer)obj;
            }
            tc.addTransfer(transfer);

            //update the next pointer to maintain
            //round robin status
            mNext = (mNext < (mCapacity -1))?
                     mNext + 1 :
                     0;

            return tc.getTXName();
        }

        /**
         * Returns the iterator to the list of transfer containers.
         *
         * @return the iterator.
         */
        public Iterator getTransferContainerIterator(){
            return mTXContainers.iterator();
        }


        /**
         * Generates the name of the transfer job, that is unique for the given
         * workflow.
         *
         * @param counter  the index for the transfer job.
         * @param type     the type of transfer job.
         * @param level    the level of the workflow.
         *
         * @return the name of the transfer job.
         */
        private String getTXJobName( int counter, int type, int level ){
            StringBuffer sb = new StringBuffer();
            switch ( type ){
                case SubInfo.STAGE_IN_JOB:
                    sb.append( Refiner.STAGE_IN_PREFIX );
                    break;

                case SubInfo.STAGE_OUT_JOB:
                    sb.append( Refiner.STAGE_OUT_PREFIX );
                    break;

                default:
                    throw new RuntimeException( "Wrong type specified " + type );
            }

            //append the job prefix if specified in options at runtime
            if ( mJobPrefix != null ) { sb.append( mJobPrefix ); }

            sb.append( mPool ).append( "_" ).append( level ).
               append( "_" ).append( counter );

           return sb.toString();
        }

        /**
         * Generates the name of the transfer job, that is unique for the given
         * workflow.
         *
         * @param counter  the index for the registration job.
         * @param level    the level of the workflow.
         *
         * @return the name of the transfer job.
         */
        private String getRegJobName( int counter,  int level ){
            StringBuffer sb = new StringBuffer();
            sb.append( Refiner.REGISTER_PREFIX );


            //append the job prefix if specified in options at runtime
            if ( mJobPrefix != null ) { sb.append( mJobPrefix ); }

            sb.append( mPool ).append( "_" ).append( level ).
               append( "_" ).append( counter );

           return sb.toString();
        }

        /**
         * Return the pool for which the transfers are grouped
         * 
         * @retrun name of pool.
         */
        public String getPoolName(){
            return this.mPool;
        }

        /**
         * Generates the name of the transfer job, that is unique for the given
         * workflow.
         *
         * @param counter  the index for the transfer job.
         * @param type     the type of transfer job.
         *
         * @return the name of the transfer job.
         */
        private String getTXJobName( int counter, int type ){
            StringBuffer sb = new StringBuffer();
            switch ( type ){
                case SubInfo.STAGE_IN_JOB:
                    sb.append( Refiner.STAGE_IN_PREFIX );
                    break;

                case SubInfo.STAGE_OUT_JOB:
                    sb.append( Refiner.STAGE_OUT_PREFIX );
                    break;

                default:
                    throw new RuntimeException( "Wrong type specified " + type );
            }


            //append the job prefix if specified in options at runtime
            if ( mJobPrefix != null ) { sb.append( mJobPrefix ); }


            sb.append(mPool).append("_").append(counter);

           return sb.toString();
        }


    }

}
