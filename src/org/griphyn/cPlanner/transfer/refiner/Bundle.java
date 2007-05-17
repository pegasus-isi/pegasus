/*
 * This file or a portion of this file is licensed under the terms of
 * the Globus Toolkit Public License, found in file GTPL, or at
 * http://www.globus.org/toolkit/download/license.html. This notice must
 * appear in redistributions of this file, with or without modification.
 *
 * Redistributions of this Software, with or without modification, must
 * reproduce the GTPL in: (1) the Software, or (2) the Documentation or
 * some other similar material which is provided with the Software (if
 * any).
 *
 * Copyright 1999-2004 University of Chicago and The University of
 * Southern California. All rights reserved.
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

/**
 * An extension of the default refiner, that allows the user to specify
 * the number of transfer nodes per execution pool.
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
     * that are being created per execution pool for the workflow.
     */
    public static final String DEFAULT_BUNDLE_FACTOR = "1";

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
    private Map mSetupMap;


    /**
     * The overloaded constructor.
     *
     * @param dag        the workflow to which transfer nodes need to be added.
     * @param properties the <code>PegasusProperties</code> object containing all
     *                   the properties required by Pegasus.
     * @param options    the options passed to the planner.
     *
     */
    public Bundle(ADag dag,PegasusProperties properties,PlannerOptions options){
        super(dag, properties,options);
        mStageInMap   = new HashMap(options.getExecutionSites().size());
        mSIBundleMap  = new HashMap();
        mRelationsMap = new HashMap();
        mSetupMap     = new HashMap();

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
                    bundle = getSiteBundleValue(siteHandle,VDS.BUNDLE_STAGE_IN_KEY,
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
                    Collection execFiles = new ArrayList(1);
                    execFiles.add(ft);
                    mTXStageInImplementation.addSetXBitJobs(job, newJobName,
                                                            execFiles,
                                                            SubInfo.STAGE_IN_JOB);
                    mLogger.log("Entered " + key + "->" +
                                mTXStageInImplementation.getSetXBitJobName(job.getName(),0),
                                LogManager.DEBUG_MESSAGE_LEVEL);
                    mSetupMap.put(key,
                                  mTXStageInImplementation.getSetXBitJobName(job.getName(),0));
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



        //add the temp set to the relations
        //relations are added to the workflow in the end.
        mRelationsMap.put(jobName,tempSet);


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
                mLogger.log("Adding transfer node " + tc.getName(),
                            LogManager.DEBUG_MESSAGE_LEVEL);
                //added in make transfer node
                //mDag.addNewJob(tc.getName());
                //we just need the execution pool in the job object
                job.executionPool = key;
                addJob(mTXStageInImplementation.createTransferJob(job,tc.getFileTransfers(),
                                                           null,tc.getName(),
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
     * @param key     the bundle key whose value needs to be searched.
     * @param deflt   the default value.
     *
     * @return the bundle factor.
     *
     * @see #DEFAULT_BUNDLE_FACTOR
     */
    protected int getSiteBundleValue(String site, String key, String deflt){
        //this should be parameterised Karan Dec 20,2005
        TransformationCatalogEntry entry  =
            mTXStageInImplementation.getTransformationCatalogEntry(site);
        SubInfo sub = new SubInfo();
        String value = (deflt == null)?
                        this.DEFAULT_BUNDLE_FACTOR:
                        deflt;

        if(entry != null){
            sub.updateProfiles(entry);
            value = (sub.vdsNS.containsKey(key))?
                     sub.vdsNS.getStringValue(key):
                     value;
        }

        return Integer.parseInt(value);
    }

    /**
     * A container class for storing the name of the transfer job, the list of
     * file transfers that the job is responsible for.
     */
    private class TransferContainer{

        /**
         * The name of the transfer job.
         */
        private String mName;

        /**
         * The collection of <code>FileTransfer</code> objects containing the
         * transfers the job is responsible for.
         */
        private Collection mFileTXList;

        /**
         * The type of the transfers the job is responsible for.
         */
        private int mTransferType;


        /**
         * The default constructor.
         */
        public TransferContainer(){
            mName         = null;
            mFileTXList   = new Vector();
            mTransferType = SubInfo.STAGE_IN_JOB;
        }

        /**
         * Sets the name of the transfer job.
         *
         * @param name  the name of the transfer job.
         */
        public void setName(String name){
            mName = name;
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
        public String getName(){
            return mName;
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
    }

    /**
     * A container to store the transfers that need to be done on a single pool.
     * The transfers are stored over a collection of Transfer Containers with
     * each transfer container responsible for one transfer job.
     */
    private class PoolTransfer{

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
                tc.setName(getTXJobName(mNext));
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

            return tc.getName();
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
         *
         * @return the name of the transfer job.
         */
        private String getTXJobName(int counter){
            StringBuffer sb = new StringBuffer();
            sb.append(Refiner.STAGE_IN_PREFIX).append(mPool).
               append("_").append(counter);

           return sb.toString();
        }

    }

}
