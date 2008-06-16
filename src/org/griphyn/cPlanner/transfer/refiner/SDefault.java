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
import org.griphyn.cPlanner.classes.FileTransfer;
import org.griphyn.cPlanner.classes.PlannerOptions;
import org.griphyn.cPlanner.classes.SubInfo;

import org.griphyn.cPlanner.common.LogManager;
import org.griphyn.cPlanner.common.PegasusProperties;

import org.griphyn.cPlanner.engine.ReplicaCatalogBridge;


import org.griphyn.cPlanner.transfer.SingleFTPerXFERJobRefiner;


import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.TreeMap;
import java.util.Set;
import java.util.StringTokenizer;

/**
 * The default single refiner, that always creates a transfer job per file
 * transfer that is required. If a compute job requires 3 files, it will
 * create 3 independant stagein jobs for that particular file.
 *
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public class SDefault extends SingleFTPerXFERJobRefiner {

    /**
     * A short description of the transfer refinement.
     */
    public static final String DESCRIPTION = "Default Single Refinement ";

    /**
     * The string holding  the logging messages
     */
    protected String mLogMsg;



    /**
     * A Map containing information about which logical file has been
     * transferred to which site and the name of the stagein transfer node
     * that is transferring the file from the location returned from
     * the replica catalog.
     * The key for the hashmap is logicalfilename:sitehandle and the value would be
     * the name of the transfer node.
     *
     */
    protected Map mFileTable;


    /**
     * The overloaded constructor.
     *
     * @param dag        the workflow to which transfer nodes need to be added.
     * @param properties the <code>PegasusProperties</code> object containing all
     *                   the properties required by Pegasus.
     * @param options    the options passed to the planner.
     *
     */
    public SDefault(ADag dag,PegasusProperties properties,PlannerOptions options){
        super(dag,properties,options);
        mFileTable = new TreeMap();
    }


    /**
     * Adds the stage in transfer nodes which transfer the input files for a job,
     * from the location returned from the replica catalog to the job's execution
     * pool. It creates a stagein job for each file to be transferred.
     *
     * @param job   <code>SubInfo</code> object corresponding to the node to
     *              which the files are to be transferred to.
     * @param files Collection of <code>FileTransfer</code> objects containing the
     *              information about source and destURL's.
     */
    public  void addStageInXFERNodes(SubInfo job,
                                     Collection files){

        String jobName = job.getName();
        String newJob;
        String pool = job.getSiteHandle();
        int counter = 0;
        String key = null;
        String msg = "Adding stage in transfer nodes for job " + jobName;
        String par = null;

        mLogger.log(msg,LogManager.DEBUG_MESSAGE_LEVEL);
        for(Iterator it = files.iterator();it.hasNext();){
            Collection stagedFiles = null;
            FileTransfer ft = (FileTransfer) it.next();
            String lfn = ft.getLFN();
            newJob = this.STAGE_IN_PREFIX + jobName + "_" + counter;

            //get the key for this lfn and pool
            //if the key already in the table
            //then remove the entry from
            //the Vector and add a dependency
            //in the graph
            key = this.constructFileKey(lfn, pool);
            par = (String) mFileTable.get(key);
            if (par != null) {
                it.remove();
                /*mLogMsg = "Adding relation " + par + " -> " + jobName +
                    " for transferring file " + lfn;
                mLogger.log(mLogMsg,LogManager.DEBUG_MESSAGE_LEVEL);*/
                addRelation(par, jobName,pool,false);
            } else {
                if(ft.isTransferringExecutableFile()){
                    //add to staged files for adding of
                    //set up job.
                    stagedFiles = new ArrayList(1);
                    stagedFiles.add(ft);
                    //the staged execution file should be having the setup
                    //job as parent if it does not preserve x bit
                    if(mTXStageInImplementation.doesPreserveXBit()){
                        mFileTable.put(key,newJob);
                    }
                    else{
                        mFileTable.put(key,
                                       mTXStageInImplementation.getSetXBitJobName(jobName,0));
                    }
                }
                else{
                    //make a new entry into the table
                    mFileTable.put(key, newJob);
                }

                //construct the stagein transfer node
                msg = "Adding new stagein transfer node named " + newJob;
                mLogger.log(msg,LogManager.DEBUG_MESSAGE_LEVEL);
                counter++;

                //add a direct dependency between compute job
                //and stagein job only if there is no
                //executables being staged
                //call to add the job
                List file = new ArrayList(1);
                file.add(ft);
                if(stagedFiles == null){
                    //add the direct relation
                    addRelation(newJob, jobName,pool,true);
                    addJob(mTXStageInImplementation.createTransferJob(
                                                          job, file,null,newJob,
                                                          SubInfo.STAGE_IN_JOB));
                }
                else{
                    //the dependency to stage in job is added via the
                    //the setup job that does the chmod
                    addJob(mTXStageInImplementation.createTransferJob(
                                                          job, file,stagedFiles,newJob,
                                                          SubInfo.STAGE_IN_JOB));
                }

            }
        }
    }




    /**
     * Adds the inter pool transfer nodes that are required for  transferring
     * the output files of the parents to the jobs execution site.
     *
     * @param job   <code>SubInfo</code> object corresponding to the node to
     *              which the files are to be transferred to.
     * @param files Collection of <code>FileTransfer</code> objects containing the
     *              information about source and destURL's.
     */
    public void addInterSiteTXNodes(SubInfo job,
                                    Collection files){

        String jobName = job.getName();
        String newJob;
        int counter = 0;
        String msg = "Adding inter pool transfer nodes for job " + job.getName();

        if (!files.isEmpty()) {
            mLogger.log(msg,LogManager.DEBUG_MESSAGE_LEVEL);

        }
        for(Iterator it = files.iterator();it.hasNext();){
            FileTransfer ft = (FileTransfer)it.next();
            newJob = this.INTER_POOL_PREFIX + jobName + "_" + counter;
            msg = "Adding new inter pool node named " + newJob;
            mLogger.log(msg,LogManager.DEBUG_MESSAGE_LEVEL);
            counter++;
            //added in make transfer node
            //mDag.addNewJob(newJob);
            addRelation(ft.getJobName(), newJob);
            addRelation(newJob, jobName);

            //call to make the subinfo
            List file = new ArrayList(1);
            file.add(ft);
            addJob(mTXInterImplementation.createTransferJob(job,file,null ,newJob,
                                                      SubInfo.INTER_POOL_JOB));
        }


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
     *
     */
    public void addStageOutXFERNodes(SubInfo job,
                                     Collection files,
                                     ReplicaCatalogBridge rcb ) {
        this.addStageOutXFERNodes(job, files, rcb, false);
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

        String jobName = job.getName();
        String newJob;
        String regJob = this.REGISTER_PREFIX + jobName;
        int counter = 1;

        String mLogMsg = "Adding stagout transfer nodes for job " + jobName;

        if (!files.isEmpty()) {
            mLogger.log(mLogMsg,LogManager.DEBUG_MESSAGE_LEVEL);

        }
        List regFiles = new ArrayList();
        for(Iterator it = files.iterator();it.hasNext();){
            FileTransfer ft = (FileTransfer)it.next();
            newJob = this.STAGE_OUT_PREFIX + jobName + "_" + counter;
            counter++;
            mLogMsg = "Adding new stageout transfer node named " + newJob;
            mLogger.log(mLogMsg, LogManager.DEBUG_MESSAGE_LEVEL);

            //call to make the subinfo
            if (!ft.getTransientTransferFlag()) {
                //not to add relation if deleted leaf
                if (!deletedLeaf) {
                    addRelation(jobName, newJob);
                }
                if (!ft.getTransientRegFlag()) {
                    addRelation(newJob, regJob);
                    regFiles.add(ft);
                }

                List file = new ArrayList(1);
                file.add(ft);
                addJob(mTXStageOutImplementation.createTransferJob(job,file,null,
                                                                   newJob,
                                                                   SubInfo.STAGE_OUT_JOB));
            }
            //no transfer node but a registration node
            else if (!ft.getTransientRegFlag()) {
                addRelation(jobName, regJob);
                regFiles.add(ft);
            }

        }

        //create the registration job if required
        if (!regFiles.isEmpty()) {
            addJob(createRegistrationJob(regJob,job, regFiles, rcb ));
        }

    }

    /**
     * Creates the registration jobs, which registers the materialized files on
     * the output site in the Replica Catalog.
     *
     * @param regJobName  The name of the job which registers the files in the
     *                    Replica Mechanism.
     * @param job         The job whose output files are to be registered in the
     *                    Replica Mechanism.
     * @param files       Collection of <code>FileTransfer</code> objects containing
     *                    the information about source and destURL's.
     * @param rcb   bridge to the Replica Catalog. Used for creating registration
     *              nodes in the workflow.
     *
     *
     * @return the registration job.
     */
    protected SubInfo createRegistrationJob( String regJobName,
                                             SubInfo job,
                                             Collection files,
                                             ReplicaCatalogBridge rcb ) {

        return rcb.makeRCRegNode(regJobName,job,files);
    }


    /**
     * Signals that the traversal of the workflow is done. This would allow
     * the transfer mechanisms to clean up any state that they might be keeping
     * that needs to be explicitly freed.
     */
    public void done(){

    }




    /**
     * Add a new job to the workflow being refined.
     *
     * @param job  the job to be added.
     */
    public void addJob(SubInfo job){
        mDAG.add(job);
    }

    /**
     * Adds a new relation to the workflow being refiner.
     *
     * @param parent    the jobname of the parent node of the edge.
     * @param child     the jobname of the child node of the edge.
     */
    public void addRelation(String parent,
                            String child){
        mLogger.log("Adding relation " + parent + " -> " + child,
                    LogManager.DEBUG_MESSAGE_LEVEL);
        mDAG.addNewRelation(parent,child);

    }


    /**
     * Adds a new relation to the workflow. In the case when the parent is a
     * transfer job that is added, the parentNew should be set only the first
     * time a relation is added. For subsequent compute jobs that maybe
     * dependant on this, it needs to be set to false.
     *
     * @param parent    the jobname of the parent node of the edge.
     * @param child     the jobname of the child node of the edge.
     * @param site      the execution pool where the transfer node is to be run.
     * @param parentNew the parent node being added, is the new transfer job
     *                  and is being called for the first time.
     */
    public void addRelation(String parent,
                            String child,
                            String site,
                            boolean parentNew){
        mLogger.log("Adding relation " + parent + " -> " + child,
                    LogManager.DEBUG_MESSAGE_LEVEL);
        mDAG.addNewRelation(parent,child);

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
     * Constructs the key for an entry to the file table. The key returned
     * is lfn:siteHandle
     *
     * @param lfn         the logical filename of the file that has to be
     *                    transferred.
     * @param siteHandle  the name of the site to which the file is being
     *                    transferred.
     *
     * @return      the key for the entry to be  made in the filetable.
     */
    protected String constructFileKey(String lfn, String siteHandle) {
        StringBuffer sb = new StringBuffer();
        sb.append(lfn).append(":").append(siteHandle);

        return sb.toString();
    }


}
