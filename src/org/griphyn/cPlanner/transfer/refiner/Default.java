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
import org.griphyn.cPlanner.classes.SubInfo;
import org.griphyn.cPlanner.classes.FileTransfer;
import org.griphyn.cPlanner.classes.PlannerOptions;

import org.griphyn.cPlanner.common.PegasusProperties;
import org.griphyn.cPlanner.common.LogManager;

import org.griphyn.cPlanner.engine.ReplicaCatalogBridge;

import org.griphyn.cPlanner.transfer.MultipleFTPerXFERJobRefiner;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Collection;
import java.util.TreeMap;
import java.util.Map;
import java.util.List;

/**
 * The default transfer refiner, that implements the multiple refiner.
 * For each compute job if required it creates the following
 *          - a single stagein transfer job
 *          - a single stageout transfer job
 *          - a single interpool transfer job
 *
 * In addition this implementation prevents file clobbering while staging in data
 * to a remote site, that is shared amongst jobs.
 *
 * @author Karan Vahi
 * @version $Revision$
 */

public class Default extends MultipleFTPerXFERJobRefiner {

    /**
     * A short description of the transfer refinement.
     */
    public static final String DESCRIPTION = "Default Multiple Refinement ";

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
    public Default(ADag dag,PegasusProperties properties,PlannerOptions options){
        super(dag,properties,options);
        mLogMsg = null;
        mFileTable = new TreeMap();
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
        String jobName = job.getName();
        String pool = job.getSiteHandle();
        int counter = 0;
        String newJobName = this.STAGE_IN_PREFIX + jobName + "_" + counter;
        String key = null;
        String msg = "Adding stagein transfer nodes for job " + jobName;
        String par = null;
        Collection stagedFiles = new ArrayList(1);

        //to prevent duplicate dependencies
        java.util.HashSet tempSet = new java.util.HashSet();
        int staged = 0;
        for (Iterator it = files.iterator();it.hasNext();) {
            FileTransfer ft = (FileTransfer) it.next();
            String lfn = ft.getLFN();

            //get the key for this lfn and pool
            //if the key already in the table
            //then remove the entry from
            //the Vector and add a dependency
            //in the graph
            key = this.constructFileKey(lfn, pool);
            par = (String) mFileTable.get(key);
            //System.out.println("lfn " + lfn + " par " + par);
            if (par != null) {
                it.remove();

                //check if tempSet does not contain the parent
                //fix for sonal's bug
                if (tempSet.contains(par)) {
                    mLogMsg =
                        "IGNORING TO ADD rc pull relation from rc tx node: " +
                        par + " -> " + jobName +
                        " for transferring file " + lfn + " to pool " + pool;

                    mLogger.log(mLogMsg,LogManager.DEBUG_MESSAGE_LEVEL);

                } else {
                    mLogMsg = /*"Adding relation " + par + " -> " + jobName +*/
                        " For transferring file " + lfn;
                    mLogger.log(mLogMsg, LogManager.DEBUG_MESSAGE_LEVEL);
                    addRelation(par,jobName,pool,false);
                    tempSet.add(par);
                }
            } else {
                if(ft.isTransferringExecutableFile()){
                    //add to staged files for adding of
                    //set up job.
                    stagedFiles.add(ft);
                    //the staged execution file should be having the setup
                    //job as parent if it does not preserve x bit
                    if(mTXStageInImplementation.doesPreserveXBit()){
                        mFileTable.put(key,newJobName);
                    }
                    else{
                        mFileTable.put(key,
                                       mTXStageInImplementation.getSetXBitJobName(jobName,staged++));
                    }
                }
                else{
                    //make a new entry into the table
                    mFileTable.put(key, newJobName);
                }
                //add the newJobName to the tempSet so that even
                //if the job has duplicate input files only one instance
                //of transfer is scheduled. This came up during collapsing
                //June 15th, 2004
                tempSet.add(newJobName);
            }
        }

        if (!files.isEmpty()) {
            mLogger.log(msg,LogManager.DEBUG_MESSAGE_LEVEL);
            msg = "Adding new stagein transfer node named " + newJobName;
            mLogger.log(msg,LogManager.DEBUG_MESSAGE_LEVEL);

            //add a direct dependency between compute job
            //and stagein job only if there is no
            //executables being staged
            if(stagedFiles.isEmpty()){
                //add the direct relation
                addRelation(newJobName, jobName, pool, true);
                addJob(mTXStageInImplementation.createTransferJob(job, files,null,
                                                                  newJobName,
                                                                  SubInfo.STAGE_IN_JOB));
            }
            else{
                //the dependency to stage in job is added via the
                //the setup job that does the chmod
                addJob(mTXStageInImplementation.createTransferJob(job,files,stagedFiles,
                                                                  newJobName,
                                                                  SubInfo.STAGE_IN_JOB));
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
        int counter = 0;
        String newJobName = this.INTER_POOL_PREFIX + jobName + "_" + counter;

        String msg = "Adding inter pool nodes for job " + jobName;
        String prevParent = null;

        String lfn = null;
        String key = null;
        String par = null;
        String pool = job.getSiteHandle();

        boolean toAdd = true;

        //to prevent duplicate dependencies
        java.util.HashSet tempSet = new java.util.HashSet();

        //node construction only if there is
        //a file to transfer
        if (!files.isEmpty()) {
            mLogger.log(msg,LogManager.DEBUG_MESSAGE_LEVEL);

            for(Iterator it = files.iterator();it.hasNext();) {
                FileTransfer ft = (FileTransfer) it.next();
                lfn = ft.getLFN();
                //System.out.println("Trying to figure out for lfn " + lfn);


                //to ensure that duplicate edges
                //are not added in the graph
                //between the parent of a node and the
                //inter tx node that transfers the file
                //to the node site.

                //get the key for this lfn and pool
                //if the key already in the table
                //then remove the entry from
                //the Vector and add a dependency
                //in the graph
                key = this.constructFileKey(lfn, pool);
                par = (String) mFileTable.get(key);
                //System.out.println("\nGot Key :" + key + " Value :" + par );
                if (par != null) {
                    //transfer of this file
                    //has already been scheduled
                    //onto the pool
                    it.remove();

                    //check if tempSet does not contain the parent
                    if (tempSet.contains(par)) {
                        mLogMsg =
                            "IGNORING TO ADD interpool relation 1 from inter tx node: " +
                            par + " -> " + jobName +
                            " for transferring file " + lfn + " to pool " +
                            pool;

                        mLogger.log(mLogMsg,LogManager.DEBUG_MESSAGE_LEVEL);

                    } else {
                        mLogMsg =
                            "Adding interpool relation 1 from inter tx node: " +
                            par + " -> " + jobName +
                            " for transferring file " + lfn + " to pool " +
                            pool;
                        mLogger.log(mLogMsg, LogManager.DEBUG_MESSAGE_LEVEL);
                        addRelation(par, jobName);
                        tempSet.add(par);
                    }
                } else {
                    //make a new entry into the table
                    mFileTable.put(key, newJobName);
                    //System.out.println("\nPut Key :" + key + " Value :" + newJobName );

                    //to ensure that duplicate edges
                    //are not added in the graph
                    //between the parent of a node and the
                    //inter tx node that transfers the file
                    //to the node site.
                    if (prevParent == null ||
                        !prevParent.equalsIgnoreCase(ft.getJobName())) {

                        mLogMsg = "Adding interpool relation 2" + ft.getJobName() +
                            " -> " + newJobName +
                            " for transferring file " + lfn + " to pool " +
                            pool;
                        mLogger.log(mLogMsg,LogManager.DEBUG_MESSAGE_LEVEL);
                        addRelation(ft.getJobName(), newJobName);
                    }

                    //we only need to add the relation between a
                    //inter tx node and a node once.
                    if (toAdd) {
                        mLogMsg = "Adding interpool relation 3" + newJobName +
                            " -> " + jobName + " for transferring file " + lfn +
                            " to pool " +
                            pool;
                        mLogger.log(mLogMsg,LogManager.DEBUG_MESSAGE_LEVEL);
                        addRelation(newJobName, jobName);
                        tempSet.add(newJobName);
                        toAdd = false;
                    }

                }

                prevParent = ft.getJobName();
            }

            //add the new job and construct it's
            //subinfo only if the vector is not
            //empty
            if (!files.isEmpty()) {
                msg = "Adding new inter pool node named " + newJobName;
                mLogger.log(msg,LogManager.DEBUG_MESSAGE_LEVEL);

                //added in make transfer node
                addJob(mTXInterImplementation.createTransferJob(job, files,null,
                                                           newJobName,
                                                           SubInfo.INTER_POOL_JOB));
            }

        }
        tempSet = null;

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

        this.addStageOutXFERNodes( job, files, rcb, false);
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
        int counter = 0;
        String newJobName = this.STAGE_OUT_PREFIX + jobName + "_" + counter;
        String regJob = this.REGISTER_PREFIX + jobName;

        mLogMsg = "Adding output pool nodes for job " + jobName;

        //separate the files for transfer
        //and for registration
        List txFiles = new ArrayList();
        List regFiles   = new ArrayList();
        for(Iterator it = files.iterator();it.hasNext();){
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

        if (!files.isEmpty()) {
            mLogger.log(mLogMsg,LogManager.DEBUG_MESSAGE_LEVEL);
            mLogMsg = "Adding new output pool node named " + newJobName;
            mLogger.log(mLogMsg,LogManager.DEBUG_MESSAGE_LEVEL);

            if (makeTNode) {
                //added in make transfer node
                //mDag.addNewJob(newJobName);
                addJob(mTXStageOutImplementation.createTransferJob(job, txFiles,null,
                                                                   newJobName,
                                                                   SubInfo.STAGE_OUT_JOB));
                if (!deletedLeaf) {
                    addRelation(jobName, newJobName);
                }
                if (makeRNode) {
                    addRelation(newJobName, regJob);
                }
            }
            else if (!makeTNode && makeRNode) {
                addRelation(jobName, regJob);

            }
            if (makeRNode) {
                //call to make the reg subinfo
                //added in make registration node
                addJob(createRegistrationJob( regJob, job, regFiles, rcb ));
            }

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
    protected SubInfo createRegistrationJob(String regJobName,
                                            SubInfo job,
                                            Collection files,
                                            ReplicaCatalogBridge rcb ) {

        return rcb.makeRCRegNode( regJobName, job, files);
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
