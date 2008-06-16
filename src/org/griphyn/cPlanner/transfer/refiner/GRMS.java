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
import org.griphyn.cPlanner.classes.Data;
import org.griphyn.cPlanner.classes.FileTransfer;
import org.griphyn.cPlanner.classes.NameValue;
import org.griphyn.cPlanner.classes.PlannerOptions;
import org.griphyn.cPlanner.classes.SubInfo;
import org.griphyn.cPlanner.classes.GRMSJob;

import org.griphyn.cPlanner.common.LogManager;
import org.griphyn.cPlanner.common.PegasusProperties;
import org.griphyn.cPlanner.common.Utility;

import org.griphyn.cPlanner.transfer.SingleFTPerXFERJobRefiner;

import org.griphyn.cPlanner.engine.ReplicaCatalogBridge;

import java.io.File;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.StringTokenizer;

/**
 * The refiner that is compatible with the GRMS system. In this the job
 * specification specifies what input and output file it needs. Hence in this
 * refiner, unlike the other refiners we do not add any transfer nodes explicitly,
 * just specify the appropriate urls and let the GRMS system do the magic of
 * transferring the files to and from the execution pools. It does not perform
 * any registration of the output files at present, and DOES NOT SUPPORT
 * RANDOM DIRECTORIES as directories on the remote side are handled by the
 * GRMS system.
 * Please Note that this has to be used in conjunction with the GRMSWriter.

 *
 * @author Karan Vahi
 * @version $Revision$
 */
public class GRMS extends SingleFTPerXFERJobRefiner {

    /**
     * A short description of the transfer refinement.
     */
    public static final String DESCRIPTION = "GRMS Refiner";

    /**
     * The string holding  the logging messages
     */
    protected String mLogMsg;


    /**
     * The overloaded constructor.
     *
     * @param dag        the workflow to which transfer nodes need to be added.
     * @param properties the <code>PegasusProperties</code> object containing all
     *                   the properties required by Pegasus.
     * @param options    the options passed to the planner.
     *
     */
    public GRMS(ADag dag,PegasusProperties properties,PlannerOptions options){
        super(dag,properties,options);
        //we have to convert all the jobs in the vector to type GRMS
        Iterator it = dag.vJobSubInfos.iterator();
        List l = new java.util.ArrayList();

        while(it.hasNext()){
            SubInfo job = (SubInfo)it.next();
            GRMSJob gjob = new GRMSJob(job);
            l.add(gjob);
            it.remove();
        }

        it = l.iterator();
        while(it.hasNext()){
            dag.vJobSubInfos.add(it.next());
        }
        l = null;

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
        GRMSJob grmsJob = (GRMSJob)job;
        String url = null;
        String sourceURL = null;

        for(Iterator it = files.iterator();it.hasNext();){
            FileTransfer ft = (FileTransfer)it.next();

            //insert the extra slash that is requried by GRMS
            sourceURL = ((NameValue)ft.getSourceURL()).getValue();
            url  = Utility.pruneURLPrefix(sourceURL);
            url += File.separator +
                   sourceURL.substring(sourceURL.indexOf(url) +
                                           url.length());
            grmsJob.addURL(ft.getLFN(),url,'i');
        }


    }




    /**
     * Adds the inter pool transfer nodes that are required for  transferring
     * the output files of the parents to the jobs execution site. They are not
     * supported in this case.
     *
     * @param job   <code>SubInfo</code> object corresponding to the node to
     *              which the files are to be transferred to.
     * @param files Collection of <code>FileTransfer</code> objects containing the
     *              information about source and destURL's.
     */
    public void addInterSiteTXNodes(SubInfo job,
                                    Collection files){

        throw new java.lang.UnsupportedOperationException(
            "Interpool operation is not supported");

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
    public void addStageOutXFERNodes( SubInfo job,
                                      Collection files,
                                      ReplicaCatalogBridge rcb ) {
        this.addStageOutXFERNodes(job, files, rcb, false);
    }

    /**
     * For GRMS we do not need to add any push transfer nodes. Instead we modify
     * the job description to specify the urls to where the materialized files
     * need to be pushed to.
     * It modifies the job input file list to point to urls of the files that are
     * to be used. The deletedLeaf flag is immaterial for this case.
     *
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
        GRMSJob grmsJob = (GRMSJob)job;

        for(Iterator it = files.iterator();it.hasNext();){
            FileTransfer ft = (FileTransfer)it.next();
            grmsJob.addURL(ft.getLFN(),((NameValue)ft.getDestURL()).getValue(),'o');
        }

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

}
