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
import org.griphyn.cPlanner.classes.Data;
import org.griphyn.cPlanner.classes.FileTransfer;
import org.griphyn.cPlanner.classes.NameValue;
import org.griphyn.cPlanner.classes.PlannerOptions;
import org.griphyn.cPlanner.classes.SubInfo;
import org.griphyn.cPlanner.classes.GRMSJob;

import org.griphyn.cPlanner.common.LogManager;
import org.griphyn.cPlanner.common.PegasusProperties;
import org.griphyn.cPlanner.common.Utility;

import org.griphyn.cPlanner.transfer.MultipleFTPerXFERJobRefiner;

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
import java.net.URL;

/**
 * A refiner that relies on the Condor file transfer mechanism to get the
 * raw input data to the remote working directory.
 *
 * <p>
 * Additionally, this will only work with local replica selector that prefers
 * file urls from the submit host for staging.
 *
 * <p>
 * In order to use the transfer implementation implemented by this class,
 * <pre>
 *     - property <code>pegasus.transfer.refiner</code> must be set to
 *       value <code>Condor</code>.
 *     - property <code>pegasus.selector.replica</code> must be set to value
 *       <code>Local</code>
 * </pre>
 *
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public class Condor extends MultipleFTPerXFERJobRefiner {

    /**
     * A short description of the transfer refinement.
     */
    public static final String DESCRIPTION = "Condor Transfer Refiner";

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
    public Condor( ADag dag, PegasusProperties properties, PlannerOptions options){
        super(dag,properties,options);
    }



    /**
     * Adds the stage in transfer nodes which transfer the input files for a job,
     * from the location returned from the replica catalog to the job's execution
     * pool. It creates a stagein job for each file to be transferred.
     *
     * @param job   <code>SubInfo</code> object corresponding to the node to
     *              which the files are to be transferred to.
     *
     * @param files Collection of <code>FileTransfer</code> objects containing the
     *              information about source and destURL's.
     */
    public  void addStageInXFERNodes( SubInfo job,
                                      Collection files ){


        Set inputFiles = job.getInputFiles();
        for( Iterator it = files.iterator(); it.hasNext(); ){
            FileTransfer ft = (FileTransfer)it.next();

            //insert the extra slash that is requried by GRMS
            String url = ((NameValue)ft.getSourceURL()).getValue();

            //remove from input files the PegasusFile object
            //corresponding to this File Transfer
            boolean removed = inputFiles.remove( ft );
            //System.out.println( "Removed " + ft.getLFN() + " " + removed );
            inputFiles.add( ft );

            //put the url in only if it is a file url
            if( url.startsWith( "file:/" ) ){
                try{
                    job.condorVariables.addIPFileForTransfer( new URL(url).getPath() );
                }
                catch( Exception e ){
                    throw new RuntimeException ( "Malformed source URL " + url );
                }
            }
            else{
                throw new RuntimeException ( "Malformed source URL. Input URL should be a file url " + url );
            }
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

        throw new java.lang.UnsupportedOperationException( "Stageout operation is not supported for " +
                                                            this.getDescription()  );

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
