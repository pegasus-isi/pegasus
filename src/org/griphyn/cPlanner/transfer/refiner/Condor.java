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
import org.griphyn.cPlanner.classes.FileTransfer;
import org.griphyn.cPlanner.classes.NameValue;
import org.griphyn.cPlanner.classes.PlannerOptions;
import org.griphyn.cPlanner.classes.SubInfo;
import org.griphyn.cPlanner.classes.TransferJob;

import org.griphyn.cPlanner.common.LogManager;
import org.griphyn.cPlanner.common.PegasusProperties;

import org.griphyn.cPlanner.transfer.MultipleFTPerXFERJobRefiner;

import org.griphyn.cPlanner.engine.ReplicaCatalogBridge;

import java.io.File;

import java.util.Collection;
import java.util.Iterator;
import java.util.Set;
import java.util.HashSet;


import java.net.URL;
import java.net.MalformedURLException;

/**
 * A refiner that relies on the Condor file transfer mechanism to get the
 * raw input data to the remote working directory. It is to be used for doing
 * the file transfers in a condor pool, while trying to run on the local
 * filesystem of the worker nodes.
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
 *     - property <code>pegasus.execute.*.filesystem.local</code> must be set to value
 *       <code>true</code>
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

        //explicitly set the stageout transfer to Condor irrespective of
        //what the user said for the time being
        //properties.setProperty( "pegasus.transfer.stageout.impl", "Condor" );
        //this.mTXStageOutImplementation = ImplementationFactory.loadInstance(
        //                                       properties,options,
        //                                       ImplementationFactory.TYPE_STAGE_OUT );


    }



    /**
     * Adds the stage in transfer nodes which transfer the input files for a job,
     * from the location returned from the replica catalog to the job's execution
     * pool.
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

            String url = ((NameValue)ft.getSourceURL()).getValue();

            //remove from input files the PegasusFile object
            //corresponding to this File Transfer and the
            //FileTransfer object instead
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

        Set outputFiles = job.getOutputFiles();
        String destinationDirectory = null;
        for( Iterator it = files.iterator(); it.hasNext(); ){
            FileTransfer ft = (FileTransfer)it.next();

            String url = ((NameValue)ft.getDestURL()).getValue();

            //put the url in only if it is a file url
            if( url.startsWith( "file:/" ) ){

                try {
                    destinationDirectory = new File(new URL(url).getPath()).getParent();
                }
                catch (MalformedURLException ex) {
                    throw new RuntimeException( "Malformed URL", ex );
                }

                //strong disconnect here, as assuming worker node execution
                //and having the SLS to the submit directory
                String pfn = "file://" + mPOptions.getSubmitDirectory() + File.separator + ft.getLFN();
                ft.removeSourceURL();
                ft.addSource( "local", pfn );

            }
            else{
                throw new RuntimeException ( "Malformed destination URL. Output URL should be a file url " + url );
            }
        }

        if( !files.isEmpty() ){
            String txName = this.STAGE_OUT_PREFIX + job.getName() + "_0" ;
            SubInfo txJob = this.createStageOutTransferJob( job,
                                                            files,
                                                            destinationDirectory,
                                                            txName );

            this.mDAG.add( txJob );
            this.addRelation( job.getName(), txName );
        }

    }


    /**
     * Constructs a  condor file transfer job that handles multiple transfers.
     * The job itself is a /bin/true job that does the stageout using the
     * transfer_input_files feature.
     *
     * @param job         the SubInfo object for the job, in relation to which
     *                    the transfer node is being added. Either the transfer
     *                    node can be transferring this jobs input files to
     *                    the execution pool, or transferring this job's output
     *                    files to the output pool.
     * @param files       collection of <code>FileTransfer</code> objects
     *                    representing the data files and staged executables to be
     *                    transferred.
     * @param directory   the directory where the transfer job needs to be executed
     * @param txJobName   the name of transfer node.
     *
     * @return  the created TransferJob.
     */
    private TransferJob createStageOutTransferJob( SubInfo job,
                                                   Collection files,
                                                   String directory,
                                                   String txJobName
                                                   ) {



        TransferJob txJob = new TransferJob();

        //run job always on the site where the compute job runs
        txJob.setSiteHandle( "local" );

        //the non third party site for the transfer job is
        //always the job execution site for which the transfer
        //job is being created.
        txJob.setNonThirdPartySite(job.getSiteHandle());

        txJob.setName( txJobName );

        txJob.setTransformation( "pegasus",
                                 "true",
                                  null  );

        txJob.setDerivation( "pegasus",
                             "true",
                             null  );


        txJob.setRemoteExecutable( "/bin/true" );


        //add input files for transfer since we are only doing for
        //creating stagein jobs
        for( Iterator it = files.iterator(); it.hasNext(); ){
            FileTransfer ft = ( FileTransfer )it.next();
            NameValue nv = ft.getSourceURL(  );


            //put the url in only if it is a file url
            String url = nv.getValue();
            if( url.startsWith( "file:/" ) ){
                try{
                    txJob.condorVariables.addIPFileForTransfer(new URL(url).
                        getPath());
                }
                catch( Exception e ){
                    throw new RuntimeException ( "Malformed source URL " + url );
                }
            }

        }


        //the intial directory is set to the directory where we need the output
        txJob.condorVariables.construct( "initialdir", directory );

        txJob.setJobType( SubInfo.STAGE_OUT_JOB );
        txJob.setVDSSuperNode( job.jobName );

        txJob.stdErr = "";
        txJob.stdOut = "";

        //the i/p and o/p files remain empty
        //as we doing just copying urls
        txJob.inputFiles = new HashSet();

        //to get the file stat information we need to put
        //the files as output files of the transfer job
        txJob.outputFiles = new HashSet( files );

        return txJob;
    }


    /**
     *
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
