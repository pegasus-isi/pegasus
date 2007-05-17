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
package org.griphyn.cPlanner.engine;

import org.griphyn.cPlanner.classes.ADag;
import org.griphyn.cPlanner.classes.FileTransfer;
import org.griphyn.cPlanner.classes.GridFTPServer;
import org.griphyn.cPlanner.classes.JobManager;
import org.griphyn.cPlanner.classes.NameValue;
import org.griphyn.cPlanner.classes.PegasusFile;
import org.griphyn.cPlanner.classes.PlannerOptions;
import org.griphyn.cPlanner.classes.ReplicaLocation;
import org.griphyn.cPlanner.classes.SiteInfo;
import org.griphyn.cPlanner.classes.SubInfo;

import org.griphyn.cPlanner.common.LogManager;
import org.griphyn.cPlanner.common.Utility;
import org.griphyn.cPlanner.common.PegasusProperties;

import org.griphyn.cPlanner.namespace.VDS;


import org.griphyn.cPlanner.selector.ReplicaSelector;
import org.griphyn.cPlanner.selector.replica.ReplicaSelectorFactory;

import org.griphyn.cPlanner.transfer.Refiner;
import org.griphyn.cPlanner.transfer.refiner.RefinerFactory;


import org.griphyn.common.catalog.ReplicaCatalogEntry;

import org.griphyn.common.catalog.transformation.TCMode;

import org.griphyn.common.util.DynamicLoader;
import org.griphyn.common.util.FactoryException;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

import java.util.Enumeration;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.Vector;

/**
 * The transfer engine, which on the basis of the pools on which the jobs are to
 * run, adds nodes to transfer the data products.
 *
 * @author Karan Vahi
 * @author Gaurang Mehta
 * @version $Revision: 1.48 $
 *
 */
public class TransferEngine extends Engine {

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
     * The original Dag object constructed after running the DaxParser. This is
     * required to transfer files to the output pool for any leaf jobs that may
     * have been deleted.
     */
    private ADag mOriginalDag;

    /**
     * The Vector of Dags subinfo objects.
     */
    private Vector mDagSubInfos;

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
     * Holds the jobs from the original dags which are deleted by the reduction
     * algorithm.
     */
    private Vector mvDelLeafJobs;

    /**
     * The interpool engine object to determine where a job should be run in case
     * of reduction engine deleting a leaf node.
     */
    private InterPoolEngine mIPEng;

    /**
     * The write handle to the temporary cache file that maybe written to hold
     * the locations of the transient files in case of deferred planning.
     */
    private PrintWriter mCacheHandle;

    /**
     * A boolean flag indicating if a cache file needs to be written.
     */
    private boolean mCacheFiles;

    /**
     * overloaded constructor
     *
     *
     * @param properties the <code>PegasusProperties</code> to be used.
     * @param options   The options specified by the user to run the planner.
     */
    public TransferEngine( ADag reducedDag,
                           Vector vDelLJobs,
                           PegasusProperties properties,
                           PlannerOptions options ) {
        //call the super class constructor for initializations
        super( properties );

        mDag = reducedDag;
        mvDelLeafJobs = vDelLJobs;
        mPOptions = options;
        mDagSubInfos = mDag.vJobSubInfos;
        mTCHandle = TCMode.loadInstance();

        //get the cache boolean flag and intialize
        //the file handle accordingly
        mCacheFiles = true;
        try{
            mTXRefiner = RefinerFactory.loadInstance(mProps, reducedDag,
                                                     options);
            mReplicaSelector = ReplicaSelectorFactory.loadInstance(mProps);
            mIPEng = new InterPoolEngine( reducedDag, properties, options);
        }
        catch(Exception e){
            //wrap all the exceptions into a factory exception
            throw new FactoryException("Transfer Engine ", e);
        }


        //log some configuration messages
        mLogger.log("Transfer Refiner loaded is [" + mTXRefiner.getDescription() +
                            "]",LogManager.CONFIG_MESSAGE_LEVEL);
        mLogger.log("ReplicaSelector loaded is [" + mReplicaSelector.description() +
                    "]",LogManager.CONFIG_MESSAGE_LEVEL);
    }

    /**
     * Returns whether a site is third party enabled or not. A site is determined
     * to be third party enabled if the transfer mode is a thirdparty based
     * transfer mode like RFT or if it is specified a third party site in the
     * by the property "vds.transfer.thirdparty.sites".
     *
     * @param site  the site handle of the site for which you want to detect
     *              third party capability.
     * @param type  the type of transfer job for which the URL is being constructed.
     *
     * @return true indicating site is third party enabled, else
     *         false indicating site is not third party enabled.
     */
    public boolean isSiteThirdParty(String site, int type) {

        return mTXRefiner.isSiteThirdParty(site,type);
    }

    /**
     * Returns the SubInfo object for the job specified.
     *
     * @param jobName  the name of the job
     */
    private SubInfo getSubInfo(String jobName) {
        return mDag.getSubInfo(jobName);
    }

    /**
     * Adds the transfer nodes to the workflow.
     *
     * @param rcb  the bridge to the ReplicaCatalog.
     */
    public void addTransferNodes( ReplicaCatalogBridge rcb ) {
        mRCBridge = rcb;
        Enumeration eSubs = mDagSubInfos.elements();
        SubInfo currentJob;
        String currentJobName;
        Vector vOutPoolTX;
        int noOfJobs = mDag.getNoOfJobs();
        int counter = 0;
        String msg;
        String outputSite = mPOptions.getOutputSite();

        //go through each job in turn
        while (eSubs.hasMoreElements() && counter < noOfJobs) {
            counter++;
            currentJob = (SubInfo) eSubs.nextElement();
            currentJobName = currentJob.jobName;

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
            if (outputSite != null &&
                outputSite.trim().length() > 0
                /*&& !currentJob.isTemp*/) {
                vOutPoolTX = getFileTX(outputSite, currentJob);
                mTXRefiner.addStageOutXFERNodes( currentJob, vOutPoolTX, rcb );
            }
        }

        //we are done with the traversal.
        mTXRefiner.done();

        //get the deleted leaf jobs o/p files to output pool
        //only if output pool is specified
        //should be moved upwards in the pool. redundancy at present
        if (outputSite != null &&
            outputSite.trim().length() > 0) {
            Enumeration e = this.mvDelLeafJobs.elements();
            while (e.hasMoreElements()) {
                currentJobName = (String) e.nextElement();
                currentJob = mOriginalDag.getSubInfo(currentJobName);

                //for a deleted node, to transfer it's output
                //the execution pool should be set to local i.e submit host
                currentJob.executionPool = "local";

                vOutPoolTX = getDeletedFileTX(outputSite, currentJob);
                mTXRefiner.addStageOutXFERNodes( currentJob, vOutPoolTX, rcb, true );
            }
        }



        //close the handle to the cache file if it is written
        closeCacheHandle();
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
        SiteInfo p = mPoolHandle.getPoolEntry( pool, "vanilla" );

        for( Iterator it = job.getOutputFiles().iterator(); it.hasNext(); ){
            PegasusFile pf = (PegasusFile)it.next();
            String  lfn = pf.getLFN();

            ReplicaLocation rl = mRCBridge.getFileLocs( lfn );

            //selLocs are all the locations found in ReplicaMechanism corr
            //to the pool pool
            ReplicaLocation selLocs = mReplicaSelector.selectReplicas( rl, pool );

            boolean flag = false;

            FileTransfer ft = null;
            //checking through all the pfn's returned on the pool
            for ( Iterator selIt = selLocs.pfnIterator(); selIt.hasNext(); ) {
                ReplicaCatalogEntry selLoc = ( ReplicaCatalogEntry ) selIt.next();
                String sourceURL = selLoc.getPFN();
                //definite inconsitency as url prefix and mount point
                //are not picked up from the same server
                String destURL = p.getURLPrefix(true) + mPoolHandle.getSeMountPoint(p) +
                                 File.separator + lfn;

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
        Vector vInterPoolFileTX = this.getInterpoolFileTX(job, vParentSubs);
        //only add if there are files to transfer
        if(!vInterPoolFileTX.isEmpty()){
            mTXRefiner.addInterSiteTXNodes(job, vInterPoolFileTX);
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
     *
     * @return        Vector of <code>FileTransfer</code> objects
     */
    private Vector getFileTX(String destPool, SubInfo job) {
        Vector vFileTX = new Vector();

        //check if there is a remote initialdir set
        String path  = job.vdsNS.getStringValue(
                                                 VDS.REMOTE_INITIALDIR_KEY );

        for( Iterator it = job.getOutputFiles().iterator(); it.hasNext(); ){
            PegasusFile pf = (PegasusFile) it.next();
            String file = pf.getLFN();


            FileTransfer ft = this.constructFileTX( pf, job.executionPool,
                                                    destPool, job.logicalName,
                                                    path);
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
     *
     * @return   the corresponding FileTransfer object
     */
    private FileTransfer constructFileTX(PegasusFile pf, String execPool,
                                         String destPool, String job, String path) {

        String lfn = pf.getLFN();
        FileTransfer ft = null;

        SiteInfo ePool = mPoolHandle.getPoolEntry(execPool, "vanilla");
        SiteInfo dPool = mPoolHandle.getPoolEntry(destPool, "vanilla");
        if (ePool == null || dPool == null) {
            mLogMsg = (ePool == null) ?
                this.poolNotFoundMsg(execPool, "vanilla") :
                this.poolNotFoundMsg(destPool, "vanilla");
            mLogger.log( mLogMsg, LogManager.ERROR_MESSAGE_LEVEL );
            throw new RuntimeException( mLogMsg );
        }

        //definite inconsitency as url prefix and mount point
        //are not picked up from the same server
        String execURL = ePool.getURLPrefix(true) +
            mPoolHandle.getExecPoolWorkDir(execPool,path) + File.separatorChar +
            lfn;

        //write out the exec url to the cache file
        //System.out.println(" ---CACHE--- " + pf + "---" + execDirURL);
        writeToCache(lfn,execURL,execPool);

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
            String sourceURL = isSiteThirdParty(execPool,SubInfo.STAGE_OUT_JOB) ?
                execURL :
                "file://" + mPoolHandle.getExecPoolWorkDir(execPool,path) +
                File.separator + lfn;

            ft = new FileTransfer(lfn,job,pf.getFlags());
            //set the transfer mode
            ft.setTransferFlag(pf.getTransferFlag());

            ft.addSource(execPool,sourceURL);


            //add all the possible destination urls iterating through
            //the list of grid ftp servers associated with the dest pool.
            List l = mPoolHandle.getGridFTPServers(destPool);
            Iterator it = l.iterator();
            String destURL = null;
            boolean first = true;
            while(it.hasNext()){
                destURL = (first)?
                          //the first entry has to be the one in the Pool object
                          dPool.getURLPrefix(false):
                          //get it from the list
                          ((GridFTPServer)it.next()).getInfo(GridFTPServer.GRIDFTP_URL);

                if(!first && destURL.equals(dPool.getURLPrefix(false))){
                    //ensures no duplicate entries. The gridftp server in the pool
                    //object is one of the servers in the list of gridftp servers.
                    continue;
                }


                //assumption of same se mount point for each gridftp server
                destURL += mPoolHandle.getSeMountPoint(dPool)
                           + File.separator + lfn;

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
    private Vector getInterpoolFileTX(SubInfo job, Vector nodes) {
        String destPool = job.executionPool;
        //contains the remote_initialdir if specified for the job
        String destRemoteDir = job.vdsNS.getStringValue(
                                                 VDS.REMOTE_INITIALDIR_KEY);

        SiteInfo desPool = mPoolHandle.getTXPoolEntry(destPool);
        SiteInfo sourcePool;

        Vector vFileTX = new Vector();

        for (Iterator it = nodes.iterator();it.hasNext();) {
            //get the parent job
            SubInfo pJob = (SubInfo)it.next();
            sourcePool = mPoolHandle.getTXPoolEntry(pJob.executionPool);

            if (((String)sourcePool.getInfo(SiteInfo.HANDLE))
                .equalsIgnoreCase(destPool)) {
                //no need to add transfers, as the parent job and child
                //job are run in the same directory on the pool
                continue;
            }

            String sourceURI = null;
            String thirdPartyDestURI = desPool.getURLPrefix(true) +
                mPoolHandle.getExecPoolWorkDir(destPool,destRemoteDir) ;
            //definite inconsitency as url prefix and mount point
            //are not picked up from the same server
            String destURI = isSiteThirdParty(destPool,SubInfo.INTER_POOL_JOB) ?
                //construct for third party transfer
                thirdPartyDestURI :
                //construct for normal transfer
                "file://" + mPoolHandle.getExecPoolWorkDir(destPool,destRemoteDir);


            for (Iterator fileIt = pJob.getOutputFiles().iterator(); fileIt.hasNext(); ){
                PegasusFile pf = (PegasusFile) fileIt.next();
                String outFile = pf.getLFN();

//       Not required as input files are Sets now Karan Sept 14, 2006
//                if (stringInPegVector(outFile, job.inputFiles)) {
            if( job.getInputFiles().contains( pf ) ){
                    String sourceURL     = null;
                    String destURL       = destURI + File.separator + outFile;
                    String thirdPartyDestURL = thirdPartyDestURI + File.separator +
                                           outFile;
                    FileTransfer ft      = new FileTransfer(outFile,pJob.jobName);
                    ft.addDestination(destPool,destURL);

                    //add all the possible source urls iterating through
                    //the list of grid ftp servers associated with the dest pool.
                    List l = mPoolHandle.getGridFTPServers(pJob.executionPool);
                    boolean first = true;
                    for(Iterator it1 = l.iterator();it1.hasNext();){
                        //definite inconsitency as url prefix and mount point
                        //are not picked up from the same server
                        sourceURI = (first)?
                            //the first entry has to be the one in the Pool object
                            sourcePool.getURLPrefix(false) :
                            //get it from the list
                            ((GridFTPServer)it1.next()).getInfo(GridFTPServer.GRIDFTP_URL);

                        if((!first && sourceURI.equals(sourcePool.getURLPrefix(false)) )){
                            //ensures no duplicate entries. The gridftp server in the pool
                            //object is one of the servers in the list of gridftp servers.
                            mLogger.log("Not adding inter pool file tx for " +
                                        outFile, LogManager.DEBUG_MESSAGE_LEVEL);
                            continue;
                        }


                        sourceURI += mPoolHandle.getExecPoolWorkDir(pJob.executionPool,
                            pJob.vdsNS.getStringValue(VDS.REMOTE_INITIALDIR_KEY));
                        sourceURL = sourceURI + File.separator + outFile;

                        if(!(sourceURL.equalsIgnoreCase(thirdPartyDestURL))){
                            //add the source url only if it does not match to
                            //the third party destination url
                            ft.addSource(pJob.executionPool, sourceURL);
                        }
                        first = false;
                    }
                    if(ft.isValid()){
                        //adding only if there is at least
                        //a single valid transfer associated.
                        vFileTX.addElement(ft);
                    }
                }
            }


        }

        return vFileTX;

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
        Vector vFileTX = new Vector();
        String jobName = job.logicalName;
        String ePool   = job.executionPool;
        //contains the remote_initialdir if specified for the job
        String eRemoteDir = job.vdsNS.getStringValue(
                                                 VDS.REMOTE_INITIALDIR_KEY);
        String sourceURL,destURL=null;
        SiteInfo ep        = mPoolHandle.getPoolEntry(ePool, "vanilla");
        //we are using the pull mode for data transfer
        String scheme  = "file";

        //sAbsPath would be just the source directory absolute path
        //dAbsPath would be just the destination directory absolute path
        String dAbsPath = mPoolHandle.getExecPoolWorkDir(ePool,eRemoteDir);
        String sAbsPath = null;

        //sDirURL would be the url to the source directory.
        //dDirURL would be the url to the destination directoy
        //and is always a networked url.
        //definite inconsitency as url prefix and mount point
        //are not picked up from the same server
        String dDirURL = ep.getURLPrefix(true) + dAbsPath;
        String sDirURL = null;
        //check if the execution pool is third party or not
        String destDir = (isSiteThirdParty(ePool, SubInfo.STAGE_IN_JOB)) ?
            //use the full networked url to the directory
            dDirURL
            :
            //use the default pull mode
            scheme + "://" + mPoolHandle.getExecPoolWorkDir(ePool,eRemoteDir);

        for( Iterator it = searchFiles.iterator(); it.hasNext(); ){
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
                destURL = ((FileTransfer)pf).removeDestURL().getValue();
                destURL = (isSiteThirdParty(ePool, SubInfo.STAGE_IN_JOB))?
                           //the destination URL is already third party
                           //enabled. use as it is
                           destURL:
                           //explicitly convert to file URL scheme
                           scheme + "://" + Utility.getAbsolutePath(destURL);
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
                    continue;
                }

                //flag an error
                throw new RuntimeException(
                           "TransferEngine.java: Can't determine a location to " +
                           "transfer input file for lfn " + lfn + " for job " +
                           job.getName());
            }

            /*
            ReplicaLocation selLoc = (nv == null)?
                //select from the various replicas
                mReplicaSelector.select( lfn, pfns, job.getSiteHandle() ):
                //we have the replica already selected
                new ReplicaLocation(nv.getValue(),nv.getKey());
            */
           ReplicaCatalogEntry selLoc = (nv == null)?
                                        //select from the various replicas
                                        mReplicaSelector.selectReplica( rl, job.getSiteHandle() ):
                                        //we have the replica already selected
                                        new ReplicaCatalogEntry( nv.getValue(), nv.getKey() );


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
            destURL   = (destURL == null)?
                        destDir + File.separator + lfn:
                        destURL;


            //we have all the chopped up combos of the urls.
            //do some funky matching on the basis of the fact
            //that each pool has one shared filesystem

            //match the source and dest 3rd party urls or
            //match the directory url knowing that lfn and
            //(source and dest pool) are same
            if(sourceURL.equalsIgnoreCase(dDirURL + File.separator + lfn)||
               ( selLoc.getResourceHandle().equalsIgnoreCase( ePool ) &&
                lfn.equals( sourceURL.substring(sourceURL.lastIndexOf(File.separator) + 1)) &&
                sAbsPath.equals( dAbsPath )
                )){
                //do not need to add any transfer node
                mLogger.log("Not transferring ip file " + lfn +
                            " for job " + job.jobName + " to site " + ePool,
                            LogManager.DEBUG_MESSAGE_LEVEL);
                continue;
            }

            //construct the file transfer object
            FileTransfer ft = (pf instanceof FileTransfer) ?
                               (FileTransfer)pf:
                               new FileTransfer(lfn,jobName);
            //the transfer mode for the file needs to be
            //propogated for optional transfers.
            ft.setTransferFlag(pf.getTransferFlag());
            ft.addSource( selLoc.getResourceHandle(), sourceURL);
            //to prevent duplicate destination urls
            if(ft.getDestURL() == null)
                ft.addDestination(ePool,destURL);
            vFileTX.add(ft);
            //we need to set destURL to null
            destURL = null;
        }

        //call addTransferNode
        if (!vFileTX.isEmpty()) {
            mTXRefiner.addStageInXFERNodes(job, vFileTX);

        }
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

        Enumeration e = nodes.elements();
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
     * It initializes the handle to temporary cache file , that holds the mappings
     * for the transient files.
     */
    private void initializeCacheHandle(){
        String file = mPOptions.getSubmitDirectory() + File.separatorChar +
                          getCacheFileName(mDag);
        try {
            mCacheHandle = (mCacheFiles) ?
                new PrintWriter(new BufferedWriter(new FileWriter(file))) :
                null;
        }
        catch (IOException e) {
            throw new RuntimeException(  "Unable to initialize cache file " + file
                                         + " for writing ", e );
        }
    }

    /**
     * It writes out to a cache file.
     */
    private void writeToCache(String lfn, String url){

        //check if the cache handle is initialized
        if(mCacheHandle == null)
            initializeCacheHandle();

        mCacheHandle.println(lfn);
        mCacheHandle.println(url);
    }

    /**
     * It writes out to a cache file.
     */
    private void writeToCache(String lfn, String url, String pool){

        //check if the cache handle is initialized
        if(mCacheHandle == null)
            initializeCacheHandle();

        mCacheHandle.print(lfn);
        if(pool != null && pool.length() > 0){
            mCacheHandle.print(" ");
            mCacheHandle.print(pool);
        }
        mCacheHandle.print("\n");
        mCacheHandle.println(url);
    }


    /**
     * It closes the handle to the cache file that is being written.
     * If the cache handle is uninitialized it does nothing.
     */
    private void  closeCacheHandle(){
        if(mCacheHandle != null)
            mCacheHandle.close();
    }


    /**
     * Constructs the basename to the cache file that is to be used
     * to log the transient files. The basename is dependant on whether the
     * basename prefix has been specified at runtime or not.
     *
     * @param adag  the ADag object containing the workflow that is being
     *              concretized.
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
            sb.append(adag.dagInfo.nameOfADag).append("_").
           append(adag.dagInfo.index);
        }
        //append the suffix
        sb.append(".cache");

        return sb.toString();

    }


}
