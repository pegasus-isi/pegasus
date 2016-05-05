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

package edu.isi.pegasus.planner.transfer;

import edu.isi.pegasus.planner.transfer.Refiner;

import edu.isi.pegasus.planner.classes.ADag;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.classes.PlannerOptions;

import edu.isi.pegasus.planner.common.PegasusProperties;
import edu.isi.pegasus.common.logging.LogManager;
import java.util.Collection;
import edu.isi.pegasus.planner.classes.FileTransfer;
import edu.isi.pegasus.planner.mapper.Creator;

import edu.isi.pegasus.planner.provenance.pasoa.XMLProducer;
import edu.isi.pegasus.planner.provenance.pasoa.producer.XMLProducerFactory;





/**
 * An abstract implementation that implements some of the common functions
 * in the Refiner Interface and member variables that are required by all the
 * refiners.
 *
 * @author Karan Vahi
 * @version $Revision$
 */

public abstract class AbstractRefiner implements Refiner{

    /**
     * The stage-in transfer implementation that the refiner requires.
     */
    protected Implementation mTXStageInImplementation ;
    
    /**
     * The stage-in symbolic link transfer implementation that refiner requires.
     */
    protected Implementation mTXSymbolicLinkImplementation;

    /**
     * The inter transfer implementation that the refiner requires.
     */
    protected Implementation mTXInterImplementation ;

    /**
     * The stage-out transfer implementation that the refiner requires.
     */
    protected Implementation mTXStageOutImplementation ;

    /**
     * The ADag object associated with the Dag. This is the object to
     * which the transfer nodes are added. This object is initialised in the
     * TransferEngine.
     */
    protected ADag mDAG;


    /**
     * The handle to the properties object holding the properties relevant to
     * Pegasus.
     */
    protected PegasusProperties mProps;

    /**
     * The options passed to the planner at runtime.
     */
    protected PlannerOptions mPOptions;

    /**
     * The logging object which is used to log all the messages.
     *
     */
    protected LogManager mLogger;

    /**
     * The handle to the Third Party State machinery.
     */
    protected TPT mTPT;


    /**
     * The handle to the Remote Transfers State machinery.
     */
    protected RemoteTransfer mRemoteTransfers;

    /**
     * The XML Producer object that records the actions.
     */
    protected XMLProducer mXMLStore;

    /**
     * Handle to the Submit directory factory, that returns the relative
     * submit directory for a job
     */
    protected Creator mSubmitDirFactory;

    /**
     * The overloaded constructor.
     *
     * @param dag        the workflow to which transfer nodes need to be added.
     * @param bag   the bag of initialization objects.
     */
    public AbstractRefiner( ADag dag,
                            PegasusBag bag ){
        mLogger = bag.getLogger();
        mDAG = dag;
        mProps = bag.getPegasusProperties();
        mPOptions = bag.getPlannerOptions();
        mTPT = new TPT( mProps );
        mTPT.buildState();
        mRemoteTransfers = new RemoteTransfer( mProps );
        mRemoteTransfers.buildState();
        mSubmitDirFactory = bag.getSubmitDirFileFactory();
        mXMLStore        = XMLProducerFactory.loadXMLProducer( mProps );
    }


    /**
     * Returns a reference to the workflow that is being refined by the refiner.
     *
     *
     * @return ADAG object.
     */
    public ADag getWorkflow(){
        return this.mDAG;
    }
    
    /**
     * Default behaviour to preserve backward compatibility when the stage in 
     * and symbolic link jobs were not separated. The symlink transfer files 
     * are added back into the files collection and passed onto 
     * legacy implementations. Refiners that want to distinguish between 
     * symlink and stagein jobs should over ride this method.
     *
     * @param job   <code>Job</code> object corresponding to the node to
     *              which the files are to be transferred to.
     * @param files Collection of <code>FileTransfer</code> objects containing the
     *              information about source and destURL's.
     * @param symlinkFiles Collection of <code>FileTransfer</code> objects containing
     *                     source and destination file url's for symbolic linking
     *                     on compute site.
     */
    public  void addStageInXFERNodes( Job job,
                                      Collection<FileTransfer> files,
                                      Collection<FileTransfer> symlinkFiles ){
        
        files.addAll( symlinkFiles );
        addStageInXFERNodes( job, files );
    }

    /**
     * Default behaviour to preserve backward compatibility when the stage in 
     * and symbolic link jobs were not separated. 
     *
     * @param job   <code>Job</code> object corresponding to the node to
     *              which the files are to be transferred to.
     * @param files Collection of <code>FileTransfer</code> objects containing the
     *              information about source and destURL's.
     */
    public  void addStageInXFERNodes( Job job,
                                      Collection<FileTransfer> files ){
        
        throw new UnsupportedOperationException( 
                "Refiner does not implement the function addStageInXFERNodes( SubInfo, Collection<FileTransfer>)");
    }
    
    /**
     * Returns a reference to the XMLProducer, that generates the XML fragment
     * capturing the actions of the refiner. This is used for provenace
     * purposes.
     *
     * @return XMLProducer
     */
    public XMLProducer getXMLProducer(){
        return this.mXMLStore;
    }
    
    /**
     * Boolean indicating whether the Transfer Refiner has a preference for
     * where a transfer job is run. By default, Refiners dont advertise any
     * preference as to where transfer jobs run.
     * 
     * @return false
     */
    public boolean refinerPreferenceForTransferJobLocation(  ){
        return false;
    }
    
    /**
     * Boolean indicating Refiner preference for transfer jobs to run locally.
     * This method should be called only if refinerPreferenceForTransferJobLocation
     * is true for a refiner.
     * 
     * @param type  the type of transfer job for which the URL is being constructed.
     *              Should be one of the following:
     *                              stage-in
     *                              stage-out
     *                              inter-pool transfer
     * 
     * @return boolean  refiner preference for transfer job to run locally or not.
     */
    public boolean refinerPreferenceForLocalTransferJobs( int type ){
        throw new UnsupportedOperationException( "Refiner does not advertise preference for local transfer jobs ");
    }

    /**
     * Returns whether a Site prefers transfers to be run on it i.e remote transfers
     * enabled.
     *
     * @param site  the name of the site.
     * @param type  the type of transfer job for which the URL is being constructed.
     *              Should be one of the following:
     *                              stage-in
     *                              stage-out
     *                              inter-pool transfer
     *
     * @return true if site is setup for remote transfers
     *
     * @see Job#STAGE_IN_JOB
     * @see Job#INTER_POOL_JOB
     * @see Job#STAGE_OUT_JOB
     */
    public boolean runTransferRemotely( String site, int type ) {
        Implementation implementation;
        //the value from the properties file
        //later on maybe picked up as profiles
        boolean runTransferRemotely = false;
        if(type == Job.STAGE_IN_JOB ){
            implementation = mTXStageInImplementation;
            runTransferRemotely = mRemoteTransfers.stageInOnRemoteSite( site );
        }
        else if(type == Job.INTER_POOL_JOB){
            implementation = mTXInterImplementation;
            runTransferRemotely         = mRemoteTransfers.interOnRemoteSite( site );
        }
        else if(type == Job.STAGE_OUT_JOB){
            implementation = mTXStageOutImplementation;
            runTransferRemotely         = mRemoteTransfers.stageOutOnRemoteSite( site );
        }/*
        else if(type == Job.SYMLINK_STAGE_IN_JOB){
            implementation = mTXSymbolicLinkImplementation;
            runTransferRemotely         = true;
        }*/
        else{
            throw new java.lang.IllegalArgumentException(
                "Invalid implementation type passed " + type);
        }

        return runTransferRemotely;
    }


    /**
     * Returns whether a Site is third party enabled or not.
     *
     * @param site  the name of the site.
     * @param type  the type of transfer job for which the URL is being constructed.
     *              Should be one of the following:
     *                              stage-in
     *                              stage-out
     *                              inter-pool transfer
     *
     * @return true pool is third party enabled
     *         false pool is not third party enabled.
     *
     * @see Job#STAGE_IN_JOB
     * @see Job#INTER_POOL_JOB
     * @see Job#STAGE_OUT_JOB
     *
     * @throws IllegalArgumentException
     */
    public boolean isSiteThirdParty(String site,int type){
        Implementation implementation;
        //the value from the properties file
        //later on maybe picked up as profiles
        boolean useTPT = false;
        if(type == Job.STAGE_IN_JOB ){
            implementation = mTXStageInImplementation;
            useTPT         = mTPT.stageInThirdParty(site);
        }
        else if(type == Job.INTER_POOL_JOB){
            implementation = mTXInterImplementation;
            useTPT         = mTPT.interThirdParty(site);
        }
        else if(type == Job.STAGE_OUT_JOB){
            implementation = mTXStageOutImplementation;
            useTPT         = mTPT.stageOutThirdParty(site);
        }/*
        else if(type == Job.SYMLINK_STAGE_IN_JOB){
            implementation = mTXSymbolicLinkImplementation;
            useTPT         = false;
        }*/
        else{
            throw new java.lang.IllegalArgumentException(
                "Invalid implementation type passed " + type);
        }

        return implementation.useThirdPartyTransferAlways()||
               useTPT;
    }

    /**
     * Returns whether the third party transfers for a particular site are to
     * be run on the remote site or the submit host.
     *
     * @param site  the name of the site.
     * @param type  the type of transfer job for which the URL is being constructed.
     *              Should be one of the following:
     *                              stage-in
     *                              stage-out
     *                              inter-pool transfer
     *
     * @return true if the transfers are to be run on remote site, else false.
     *
     * @see Job#STAGE_IN_JOB
     * @see Job#INTER_POOL_JOB
     * @see Job#STAGE_OUT_JOB
     */
    public boolean runTPTOnRemoteSite(String site,int type){
        //Implementation implementation;
        //the value from the properties file
        //later on maybe picked up as profiles
        boolean remoteTPT = false;
        if(type == Job.STAGE_IN_JOB ){
            //implementation = mTXStageInImplementation;
            remoteTPT      = mTPT.stageInThirdPartyRemote(site);
        }
        else if(type == Job.INTER_POOL_JOB){
            //implementation = mTXInterImplementation;
            remoteTPT      = mTPT.interThirdPartyRemote(site);
        }
        else if(type == Job.STAGE_OUT_JOB){
            //implementation = mTXStageOutImplementation;
            remoteTPT      = mTPT.stageOutThirdPartyRemote(site);
        }
        else{
            throw new java.lang.IllegalArgumentException(
                "Invalid implementation type passed " + type);
        }

        return remoteTPT;

    }



    /**
     * Logs configuration messages regarding the type of implementations loaded
     * for various type of transfer node creations.
     */
    protected void logConfigMessages(){
        //log a message
        mLogger.log("Transfer Implementation loaded for Stage-In   [" +
                    mTXStageInImplementation.getDescription() + "]",
                    LogManager.CONFIG_MESSAGE_LEVEL);
        mLogger.log("Transfer Implementation loaded for symbolic linking Stage-In  [" +
                            mTXSymbolicLinkImplementation.getDescription() + "]",
                            LogManager.CONFIG_MESSAGE_LEVEL);
        mLogger.log("Transfer Implementation loaded for Inter Site [" +
                    mTXInterImplementation.getDescription() + "]",
                    LogManager.CONFIG_MESSAGE_LEVEL);
        mLogger.log("Transfer Implementation loaded for Stage-Out  [" +
                            mTXStageOutImplementation.getDescription() + "]",
                            LogManager.CONFIG_MESSAGE_LEVEL);

    }


}
