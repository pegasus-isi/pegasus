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

package org.griphyn.cPlanner.transfer;

import org.griphyn.cPlanner.transfer.Refiner;

import org.griphyn.cPlanner.classes.ADag;
import org.griphyn.cPlanner.classes.PegasusBag;
import org.griphyn.cPlanner.classes.SubInfo;
import org.griphyn.cPlanner.classes.PlannerOptions;

import org.griphyn.cPlanner.common.PegasusProperties;
import edu.isi.pegasus.common.logging.LogManager;
import java.util.Collection;
import org.griphyn.cPlanner.classes.FileTransfer;
import org.griphyn.cPlanner.common.TPT;

import org.griphyn.cPlanner.provenance.pasoa.XMLProducer;
import org.griphyn.cPlanner.provenance.pasoa.producer.XMLProducerFactory;





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
     * The XML Producer object that records the actions.
     */
    protected XMLProducer mXMLStore;


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
     * @param job   <code>SubInfo</code> object corresponding to the node to
     *              which the files are to be transferred to.
     * @param files Collection of <code>FileTransfer</code> objects containing the
     *              information about source and destURL's.
     * @param symlinkFiles Collection of <code>FileTransfer</code> objects containing
     *                     source and destination file url's for symbolic linking
     *                     on compute site.
     */
    public  void addStageInXFERNodes( SubInfo job,
                                      Collection<FileTransfer> files,
                                      Collection<FileTransfer> symlinkFiles ){
        
        files.addAll( symlinkFiles );
        addStageInXFERNodes( job, files );
    }

    /**
     * Default behaviour to preserve backward compatibility when the stage in 
     * and symbolic link jobs were not separated. 
     *
     * @param job   <code>SubInfo</code> object corresponding to the node to
     *              which the files are to be transferred to.
     * @param files Collection of <code>FileTransfer</code> objects containing the
     *              information about source and destURL's.
     */
    public  void addStageInXFERNodes( SubInfo job,
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
     * @see SubInfo#STAGE_IN_JOB
     * @see SubInfo#INTER_POOL_JOB
     * @see SubInfo#STAGE_OUT_JOB
     *
     * @throws IllegalArgumentException
     */
    public boolean isSiteThirdParty(String site,int type){
        Implementation implementation;
        //the value from the properties file
        //later on maybe picked up as profiles
        boolean useTPT = false;
        if(type == SubInfo.STAGE_IN_JOB ){
            implementation = mTXStageInImplementation;
            useTPT         = mTPT.stageInThirdParty(site);
        }
        else if(type == SubInfo.INTER_POOL_JOB){
            implementation = mTXInterImplementation;
            useTPT         = mTPT.interThirdParty(site);
        }
        else if(type == SubInfo.STAGE_OUT_JOB){
            implementation = mTXStageOutImplementation;
            useTPT         = mTPT.stageOutThirdParty(site);
        }
        else if(type == SubInfo.SYMLINK_STAGE_IN_JOB){
            implementation = mTXSymbolicLinkImplementation;
            useTPT         = false;
        }
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
     * @see SubInfo#STAGE_IN_JOB
     * @see SubInfo#INTER_POOL_JOB
     * @see SubInfo#STAGE_OUT_JOB
     */
    public boolean runTPTOnRemoteSite(String site,int type){
        //Implementation implementation;
        //the value from the properties file
        //later on maybe picked up as profiles
        boolean remoteTPT = false;
        if(type == SubInfo.STAGE_IN_JOB ){
            //implementation = mTXStageInImplementation;
            remoteTPT      = mTPT.stageInThirdPartyRemote(site);
        }
        else if(type == SubInfo.INTER_POOL_JOB){
            //implementation = mTXInterImplementation;
            remoteTPT      = mTPT.interThirdPartyRemote(site);
        }
        else if(type == SubInfo.STAGE_OUT_JOB){
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
