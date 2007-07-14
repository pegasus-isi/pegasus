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
package org.griphyn.cPlanner.transfer;

import org.griphyn.cPlanner.transfer.Refiner;

import org.griphyn.cPlanner.classes.ADag;
import org.griphyn.cPlanner.classes.SubInfo;
import org.griphyn.cPlanner.classes.PlannerOptions;

import org.griphyn.cPlanner.common.PegasusProperties;
import org.griphyn.cPlanner.common.LogManager;
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
     * @see org.griphyn.cPlanner.common.LogManager
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
     * @param properties the <code>PegasusProperties</code> object containing all
     *                   the properties required by Pegasus.
     * @param options    the options passed to the planner.
     */
    public AbstractRefiner(ADag dag,
                           PegasusProperties properties,
                           PlannerOptions options){
        mLogger = LogManager.getInstance();
        mDAG = dag;
        mProps = properties;
        mPOptions = options;
        mTPT = new TPT(properties);
        mTPT.buildState();
        mXMLStore        = XMLProducerFactory.loadXMLProducer( properties );
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
        Implementation implementation;
        //the value from the properties file
        //later on maybe picked up as profiles
        boolean remoteTPT = false;
        if(type == SubInfo.STAGE_IN_JOB ){
            implementation = mTXStageInImplementation;
            remoteTPT      = mTPT.stageInThirdPartyRemote(site);
        }
        else if(type == SubInfo.INTER_POOL_JOB){
            implementation = mTXInterImplementation;
            remoteTPT      = mTPT.interThirdPartyRemote(site);
        }
        else if(type == SubInfo.STAGE_OUT_JOB){
            implementation = mTXStageOutImplementation;
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
        mLogger.log("Transfer Implementation loaded for Inter Site [" +
                    mTXInterImplementation.getDescription() + "]",
                    LogManager.CONFIG_MESSAGE_LEVEL);
        mLogger.log("Transfer Implementation loaded for Stage-Out  [" +
                            mTXStageOutImplementation.getDescription() + "]",
                            LogManager.CONFIG_MESSAGE_LEVEL);

    }


}
