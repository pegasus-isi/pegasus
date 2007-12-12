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

package org.griphyn.cPlanner.transfer.implementation;

import org.griphyn.cPlanner.classes.ADag;
import org.griphyn.cPlanner.classes.SubInfo;
import org.griphyn.cPlanner.classes.TransferJob;
import org.griphyn.cPlanner.classes.PlannerOptions;
import org.griphyn.cPlanner.classes.FileTransfer;
import org.griphyn.cPlanner.classes.SiteInfo;
import org.griphyn.cPlanner.classes.JobManager;
import org.griphyn.cPlanner.classes.NameValue;

import org.griphyn.cPlanner.common.LogManager;
import org.griphyn.cPlanner.common.PegasusProperties;

import org.griphyn.cPlanner.transfer.Implementation;
import org.griphyn.cPlanner.transfer.MultipleFTPerXFERJob;

import org.griphyn.common.catalog.TransformationCatalogEntry;

import org.griphyn.common.classes.TCType;

import org.griphyn.common.util.Separator;

import org.griphyn.cPlanner.cluster.aggregator.JobAggregatorFactory;
import org.griphyn.cPlanner.cluster.JobAggregator;

import java.io.File;
import java.io.FileWriter;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.LinkedList;
import java.util.Iterator;
import org.griphyn.cPlanner.transfer.Refiner;
import org.griphyn.cPlanner.classes.Profile;
import java.util.ArrayList;


/**
 * A Windward implementation that uses the seqexec client to execute
 *
 * -DC Transfer client to fetch the raw data sources
 * -Pegasus transfer client to fetch the patterns from the pattern catalog.
 *
 * @author Karan Vahi
 * @version $Revision$
 */

public class Windward extends Abstract
                      implements MultipleFTPerXFERJob  {




    /**
     * The prefix to identify the raw data sources.
     */
    public static final String DATA_SOURCE_PREFIX = "DS";

    /**
     * A short description of the transfer implementation.
     */
    public static final String DESCRIPTION = "Seqexec Transfer Wrapper around Pegasus Transfer and DC Transfer Client";

    /**
     * The transformation namespace for for the transfer job.
     */
    public static final String TRANSFORMATION_NAMESPACE = "windward";


    /**
     * The name of the underlying transformation that is queried for in the
     * Transformation Catalog.
     */
    public static final String TRANSFORMATION_NAME = "dc-transfer";

    /**
     * The version number for the transfer job.
     */
    public static final String TRANSFORMATION_VERSION = null;

    /**
     * The derivation namespace for for the transfer job.
     */
    public static final String DERIVATION_NAMESPACE = "windward";

    /**
     * The name of the underlying derivation.
     */
    public static final String DERIVATION_NAME = "dc-transfer";

    /**
     * The derivation version number for the transfer job.
     */
    public static final String DERIVATION_VERSION = null;


    /**
     * The handle to the transfer implementation.
     */
    private Transfer mPegasusTransfer;

    /**
     * The seqexec job aggregator.
     */
    private JobAggregator mSeqExecAggregator;

    /**
     * The overloaded constructor, that is called by the Factory to load the
     * class.
     *
     * @param properties  the properties object.
     * @param options     the options passed to the Planner.
     */
    public Windward( PegasusProperties properties,
                     PlannerOptions options) {
        super(properties, options);

        //should probably go through the factory
        mPegasusTransfer = new Transfer( properties, options );

        //just to pass the label have to send an empty ADag.
        //should be fixed
        ADag dag = new ADag();
        dag.dagInfo.setLabel( "windward" );
        mSeqExecAggregator = JobAggregatorFactory.loadInstance( JobAggregatorFactory.SEQ_EXEC_CLASS,
                                                                properties,
                                                                options.getSubmitDirectory(),
                                                                dag  );
    }

    /**
     * Sets the callback to the refiner, that has loaded this implementation.
     *
     * @param refiner  the transfer refiner that loaded the implementation.
     */
    public void setRefiner(Refiner refiner){
        super.setRefiner( refiner );
        //also set the refiner for hte internal pegasus transfer
        mPegasusTransfer.setRefiner( refiner );
    }


    /**
     *
     *
     * @param job         the SubInfo object for the job, in relation to which
     *                    the transfer node is being added. Either the transfer
     *                    node can be transferring this jobs input files to
     *                    the execution pool, or transferring this job's output
     *                    files to the output pool.
     * @param files       collection of <code>FileTransfer</code> objects
     *                    representing the data files and staged executables to be
     *                    transferred.
     * @param execFiles   subset collection of the files parameter, that identifies
     *                    the executable files that are being transferred.
     * @param txJobName   the name of transfer node.
     * @param jobClass    the job Class for the newly added job. Can be one of the
     *                    following:
     *                              stage-in
     *                              stage-out
     *                              inter-pool transfer
     *
     * @return  the created TransferJob.
     */
    public TransferJob createTransferJob( SubInfo job,
                                          Collection files,
                                          Collection execFiles,
                                          String txJobName,
                                          int jobClass) {


        //iterate through all the files and identify the patterns
        //and the other data sources
        Collection rawDataSources = new LinkedList();
        Collection patterns       = new LinkedList();

        for( Iterator it = files.iterator(); it.hasNext(); ){
            FileTransfer ft = ( FileTransfer )it.next();
            if( ft.getLFN().startsWith( DATA_SOURCE_PREFIX ) ){
                //it a raw data source
                rawDataSources.add( ft );
            }
            else{
                //everything else is a pattern
                patterns.add( ft );
            }
        }

        List txJobs = new LinkedList();

        //use the Pegasus Transfer to handle the patterns
        TransferJob patternTXJob = null;
        String patternTXJobStdin = null;
        if( !patterns.isEmpty() ){
            patternTXJob = mPegasusTransfer.createTransferJob( job,
                                                               patterns,
                                                               null,
                                                               txJobName,
                                                               jobClass );

            //get the stdin and set it as lof in the arguments
            patternTXJobStdin = patternTXJob.getStdIn();
            StringBuffer patternArgs = new StringBuffer();
            patternArgs.append( patternTXJob.getArguments() ).append( " " ).
                append( patternTXJobStdin );
            patternTXJob.setArguments( patternArgs.toString() );
            patternTXJob.setStdIn( "" );
            txJobs.add( patternTXJob );
        }


        TransformationCatalogEntry tcEntry = this.getTransformationCatalogEntry( job.getSiteHandle() );
        if(tcEntry == null){
            //should throw a TC specific exception
            StringBuffer error = new StringBuffer();
            error.append( "Could not find entry in tc for lfn " ).append( getCompleteTCName() ).
                  append(" at site " ).append( job.getSiteHandle());
            mLogger.log( error.toString(), LogManager.ERROR_MESSAGE_LEVEL);
            throw new RuntimeException( error.toString() );
        }



        //use the DC transfer client to handle the data sources
        for( Iterator it = rawDataSources.iterator(); it.hasNext(); ){
            FileTransfer ft = (FileTransfer)it.next();
            TransferJob dcTXJob = new TransferJob();

            dcTXJob.namespace   = tcEntry.getLogicalNamespace();
            dcTXJob.logicalName = tcEntry.getLogicalName();
            dcTXJob.version     = tcEntry.getLogicalVersion();

            dcTXJob.dvNamespace = this.DERIVATION_NAMESPACE;
            dcTXJob.dvName      = this.DERIVATION_NAME;
            dcTXJob.dvVersion   = this.DERIVATION_VERSION;

            dcTXJob.setRemoteExecutable( tcEntry.getPhysicalTransformation() );


            dcTXJob.setArguments( quote( ((NameValue)ft.getSourceURL()).getValue() ) + " " +
                                  quote( ((NameValue)ft.getDestURL()).getValue() ) );
            dcTXJob.setRemoteExecutable( "java" );
            dcTXJob.setStdIn( "" );
            dcTXJob.setStdOut( "" );
            dcTXJob.setStdErr( "" );
            dcTXJob.setSiteHandle( job.getSiteHandle() );

            //the profile information from the transformation
            //catalog needs to be assimilated into the job
            dcTXJob.updateProfiles(tcEntry);


            txJobs.add( dcTXJob );
        }


        //now lets merge all these jobs
        SubInfo merged = mSeqExecAggregator.construct( txJobs, "transfer", txJobName  );
        TransferJob txJob = new TransferJob( merged );


        //set the name of the merged job back to the name of
        //transfer job passed in the function call
        txJob.setName( txJobName );
        txJob.setJobType( jobClass );

        //if a pattern job was constructed add the pattern stdin
        //as an input file for condor to transfer
        if( patternTXJobStdin != null ){
            txJob.condorVariables.addIPFileForTransfer( patternTXJobStdin );
        }
        //take care of transfer of proxies
        this.checkAndTransferProxy( txJob );

        //apply the priority to the transfer job
        this.applyPriority( txJob );



        if(execFiles != null){
            //we need to add setup jobs to change the XBit
            super.addSetXBitJobs( job, txJob, execFiles );
        }


        return txJob;
    }




    /**
     * Returns a textual description of the transfer implementation.
     *
     * @return a short textual description
     */
    public  String getDescription(){
        return this.DESCRIPTION;
    }

    /**
     * Returns a boolean indicating whether the transfer protocol being used by
     * the implementation preserves the X Bit or not while staging.
     *
     * @return boolean
     */
    public boolean doesPreserveXBit(){
        return false;
    }


    /**
     * Return a boolean indicating whether the transfers to be done always in
     * a third party transfer mode. A value of false, results in the
     * direct or peer to peer transfers being done.
     * <p>
     * A value of false does not preclude third party transfers. They still can
     * be done, by setting the property "pegasus.transfer.*.thirdparty.sites".
     *
     * @return boolean indicating whether to always use third party transfers
     *         or not.
     *
     * @see PegasusProperties#getThirdPartySites(String)
     */
    public boolean useThirdPartyTransferAlways(){
        return false;
    }

    /**
     * Retrieves the transformation catalog entry for the executable that is
     * being used to transfer the files in the implementation.
     *
     * @param siteHandle  the handle of the  site where the transformation is
     *                    to be searched.
     *
     * @return  the transformation catalog entry if found, else null.
     */
    public TransformationCatalogEntry getTransformationCatalogEntry( String siteHandle ){
        List tcentries = null;
        try {
            //namespace and version are null for time being
            tcentries = mTCHandle.getTCEntries(this.TRANSFORMATION_NAMESPACE,
                                               this.TRANSFORMATION_NAME,
                                               this.TRANSFORMATION_VERSION,
                                               siteHandle,
                                               TCType.INSTALLED);
        } catch (Exception e) {
            mLogger.log(
                "Unable to retrieve entry from TC for " + getCompleteTCName()
                + " Cause:" + e, LogManager.DEBUG_MESSAGE_LEVEL );
        }

        return ( tcentries == null ) ?
                 this.defaultTCEntry( this.TRANSFORMATION_NAMESPACE,
                                      this.TRANSFORMATION_NAME,
                                      this.TRANSFORMATION_VERSION,
                                      siteHandle ): //try using a default one
                 (TransformationCatalogEntry) tcentries.get(0);



    }


    /**
     * Quotes a URL and returns it
     *
     * @param url String
     * @return quoted url
     */
    protected String quote( String url ){
        StringBuffer q = new StringBuffer();
        q.append( "'" ).append( url ).append( "'" );
        return q.toString();
    }

    /**
     * Returns a default TC entry to be used for the DC transfer client.
     *
     * @param namespace  the namespace of the transfer transformation
     * @param name       the logical name of the transfer transformation
     * @param version    the version of the transfer transformation
     *
     * @param site  the site for which the default entry is required.
     *
     *
     * @return  the default entry.
     */
    protected  TransformationCatalogEntry defaultTCEntry(
                                                       String namespace,
                                                       String name,
                                                       String version,
                                                       String site ){

        TransformationCatalogEntry defaultTCEntry = null;
        //check if DC_HOME is set
        String dcHome = mSCHandle.getEnvironmentVariable( site, "DC_HOME" );

        mLogger.log( "Creating a default TC entry for " +
                     Separator.combine( namespace, name, version ) +
                     " at site " + site,
                     LogManager.DEBUG_MESSAGE_LEVEL );

        //if home is still null
        if ( dcHome == null ){
            //cannot create default TC
            mLogger.log( "Unable to create a default entry for " +
                         Separator.combine( namespace, name, version ) +
                         " as DC_HOME is not set in Site Catalog" ,
                         LogManager.DEBUG_MESSAGE_LEVEL );
            //set the flag back to true
            return defaultTCEntry;
        }

        //get the essential environment variables required to get
        //it to work correctly
        List envs = this.getEnvironmentVariables( site );
        if( envs == null ){
            //cannot create default TC
            mLogger.log( "Unable to create a default entry for as could not construct necessary environment " +
                         Separator.combine( namespace, name, version ) ,
                         LogManager.DEBUG_MESSAGE_LEVEL );
            //set the flag back to true
            return defaultTCEntry;
        }


        //remove trailing / if specified
        dcHome = ( dcHome.charAt( dcHome.length() - 1 ) == File.separatorChar )?
                   dcHome.substring( 0, dcHome.length() - 1 ):
                   dcHome;

        //construct the path to the jar
        StringBuffer path = new StringBuffer();
        path.append( dcHome ).append( File.separator ).
             append( "bin" ).append( File.separator ).
             append( "dc-client" );


        defaultTCEntry = new TransformationCatalogEntry( namespace,
                                                         name,
                                                         version );

        defaultTCEntry.setPhysicalTransformation( path.toString() );
        defaultTCEntry.setResourceId( site );
        defaultTCEntry.setType( TCType.INSTALLED );
        defaultTCEntry.setProfiles( envs );

        //register back into the transformation catalog
        //so that we do not need to worry about creating it again
        try{
            mTCHandle.addTCEntry( defaultTCEntry , false );
        }
        catch( Exception e ){
            //just log as debug. as this is more of a performance improvement
            //than anything else
            mLogger.log( "Unable to register in the TC the default entry " +
                          defaultTCEntry.getLogicalTransformation() +
                          " for site " + site, e,
                          LogManager.DEBUG_MESSAGE_LEVEL );
        }
        mLogger.log( "Created entry with path " + defaultTCEntry.getPhysicalTransformation(),
                     LogManager.DEBUG_MESSAGE_LEVEL );
        return defaultTCEntry;
    }

    /**
     * Returns the environment profiles that are required for the default
     * entry to sensibly work.
     *
     * @param site the site where the job is going to run.
     *
     * @return List of environment variables, else null in case where the
     *         required environment variables could not be found.
     */
    protected List getEnvironmentVariables( String site ){
        List result = new ArrayList(1) ;

        //create the CLASSPATH from home
        String java = mSCHandle.getEnvironmentVariable( site, "JAVA_HOME" );
        if( java == null ){
            mLogger.log( "JAVA_HOME not set in site catalog for site " + site,
                         LogManager.DEBUG_MESSAGE_LEVEL );
            return null;
        }

        //we have both the environment variables
        result.add( new Profile( Profile.ENV, "JAVA_HOME", java ) );

        return result;
    }

    /**
     * Returns the complete name for the transformation.
     *
     * @return the complete name.
     */
    protected String getCompleteTCName(){
        return Separator.combine(this.TRANSFORMATION_NAMESPACE,
                                 this.TRANSFORMATION_NAME,
                                 this.TRANSFORMATION_VERSION);
    }



}
