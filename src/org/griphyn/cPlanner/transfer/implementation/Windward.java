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


package org.griphyn.cPlanner.transfer.implementation;

import edu.isi.pegasus.common.logging.LoggingKeys;
import edu.isi.pegasus.planner.catalog.site.classes.GridGateway;
import edu.isi.pegasus.planner.catalog.site.classes.SiteCatalogEntry;
import org.griphyn.cPlanner.classes.ADag;
import org.griphyn.cPlanner.classes.SubInfo;
import org.griphyn.cPlanner.classes.TransferJob;
import org.griphyn.cPlanner.classes.FileTransfer;
import org.griphyn.cPlanner.classes.PegasusBag;

import edu.isi.pegasus.common.logging.LogManager;

import org.griphyn.cPlanner.transfer.MultipleFTPerXFERJob;

import edu.isi.pegasus.planner.catalog.transformation.TransformationCatalogEntry;

import edu.isi.pegasus.planner.catalog.transformation.classes.TCType;

import org.griphyn.common.util.Separator;

import org.griphyn.cPlanner.cluster.aggregator.JobAggregatorFactory;
import org.griphyn.cPlanner.cluster.JobAggregator;

import java.io.File;

import java.util.Collection;
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
     * The handle to the BAE transfer implementation.
     */
    private BAE mBAESITransfer;
    
    /**
     * The handle to the BAE stageout transfer implementation.
     */
    private BAERIC mBAESOTransfer;
    
    
    /**
     * The seqexec job aggregator.
     */
    private JobAggregator mSeqExecAggregator;
    
    /**
     * The refiner beig used.
     */
    private Refiner mRefiner;

    /**
     * The overloaded constructor, that is called by the Factory to load the
     * class.
     *
     * @param bag   the bag of initialization objects.
     */
    public Windward( PegasusBag bag ) {
        super( bag );

        //should probably go through the factory
        mPegasusTransfer = new Transfer( bag );
        mBAESITransfer     = new BAE( bag );
        mBAESOTransfer     = new BAERIC( bag );

        //just to pass the label have to send an empty ADag.
        //should be fixed
        ADag dag = new ADag();
        dag.dagInfo.setLabel( "windward" );

        mSeqExecAggregator = JobAggregatorFactory.loadInstance( JobAggregatorFactory.SEQ_EXEC_CLASS,
                                                                dag,
                                                                bag  );
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
        mBAESITransfer.setRefiner(refiner);
        mBAESOTransfer.setRefiner( refiner );
        mRefiner = refiner;
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

        
        if( jobClass == SubInfo.STAGE_IN_JOB ){
            return createSITransferJob( job, files, execFiles, txJobName );
        }
        else if ( jobClass == SubInfo.STAGE_OUT_JOB ){
            return createSOTransferJob( job, files, execFiles, txJobName );
        }
        else{
            throw new RuntimeException( "Windward Transfer Implementation does not support jobs of class "  + jobClass );
        }
    }



    /**
     * Creates a stagein transfer job that stages in data using BAE transfer client
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
     *
     * @return  the created TransferJob.
     */
    public TransferJob createSITransferJob( SubInfo job,
                                          Collection files,
                                          Collection execFiles,
                                          String txJobName) {

        
        int jobClass = SubInfo.STAGE_IN_JOB;
                                          
        //iterate through all the files and identify the patterns
        //and the other data sources
        Collection rawDataSources = new LinkedList();
        Collection patterns       = new LinkedList();

        for( Iterator it = files.iterator(); it.hasNext(); ){
            FileTransfer ft = ( FileTransfer )it.next();
            
            //separate patterns from datasets
            if( ft.getType() == FileTransfer.OTHER_FILE ){
                patterns.add( ft );
            }
            else{
                rawDataSources.add( ft );
            }
            
        }

        List txJobs = new LinkedList();

        //use the Pegasus Transfer to handle the patterns
        TransferJob patternTXJob = null;
        String patternTXJobStdin = null;
        List<String> txJobIDs = new LinkedList<String>();
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
            txJobIDs.add( patternTXJob.getID() );
        }


      

        //this should in fact only be set
        // for non third party pools
        //we first check if there entry for transfer universe,
        //if no then go for globus
//        SiteInfo ePool = mSCHandle.getTXPoolEntry( job.getSiteHandle() );
//        JobManager jobmanager = ePool.selectJobManager(this.TRANSFER_UNIVERSE,true);
        SiteCatalogEntry ePool = mSiteStore.lookup( job.getSiteHandle() );
        GridGateway jobmanager = ePool.selectGridGateway( GridGateway.JOB_TYPE.transfer );


        //use the DC transfer client to handle the data sources        
        for( Iterator it = rawDataSources.iterator(); it.hasNext(); ){
            FileTransfer ft = (FileTransfer)it.next();            
            List<FileTransfer> l = new LinkedList<FileTransfer>();
            l.add( ft );
            TransferJob dcTXJob = mBAESITransfer.createTransferJob( job,
                                                                  l,
                                                                  null,
                                                                  txJobName, 
                                                                  jobClass );
            txJobs.add( dcTXJob );
            txJobIDs.add( dcTXJob.getID() );
        }

        //only merging if more than only one data set being staged
        TransferJob txJob = null;
        if( txJobs.size() > 1 ){        
            //now lets merge all these jobs
            SubInfo merged = mSeqExecAggregator.construct( txJobs, "transfer", txJobName  );
            txJob = new TransferJob( merged );


            //set the name of the merged job back to the name of
            //transfer job passed in the function call
            txJob.setName( txJobName );
            txJob.setJobType( jobClass );
        }else{
            txJob = (TransferJob) txJobs.get( 0 );
        }

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

        mLogger.logEntityHierarchyMessage( LoggingKeys.DAX_ID, mRefiner.getWorkflow().getAbstractWorkflowID(),
                                           LoggingKeys.JOB_ID, txJobIDs );

        return txJob;
    }


     /**
     * Creates a stage-out transfer job that stages out data using BAE RIC client
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
     *
     * @return  the created TransferJob.
     */
    public TransferJob createSOTransferJob( SubInfo job,
                                          Collection files,
                                          Collection execFiles,
                                          String txJobName) {

        
        int jobClass = SubInfo.STAGE_OUT_JOB;
                                          
        

        List txJobs = new LinkedList();
        List<String> txJobIDs = new LinkedList<String>();
        
        //this should in fact only be set
        // for non third party pools
        //we first check if there entry for transfer universe,
        //if no then go for globus
//        SiteInfo ePool = mSCHandle.getTXPoolEntry( job.getSiteHandle() );
//        JobManager jobmanager = ePool.selectJobManager(this.TRANSFER_UNIVERSE,true);
        SiteCatalogEntry ePool = mSiteStore.lookup( job.getSiteHandle() );
        GridGateway jobmanager = ePool.selectGridGateway( GridGateway.JOB_TYPE.transfer );


        //use the DC transfer client to handle the data sources        
        for( Iterator it = files.iterator(); it.hasNext(); ){
            FileTransfer ft = (FileTransfer)it.next();            
            List<FileTransfer> l = new LinkedList<FileTransfer>();
            l.add( ft );
            TransferJob dcTXJob = mBAESOTransfer.createTransferJob( job,
                                                                  l,
                                                                  null,
                                                                  txJobName, 
                                                                  jobClass );
            txJobs.add( dcTXJob );
            txJobIDs.add( dcTXJob.getID() );
      
            
        }

        //only merging if more than only one data set being staged
        TransferJob txJob = null;
        if( txJobs.size() > 1 ){        
            //now lets merge all these jobs
            SubInfo merged = mSeqExecAggregator.construct( txJobs, "transfer", txJobName  );
            txJob = new TransferJob( merged );


            //set the name of the merged job back to the name of
            //transfer job passed in the function call
            txJob.setName( txJobName );
            txJob.setJobType( jobClass );
        }else{
            txJob = (TransferJob) txJobs.get( 0 );
        }

        //append some logging parameters
        mLogger.add( LoggingKeys.DAG_ID , mProps.getProperty( "pegasus.windward.wf.id" ) ).
                add( LoggingKeys.JOB_ID , txJob.getID() );
        for( Iterator it = files.iterator(); it.hasNext(); ){
            FileTransfer ft = (FileTransfer)it.next();     
            mLogger.add( "dataset.id" , ft.getLFN() );
        }       
        mLogger.add( "Ingestion job created" );
        mLogger.logAndReset( LogManager.INFO_MESSAGE_LEVEL );
        
        //take care of transfer of proxies
        this.checkAndTransferProxy( txJob );

        //apply the priority to the transfer job
        this.applyPriority( txJob );



        if(execFiles != null){
            //we need to add setup jobs to change the XBit
            super.addSetXBitJobs( job, txJob, execFiles );
        }

        mLogger.logEntityHierarchyMessage( LoggingKeys.DAX_ID, mRefiner.getWorkflow().getAbstractWorkflowID(),
                                           LoggingKeys.JOB_ID, txJobIDs );

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
        String dcHome = mSiteStore.getEnvironmentVariable( site, "DC_HOME" );

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
        //add the DC home to environments
        envs.add( new Profile( Profile.ENV, "DC_HOME", dcHome ) );

        //remove trailing / if specified
        dcHome = ( dcHome.charAt( dcHome.length() - 1 ) == File.separatorChar )?
                   dcHome.substring( 0, dcHome.length() - 1 ):
                   dcHome;

        //construct the path to the jar
        StringBuffer path = new StringBuffer();
        path.append( dcHome ).append( File.separator ).
             append( "bin" ).append( File.separator ).
             append( "dc-transfer" );


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
        String java = mSiteStore.getEnvironmentVariable( site, "JAVA_HOME" );
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
