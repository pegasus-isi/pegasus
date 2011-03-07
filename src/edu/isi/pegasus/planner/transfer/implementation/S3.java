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


package edu.isi.pegasus.planner.transfer.implementation;

import edu.isi.pegasus.common.logging.LoggingKeys;
import edu.isi.pegasus.planner.catalog.site.classes.GridGateway;
import edu.isi.pegasus.planner.catalog.site.classes.SiteCatalogEntry;
import edu.isi.pegasus.planner.classes.ADag;
import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.classes.TransferJob;
import edu.isi.pegasus.planner.classes.FileTransfer;
import edu.isi.pegasus.planner.classes.PegasusBag;

import edu.isi.pegasus.common.logging.LogManager;

import edu.isi.pegasus.planner.transfer.MultipleFTPerXFERJob;

import edu.isi.pegasus.planner.catalog.transformation.TransformationCatalogEntry;

import edu.isi.pegasus.planner.catalog.transformation.classes.TCType;

import edu.isi.pegasus.common.util.Separator;

import edu.isi.pegasus.planner.cluster.aggregator.JobAggregatorFactory;
import edu.isi.pegasus.planner.cluster.JobAggregator;

import java.io.File;

import java.util.Collection;
import java.util.List;
import java.util.LinkedList;
import java.util.Iterator;
import edu.isi.pegasus.planner.transfer.Refiner;
import edu.isi.pegasus.planner.classes.Profile;
import java.util.ArrayList;
import java.util.HashSet;


/**
 * A S3 implementation that uses the seqexec client to execute
 *
 * -s3cmd client to stagein and stageout multiple files from a S3 bucked in
 *  a single job.
 * 
 * <p>
 * The use of seqexec to wrap multiple invocations of s3cmd allow us to get 
 * around the issue of s3cmd client only transferring one file per invocation. 
 * 
 * <p>
 * 
 * In order to use the transfer implementation implemented by this class,
 * <pre>
 *        - the property pegasus.transfer.*.impl must be set to value S3.
 * </pre>
 *
 * 
 * 
 * @author Karan Vahi
 * @version $Revision$
 */

public class S3 extends Abstract
                      implements MultipleFTPerXFERJob  {


    /**
     * A short description of the transfer implementation.
     */
    public static final String DESCRIPTION = "Seqexec Transfer Wrapper around s3cmd";

    /**
     * The transformation namespace for for the transfer job.
     */
    public static final String TRANSFORMATION_NAMESPACE = "amazon";


    /**
     * The name of the underlying transformation that is queried for in the
     * Transformation Catalog.
     */
    public static final String TRANSFORMATION_NAME = "s3cmd";

    /**
     * The version number for the transfer job.
     */
    public static final String TRANSFORMATION_VERSION = null;

    /**
     * The derivation namespace for for the transfer job.
     */
    public static final String DERIVATION_NAMESPACE = "amazon";

    /**
     * The name of the underlying derivation.
     */
    public static final String DERIVATION_NAME = "s3cmd";

    /**
     * The derivation version number for the transfer job.
     */
    public static final String DERIVATION_VERSION = null;


    /**
     * The handle to the S3Cmd transfer implementation.
     */
    private S3Cmd mS3Transfer;
    
    
    /**
     * The seqexec job aggregator.
     */
    private JobAggregator mSeqExecAggregator;
    
    /**
     * The submit directory for the workflow.
     */
    private String mSubmitDirectory;

    /**
     * The overloaded constructor, that is called by the Factory to load the
     * class.
     *
     * @param bag   the bag of initialization objects.
     */
    public S3( PegasusBag bag ) {
        super( bag );
        mSubmitDirectory = bag.getPlannerOptions().getSubmitDirectory();
        //should probably go through the factory
        mS3Transfer = new S3Cmd( bag );
        
        //just to pass the label and timestamp have to send an empty ADag.
        //should be fixed
        ADag dag = new ADag();
        dag.dagInfo.setLabel( "s3" );
        dag.dagInfo.setDAXMTime( new File( bag.getPlannerOptions().getDAX() ) );

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
        mS3Transfer.setRefiner( refiner );
        mRefiner = refiner;
    }


    /**
     *
     *
     * @param job         the Job object for the job, in relation to which
     *                    the transfer node is being added. Either the transfer
     *                    node can be transferring this jobs input files to
     *                    the execution pool, or transferring this job's output
     *                    files to the output pool.
     * @param site        the site where the transfer job should run.
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
    public TransferJob createTransferJob( Job job,
                                          String site,
                                          Collection files,
                                          Collection execFiles,
                                          String txJobName,
                                          int jobClass) {

     
        List txJobs = new LinkedList();
        List<String> txJobIDs = new LinkedList<String>();
        
        //this should in fact only be set
        // for non third party pools
        SiteCatalogEntry ePool = mSiteStore.lookup( "local" );
        GridGateway jobmanager = ePool.selectGridGateway( GridGateway.JOB_TYPE.transfer );


        //use the S3 transfer client to handle the data sources        
        for( Iterator it = files.iterator(); it.hasNext(); ){
            FileTransfer ft = (FileTransfer)it.next();            
            List<FileTransfer> l = new LinkedList<FileTransfer>();
            l.add( ft );
            TransferJob s3Job = mS3Transfer.createTransferJob( job,
                                                               site,
                                                                  l,
                                                                  null,
                                                                  txJobName, 
                                                                  jobClass );
            
            txJobs.add( s3Job );
            txJobIDs.add( s3Job.getID() );
        }

        //only merging if more than only one data set being staged
        TransferJob txJob = null;
        if( txJobs.size() > 1 ){        
            //now lets merge all these jobs
            Job merged = mSeqExecAggregator.constructAbstractAggregatedJob( txJobs, "transfer", txJobName  );
            String stdIn = txJobName + ".in";
            //rename the stdin file to make in accordance with tx jobname
            File f = new File( mSubmitDirectory, merged.getStdIn() );
            f.renameTo( new File( mSubmitDirectory, stdIn ) );
            merged.setStdIn( stdIn );
            
            txJob = new TransferJob( merged );

            
            //set the name of the merged job back to the name of
            //transfer job passed in the function call
            txJob.setName( txJobName );
            txJob.setJobType( jobClass );
            
            
        }else{
            txJob = (TransferJob) txJobs.get( 0 );
        }
        
        //take care of transfer of proxies
        this.checkAndTransferProxy( txJob );

        //apply the priority to the transfer job
        this.applyPriority( txJob );

        if(execFiles != null){
            //we need to add setup jobs to change the XBit
            super.addSetXBitJobs( job, txJob, execFiles );
        }

        //to get the file stat information we need to put
        //the files as output files of the transfer job
        txJob.outputFiles = new HashSet( files );
        
        //set the non third party site as this job
        //always run on local host.
        txJob.setNonThirdPartySite( job.getSiteHandle() );
        
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
        return S3.DESCRIPTION;
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
        return true;
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
            tcentries = mTCHandle.lookup(S3.TRANSFORMATION_NAMESPACE,
                                               S3.TRANSFORMATION_NAME,
                                               S3.TRANSFORMATION_VERSION,
                                               siteHandle,
                                               TCType.INSTALLED);
        } catch (Exception e) {
            mLogger.log(
                "Unable to retrieve entry from TC for " + getCompleteTCName()
                + " Cause:" + e, LogManager.DEBUG_MESSAGE_LEVEL );
        }

        return ( tcentries == null ) ?
                 null: //try using a default one
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
        return Separator.combine(S3.TRANSFORMATION_NAMESPACE,
                                 S3.TRANSFORMATION_NAME,
                                 S3.TRANSFORMATION_VERSION);
    }



}
