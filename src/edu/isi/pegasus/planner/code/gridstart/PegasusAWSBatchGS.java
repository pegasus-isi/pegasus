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
package edu.isi.pegasus.planner.code.gridstart;

import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.planner.classes.ADag;
import edu.isi.pegasus.planner.classes.AggregatedJob;
import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.cluster.JobAggregator;
import edu.isi.pegasus.planner.cluster.aggregator.AWSBatch;
import edu.isi.pegasus.planner.code.GridStart;
import edu.isi.pegasus.planner.code.generator.condor.CondorStyle;
import edu.isi.pegasus.planner.code.generator.condor.CondorStyleException;
import edu.isi.pegasus.planner.code.generator.condor.CondorStyleFactory;
import edu.isi.pegasus.planner.partitioner.graph.GraphNode;
import java.io.File;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 *
 * Wraps a job to be run via pegasus-aws-batch on the AWS Batch service.
 * 
 * This is achieved by first wrapping the job with PegasusLite and then running
 * the task using pegasus-aws-batch-launch.sh on a AWS Batch fetch and run
 * container.
 * 
 * @author Karan Vahi
 */
public class PegasusAWSBatchGS implements GridStart {

    /**
     * The environment variable that designates the key used by tool pegasus-aws-batch 
     * for file transfers.
     */
    public static final String TRANSFER_INPUT_FILES_KEY = "TRANSFER_INPUT_FILES";
    
    /**
     * The environment variable that designates the key used by fetch_and_run.sh
     * executable in batch containers 
     */
    public static final String BATCH_FILE_TYPE_KEY = "BATCH_FILE_TYPE";
    
    /**
     * The environment variable that designates the key used by fetch_and_run.sh
     * executable for batch containers to pull the user script from s3
     */
    public static final String BATCH_FILE_S3_URL_KEY = "BATCH_FILE_S3_URL";
    
    /**
     * The basename of the class that is implmenting this. Could have
     * been determined by reflection.
     */
    public static final String CLASSNAME = "PegasusAWSBatchGS";

    /**
     * The SHORTNAME for this implementation.
     */
    public static final String SHORT_NAME = "pegasus-aws-batch";

    public static final String SEPARATOR = "########################";
    public static final char SEPARATOR_CHAR = '#';
    public static final String  MESSAGE_PREFIX = "[Pegasus AWS Batch Gridstart] ";
    public static final int  MESSAGE_STRING_LENGTH = 80;
    
    private PegasusBag mBag;
    
    private ADag mDAG;
    
    /**
     * Instance to Pegasus Lite for wrapping jobs
     */
    private PegasusLite mPegasusLite;
    
    /**
     * The logging instance
     */
    private LogManager mLogger;
    
    private String mSubmitDir;
    
    /**
     * The style factory to associate credentials
     */
    private CondorStyleFactory mStyleFactory;
    
    /**
     * Tracks credentials to be transferred for a clustered job
     */
    public Set<String> mCurrentClusteredJobCredentials;
    
    public PegasusAWSBatchGS(){
        mPegasusLite = new PegasusLite();
    }

    /**
     * 
     * @param bag
     * @param dag 
     */
    public void initialize(PegasusBag bag, ADag dag) {
        mBag       = bag;
        mDAG       = dag;
        mLogger    = bag.getLogger();
        
        mSubmitDir = bag.getPlannerOptions().getSubmitDirectory();
        mPegasusLite.initialize(bag, dag);
        mStyleFactory     = new CondorStyleFactory();
        mStyleFactory.initialize(bag);
    }

    /**
     * Enables a clustered job.
     * 
     * @param job
     * @param isGlobusJob
     * @return 
     */
    public boolean enable(AggregatedJob job, boolean isGlobusJob) {
        if( !job.getTXName().equals( AWSBatch.COLLAPSE_LOGICAL_NAME) ){
            throw new RuntimeException( "Aggregated job not clustered using AWSBatch - " + job.getTXName() );
        }
        mCurrentClusteredJobCredentials = new HashSet();
        boolean enable = true;
        String relativeDir = job.getRelativeSubmitDirectory();
        for( Iterator<GraphNode> it = job.nodeIterator(); it.hasNext();  ) {
            GraphNode node = it.next();
            Job constitutentJob = (Job) node.getContent();

            if( constitutentJob instanceof AggregatedJob ){
                //slurp in contents of it's stdin
                throw new RuntimeException( "Enabling of clustered jobs within a cluster not supported with " + AWSBatch.PEGASUS_AWS_BATCH_LAUNCH_BASENAME );
            }
            //we need to set the relative dir of constituent jobs to 
            //the clustered job itself
            constitutentJob.setRelativeSubmitDirectory( relativeDir );
            
            enable = enable && this.enable(constitutentJob, isGlobusJob);
            
        }
        
        //set the credentials for clustered job
        job.condorVariables.addIPFileForTransfer(mCurrentClusteredJobCredentials);
        mLogger.log( PegasusAWSBatchGS.MESSAGE_PREFIX + "Credentials to be transferred for job  - " + job.getID() + " " + job.condorVariables.getIPFilesForTransfer(),
                      LogManager.DEBUG_MESSAGE_LEVEL );
        
        //we enable the clustered job ourselves
        JobAggregator aggregator = job.getJobAggregator();
        if( aggregator == null ){
            throw new RuntimeException( "Clustered job not associated with a job aggregator " + job.getID() );
        }
        //all the constitutent jobs are enabled.
        //get the job aggregator to render the job 
        //to it's executable form
        aggregator.makeAbstractAggregatedJobConcrete( job  );
        
        //set up stdout and stderr for the clustered job
        construct(job,"output", job.getFileFullPath( mSubmitDir, ".out") );
        if (isGlobusJob) {
            construct(job,"transfer_output","true");
        }
        construct(job,"error", job.getFileFullPath( mSubmitDir, ".err") );
        if (isGlobusJob) {
            construct(job,"transfer_error","true");
        }
        
        return enable;
    }

    /**
     * Enables a single job to be launched via PegasusLite instance.
     * 
     * @param job
     * @param isGlobusJob
     * 
     * @return 
     */
    public boolean enable(Job job, boolean isGlobusJob) {
        mLogger.log( PegasusAWSBatchGS.MESSAGE_PREFIX + "Enabling task - " + job.getID() ,
                      LogManager.DEBUG_MESSAGE_LEVEL );
        
        boolean result =  this.mPegasusLite.enable(job, isGlobusJob);
        
        //each constituent job pegasus lite script has to refer by basename only
        //and add the executable for transfer input file
        String executable = job.getRemoteExecutable();
        job.setRemoteExecutable( AWSBatch.PEGASUS_AWS_BATCH_LAUNCH_BASENAME );
        job.condorVariables.addIPFileForTransfer( executable );
        job.setArguments( new File( executable ).getName() );

        //since in this we are running each task making up the clustered job via
        //pegasus lite, the credentials have to be handled per task, not at the 
        //clustered job level in the code generator
        String jobCredentials = updateJobEnvForCredentials( job );
        for( String credential: jobCredentials.split(",")){
            mCurrentClusteredJobCredentials.add( credential );
        }
        
        //add each file transfer via condor to pegasus-aws-batch 
        //mechanism
        String csFiles = job.condorVariables.getIPFilesForTransfer();
        if( csFiles != null ){
            //we want all files other than pegasus-lite-common.sh
            String[] files = csFiles.split( "," );
            StringBuilder sb = new StringBuilder();
            boolean match = true;
            for( String file: files ){
                if( match && file.equals( mPegasusLite.mLocalPathToPegasusLiteCommon) ){
                    //no need to match further
                    match = false;
                    continue;
                }
                //add file
                sb.append( file ).append( "," );
            }
            //remove trailing slash
            int index = sb.lastIndexOf( "," );
            String csv = ( index == - 1 )? sb.toString(): sb.substring( 0, index );
            if( csv.length() > 0 ){
                job.envVariables.construct( PegasusAWSBatchGS.TRANSFER_INPUT_FILES_KEY, csv );
            }
        }
        
        //add the environment variables required for fetch_and_run.sh script in
        //the container
        job.envVariables.construct( PegasusAWSBatchGS.BATCH_FILE_TYPE_KEY,  "script" );
        job.envVariables.construct( PegasusAWSBatchGS.BATCH_FILE_S3_URL_KEY,  "s3://pegasus-aws-batch" );
        
        return result;
    }

    /**
     * Pass through to PegasusLite instance
     * @param fullPath 
     */
    public void useFullPathToGridStarts(boolean fullPath) {
        mPegasusLite.useFullPathToGridStarts(fullPath);
    }

    /**
     * Pass through to PegasusLite instance
     * 
     * @return boolean
     */
    public boolean canSetXBit() {
        return mPegasusLite.canSetXBit();
    }

     /**
     * Pass through to PegasusLite instance
     * 
     * @return String
     */
    public String getWorkerNodeDirectory(Job job) {
        return mPegasusLite.getWorkerNodeDirectory(job);
    }

   /**
     * Returns the value of the  profile with key as Pegasus.GRIDSTART_KEY,
     * that would result in the loading of this particular implementation.
     * It is usually the name of the implementing class without the
     * package name.
     *
     * @return the value of the profile key.
     */
    public  String getVDSKeyValue(){
        return PegasusAWSBatchGS.CLASSNAME;
    }


    /**
     * Returns a short textual description in the form of the name of the class.
     *
     * @return  short textual description.
     */
    public String shortDescribe(){
        return PegasusAWSBatchGS.SHORT_NAME;
    }

    /**
     * Returns the SHORT_NAME for the POSTScript implementation that is used
     * to be as default with this GridStart implementation.
     *
     * @return  the identifier for the default POSTScript implementation for
     *          kickstart gridstart module.
     *
     */
    public String defaultPOSTScript(){
        return mPegasusLite.defaultPOSTScript();
    }
 
    /**
     * Constructs a condor variable in the condor profile namespace
     * associated with the constituentJob. Overrides any preexisting key values.
     *
     * @param constituentJob   contains the constituentJob description.
     * @param key   the key of the profile.
     * @param value the associated value.
     */
    private void construct(Job job, String key, String value){
        job.condorVariables.construct(key,value);
    }

    /**
     * Updates a job environment to hold any credential information required
     * 
     * @param job 
     * 
     * @retrun a comma separate list of paths to credentials to transfer
     * 
     */
    private String updateJobEnvForCredentials(Job job) {
        mLogger.log( PegasusAWSBatchGS.MESSAGE_PREFIX + "Credentials for task " + job.getID() + " - " + job.getCredentialTypes(),
                      LogManager.DEBUG_MESSAGE_LEVEL );
        
        //we dont want credentials transferred per task making up the clustered job
        String before = job.condorVariables.getIPFilesForTransfer();
        job.condorVariables.removeIPFilesForTransfer();
        
        //we do it via style and avoid duplication of code
        CondorStyle cs = mStyleFactory.loadInstance( job );
        
        try {
            cs.apply(job);
        } catch (CondorStyleException ex) {
           throw new RuntimeException( PegasusAWSBatchGS.MESSAGE_PREFIX + " Unable to associate credentials for task " + job.getID() ,ex );
        }
        
        String credentials = job.condorVariables.getIPFilesForTransfer();
        job.condorVariables.removeIPFilesForTransfer();
        //preserver the original file transfer
        if( before != null ){
            job.condorVariables.addIPFileForTransfer( Arrays.asList( before.split( ",") ));
        }
        
        return (credentials == null) ? "" : credentials;
    }

}
