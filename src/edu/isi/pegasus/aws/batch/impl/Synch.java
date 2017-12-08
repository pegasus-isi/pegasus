/**
 *  Copyright 2007-2017 University Of Southern California
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

package edu.isi.pegasus.aws.batch.impl;


import edu.isi.pegasus.aws.batch.common.AWSJobstateWriter;
import edu.isi.pegasus.aws.batch.builder.ComputeEnvironment;
import edu.isi.pegasus.aws.batch.builder.JobQueue;
import edu.isi.pegasus.aws.batch.builder.JobDefinition;
import edu.isi.pegasus.aws.batch.classes.AWSJob;
import edu.isi.pegasus.aws.batch.classes.Tuple;
import edu.isi.pegasus.aws.batch.common.CloudWatchLog;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.Priority;
        
import software.amazon.awssdk.regions.Region;

import software.amazon.awssdk.services.batch.*;
import software.amazon.awssdk.services.batch.model.*;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketConfiguration;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.CreateBucketResponse;
import software.amazon.awssdk.services.s3.model.DeleteBucketRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.sync.RequestBody;



/**
 *
 * @author Karan Vahi
 */
public class Synch {

    /**
     * The ARN prefix identifier
     */
    public static final String ARN_PREFIX = "arn:aws";
    
    /**
     * The s3 prefix
     */
    public static final String S3_PREFIX ="s3://";

    
    public enum BATCH_ENTITY_TYPE{ compute_environment, job_defintion, job_queue, s3_bucket};
    
    public static final String AWS_PROPERTY_PREFIX = "aws";
    
    public static final String AWS_BATCH_PROPERTY_PREFIX = "aws.batch";
    
     
    public static final String JOB_DEFINITION_SUFFIX = "-job-definition";
    
    public static final String JOB_QUEUE_SUFFIX = "-job-queue";
    
    public static final String COMPUTE_ENV_SUFFIX = "-compute-env";
    
    public static final String S3_BUCKET_SUFFIX = "-bucket";
    
    public static final String CLOUD_WATCH_BATCH_LOG_GROUP =  "/aws/batch/job";
    
    public static final String TRANSFER_INPUT_FILES_KEY = "TRANSFER_INPUT_FILES";
    
    /**
     * A value to trigger creation of job queue even if user did not specify in 
     * case of running jobs.
     */
    public static final String NULL_VALUE = "NULL";
    
    /**
     * maximum sleep time in seconds
     */
    public static final long MAX_SLEEP_TIME = 32 *1000;
    
   
    private Map<String,AWSJob> mJobMap;
   
    
    private String mPrefix;
    private BatchClient mBatchClient;

    
    /**
     * The amazon account name
     */
    private  String mAWSAccountID;  
    
    private Region mAWSRegion;
    
    private ExecutorService mExecutorService;
    
    private String mJobDefinitionARN;
    
    private String mComputeEnvironmentARN;
    
    private String mJobQueueARN;
    
    private String mS3Bucket;
    
    /**
     * A map to track what associated batch entities need to be 
     * deleted
     */
    private EnumMap<BATCH_ENTITY_TYPE,Boolean> mDeleteOnExit;
    
    private final List mSubmitResponses = new LinkedList();
    
    /**
     * Boolean to track if user is done with job submissions
     */
    private boolean mDoneWithJobSubmits;
    
    /**
     * Future to track end of monitoring thread
     */
    private Future mMonitoringThreadFuture;
    
    private Logger mLogger;
    
    
    private AWSJobstateWriter mJobstateWriter;
    
    public Synch(){
        
    }
    
    /**
     * Initialize the log.
     * 
     * @param properties properties with pegasus prefix stripped.
     * @param level
     * @param jsonFileMap
     * @throws IOException
     */
    public void initialze( Properties properties, Level level, EnumMap<BATCH_ENTITY_TYPE, String> jsonFileMap ) throws IOException{
        //"405596411149";
        mLogger       = Logger.getLogger( Synch.class.getName() ); 
        mLogger.setLevel(level);
        mAWSAccountID  = getProperty( properties, Synch.AWS_PROPERTY_PREFIX , "account" );
        mAWSRegion     = Region.of( getProperty( properties, Synch.AWS_PROPERTY_PREFIX, "region") );//"us-west-2" 
        mPrefix        = getProperty( properties, Synch.AWS_BATCH_PROPERTY_PREFIX,  "prefix" );
        mDeleteOnExit  = new EnumMap<>(BATCH_ENTITY_TYPE.class);
        
        mJobstateWriter = new AWSJobstateWriter();
        mJobstateWriter.initialze( new File("."), mLogger);
        
        mJobMap = new HashMap();
        mExecutorService = Executors.newFixedThreadPool(2);
        mBatchClient = BatchClient.builder().region( mAWSRegion ).build();
        mDoneWithJobSubmits = false;
    }
    
    /**
     * Does the setup of the various associated entitites for AWS Batch to
     * accept jobs.
     * 
     * @param entities      entitites to be setup
     * @param allRequired   whehter all entities should be present
     */
    public void setup( EnumMap<BATCH_ENTITY_TYPE, String> entities, boolean allRequired) {
        boolean delete = true;
        
        String value = getEntityValue(entities, BATCH_ENTITY_TYPE.job_defintion, allRequired );
        if( value != null ){
            if( value.startsWith( ARN_PREFIX ) ){
                mJobDefinitionARN = value;
                mLogger.info("Using existing Job Definition " + mJobDefinitionARN );
                delete = false;
            }
            else{
                mJobDefinitionARN = createJobDefinition( new File(value), 
                                                               constructDefaultName( Synch.JOB_DEFINITION_SUFFIX));
                mLogger.info("Created Job Definition " + mJobDefinitionARN );
            }
            mDeleteOnExit.put(BATCH_ENTITY_TYPE.job_defintion, delete );
        }
         
        value = getEntityValue(entities, BATCH_ENTITY_TYPE.compute_environment, allRequired );
        delete = true;
        if( value != null ){
            if( value.startsWith( ARN_PREFIX ) ){
                mComputeEnvironmentARN = value;
                mLogger.info("Using existing Compute Environment " + mComputeEnvironmentARN );
                delete = false;
            }
            else{
                mComputeEnvironmentARN = createComputeEnvironment( new File(value), 
                                                                   constructDefaultName( Synch.COMPUTE_ENV_SUFFIX) );
                mLogger.info( "Created Compute Environment " + mComputeEnvironmentARN );
            }
            mDeleteOnExit.put(BATCH_ENTITY_TYPE.compute_environment, delete );
        }

        value = getEntityValue(entities, BATCH_ENTITY_TYPE.job_queue, allRequired );
        delete = true;
        if( value != null ){
            if( value.startsWith( ARN_PREFIX ) ){
                mJobQueueARN = value;
                delete = false;
                mLogger.info("Using existing Job Queue " + mJobQueueARN );
            }
            else{
                mJobQueueARN = this.createQueue( (value.equalsIgnoreCase( Synch.NULL_VALUE )) ? null : new File(value),
                                                 mComputeEnvironmentARN, 
                                                 constructDefaultName( Synch.JOB_QUEUE_SUFFIX ));
                mLogger.info( "Created Job Queue " + mJobQueueARN );
            }
            mDeleteOnExit.put(BATCH_ENTITY_TYPE.job_queue, delete );
        }
        
        value = getEntityValue(entities, BATCH_ENTITY_TYPE.s3_bucket, allRequired );
        delete = true;
        if( value != null ){
            if( value.startsWith( S3_PREFIX ) ){
                //strip out s3 prefix
                mS3Bucket = value.substring( S3_PREFIX.length() );
                delete = false;
                mLogger.info("Using existing S3 Bucket " + mS3Bucket );
            }
            else{
                mS3Bucket = this.createS3Bucket( constructDefaultName( Synch.S3_BUCKET_SUFFIX ));
                mLogger.info( "Created S3 bucket " + mS3Bucket );
            }
            mDeleteOnExit.put(BATCH_ENTITY_TYPE.s3_bucket, delete );
        }
    }

    
    /**
     * Deletes the setup done for batch
     */
    private boolean deleteSetup(){
        EnumMap<BATCH_ENTITY_TYPE, String> entities = new EnumMap<>(BATCH_ENTITY_TYPE.class);
        if( mDeleteOnExit.get(BATCH_ENTITY_TYPE.job_queue) ){
            entities.put(BATCH_ENTITY_TYPE.job_queue, mJobQueueARN );
        }
        if( mDeleteOnExit.get(BATCH_ENTITY_TYPE.compute_environment) ){
            entities.put(BATCH_ENTITY_TYPE.compute_environment, mComputeEnvironmentARN );
        }
        if( mDeleteOnExit.get(BATCH_ENTITY_TYPE.job_defintion) ){
            entities.put(BATCH_ENTITY_TYPE.job_defintion, mJobDefinitionARN );
        }
        if( mDeleteOnExit.get(BATCH_ENTITY_TYPE.s3_bucket) ){
            entities.put(BATCH_ENTITY_TYPE.s3_bucket, mS3Bucket );
        }
        return this.deleteSetup(entities);
    }
     
    /** 
     * Does the setup of the various associated entitites for AWS Batch to
     * accept jobs.
     * 
     * @param entities 
     * @return 
     */     
    public boolean deleteSetup( EnumMap<BATCH_ENTITY_TYPE, String> entities) {
        boolean deleted = true;
        String value = this.getEntityValue(entities, BATCH_ENTITY_TYPE.job_queue, false);
        if( value != null ){
            mLogger.info( "Attempting to delete job queue " + value );
            deleted = deleteQueue( value );
        }
        
        value = this.getEntityValue(entities, BATCH_ENTITY_TYPE.compute_environment, false);
        if( deleted && value != null ){
            //compute environment can only be deleted if job queue has been
            mLogger.info( "Attempting to delete compute environment " + value );
            deleted = this.deleteComputeEnvironment( value );
        }
        value = this.getEntityValue(entities, BATCH_ENTITY_TYPE.job_defintion, false);
        if( value != null ){
            mLogger.info( "Attempting to delete job definition " + value );
            deleted = this.deleteJobDefinition( value );
        }
        value = this.getEntityValue(entities, BATCH_ENTITY_TYPE.s3_bucket, false);
        if( value != null ){
            if( value.startsWith( S3_PREFIX ) ){
                value = value.substring( S3_PREFIX.length() );
            }
            mLogger.info( "Attempting to delete S3 bucket " + value );
            deleted = this.deleteS3Bucket( value );
        }
        mLogger.info("Deleted Setup - " + deleted );
        return deleted;
    }
    
    public AWSJob.JOBSTATE getJobState( String id ){
        return this.mJobMap.get(id).getJobState();
    }
    
   
   
    
    
    public void submit( AWSJob job ){
        if( this.receivedSignalToExitAfterJobsComplete() ){
            throw new RuntimeException( "Received signal to exit. Cannot accept more job submissions" );
        }
        //submit the jobs first before polling
        job.setState(AWSJob.JOBSTATE.unsubmitted);
        
        //we need to set and override job queue ARN etc for time being
        job.setJobDefinitionARN( this.mJobDefinitionARN );
        job.setJobQueueARN(this.mJobQueueARN );
        SubmitJobRequest jobRequest = job.createAWSBatchSubmitRequest();
        
        //handle file transfers if any before submitting job
        String files = job.getEnvironmentVariable( Synch.TRANSFER_INPUT_FILES_KEY );
        if( files != null ){
            transferInputFiles( this.mS3Bucket, Arrays.asList( files.split( ",") ));
            mLogger.info( "Uploaded files " + files + " for task " + job.getID() );
        }
        
        mLogger.debug( "Submitting job " + jobRequest );
        
        try{
           Future<SubmitJobResponse> submitJobFuture = mExecutorService.submit( () -> mBatchClient.submitJob( jobRequest ) );
           addSubmitJobResponse( submitJobFuture );
        }
        catch( Exception e ){
            mLogger.error( "Unable to submit job " + job, e );
        }
        addJob( job );
        
    }
    
    private void addSubmitJobResponse( Future<SubmitJobResponse> response ){
        synchronized( this.mSubmitResponses ){
            this.mSubmitResponses.add(response);
        }
    }
    
    private void addJob(  AWSJob job ){
        synchronized( this.mJobMap ){
            mJobMap.put( job.getID(), job );
        }
    }
    
    private void submit( Collection<AWSJob> jobs ){
        //submit the jobs first before polling
        Collection<Future<SubmitJobResponse>> submitResponses = new LinkedList();
        for( AWSJob job: jobs ){
            SubmitJobRequest sampleJobRequest = job.createAWSBatchSubmitRequest();
            Future<SubmitJobResponse> submitJobFuture = mExecutorService.submit( () -> mBatchClient.submitJob(sampleJobRequest) );
            submitResponses.add(submitJobFuture);
            job.setState(AWSJob.JOBSTATE.unsubmitted);
            mJobMap.put( job.getID(), job );
        }
        
        while(true){
            for( Iterator<Future<SubmitJobResponse>> it = submitResponses.iterator(); it.hasNext(); ){
                Future<SubmitJobResponse> future = it.next();
                if( future.isDone() ){
                    try {
                        SubmitJobResponse response = future.get();
                        String jobID = response.jobId();
                        AWSJob j = mJobMap.get( response.jobName() );
                        j.setAWSJobID( jobID );
                        j.setState(AWSJob.JOBSTATE.submitted);
                        mLogger.info( "Submitted Job " + response.jobName() + " with id " +  jobID);
                        it.remove();
                    } catch (InterruptedException ex) {
                        mLogger.log( Priority.ERROR, null, ex);
                    } catch (ExecutionException ex) {
                        mLogger.log( Priority.ERROR, null, ex);
                    }
                   
                }
            }
            if( submitResponses.isEmpty() ){
                break;
            }
            //System.out.println( "Number of submit responses remaining " + submitResponses.size() );
        }
        mLogger.info( "Done with submission of jobs ");
        
    }
    
    public void monitor(){
        mMonitoringThreadFuture = this.mExecutorService.submit( ()-> this.monitor(mPrefix) );
    }
    
    public void monitor( String basename ){
        long sleepTime = 5 * 1000;
        Set<String> awsJobIDs = new HashSet();
        
        //first go through the internal job map to see if there 
        //are any previously submitted jobs
        synchronized( this.mJobMap ){
            for( Map.Entry<String,AWSJob> entry : this.mJobMap.entrySet() ){
                AWSJob j = entry.getValue();
                if( j.getJobState() == AWSJob.JOBSTATE.submitted ){
                    awsJobIDs.add( j.getAWSJobID() );
                }
            }
        }
        
        
        int numDone = 0;
        int total = awsJobIDs.size();
        Set<String> doneJobs = new HashSet();
        BatchClient batchClient = BatchClient.builder().region( mAWSRegion ).build();
        CloudWatchLog cwl       = new CloudWatchLog();
        cwl.initialze( mAWSRegion, mLogger.getLevel(), CLOUD_WATCH_BATCH_LOG_GROUP);
        while(true){
            //go through unprocessed jobs that have been submitted
            //in another thread
            List<Tuple> submittedJobs = new LinkedList();
            mLogger.debug( "Going to traverse through submitted futures ");
            synchronized( this.mSubmitResponses ){
                for( Iterator<Future<SubmitJobResponse>> it = mSubmitResponses.iterator(); it.hasNext(); ){
                    Future<SubmitJobResponse> future = it.next();
                    if( future.isDone() ){
                        try {
                            SubmitJobResponse response = future.get();
                            String awsJobID = response.jobId();
                            mLogger.debug( "Future received " + response );
                            submittedJobs.add( new Tuple( response.jobName(), awsJobID ) );
                            total++;
                            it.remove();
                        }
                        catch( Exception ex ){
                            complainAndShutdown( ex );
                            return;
                        }
                    }
                }
            }
            
            synchronized( this.mJobMap ){
                for( Tuple<String,String> tuple: submittedJobs ){
                    AWSJob j = mJobMap.get( tuple.getKey()  );
                    j.setAWSJobID( tuple.getValue() );
                    j.setState(AWSJob.JOBSTATE.submitted);
                    awsJobIDs.add( j.getAWSJobID() );
                    mLogger.info("Submitted Job " + j.getID() + " with AWS id " +  j.getAWSJobID());
                    mJobstateWriter.log(j.getID(), j.getAWSJobID(), AWSJob.JOBSTATE.submitted );
                }
            }
            //now query AWS Batch for the jobs
            try {
                
                ListJobsRequest listSucceededJobsRequest = createListJobRequest( basename, JOB_QUEUE_SUFFIX, JobStatus.SUCCEEDED );
                ListJobsRequest listFailedJobsRequest    = createListJobRequest( basename, JOB_QUEUE_SUFFIX, JobStatus.FAILED );
                
                //first query for succeeded
                mLogger.debug( "Querying for successful jobs ");
                ListJobsResponse listJobsResponse = batchClient.listJobs( listSucceededJobsRequest );
                mLogger.debug( "Retrieved  " + listJobsResponse.jobSummaryList().size() + " responses ");
                for( JobSummary summary: listJobsResponse.jobSummaryList() ){
                    String succeededJobID = summary.jobId();
                    if ( awsJobIDs.contains( succeededJobID ) ){
                        if( !doneJobs.contains(succeededJobID) ){
                            mLogger.info( "Job Succeeded "  + succeededJobID  );
                            mJobstateWriter.log(summary.jobName(), summary.jobId() , AWSJob.JOBSTATE.succeeded );
                            doneJobs.add( summary.jobId() );
                            numDone++;
                            
                            mLogger.debug("Querying for succeeded job details "  + succeededJobID  );
                            File log = cwl.retrieve(succeededJobID);
                            mLogger.debug("Log retreived for "  + succeededJobID + " to " + log  );
                        }
                    }
                }
                
                mLogger.debug( "Sleeping before querying for failure ");
                Thread.sleep( sleepTime );
                if( numDone < total ){
                    // check for failed jobs
                    listJobsResponse = batchClient.listJobs( listFailedJobsRequest );
                    for( JobSummary summary: listJobsResponse.jobSummaryList() ){
                        String failedJobID = summary.jobId();
                        if ( awsJobIDs.contains(failedJobID) ){
                            if( !doneJobs.contains(failedJobID) ){
                                mLogger.info("Job Failed "  + failedJobID   );
                                mJobstateWriter.log(summary.jobName(), summary.jobId() , AWSJob.JOBSTATE.failed );
                                doneJobs.add( summary.jobId() );
                                //remove the job so that we don't query for detail
                                awsJobIDs.remove(failedJobID);
                                numDone++;
                                
                                mLogger.debug("Querying for failed job details "  + failedJobID   );
                                File log = cwl.retrieve(failedJobID);
                                mLogger.debug("Log retreived for "  + failedJobID + " to " + log  );
                            }
                        }
                    }
                }
                
                mLogger.debug(  numDone + " jobs done of total of " + total );
                if( numDone < total ){
                    // still total is not done
                    mLogger.debug( "Sleeping before querying for status of remaining jobs ");
                    Thread.sleep( sleepTime );
                    //now we query current state for jobs
                    DescribeJobsRequest jobsRequest = DescribeJobsRequest.builder().
                                                                jobs(awsJobIDs).
                                                             build();
                    DescribeJobsResponse jobsResponse = batchClient.describeJobs(jobsRequest);
                    for( JobDetail jobDetail: jobsResponse.jobs() ){
                        mLogger.debug("Current Status of Job " + jobDetail.jobId() + "->" + jobDetail.status() + " with reason " + jobDetail.statusReason() );
                        mJobstateWriter.log(jobDetail.jobName(), jobDetail.jobId() , AWSJob.JOBSTATE.valueOf(jobDetail.status().toLowerCase()) );
                        mLogger.debug( "Detailed Job detail " + jobDetail );
                    }
                }
                else{
                    if( receivedSignalToExitAfterJobsComplete() ){
                        synchronized( this.mSubmitResponses ){
                            if( this.mSubmitResponses.isEmpty() ){
                                mLogger.info( "*** All jobs done *** ");
                                break;
                            }
                            mLogger.debug( "Waiting for " + this.mSubmitResponses.size() + " responses ");
                        }
                        
                    }
                }
       
            }
            catch( Exception ex ){
                complainAndShutdown( ex );
                return;
            }
            
        }
        
        
        mLogger.info( "Done monitoring" );
        
        try {
            batchClient.close();
        } catch (Exception ex) {
            mLogger.error( null, ex);
        }
        shutdown();
        mLogger.info( "Thread Executor Shutdown successfully " );
        
    }
    
   
    public synchronized void signalToExitAfterJobsComplete(){
        mLogger.info( "****Recieved signal to exit after completion of jobs****" );
        mDoneWithJobSubmits = true;
    }
    
    /**
     * Waits on the monitoring thread future to return, to indicate that 
     * that all jobs are completed.
     */
    public void awaitTermination(){
        try {
            mMonitoringThreadFuture.get();
        } 
        catch (InterruptedException ie) {
            mLogger.error( "Interruppted while waiting for monitoring thread to complete " , ie);
        }
        catch( ExecutionException e ){
            mLogger.error( "Execution exception encountered while waiting for monitoring thread to complete " , e);
            
        }
    }
    
    public synchronized boolean receivedSignalToExitAfterJobsComplete(){
       return mDoneWithJobSubmits;
    }
    
    /**
     * Handles any exceptions thrown and exits
     * 
     * @param ex 
     */
    protected void complainAndShutdown(Exception ex) {
        if( ex instanceof InterruptedException  ){
             mLogger.error( "Monitoring Thread was interrupted", ex);
        }
        if( ex instanceof ExecutionException ){
            mLogger.error("AWS Client Exception", ex);
        }
        else{
            mLogger.error( "Unknown Exception ", ex );
        }
        this.shutdown();
        return;
    }

    /**
     * Shutdown the thread and exit
     */
    protected void shutdown() {
        this.deleteSetup();
        try {
            mBatchClient.close();
        } catch (Exception ex) {
            mLogger.error( null, ex);
        }
        mLogger.error("Shutting down threads ...");
        if( this.mExecutorService != null ){
            mExecutorService.shutdown(); // Disable new tasks from being submitted
            try {
                // Wait a while for existing tasks to terminate
                if (!mExecutorService.awaitTermination(60, TimeUnit.SECONDS)) {
                    mExecutorService.shutdownNow(); // Cancel currently executing tasks
                    // Wait a while for tasks to respond to being cancelled
                    if (!mExecutorService.awaitTermination(60, TimeUnit.SECONDS))
                        mLogger.error("Executor Service did not terminate");
                    }
            } catch (InterruptedException ie) {
                // (Re-)Cancel if current thread also interrupted
                mExecutorService.shutdownNow();
                // Preserve interrupt status
                Thread.currentThread().interrupt();
            }
        }
    }
    
   
    public boolean deleteJobDefinition( String arn ){
       
       
        DeregisterJobDefinitionRequest request =  DeregisterJobDefinitionRequest.builder().
                                                                            jobDefinition( arn ).build();
        DeregisterJobDefinitionResponse response = mBatchClient.deregisterJobDefinition(request);
        
        mLogger.info( "Deleted job definition " + response.toString() + "  - " + arn);  
        
        return true;
        
    }
   
    /**
     * Creates a AWSJob Definiton corresponding to the description in the 
 JSON file conforming to AWS Batch HTTP specification
     * 
     * @param json  the file
     * @param name  the name to assign if does not exist
     * 
     * @return job definition ARN
     */
    public String createJobDefinition( File json,  String name  ){
        //RegisterJobDefinitionRequest jobDefinition = this.getTestRegisterJobDefinitionRequest( mPrefix , JOB_DEFINITION_SUFFIX );
        RegisterJobDefinitionRequest jobDefinition = new JobDefinition().createRegisterJobDefinitionRequestFromHTTPSpec(json, name);
        RegisterJobDefinitionResponse jdResponse = mBatchClient.registerJobDefinition( jobDefinition );
        mLogger.debug( "Created Job Definition "  + jdResponse );
        return jdResponse.jobDefinitionArn();
    }
    
    /**
     * Creates a compute environment corresponding to the description in the 
     * JSON file conforming to AWS Batch HTTP specification
     * 
     * @param json  the file
     * @param name  the name to assign if does not exist
     * 
     * @return compute environment ARN
     */
    public String createComputeEnvironment( File json,  String name  ){
        //CreateComputeEnvironmentRequest computeEnvRequest = getTestComputeEnvironmentRequest( mPrefix , COMPUTE_ENV_SUFFIX);
        CreateComputeEnvironmentRequest computeEnvRequest = new ComputeEnvironment().createComputeEnvironmentRequestFromHTTPSpec(json, name);
        CreateComputeEnvironmentResponse computeEnvResponse = mBatchClient.createComputeEnvironment(computeEnvRequest);
        String arn = computeEnvResponse.computeEnvironmentArn();
        mLogger.debug( "Created Compute Environment " + computeEnvResponse );
        boolean valid = false;
        int retry = 0;
        long sleepTime = 2 * 1000 ;
        while( !valid && retry < 3 ){
            //query to see if it has been enabled
            DescribeComputeEnvironmentsRequest describeComputeEnv = DescribeComputeEnvironmentsRequest.builder().
                                                                                                        computeEnvironments( arn ).
                                                                                                        build();
            DescribeComputeEnvironmentsResponse describeCEResponse = mBatchClient.describeComputeEnvironments(describeComputeEnv);
            for( ComputeEnvironmentDetail detail : describeCEResponse.computeEnvironments()){
                mLogger.debug(detail.computeEnvironmentArn() + "," +  detail.state() + "," + detail.status() );
                valid = detail.status().equals( CEStatus.VALID.toString() );
            }
            try {
                Thread.sleep( sleepTime );
            } catch (InterruptedException ex) {
                mLogger.log( Priority.ERROR, null, ex);
            }
            sleepTime += sleepTime;
        }
        if( !valid ){
            throw new RuntimeException( "Compute Environment still not valid after 3 retries " + arn);
        }
        mLogger.info( "Compute Environment Enabled " + arn );
       return arn;
    }
    
    
    
    /**
     * Creates a AWSJob Queue corresponding to the description in the 
 JSON file conforming to AWS Batch HTTP specification
     * 
     * @param json  the file
     * @param computeEnvironmentArn the CE ARN to assign if it does not exist
     * @param name  the name to assign if does not exist
     * 
     * @return job queue ARN
     */
    public String createQueue( File json, String computeEnvironmentArn, String name ){
        //CreateJobQueueRequest jobQueueRequest =getTestJobQueueRequest( mPrefix , JOB_QUEUE_SUFFIX, computeEnvironmentArn );
        CreateJobQueueRequest jobQueueRequest = new JobQueue().createJobQueueRequestFromHTTPSpec(json, computeEnvironmentArn, name);
        CreateJobQueueResponse jobQueueResponse = mBatchClient.createJobQueue(jobQueueRequest);
        String arn  = jobQueueResponse.jobQueueArn();
        
        mLogger.info( "Created Job Queue " + arn );
        mLogger.debug( "Created Job Queue " + jobQueueResponse );
        
        boolean valid = false;
        int retry = 0;
        long sleepTime = 2 * 1000 ;
        while( !valid && retry < 3 ){
            //query to see if it has been enabled
            DescribeJobQueuesRequest describeJobQueue = DescribeJobQueuesRequest.builder().jobQueues( arn ).build();   
            DescribeJobQueuesResponse describeJQResponse = mBatchClient.describeJobQueues(describeJobQueue );
            for( JobQueueDetail detail : describeJQResponse.jobQueues() ){
                mLogger.debug( detail.jobQueueArn()+ "," +  detail.state() + "," + detail.status() );
                valid = detail.status().equals( JQStatus.VALID.toString() );
            }
            try {
                Thread.sleep( sleepTime );
            } catch (InterruptedException ex) {
                mLogger.log( Priority.ERROR, null, ex);
            }
        }
        if( !valid ){
            throw new RuntimeException( "Job Queue still not valid after 3 retries " + arn);
        }
        
        return arn;
    }
    
    /**
     * Creates a S3 bucket with the given name
     * 
     * @param name
     * 
     * @return 
     */
    public String createS3Bucket(String name)  {
        S3Client s3Client = S3Client.builder().region(mAWSRegion).build();
        CreateBucketResponse cbr = s3Client.createBucket( CreateBucketRequest.builder().
                                                                    bucket(name).
                                                                    createBucketConfiguration(CreateBucketConfiguration.builder().
                                                                        locationConstraint(mAWSRegion.value())
                                                                                                 .build()).
                                                                    build() );
        
        
        URL url;
        try {
            url = new URL( cbr.location() );
        } catch (MalformedURLException ex) {
            throw new RuntimeException( "Invalid location returned for s3 bucket " + cbr.location(), ex );
        }
        String hostname = url.getHost();
        //strip .s3.amazonaws.com suffix
        
        return hostname.endsWith(".s3.amazonaws.com" )?
                    hostname.substring(0, hostname.length() - ".s3.amazonaws.com".length() ):
                hostname;
    }
    
    /**
     * Delete a S3 bucket with the given name
     * 
     * @param name
     * 
     * @return 
     */
    public boolean deleteS3Bucket(String name) {
        boolean deleted = true;
        ListObjectsV2Request listObjectsV2Request = ListObjectsV2Request.builder().bucket(name).build();
        ListObjectsV2Response listObjectsV2Response;
        S3Client s3Client = S3Client.builder().region(mAWSRegion).build();
        do {
            listObjectsV2Response = s3Client.listObjectsV2(listObjectsV2Request);
            if( listObjectsV2Response.contents() != null ){
                //detelete the files in the bucket
                for (S3Object s3Object : listObjectsV2Response.contents()) {
                    mLogger.debug( "Deleteing file " + s3Object.key() + " from bucket " + name);
                    s3Client.deleteObject(DeleteObjectRequest.builder().bucket( name ).key(s3Object.key()).build());
                }
            }

            listObjectsV2Request = ListObjectsV2Request.builder().bucket( name )
                                                       .continuationToken(listObjectsV2Response.nextContinuationToken())
                                                       .build();

        } while (listObjectsV2Response.isTruncated());
        
        // Delete empty bucket
        DeleteBucketRequest deleteBucketRequest = DeleteBucketRequest.builder().bucket( name ).build();
        s3Client.deleteBucket(deleteBucketRequest);
        return deleted;
    }
    
    /**
     * Transfers the input files to the specified bucket
     * 
     * @param files 
     */
    public void transferInputFiles( List<String> files) {
        this.transferInputFiles( mS3Bucket, files);
    }
    /**
     * Transfers the input files to the specified bucket
     * 
     * @param bucket
     * @param files 
     */
    public void transferInputFiles(String bucket, List<String> files) {
        S3Client s3Client = S3Client.builder().region(mAWSRegion).build();
        for( String f: files){
            File file = new File(f);
            if( file.exists() ){
                mLogger.debug( "Attempting to upload file " + file + " to bucket " + bucket);
                s3Client.putObject(PutObjectRequest.builder().bucket(bucket).key( file.getName() )
                                     .build(),RequestBody.of(file ));
                mLogger.debug( "Uploaded file " + file + " to bucket " + bucket);
            }
            else{
                throw new RuntimeException( "Unable file does not exist " + f );
            }
        }
        try {
            s3Client.close();
        } catch (Exception ex) {
           mLogger.error( "Unable to close the s3 client", ex);
        }
        
    }
    
    
    
    public boolean deleteQueue( String arn ){
        //first we update queue to disable it
        boolean deleted = false;
        int retry = 0;
        long sleepTime = 2 * 1000 ;
        
        //update it's state
        UpdateJobQueueRequest updateJobQueue = UpdateJobQueueRequest.builder().
                                                                        jobQueue( arn ).
                                                                        state(JQState.DISABLED).
                                                                        build();   
        UpdateJobQueueResponse response = mBatchClient.updateJobQueue( updateJobQueue );
        //String arn = response.jobQueueArn();

        boolean disabled = false;
        while( !disabled && retry <= 5 ){
            DescribeJobQueuesRequest describeJobQueue = DescribeJobQueuesRequest.builder().jobQueues( arn ).build();   
            DescribeJobQueuesResponse describeJQResponse = mBatchClient.describeJobQueues(describeJobQueue );
            for( JobQueueDetail detail : describeJQResponse.jobQueues() ){
                mLogger.debug( detail.jobQueueArn()+ "," +  detail.state() + "," + detail.status() );
                disabled = !detail.status().equals( JQStatus.UPDATING.toString() );
            }
            try {
                Thread.sleep( sleepTime );
            } catch (InterruptedException ex) {
                mLogger.log( Priority.ERROR, null, ex);
            }
            retry++;
        }
        mLogger.debug( "Job Queue disabled " + arn );
        retry = 0;
        if( disabled ){
            DeleteJobQueueRequest deleteJQ = DeleteJobQueueRequest.builder().jobQueue( arn ).build();
            DeleteJobQueueResponse deleteJQResponse = mBatchClient.deleteJobQueue(deleteJQ);
            mLogger.debug( deleteJQResponse );
            while( !deleted ){
                DescribeJobQueuesRequest describeJobQueue = DescribeJobQueuesRequest.builder().jobQueues( arn ).build();   
                DescribeJobQueuesResponse describeJQResponse = mBatchClient.describeJobQueues(describeJobQueue );
                ///empty response means deleted!!
                deleted = true;
                for( JobQueueDetail detail : describeJQResponse.jobQueues() ){
                    mLogger.debug( "RETRY " + retry + " " + detail.jobQueueArn()+ "," +  detail.state() + "," + detail.status() );
                    deleted = detail.status().equals( JQStatus.DELETED.toString() );
                }
                try {
                    mLogger.debug( "Sleeping for " + sleepTime );
                    Thread.sleep( sleepTime );
                } catch (InterruptedException ex) {
                    mLogger.log( Priority.ERROR, null, ex);
                }
                retry++;
                sleepTime = ( sleepTime < MAX_SLEEP_TIME )? 
                                        sleepTime + sleepTime:
                                        sleepTime;
            }
        }
        
        mLogger.info( "Job Queue deleted after " + retry + " retries - " + arn);
        return deleted;
    }
    
    
    
    
    public boolean deleteComputeEnvironment( String arn ){
        //first we update queue to disable it
        boolean deleted = false;
        int retry = 0;
        long sleepTime = 2 * 1000 ;
        
        //update it's state
        //first update to disabled
        UpdateComputeEnvironmentRequest.Builder updateBuilder = UpdateComputeEnvironmentRequest.builder();
        updateBuilder.computeEnvironment( arn );
        updateBuilder.state(CEState.DISABLED);
        UpdateComputeEnvironmentResponse updateComputeEnvResponse = mBatchClient.updateComputeEnvironment( updateBuilder.build());
        mLogger.debug( "Updated Compute Environment to " + updateComputeEnvResponse.toString() );

        boolean disabled = false;
        while( !disabled && retry < 5 ){
            DescribeComputeEnvironmentsRequest	describeCE =  DescribeComputeEnvironmentsRequest.builder().computeEnvironments( arn ).build();   
            DescribeComputeEnvironmentsResponse describeCEResponse = mBatchClient.describeComputeEnvironments(describeCE );
            for( ComputeEnvironmentDetail detail : describeCEResponse.computeEnvironments()){
                mLogger.debug( detail.computeEnvironmentArn() + "," +  detail.state() + "," + detail.status() );
                disabled = !detail.status().equals( CEStatus.UPDATING.toString() );
            }
            try {
                Thread.sleep( sleepTime );
            } catch (InterruptedException ex) {
                mLogger.log( Priority.ERROR, null, ex);
            }
            retry++;
        }
        
        retry = 0;
        if( disabled ){
            DeleteComputeEnvironmentRequest request = DeleteComputeEnvironmentRequest.builder().computeEnvironment(arn).build();
            DeleteComputeEnvironmentResponse response = mBatchClient.deleteComputeEnvironment(request);
            while( !deleted  ){
                DescribeComputeEnvironmentsRequest	describeCE =  DescribeComputeEnvironmentsRequest.builder().computeEnvironments( arn ).build();   
                DescribeComputeEnvironmentsResponse describeCEResponse = mBatchClient.describeComputeEnvironments(describeCE );              
                ///empty response means deleted!!
                deleted = true;
                for( ComputeEnvironmentDetail detail : describeCEResponse.computeEnvironments() ){
                    mLogger.debug( "RETRY " + retry + " " + detail.computeEnvironmentArn()+ "," +  detail.state() + "," + detail.status() );
                    deleted = detail.status().equals( CEStatus.DELETED.toString() );
                }
                try {
                    mLogger.debug( "Sleeping for " + sleepTime );
                    Thread.sleep( sleepTime );
                } catch (InterruptedException ex) {
                    mLogger.log( Priority.ERROR, null, ex);
                }
                retry++;
                sleepTime = ( sleepTime < MAX_SLEEP_TIME )? 
                                        sleepTime + sleepTime:
                                        sleepTime;
            }
        }
        
        mLogger.info( "Compute Environment deleted  after " + retry + " retries - " + arn);
        return deleted;
    }
    
    
   
    
    public ListJobsRequest createListJobRequest(String basename, String jobQueueSuffix,  JobStatus status) {
       ListJobsRequest ljr = ListJobsRequest.builder().
                                                    jobQueue( basename + JOB_QUEUE_SUFFIX ).
                                                    jobStatus( status ).
                                             build();
       return ljr;
        
    }

    /**
     * 
     * @param suffix
     * @return 
     */
    private String constructDefaultName(String suffix ) {
        if( mPrefix == null ){
            throw new RuntimeException( "Prefix is undefined" );
        }
        return this.mPrefix + suffix;
    }
    
    /**
     * Returns value and throws an exception if required
     * 
     * @param map
     * @param type
     * @param required
     * 
     * @return 
     */
    private String getEntityValue(EnumMap<BATCH_ENTITY_TYPE, String> map, BATCH_ENTITY_TYPE type, boolean required) {
        String value = map.get( type );
        
        if( value == null && required ){
            throw new RuntimeException( type + " needs to be specified ");
        }
        
        return value;
                
    }
    
    /**
     * Retrieves a property from the object. If not exists throws a runtime 
     * exception
     * 
     * @param properties
     * @param prefix
     * @param name
     * @return 
     */
    private String getProperty(Properties properties, String prefix, String name) {
        String property = prefix + "." + name;
        String value = null;
        if( properties.containsKey( property) ){
            value = properties.getProperty( property );
        }
        
        if( value == null ){
            throw new RuntimeException( "Please specify the following property in the properties " + property );
        }
        
        return value;
    }
    
    
    

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws IOException {
        
        //log group: aws/batch/job job defn: arn:aws:batch:us-west-2:405596411149:job-definition/karan-batch-synch-test-job-definition:5 task arn: arn:aws:ecs:us-west-2:405596411149:task/5f659be6-4ca5-4150-bc44-dacbcb63b696
        //String taskARN = "arn:aws:ecs:us-west-2:405596411149:task/5f659be6-4ca5-4150-bc44-dacbcb63b696";
        //String jobDefinition = "arn:aws:batch:us-west-2:405596411149:job-definition/karan-batch-synch-test-job-definition:5";           
         
        /*
        Synch sc = new Synch();
        Properties props = new Properties() ;
        props.setProperty( "prefix", "karan-batch-synch-test" );
        sc.initialze( props );
        sc.retrieveCloudWatchLog( "/aws/batch/job", "karan-batch-synch-test-job-definition/default/d9dcaa2c-2a7c-4e9d-8da4-74705e53a9e4" );        
        System.exit(1);
        */
      
        Synch sc = new Synch();
        Properties props = new Properties() ;
        props.setProperty( Synch.AWS_PROPERTY_PREFIX + ".region", "us-west-2" );
        props.setProperty( Synch.AWS_PROPERTY_PREFIX + ".account", "405596411149" );
        props.setProperty( Synch.AWS_BATCH_PROPERTY_PREFIX + ".prefix", "karan-batch-synch-test-1" );
        EnumMap<Synch.BATCH_ENTITY_TYPE,String> jsonMap = new EnumMap<Synch.BATCH_ENTITY_TYPE,String>( Synch.BATCH_ENTITY_TYPE.class);
        sc.initialze( props, Level.DEBUG, jsonMap );
        
        
        sc.monitor();
        for( int i = 1; i <=1  ;i++) {
            AWSJob j = new AWSJob();
            j.setID( "job" + i );
            j.setCommand( "myjob.sh", "60");
            sc.submit( j );
        }
        sc.signalToExitAfterJobsComplete();
        
        /*
        List<Job> jobs = new LinkedList();
        for( int i = 1; i <=2  ;i++) {
            AWSJob j = new AWSJob();
            j.setID( "job" + i );
            j.setCommand( "myjob.sh", "60");
            jobs.add( j );
        }
        sc.submit(jobs);
        sc.monitor( "karan-batch-synch-test"  );
        */
       
    }
    
}

 /*
    public void monitorSynch( String basename ){
        long sleepTime = 10 * 1000;
        Set<String> awsJobIDs = new HashSet();
        for( Map.Entry<String,AWSJob> entry : this.mJobMap.entrySet() ){
            AWSJob j = entry.getValue();
            if( j.getJobState() == AWSJob.JOBSTATE.submitted ){
                awsJobIDs.add( j.getAWSJobID() );
            }
        }
        
        
        //monitor the jobs
        int numDone = 0;
        int total = awsJobIDs.size();
        Set<String> doneJobs = new HashSet();
        while( true && numDone < total ){
            try {
                
                ListJobsRequest listSucceededJobsRequest = createListJobRequest( basename, JOB_QUEUE_SUFFIX, JobStatus.SUCCEEDED );
                ListJobsRequest listFailedJobsRequest    = createListJobRequest( basename, JOB_QUEUE_SUFFIX, JobStatus.FAILED );
                
                //first query for succeeded
                System.out.println( "Querying for successful jobs ");
                ListJobsResponse listJobsResponse = mBatchClient.listJobs( listSucceededJobsRequest );
                System.out.println( "Retrieved  " + listJobsResponse.jobSummaryList().size() + " responses ");
                for( JobSummary summary: listJobsResponse.jobSummaryList() ){
                    String succeededJobID = summary.jobId();
                    if ( awsJobIDs.contains( succeededJobID) ){
                        if( !doneJobs.contains(succeededJobID) ){
                            System.out.println( "AWSJob Succeeded "  + succeededJobID + " " + new Date() );
                            doneJobs.add( summary.jobId() );
                            numDone++;
                        }
                    }
                }
                
                System.out.println( "Sleeping before querying for failure ");
                Thread.sleep( sleepTime );
                if( numDone < total ){
                    // check for failed jobs
                    listJobsResponse = mBatchClient.listJobs( listFailedJobsRequest );
                    for( JobSummary summary: listJobsResponse.jobSummaryList() ){
                        String failedJobID = summary.jobId();
                        if ( awsJobIDs.contains(failedJobID) ){
                            if( !doneJobs.contains(failedJobID) ){
                                System.out.println("AWSJob Failed "  + failedJobID + " " + new Date() );
                                doneJobs.add( summary.jobId() );
                                
                                //remove the job so that we don't query for detail
                                awsJobIDs.remove(failedJobID);
                                
                                numDone++;
                            }
                        }
                    }
                }
                
                if( numDone < total ){
                    // still total is not done
                    System.out.println( "Sleeping before querying for status of remaining jobs ");
                    Thread.sleep( sleepTime );
                    //now we query current state for jobs
                    DescribeJobsRequest jobsRequest = DescribeJobsRequest.builder().
                                                                jobs(awsJobIDs).
                                                             build();
                    DescribeJobsResponse jobsResponse = mBatchClient.describeJobs(jobsRequest);
                    for( JobDetail jobDetail: jobsResponse.jobs() ){
                        System.out.println( "Current Status of AWSJob " + jobDetail.jobId() + "->" + jobDetail.status() + " with reason " + jobDetail.statusReason() );
                        System.out.println( "Detailed AWSJob detail " + jobDetail );
                    }
                }
       
            } catch (InterruptedException ex) {
                 mLogger.log( Priority.ERROR, null, ex);
            }
        }
        System.out.println( "Done monitoring");
        shutdown();
    }
    */
    