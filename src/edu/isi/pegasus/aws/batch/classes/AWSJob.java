/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.isi.pegasus.aws.batch.classes;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import software.amazon.awssdk.services.batch.model.ContainerOverrides;
import software.amazon.awssdk.services.batch.model.KeyValuePair;
import software.amazon.awssdk.services.batch.model.SubmitJobRequest;


public  class AWSJob {

    public enum JOBSTATE{ unsubmitted, submitted, pending, runnable, starting, running, terminated, succeeded, failed};

    private String mID;

    private String mAWSBatchID;

    private JOBSTATE mState;

    private String mExecutable;

    private String mArguments;

    private String mJobQueueARN;

    private String mJobDefinitionARN;
    
    private final Map<String,String> mEnvironmentVariables;

    public AWSJob() {
        mState = AWSJob.JOBSTATE.unsubmitted;
        mEnvironmentVariables = new HashMap();
    }

    public void setID( String id ){
        mID = id;
    }

    public String getID(){
        return mID;
    }

    public void setState( AWSJob.JOBSTATE state ){
        mState = state;
    }

    public void setCommand( String executable, String arguments ){
        mExecutable = executable;
        mArguments = arguments;
    }

    public String getExecutable( ){
        return this.mExecutable;
    }
    
    public String getArguments( ){
        return this.mArguments;
    }
    
    public void setAWSJobID( String id ){
        mAWSBatchID = id;
    }

    public String getAWSJobID(  ){
        return mAWSBatchID;
    }

    public void setJobQueueARN( String arn ){
        mJobQueueARN = arn;
    }

    public String getJobQueueARN(  ){
        return mJobQueueARN;
    }

    public void setJobDefinitionARN( String arn ){
        mJobDefinitionARN = arn;
    }

    public String getJobDefinitionARN(  ){
        return mJobDefinitionARN;
    }

    public AWSJob.JOBSTATE getJobState() {
        return this.mState;
    }
    
    public void addEnvironmentVariable( String key, String value ){
        this.mEnvironmentVariables.put(  key,value );
    }

    public Iterator<Map.Entry<String,String>> getEnvironmentVariablesIterator(){
        return this.mEnvironmentVariables.entrySet().iterator();
    }
    
    public String getEnvironmentVariable( String key ){
        return this.mEnvironmentVariables.get(key);
    }

    /**
     * Creates a submit job request for submission to AWS Batch
     * 
     * @return 
     */
    public SubmitJobRequest createAWSBatchSubmitRequest(){
        SubmitJobRequest.Builder builder = SubmitJobRequest.builder();
        builder.jobDefinition( this.getJobDefinitionARN() );
        builder.jobName(this.getID() );
        builder.jobQueue( this.getJobQueueARN());


        //container overrides for command options
        ContainerOverrides.Builder coBuilder = ContainerOverrides.builder();
        coBuilder.command( this.getExecutable(), this.getArguments() );

        Collection<KeyValuePair> envs = new LinkedList();
        for( Iterator <Map.Entry<String,String>> it = this.getEnvironmentVariablesIterator(); it.hasNext(); ){
            Map.Entry<String,String> tuple = it.next();
            envs.add(KeyValuePair.builder().name( tuple.getKey()).value( tuple.getValue()).build() );
        }
        coBuilder.environment(envs);
        builder.containerOverrides( coBuilder.build());
        return builder.build();
    }
}
