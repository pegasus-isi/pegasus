/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.isi.pegasus.aws.batch.classes;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
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
    
    private final Set<Tuple<String,String>> mEnvironmentVariables;

    public AWSJob() {
        mState = AWSJob.JOBSTATE.unsubmitted;
        mEnvironmentVariables = new HashSet();
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
        this.mEnvironmentVariables.add( new Tuple(key,value));
    }

    public Collection<Tuple<String,String>> getEnvironmentVariables(){
        return this.mEnvironmentVariables;
    }
    
    public String getEnvironmentVariable( String key ){
        String value = null;
        for( Tuple t: this.getEnvironmentVariables()){
            if( t.getKey().equals( key ) ){
                return (String)t.getValue();
            }
        }
        return value;
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
        for( Tuple<String,String> tuple: this.getEnvironmentVariables()){
            envs.add(KeyValuePair.builder().name( tuple.getKey()).value( tuple.getValue()).build() );
        }
        coBuilder.environment(envs);
        builder.containerOverrides( coBuilder.build());
        return builder.build();
    }
}
