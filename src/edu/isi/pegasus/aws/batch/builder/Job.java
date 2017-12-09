/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.isi.pegasus.aws.batch.builder;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MapperFeature;
import edu.isi.pegasus.aws.batch.classes.AWSJob;
import static edu.isi.pegasus.aws.batch.impl.Synch.JOB_DEFINITION_SUFFIX;
import static edu.isi.pegasus.aws.batch.impl.Synch.JOB_QUEUE_SUFFIX;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedList;
import java.util.List;
import software.amazon.awssdk.services.batch.model.ContainerOverrides;
import software.amazon.awssdk.services.batch.model.KeyValuePair;
import software.amazon.awssdk.services.batch.model.SubmitJobRequest;

/**
 * A Builder to get objects related to Submitting a job
 * The JSON specification  
 * 
 * Sytax of JSON document parsed is below
 * <pre>
 * {
    "SubmitJob": [
      {
         "jobDefinition": "XXXX",
         "jobName": "example-job-1",
         "jobQueue": "XXXX",
         "executable": "myjob.sh" ,
         "arguments": "60", 
         "environment": [ 
            { 
               "name": "string",
               "value": "string"
            }
         ]
      }
   ]
  }
 * </pre>
 * 
 * @author Karan Vahi
 */
public class Job {
    
    
    public Job(){
        
    }
    
    
    
    
    public List<AWSJob> createJob( File f ){
        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(MapperFeature.ACCEPT_CASE_INSENSITIVE_PROPERTIES, true);
        InputStream input = null;
        List<AWSJob> ceRequests = new LinkedList();
        try {
            input = new FileInputStream( f );
            JsonNode root = mapper.readTree( input );
            JsonNode node = root;
            if( node.has( "SubmitJob") ){
                node = node.get( "SubmitJob" );
                //System.out.println( node );
                if( node.isArray() ){
                    for( JsonNode ceNode : node ){
                        ceRequests.add( this.createAWSJob ( ceNode ));    
                    }
                }
                else{
                    throw new RuntimeException( "SubmitJob value should be of type array ");
                }
            }            
            
        } catch (FileNotFoundException ex) {
            throw new RuntimeException( "Unable to find file f " + f , ex );
        }
        catch (IOException ex) {
            throw new RuntimeException( "Unable to read json from file f " + f , ex );
        }
        return ceRequests;
    }
    
   
    /**
     * Populates the builder , with information from the node
     * 
     * @param node  the JSON node representing the node value 
     */
    private AWSJob createAWSJob( final JsonNode node ) {
        /*
          {
            "jobDefinition": "XXXX",
            "jobName": "example-job-1",
            "jobQueue": "XXXX",
            "executable": "myjob.sh" ,
            "arguments": "60", 
            "environment": [ 
               { 
                  "name": "string",
                  "value": "string"
               }
            ]
         }
        
        */
        AWSJob j = new AWSJob();
        if( node.has( "jobDefinition") ){
            j.setJobDefinitionARN( node.get( "jobDefinition" ).asText()) ;
        }
        if( node.has( "jobQueue") ){
            j.setJobQueueARN(node.get( "jobQueue" ).asText()) ;
        }
        if( node.has( "jobName") ){
            j.setID(node.get( "jobName" ).asText()) ;
        }
        String executable = null, arguments = null;
        if( node.has( "executable") ){
            executable = node.get( "executable" ).asText() ;
        }
        if( node.has( "arguments") ){
            arguments = node.get( "arguments" ).asText() ;
        }
        j.setCommand(executable, arguments);
        
        
        if( node.has( "environment") ){
            JsonNode envVariables = node.get( "environment" );
            //System.out.println( "TAGs are " + envVariables);
            if( envVariables.isArray()){
                for( JsonNode envVariable: envVariables ){
                    String key = null;
                    String value = null;
                    if( envVariable.has( "name") ){
                        key = envVariable.get( "name" ).asText();
                    }
                    if( envVariable.has( "value") ){
                        value = envVariable.get( "value" ).asText();
                    }
                    j.addEnvironmentVariable(key, value);
                }
            }
        }
        /*
        for ( Tuple t: j.getEnvironmentVariablesIterator()){
            System.out.println( t );
        }
        */
        return j;
    }
    
    private SubmitJobRequest createTestSubmitJobRequest( String basename, AWSJob job ) {
        SubmitJobRequest.Builder builder = SubmitJobRequest.builder();
        builder.jobDefinition( basename + JOB_DEFINITION_SUFFIX );
        builder.jobName( job.getID() );
        builder.jobQueue( basename + JOB_QUEUE_SUFFIX );
       
        
        //container overrides for command options
        ContainerOverrides co = ContainerOverrides.builder().
                                                    command(job.getExecutable(), job.getArguments() ).
                                                    environment( KeyValuePair.builder().name( "BATCH_FILE_TYPE").value( "script").build(),
                                                                 KeyValuePair.builder().name( "BATCH_FILE_S3_URL").value( "s3://pegasus-aws-batch/myjob.sh").build()).build();
        builder.containerOverrides(co);
        return builder.build();
    }



        
    public static void main(String[] args ){
        Job jd = new Job();
        
        
        
        System.out.println( jd.createJob(new File("/Users/vahi/NetBeansProjects/AWS-Batch/test/sample-job-submit.json")  ));
    }
    
}
