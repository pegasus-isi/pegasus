/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.isi.pegasus.aws.batch.builder;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MapperFeature;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import software.amazon.awssdk.services.batch.model.ComputeEnvironmentOrder;

import software.amazon.awssdk.services.batch.model.CreateJobQueueRequest;
import software.amazon.awssdk.services.batch.model.JQState;

/**
 * A Builder to get objects related to Job Queue
 * The JSON specification can be found in the AWS Batch documentation.
 * 
 * http://docs.aws.amazon.com/batch/latest/APIReference/API_CreateJobQueue.html
 * 
 * @author Karan Vahi
 */
public class JobQueue {
    
    
    public JobQueue(){
        
    }
    
    /**
     * Parses and reads in a create job queue request from a JSON 
     * documentation containing the request in the same format as expected by
     * AWS Batch HTTP specification.
     * 
     * @param f the json filel
     * @param ceARN compute environment ARN
     * @param name   the name to assign if doels not exist already
     * @return 
     */
    public CreateJobQueueRequest createJobQueueRequestFromHTTPSpec( File f, String ceARN, String name ){
        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(MapperFeature.ACCEPT_CASE_INSENSITIVE_PROPERTIES, true);
        InputStream input = null;
        try {
            if( f != null && f.exists() ){
                input = new FileInputStream( f );
                JsonNode root =  mapper.readTree( input );
                System.out.println(  root );
                return this.createJobQueueRequest(root, name) ;
            }
            else{
                //create a default one
                CreateJobQueueRequest.Builder builder  = CreateJobQueueRequest.builder();
                builder.jobQueueName( name );
                builder.priority(1); //lower the better
                ComputeEnvironmentOrder ceOrder = ComputeEnvironmentOrder.builder().
                                                               computeEnvironment(ceARN).
                                                               order(1).build();
                builder.computeEnvironmentOrder(ceOrder);
                builder.state(JQState.ENABLED);
                return builder.build();
            }
        } 
        catch (FileNotFoundException ex) {
            throw new RuntimeException( "Unable to find file f " + f , ex );
        }
        catch (IOException ex) {
            throw new RuntimeException( "Unable to read json from file f " + f , ex );
        }
    }
    
    public List<CreateJobQueueRequest> createJobQueueRequest( File f ){
        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(MapperFeature.ACCEPT_CASE_INSENSITIVE_PROPERTIES, true);
        InputStream input = null;
        List<CreateJobQueueRequest> ceRequests = new LinkedList();
        try {
            input = new FileInputStream( f );
            JsonNode root = mapper.readTree( input );
            JsonNode node = root;
            if( node.has( "CreateJobQueue") ){
                node = node.get( "CreateJobQueue" );
                //System.out.println( node );
                if( node.isArray() ){
                    for( JsonNode ceNode : node ){
                        if( ceNode.has( "input") ){
                            ceRequests.add( this.createJobQueueRequest ( ceNode.get( "input" ), null ));
                         }
                    }
                }
                else{
                    throw new RuntimeException( "CreateJobQueue value should be of type array ");
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
     *  @param node the json file
     * @param name   the name to assign if does not exist already
     */
    private CreateJobQueueRequest createJobQueueRequest( final JsonNode node,String name) {
        
        CreateJobQueueRequest.Builder builder  = CreateJobQueueRequest.builder();
        /*
        {
            "computeEnvironmentOrder": [ 
               { 
                  "computeEnvironment": "string",
                  "order": number
               }
            ],
            "jobQueueName": "string",
            "priority": number,
            "state": "string"
         }
        */
        if( node.has( "computeEnvironmentOrder") ){
            builder.computeEnvironmentOrder( this.createComputeEnvironmentOrder( node.get( "computeEnvironmentOrder" ) ));
        }
        
        if( node.has( "jobQueueName") ){
            builder.jobQueueName( node.get( "jobQueueName").asText() );
        }
        else{
            builder.jobQueueName( name );
        }
        if( node.has( "priority") ){
            builder.priority( node.get( "priority" ).asInt() );
        }
       if( node.has( "state" )){
           builder.state( node.get( "state" ).asText() );
       }
       else{
           builder.state( JQState.ENABLED);
       }
       //System.out.println( "YEAH" + builder.build() );
       return builder.build();
    }


    private Collection<ComputeEnvironmentOrder> createComputeEnvironmentOrder( final JsonNode node ) {
        List<ComputeEnvironmentOrder> ceos = new LinkedList();
        /*
        [ 
               { 
                  "computeEnvironment": "string",
                  "order": number
               }
            ]
        */
        if( node.isArray() ){
            for(JsonNode ceo: node ){
                ComputeEnvironmentOrder.Builder builder = ComputeEnvironmentOrder.builder();
                if( ceo.has( "computeEnvironment") ){
                    builder.computeEnvironment( ceo.get( "computeEnvironment" ).asText() );
                }
                if( ceo.has( "order") ){
                    builder.order( ceo.get( "order" ).asInt() );
                }
                ceos.add( builder.build() );
            }
        }
        else{
            throw new RuntimeException( "Expecting JSON Node as array . Got " + node );
        }
        
        
        return ceos;
    }

    
    private CreateJobQueueRequest getTestJobQueueRequest(String basename, String suffix,  String computeEnvironmentArn) {
        CreateJobQueueRequest.Builder builder = CreateJobQueueRequest.builder();
        builder.jobQueueName( basename + suffix );
        builder.priority(1); //lower the better
        ComputeEnvironmentOrder ceOrder = ComputeEnvironmentOrder.builder().
                                                       computeEnvironment(computeEnvironmentArn).
                                                       order(1).build();
        builder.computeEnvironmentOrder(ceOrder);
        builder.state(JQState.ENABLED);
        return builder.build();
    }

    
        
    public static void main(String[] args ){
        JobQueue jq = new JobQueue();
        
        
        System.out.println(jq.createJobQueueRequestFromHTTPSpec( new File("/Users/vahi/NetBeansProjects/AWS-Batch/job-queue-http.json") , null , null));
    }
    
}
