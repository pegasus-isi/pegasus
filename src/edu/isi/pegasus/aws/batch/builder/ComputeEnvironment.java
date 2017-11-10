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
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import software.amazon.awssdk.services.batch.model.CEState;
import software.amazon.awssdk.services.batch.model.CEType;
import software.amazon.awssdk.services.batch.model.CRType;
import software.amazon.awssdk.services.batch.model.ComputeResource;

import software.amazon.awssdk.services.batch.model.CreateComputeEnvironmentRequest;

/**
 * A Builder to get objects related to Compute Environments
 * The JSON specification can be found in the AWS Batch documentation.
 * 
 * http://docs.aws.amazon.com/batch/latest/APIReference/API_CreateComputeEnvironment.html 
 * 
 * @author Karan Vahi
 */
public class ComputeEnvironment {
    
    public static final String AWS_BATCH_SERVICE_ROLE = "AWSBatchServiceRole";
    
    public static final  String ECS_INSTANCE_ROLE ="ecsInstanceRole" ;
    
    
    public ComputeEnvironment(){
        
    }
    
    /**
     * Parses and reads in a create compute environment request from a JSON 
     * documentation containing the request in the same format as expected by
     * AWS Batch HTTP specification.
     * 
     * @param f
     * @param name name to assign
     * 
     * @return 
     */
    public CreateComputeEnvironmentRequest createComputeEnvironmentRequestFromHTTPSpec( File f , String name ){
        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(MapperFeature.ACCEPT_CASE_INSENSITIVE_PROPERTIES, true);
        InputStream input = null;
        try {
            input = new FileInputStream( f );
            JsonNode root = mapper.readTree( input );
            //System.out.println(  root );
            return this.createComputeEnvironmentRequest(root, name) ;
        } catch (FileNotFoundException ex) {
            throw new RuntimeException( "Unable to find file f " + f , ex );
        }
        catch (IOException ex) {
            throw new RuntimeException( "Unable to read json from file f " + f , ex );
        }
    }
    
    public List<CreateComputeEnvironmentRequest> createComputeEnvironmentRequest( File f, String name ){
        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(MapperFeature.ACCEPT_CASE_INSENSITIVE_PROPERTIES, true);
        InputStream input = null;
        List<CreateComputeEnvironmentRequest> ceRequests = new LinkedList();
        try {
            input = new FileInputStream( f );
            JsonNode root = mapper.readTree( input );
            JsonNode node = root;
            if( node.has( "CreateComputeEnvironment") ){
                node = node.get( "CreateComputeEnvironment" );
                //System.out.println( node );
                if( node.isArray() ){
                    for( JsonNode ceNode : node ){
                        if( ceNode.has( "input") ){
                            ceRequests.add( this.createComputeEnvironmentRequest ( ceNode.get( "input" ), name ));
                         }
                    }
                }
                else{
                    throw new RuntimeException( "CreateComputeEnvironment value should be of type array ");
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
     * @param node  the JSON node representing the node value 
     */
    private CreateComputeEnvironmentRequest createComputeEnvironmentRequest( final JsonNode node, String name) {
        
        CreateComputeEnvironmentRequest.Builder builder  = CreateComputeEnvironmentRequest.builder();
        if( node.has( "type") ){
            builder.type(node.get( "type" ).asText() );
        }
        if( node.has( "computeEnvironmentName") ){
            builder.computeEnvironmentName(node.get( "computeEnvironmentName").asText() );
        }
        else{
             builder.computeEnvironmentName( name );
        }
        if( node.has( "computeResources") ){
            builder.computeResources(createComputeResource(node.get( "computeResources" )) );
        }
       if( node.has( "serviceRole" )){
           builder.serviceRole(node.get( "serviceRole").asText() );
       }
       if( node.has( "state") ){
           builder.state(node.get( "state" ).asText() );
       }
       //System.out.println( "YEAH" + builder.build() );
       return builder.build();
    }


    private ComputeResource createComputeResource( final JsonNode computeResource ) {
        ComputeResource.Builder builder  = ComputeResource.builder();
        //System.out.println( computeResource );
        
        if( computeResource.has( "type" ) ){
            builder.type( computeResource.get( "type" ).asText() );
        }
        if( computeResource.has( "desiredvCpus" ) ){
            builder.desiredvCpus( computeResource.get( "desiredvCpus" ).asInt() );
        }
        if( computeResource.has( "ec2KeyPair" ) ){
            builder.ec2KeyPair( computeResource.get( "ec2KeyPair" ).asText() );
        }
        if( computeResource.has( "instanceRole" ) ){
            builder.instanceRole( computeResource.get( "instanceRole" ).asText() );
        }
        if( computeResource.has( "maxvCpus" ) ){
            builder.maxvCpus( computeResource.get( "maxvCpus" ).asInt() );
        }
        if( computeResource.has( "minvCpus" ) ){
            builder.minvCpus( computeResource.get( "minvCpus" ).asInt() );
        }
        if( computeResource.has( "bidPercentage" ) ){
            builder.bidPercentage( computeResource.get( "bidPercentage").asInt() );
        }
        if( computeResource.has( "instanceTypes" ) ){
            JsonNode instanceTypes = computeResource.get( "instanceTypes" );
            if( instanceTypes.isArray() ){
                for(JsonNode id: instanceTypes ){
                    builder.instanceTypes( id.asText() );
                }
            }
            else{
                throw new RuntimeException( "instanceTypes should be an array");
            }
        }
        if( computeResource.has( "securityGroupIds" ) ){
            JsonNode securityGroupIds = computeResource.get( "securityGroupIds" );
            if( securityGroupIds.isArray() ){
                for(JsonNode id: securityGroupIds ){
                    builder.securityGroupIds( id.asText() );
                }
            }
            else{
                throw new RuntimeException( "securityGroupIds should be an array");
            }
        }
        if( computeResource.has( "subnets" ) ){
            JsonNode subnets = computeResource.get( "subnets" );
            if( subnets.isArray() ){
                for(JsonNode id: subnets ){
                    builder.subnets( id.asText() );
                }
            }
            else{
                throw new RuntimeException( "subnets should be an array");
            }
        }
        if( computeResource.has( "tags") ){
            JsonNode tags = computeResource.get( "tags" );
            //System.out.println( "TAGs are " + tags);
            Map<String,String> m = new HashMap();
            for( Iterator<Map.Entry<String,JsonNode>> it = tags.fields();it.hasNext(); ){
                Map.Entry<String,JsonNode> entry = it.next();
                m.put( entry.getKey(), entry.getValue().asText() );
            }
            builder.tags(m);
        }
        if( computeResource.has( "spotIamFleetRole") ){
            builder.spotIamFleetRole( computeResource.get( "spotIamFleetRole").asText() );
        }
        return builder.build();
    }
    
    public CreateComputeEnvironmentRequest getTestComputeEnvironmentRequest( String accountID, String basename, String suffix ){
        CreateComputeEnvironmentRequest.Builder builder = CreateComputeEnvironmentRequest.builder();
        
        builder.computeEnvironmentName( basename + suffix );
        builder.type(CEType.MANAGED);
        builder.serviceRole("arn:aws:iam::" + accountID + ":role/" + AWS_BATCH_SERVICE_ROLE); 
        builder.state(CEState.ENABLED);
       
        ComputeResource.Builder crBuilder = ComputeResource.builder();
        crBuilder.type(CRType.EC2);
        //crBuilder.bidPercentage( 20 );
        crBuilder.instanceTypes( "optimal" );
        crBuilder.instanceRole( "arn:aws:iam::" + accountID +  ":instance-profile/" + ECS_INSTANCE_ROLE );
        crBuilder.subnets( "subnet-a9bb63cc") ;
        crBuilder.securityGroupIds( "sg-91d645f4" );
        
        //crBuilder.subnets( "vpc-cafe33af");
        
        crBuilder.minvCpus( 0 );
        crBuilder.maxvCpus( 2 );
        crBuilder.desiredvCpus( 0 );
        builder.computeResources(crBuilder.build());
        
        return builder.build();
    }

        
    public static void main(String[] args ){
        ComputeEnvironment ce = new ComputeEnvironment();
        
        List<CreateComputeEnvironmentRequest> ces = ce.createComputeEnvironmentRequest( new File("/Users/vahi/NetBeansProjects/AWS-Batch/compute-env.json"), "test" );
        System.out.println(ces);
        
        System.out.println( ce.createComputeEnvironmentRequestFromHTTPSpec( new File("/Users/vahi/NetBeansProjects/AWS-Batch/compute-env-http.json") , "test"));
    }
    
}
