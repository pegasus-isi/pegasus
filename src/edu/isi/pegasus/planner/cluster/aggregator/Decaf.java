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
package edu.isi.pegasus.planner.cluster.aggregator;

import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.planner.classes.ADag;
import edu.isi.pegasus.planner.classes.AggregatedJob;
import edu.isi.pegasus.planner.classes.DataFlowJob;
import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.cluster.JobAggregator;
import edu.isi.pegasus.planner.common.PegasusProperties;
import edu.isi.pegasus.planner.namespace.Namespace;
import edu.isi.pegasus.planner.namespace.Pegasus;
import edu.isi.pegasus.planner.partitioner.graph.GraphNode;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import javax.json.Json;
import javax.json.stream.JsonGenerator;

import javax.json.stream.JsonGeneratorFactory;

/**
 * Decaf data flows are represented as clustered job. DECAF implementation to
 * render the data flow in the DECAF JSON.
 *
 * @author Karan Vahi
 */
public class Decaf implements JobAggregator{
    
    
    /**
     * The key indicating the number of processors to run the job on.
     */
    private static final String NPROCS_KEY ="nprocs";
    
    /**
     * The base submit directory for the workflow.
     */
    protected String mWFSubmitDirectory;

    /**
     * The object holding all the properties pertaining to Pegasus.
     */
    protected PegasusProperties mProps;

    /**
     * The handle to the LogManager that logs all the messages.
     */
    protected LogManager mLogger;

    public void initialize(ADag dag, PegasusBag bag) {
        
        mLogger = bag.getLogger();
        mProps  = bag.getPegasusProperties();
        mWFSubmitDirectory = bag.getPlannerOptions().getSubmitDirectory() ;
    }

   
    /**
     * Enables the abstract clustered job for execution and converts it to it's 
     * executable form
     * 
     * @param job          the abstract clustered job
     */
    public void makeAbstractAggregatedJobConcrete( AggregatedJob job ){
        
        //figure out name and directoryu
        String name = job.getID() + ".json";
        
        //traverse through the nodes making up the Data flow job
        //and update resource requirements
         for( Iterator it = job.nodeIterator(); it.hasNext(); ){
            GraphNode n = (GraphNode) it.next();
            Job j = (Job) n.getContent();
            Namespace decafProfiles = j.getSelectorProfiles();
            if( decafProfiles.containsKey( Decaf.NPROCS_KEY)  ){
                j.vdsNS.construct(Pegasus.CORES_KEY, (String) decafProfiles.get( Decaf.NPROCS_KEY ));
            }
         }
            
        //PM-833 the .in file should be in the same directory where all job submit files go
        File directory = new File( this.mWFSubmitDirectory, job.getRelativeSubmitDirectory() );
        File jsonFile = new File( directory, name );
        Writer writer;
        try {
            writer = new BufferedWriter(new FileWriter( jsonFile ) );
        } catch (IOException ex) {
            throw new RuntimeException( "Unable to open file " + jsonFile + " for writing ", ex );
        }

        //generate the json file for the data flow job
        writeOutDECAFJsonWorkflow( (DataFlowJob) job, writer );
        
        
    }
   
    
    /**
     * The aggregated job is already constructed during parsing of the DAX.
     * Not implemented.
     * 
     * @param jobs the list of <code>SubInfo</code> objects that need to be
     *             collapsed. All the jobs being collapsed should be scheduled
     *             at the same pool, to maintain correct semantics.
     * @param name  the logical name of the jobs in the list passed to this
     *              function.
     * @param id   the id that is given to the new job.
     *
     * @return  the <code>SubInfo</code> object corresponding to the aggregated
     *          job containing the jobs passed as List in the input,
     *          null if the list of jobs is empty
     */
    public AggregatedJob constructAbstractAggregatedJob(List jobs,String name,String id){
        throw new UnsupportedOperationException("Not supported yet.");  
    }

    /**
     * A boolean indicating whether ordering is important while traversing 
     * through the aggregated job.
     * 
     * @return a boolean
     */
    public boolean topologicalOrderingRequired(){
        //we don't care about ordering, and decaf jobs can have cycles
        return false;
    }
    
    
    /**
     * Setter method to indicate , failure on first consitutent job should
     * result in the abort of the whole aggregated job.
     *
     * @param fail  indicates whether to abort or not .
     */
    public void setAbortOnFirstJobFailure( boolean fail){
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    /**
     * Returns a boolean indicating whether to fail the aggregated job on
     * detecting the first failure during execution of constituent jobs.
     *
     * @return boolean indicating whether to fail or not.
     */
    public boolean abortOnFristJobFailure(){
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }


    /**
     * Determines whether there is NOT an entry in the transformation catalog
     * for the job aggregator executable on a particular site.
     *
     * @param site       the site at which existence check is required.
     *
     * @return boolean  true if an entry does not exists, false otherwise.
     */
    public boolean entryNotInTC(String site){
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    /**
     * Returns the logical name of the transformation that is used to
     * collapse the jobs.
     *
     * @return the the logical name of the collapser executable.
     */
    public String getClusterExecutableLFN(){
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }


    
    /**
     * Returns the executable basename of the clustering executable used.
     * 
     * @return the executable basename.
     */
    public String getClusterExecutableBasename(){
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    /**
     * Writes out the data flow as decaf workflow represented as JSON. Sample
     * DECAF 2 node workflow represented here.
     * <pre>
            {
           "workflow": {
               "filter_level": "NONE", 
               "nodes": [
                   {
                       "nprocs": 4, 
                       "start_proc": 0, 
                       "func": "prod"
                   }, 
                   {
                       "nprocs": 2, 
                       "start_proc": 6, 
                       "func": "con"
                   }
               ], 
               "edges": [
                   {
                       "nprocs": 2, 
                       "start_proc": 4, 
                       "source": 0, 
                       "func": "dflow", 
                       "prod_dflow_redist": "count", 
                       "path": "/data/scratch/vahi/software/install/decaf/default/examples/direct/mod_linear_2nodes.so", 
                       "dflow_con_redist": "count", 
                       "target": 1
                   }
               ]
           }
       }
     * </pre>
     * 
     * @param dataFlowJob
     * @param writer 
     */
    private void writeOutDECAFJsonWorkflow(DataFlowJob job, Writer writer ) {
        Map<String, Object> properties = new HashMap<String, Object>(1);
        properties.put(JsonGenerator.PRETTY_PRINTING, true);
        JsonGeneratorFactory factory = Json.createGeneratorFactory( properties );
        JsonGenerator generator = factory.createGenerator( writer );
       
        //separate out the nodes and the link jobs from the collection.
        //linked list is important as order determine the source and target
        //in links
        LinkedList<Job> nodes = new LinkedList();
        LinkedList<DataFlowJob.Link> links = new LinkedList();
        //maps tasks logical id to index in the nodes array of json
        Map<String,Integer> logicalIDToIndex = new HashMap();
        int index = 0;
        for( Iterator it = job.nodeIterator(); it.hasNext(); ){
            GraphNode n = (GraphNode) it.next();
            Job j = (Job) n.getContent();
            if( j instanceof DataFlowJob.Link ){
                links.add((DataFlowJob.Link) j);
            }
            else{
                logicalIDToIndex.put( j.getLogicalID(), index++ );
                nodes.add( j );
            }
        }
        
        
        generator.writeStartObject();
        generator.writeStartObject( "workflow" ).
                    write( "filter_level", "NONE" );
        generator.writeStartArray( "nodes" );
        for (Job j : nodes) {
            //decaf attributes are stored as selector profiles
            Namespace decafAttrs =j.getSelectorProfiles();
            generator.writeStartObject();
            for( Iterator profileIt = decafAttrs.getProfileKeyIterator(); profileIt.hasNext(); ){
                String key = (String)profileIt.next();
                String value = (String)decafAttrs.get( key );
                //check for int values
                Integer v = -1;
                try{
                    v = Integer.parseInt( value );
                }
                catch( Exception e ){}
                
                if( v == -1 ){
                    generator.write( key, value );

                }
                else{   
                    generator.write( key, v );
                }
            }
            generator.writeEnd();
        }
        generator.writeEnd();// for nodes
        
        generator.writeStartArray( "edges" );
        for (DataFlowJob.Link j : links) {
            //decaf attributes are stored as selector profiles
            Namespace decafAttrs =j.getSelectorProfiles();
            generator.writeStartObject();
            
            //write out source and target
            generator.write( "source", logicalIDToIndex.get( j.getParentID() ) );
            generator.write( "target", logicalIDToIndex.get( j.getChildID() ) );
            
            for( Iterator profileIt = decafAttrs.getProfileKeyIterator(); profileIt.hasNext(); ){
                String key = (String)profileIt.next();
                String value = (String)decafAttrs.get( key );
                
                //check for int values
                Integer v = -1;
                try{
                    v = Integer.parseInt( value );
                }
                catch( Exception e ){}
                
                if( v == -1 ){
                    generator.write( key, value );

                }
                else{   
                    generator.write( key, v );
                }
            }
            generator.writeEnd();
        }
        generator.writeEnd();// for nodes
        
        
        
        generator.writeEnd();//for workflow
        generator.writeEnd();//for document
        
        generator.close();
    }

}
