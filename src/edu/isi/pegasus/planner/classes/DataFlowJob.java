/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.isi.pegasus.planner.classes;

import edu.isi.pegasus.planner.namespace.Namespace;
import edu.isi.pegasus.planner.namespace.Pegasus;
import edu.isi.pegasus.planner.namespace.Selector;
import edu.isi.pegasus.planner.partitioner.graph.GraphNode;
import edu.isi.pegasus.planner.partitioner.graph.MapGraph;
import java.util.LinkedList;
import java.util.List;

/**
 * A stub data flow job for DECAF integration
 * 
 * @author Karan Vahi
 */
public class DataFlowJob extends AggregatedJob{
    
    private List<Link> mEdges;
    
    /**
     * The default constructor.
     */
    public DataFlowJob() {
        super();
        mEdges = new LinkedList();
        //data flow job cannot be executed via kickstart
        this.vdsNS.construct( Pegasus.GRIDSTART_KEY, "none" );
    }

    /**
     * The overloaded constructor.
     *
     * @param job the job whose shallow copy is created, and is the main job.
     */
    public DataFlowJob( Job job ) {
        this( job , -1 );
    }
    
    /**
     * The overloaded constructor.
     *
     * @param job the job whose shallow copy is created, and is the main job.
     * @param num the number of constituent jobs.
     */
    public DataFlowJob(Job job,int num) {
        super(job, num );
        mEdges = new LinkedList();
        //data flow job cannot be executed via kickstart
        this.vdsNS.construct( Pegasus.GRIDSTART_KEY, "none" );
    }
    
    /**
     * Add Link
     * 
     * @param e 
     */
    public void addEdge( Link e ){
        mEdges.add( e );
    }

    /**
     * A link job to indicate a job in the data flow that does data transformation
     * between two jobs
     */
    public static class Link  extends Job{

        private GraphNode mParentJob;
        private GraphNode mChildJob;
        

        public Link(){
            super();
        }
        
        public void setLink( String parent, String child ){
            mParentJob = new GraphNode( parent );
            mChildJob  = new GraphNode( child );
        }
        
        public String getParentID(){
           return mParentJob.getID();
        }
        
        
        public String getChildID(){
           return mChildJob.getID();
        }

        @Override
        public String toString() {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }

    }
}
