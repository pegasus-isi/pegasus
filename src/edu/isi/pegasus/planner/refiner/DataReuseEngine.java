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

package edu.isi.pegasus.planner.refiner;


import edu.isi.pegasus.common.logging.LoggingKeys;
import edu.isi.pegasus.planner.classes.ADag;
import edu.isi.pegasus.planner.classes.PegasusFile;
import edu.isi.pegasus.planner.classes.SubInfo;

import edu.isi.pegasus.planner.partitioner.graph.Graph;
import edu.isi.pegasus.planner.partitioner.graph.GraphNode;
import edu.isi.pegasus.planner.partitioner.graph.Adapter;

import edu.isi.pegasus.common.logging.LogManager;

import edu.isi.pegasus.planner.provenance.pasoa.XMLProducer;
import edu.isi.pegasus.planner.provenance.pasoa.producer.XMLProducerFactory;

import edu.isi.pegasus.planner.provenance.pasoa.PPS;
import edu.isi.pegasus.planner.provenance.pasoa.pps.PPSFactory;

import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.partitioner.graph.Bag;


import java.util.Set;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * The data reuse engine reduces the workflow on the basis of existing output
 * files of the workflow found in the Replica Catalog. The algorithm works in
 * two passes.
 *
 * <p>
 * In the first pass , we determine all the jobs whose output files exist in
 * the Replica Catalog.  An output file with the transfer flag set to false is
 * treated equivalent to the file existing in the Replica Catalog , if
 * <pre>
 *  - the output file is not an input to any of the children of the job X
 *  </pre>
 *
 * In the second pass, we remove the job whose output files exist in the
 * Replica Catalog and try to cascade the deletion upwards to the parent
 * jobs. We start the breadth first traversal of the workflow bottom up.
 * A node is marked for deletion if -
 *
 * <pre>
 *  ( It is already marked for deletion in pass 1
 *      OR
 *      ( ALL of it's children have been marked for deletion
 *        AND
 *        Node's output files have transfer flags set to false
 *      )
 *  )
 * </pre>
 *
 * @author Karan Vahi
 * @version $Revision$
 *
 */

public class DataReuseEngine extends Engine implements Refiner{

    /**
     * List of all deleted jobs during workflow reduction.
     */
    private List<SubInfo> mAllDeletedJobs;


    /**
     * The XML Producer object that records the actions.
     */
    private XMLProducer mXMLStore;

    /**
     * The workflow object being worked upon.
     */
    private ADag mWorkflow;

    /**
     * The constructor
     *
     * @param orgDag    The original Dag object
     * @param bag       the bag of initialization objects.
     */
    public DataReuseEngine( ADag orgDag, PegasusBag bag ){
        super( bag) ;

        mAllDeletedJobs  = new LinkedList();
        mXMLStore        = XMLProducerFactory.loadXMLProducer( mProps );
        mWorkflow        = orgDag;
    }



    /**
     * Returns a reference to the workflow that is being refined by the refiner.
     *
     *
     * @return ADAG object.
     */
    public ADag getWorkflow(){
        return this.mWorkflow;
    }

    /**
     * Returns a reference to the XMLProducer, that generates the XML fragment
     * capturing the actions of the refiner. This is used for provenace
     * purposes.
     *
     * @return XMLProducer
     */
    public XMLProducer getXMLProducer(){
        return this.mXMLStore;
    }


    /**
     * Reduces the workflow on the basis of the existence of lfn's in the
     * replica catalog. The existence of files, is determined via the bridge.
     *
     * @param workflow   the workflow to be reduced.
     * @param rcb        instance of the replica catalog bridge.
     *
     * @return the reduced dag
     *
     */
    public ADag reduceWorkflow( ADag workflow,  ReplicaCatalogBridge rcb ){

        //clone the original workflow. it will be reduced later on
        ADag reducedWorkflow = (ADag) workflow.clone();

        //we first need to convert internally into graph format
        Graph reducedGraph =  this.reduceWorkflow( Adapter.convert( reducedWorkflow ), rcb );

        //convert back to ADag and return

        //we need to reset the jobs and the relations in it
        reducedWorkflow.clearJobs();

        //traverse through the graph and jobs and edges
        for( Iterator it = reducedGraph.nodeIterator(); it.hasNext(); ){
            GraphNode node = ( GraphNode )it.next();

            //get the job associated with node
            reducedWorkflow.add( ( SubInfo )node.getContent() );

            //all the children of the node are the edges of the DAG
            for( Iterator childrenIt = node.getChildren().iterator(); childrenIt.hasNext(); ){
                GraphNode child = ( GraphNode ) childrenIt.next();
//                System.out.println(  node.getID() + " -> "  + child.getID() );
                reducedWorkflow.addNewRelation( node.getID(), child.getID() );
            }
        }

        mWorkflow = reducedWorkflow;
        return reducedWorkflow;
    }


    /**
     * Reduces the workflow on the basis of the existence of lfn's in the
     * replica catalog. The existence of files, is determined via the bridge.
     *
     * @param workflow   the workflow to be reduced.
     * @param rcb        instance of the replica catalog bridge.
     *
     * @return the reduced dag. The input workflow object is returned reduced.
     *
     */
    public Graph reduceWorkflow( Graph workflow,  ReplicaCatalogBridge rcb ){

        //search for the replicas of the files. The search list
        //is already present in Replica Catalog Bridge
        Set<String> filesInRC = rcb.getFilesInReplica();

        //we reduce the dag only if the
        //force option is not specified.
        if(mPOptions.getForce())
            return workflow;

        //load the PPS implementation
        PPS pps = PPSFactory.loadPPS( this.mProps );

        //mXMLStore.add( "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" );
        mXMLStore.add( "<workflow url=\"" + mPOptions.getDAX() + "\">" );

        //call the begin workflow method
        try{
            pps.beginWorkflowRefinementStep(this, PPS.REFINEMENT_REDUCE , true);
        }
        catch( Exception e ){
            throw new RuntimeException( "PASOA Exception", e );
        }

        //clear the XML store
        mXMLStore.clear();


        mLogger.log("Reducing the workflow",LogManager.DEBUG_MESSAGE_LEVEL);
        mLogger.logEventStart( LoggingKeys.EVENT_PEGASUS_REDUCE, LoggingKeys.DAX_ID, mWorkflow.getAbstractWorkflowID() );
           
        //figure out jobs whose output files already exist in the Replica Catalog
        List<GraphNode> originalJobsInRC = getJobsInRC( workflow ,filesInRC );
        //mAllDeletedJobs = (Vector)mOrgJobsInRC.clone();
        //firstPass( originalJobsInRC );
        Graph reducedWorkflow = cascadeDeletionUpwards( workflow, originalJobsInRC );
        
        mLogMsg = "Nodes/Jobs Deleted from the Workflow during reduction ";
        mLogger.log( mLogMsg,LogManager.INFO_MESSAGE_LEVEL );
        for( SubInfo job : this.mAllDeletedJobs){
            mLogger.log("\t" + job.getID(), LogManager.INFO_MESSAGE_LEVEL );
            mXMLStore.add( "<removed job = \"" + job.getID() + "\"/>" );
            mXMLStore.add( "\n" );
        }
        mLogger.log( mLogMsg +  " - DONE", LogManager.INFO_MESSAGE_LEVEL );


        //call the end workflow method for pasoa interactions
        try{
          

            for( Iterator it = reducedWorkflow.nodeIterator(); it.hasNext(); ){
                GraphNode node = ( GraphNode )it.next();
                pps.isIdenticalTo( node.getName(), node.getName() );
            }

            pps.endWorkflowRefinementStep( this );
        }
        catch( Exception e ){
            throw new RuntimeException( "PASOA Exception", e );
        }


        mLogger.logEventCompletion();
        return reducedWorkflow;
    }


    /**
     * This returns all the jobs deleted from the workflow after the reduction
     * algorithm has run.
     *
     * @return  List containing the <code>SubInfo</code> of deleted leaf jobs.
     */
    public List<SubInfo> getDeletedJobs(){
       return this.mAllDeletedJobs;
    }

    /**
     * This returns all the deleted jobs that happen to be leaf nodes. This
     * entails that the output files  of these jobs be transferred
     * from the location returned by the Replica Catalog to the
     * pool specified. This is a subset of mAllDeletedJobs
     * Also to determine the deleted leaf jobs it refers the original
     * dag, not the reduced dag.
     *
     * @return  List containing the <code>SubInfo</code> of deleted leaf jobs.
     */
    public List<SubInfo> getDeletedLeafJobs(){
        mLogger.log( "Date Reuse Engine no longer tracks deleted leaf jobs. Returning empty list ",
                     LogManager.DEBUG_MESSAGE_LEVEL );
        List<SubInfo> delLeafJobs = new LinkedList();

       
        return delLeafJobs;
    }


    /**
     * Returns all the jobs whose output files exist in the Replica Catalog.
     * An output file with the transfer flag set to false is treated equivalent
     * to the file being in the Replica Catalog , if
     *
     * - the output file is not an input to any of the children of the job X
     *
     * @param workflow   the workflow object
     * @param filesInRC  Set of <code>String</code> objects corresponding to the
     *                   logical filenames of files that are found to be in the
     *                   Replica Catalog.
     *
     * @return a List of GraphNodes with their Boolean bag value set to true.
     *
     * @see org.griphyn.cPlanner.classes.SubInfo
     */
    private List<GraphNode> getJobsInRC(Graph workflow ,Set filesInRC){
        List<GraphNode> jobsInReplica = new LinkedList();
        int noOfOutputFilesInJob = 0;
        int noOfSuccessfulMatches = 0;

        if( workflow.isEmpty() ){
            String msg = "ReductionEngine: The set of jobs in the workflow " +
                         "\n is empty.";
            mLogger.log( msg, LogManager.DEBUG_MESSAGE_LEVEL );
            return jobsInReplica;
        }


        mLogger.log("Jobs whose o/p files already exist",
                    LogManager.DEBUG_MESSAGE_LEVEL);
        //iterate through all the nodes in the graph
        for( Iterator it = workflow.nodeIterator(); it.hasNext(); ){
            GraphNode node = (GraphNode)it.next();
            SubInfo job =  (SubInfo)node.getContent();
            Set<PegasusFile> outputFiles = job.getOutputFiles();

            String jobName = job.jobName;

            if( job.getOutputFiles().isEmpty() ){
                //a job with no output file should not be
                //marked as a job in the RC
                //Otherwise it can result in whole workflow being reduced
                //if such a node is the leaf of the workflow.
                mLogger.log("Job "  + job.getName() + " has no o/p files",
                            LogManager.DEBUG_MESSAGE_LEVEL);
                continue;
            }


            /* Commented on Oct10. This ended up making the
            Planner doing duplicate transfers
            if(subInfo.stdOut.length()>0)
                vJobOutputFiles.addElement(subInfo.stdOut);
            */

            noOfOutputFilesInJob = outputFiles.size();

            //traversing through the output files of that particular job
            for( PegasusFile pf : outputFiles ){

                if(filesInRC.contains(pf.getLFN()) ){
                    noOfSuccessfulMatches++;
                }
                else if ( pf.getTransientTransferFlag() ){
                    //successful match only if the output file is not an input
                    //to any of the children of the job X
                    boolean input = true;
                    for( Iterator cit = node.getChildren().iterator(); cit.hasNext(); ){
                        GraphNode child = (GraphNode) cit.next();
                        SubInfo childJob = (SubInfo)child.getContent();
                        if( childJob.getInputFiles().contains( pf ) ){
                            input = false;
                            break;
                        }
                    }
                    if( input ){
                        noOfSuccessfulMatches++;
                    }
                }
            }

            //we add a job to list of jobs whose output files already exist
            //only if noOfSuccessFulMatches is equal to the number of output
            //files in job
            if(noOfOutputFilesInJob == noOfSuccessfulMatches){
                mLogger.log("\t" + jobName, LogManager.DEBUG_MESSAGE_LEVEL);

                //COLOR the node as while
                node.setColor( GraphNode.BLACK_COLOR );
                jobsInReplica.add( node );
            }
            //reinitialise the variables
            noOfSuccessfulMatches = 0;
            noOfOutputFilesInJob = 0;
        }
        mLogger.log("Jobs whose o/p files already exist - DONE",
                     LogManager.DEBUG_MESSAGE_LEVEL);
        return jobsInReplica;

    }







    /**
     * Cascade the deletion of the jobs upwards in the workflow. We start a
     * breadth first traversal of the workflow bottom up. A node is marked for
     * deletion if -
     *
     * <pre>
     *  ( It is already marked for deletion
     *      OR
     *      ( ALL of it's children have been marked for deletion
     *        AND
     *        Node's output files have transfer flags set to false
     *      )
     *  )
     * </pre>
     * 
     * @param workflow          the worfklow to be deduced
     * @param originalJobsInRC  list of nodes found to be in the Replica Catalog.
     */
    protected Graph cascadeDeletionUpwards(Graph workflow, List<GraphNode> originalJobsInRC) {
        LinkedList<GraphNode> queue = new LinkedList();
        int currentDepth = -1;

        //sanity intialization of all nodes depth
        //also associate a boolean bag with the nodes
        //that tracks whether a node has been traversed or not
        for( Iterator it = workflow.nodeIterator(); it.hasNext(); ){
            GraphNode node = ( GraphNode )it.next();
            node.setDepth( currentDepth );
            BooleanBag bag = new BooleanBag();
            node.setBag(bag);

        }

        //intialize all the leave nodes depth to 0
        //and put them in the queue
        currentDepth = 0;
        for( Iterator<GraphNode> it = workflow.getLeaves().iterator(); it.hasNext(); ){
            GraphNode node = it.next();
            node.setDepth( currentDepth );
            queue.add( node );
        }

        //A node with COLOR set to BLACK means it is marked for deletion

        //start the bottom up traversal
        while( !queue.isEmpty() ){
            GraphNode node  = (GraphNode)queue.removeFirst();

            int depth  = node.getDepth();

            //System.out.println( "Traversing " + node.getID() );

            //traverse through all the parents and add to the queue
            for( Iterator it = node.getParents().iterator(); it.hasNext(); ){
                GraphNode parent = (GraphNode)it.next();
                //if the parent has already made it's way to the queue
                //dont add again. this is when multiple nodes have same parent
                //if( parent.isColor( GraphNode.GRAY_COLOR ) ){
                if( ((BooleanBag)parent.getBag()).getBooleanValue() ){
                    continue;
                }

                parent.setDepth( depth + 1 );
                //parent.setColor( GraphNode.GRAY_COLOR );
                ((BooleanBag)parent.getBag()).add(true);
                //System.out.println( "Adding parent " + parent.getID() );
                queue.addLast( parent );
                
            }


            if( !node.isColor( GraphNode.BLACK_COLOR ) ){
                //If a node is not already marked for deletion , it  can be marked
                //for deletion if
                //    a) all it's children have been marked for deletion AND
                //    b) node's output files have transfer flags set to false
                boolean delete = true;
                for( Iterator cit = node.getChildren().iterator(); cit.hasNext(); ){
                    GraphNode child = (GraphNode)cit.next();
                    if( !child.isColor( GraphNode.BLACK_COLOR  ) ){
                        delete = false;
                        break;
                    }
                }
                if( delete ){
                    //all the children are deleted. However delete only if
                    // all the output files have transfer flags set to false
                    if( /*node.isColor( GraphNode.BLACK_COLOR ) ||*/ !transferOutput( node ) ){
                        mLogger.log( "Node can be deleted "  + node.getID() ,
                                     LogManager.DEBUG_MESSAGE_LEVEL );
                        node.setColor( GraphNode.BLACK_COLOR );
                    }
                }
            }

            //if the node is colored BLACK at this point
            //remove the node from the workflow
            if( node.isColor( GraphNode.BLACK_COLOR ) ){
                mLogger.log( "Removing node from the workflow "  + node.getID() ,
                                 LogManager.DEBUG_MESSAGE_LEVEL );
                this.mAllDeletedJobs.add( (SubInfo)node.getContent() );
                workflow.remove( node.getID() );
            }

        }

        return workflow;
    }

    /**
     * Returns whether a user wants output transferred for a node or not.
     * If no output files  are associated , true will be returned
     *
     * @param node   the GraphNode
     *
     * @return boolean
     */
    protected boolean transferOutput(GraphNode node) {
        boolean result = false;

        SubInfo job = (SubInfo)node.getContent();

        if( job.getOutputFiles().isEmpty() ){
            //no output files means we should not delete the job automatically
            //JIRA PM-24
            return true;
        }

        for( Iterator it = job.getOutputFiles().iterator(); it.hasNext(); ){
            PegasusFile pf = (PegasusFile)it.next();
            if( !pf.getTransientTransferFlag() ){
                result = true;
                break;
            }
        }

        return result;
    }



    


    /**
     * A bag implementation that cam be used to hold a boolean value associated with the
     * graph node
     *
     */
    public class BooleanBag implements Bag {

        /**
         * The boolean value
         */
        private boolean mBoolean;


        /**
         * The default constructor.
         */
        public BooleanBag(){
            mBoolean = false;
        }

        /**
         * Returns the boolean value
         *
         * @return
         */
        public boolean getBooleanValue(){
           return mBoolean;
        }

        /**
         * For all keys returns the boolean value
         * 
         * @param key
         * @return
         */
        public Object get(Object key) {
            return  mBoolean;
        }

        /**
         * Ignores the key and only adds the value .
         * The value should be a boolean
         *
         * @param key
         * @param value
         *
         * @return
         */
        public boolean add(Object key, Object value) {
            if (!(value instanceof Boolean )){
                throw new IllegalArgumentException( "Boolean Bag only accepts boolean values" + value );
            }
            mBoolean = (Boolean)value;

            return true;
        }

        /**
         * Returns false. You cannot associate a key with this bag.
         *
         * @param key
         *
         * @return false
         */
        public boolean containsKey(Object key) {
            return false;
        }

        /**
         * Adds a boolean value to the bag
         *
         * @param b the boolean value
         */
        public void add(boolean b) {
            this.add( null, b );
        }


    }
}
