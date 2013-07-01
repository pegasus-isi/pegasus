/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.isi.pegasus.planner.refiner;

import edu.isi.pegasus.planner.classes.ADag;
import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.partitioner.graph.Adapter;
import edu.isi.pegasus.planner.partitioner.graph.Graph;
import edu.isi.pegasus.planner.partitioner.graph.GraphNode;
import edu.isi.pegasus.planner.refiner.cleanup.CleanupFactory;
import edu.isi.pegasus.planner.refiner.cleanup.CleanupStrategy;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.Stack;

/**
 * An algorithm to prune remove edges in the workflow, based on a DFS of a graph
 * and doing least common ancestor tranversals to detect duplicate edges.
 * 
 * @author Rajiv Mayani
 * @author Karan Vahi
 */
public class ReduceEdges {
    private int mCurrentDepth;
    
    public ReduceEdges(){
        
    }
    
    /**
     * Prunes redundant edges from the workflow. 
     * For example if 
     *  A->B->C and A->D exists, we can delete edge A->D
     * 
     * @param dag  the workflow
     * 
     * @return 
     */
    public ADag prune( ADag dag ){
        ADag result;

       
        //we first need to convert internally into graph format
        Graph resultGraph =  this.prune( Adapter.convert(dag ) );

        //convert back to ADag and return
        result = dag;
        //we need to reset the jobs and the relations in it
        result.clearJobs();

        //traverse through the graph and jobs and edges
        for( Iterator it = resultGraph.nodeIterator(); it.hasNext(); ){
            GraphNode node = ( GraphNode )it.next();

            //get the job associated with node
            result.add( ( Job )node.getContent() );

            //all the children of the node are the edges of the DAG
            for( Iterator childrenIt = node.getChildren().iterator(); childrenIt.hasNext(); ){
                GraphNode child = ( GraphNode ) childrenIt.next();
                result.addNewRelation( node.getID(), child.getID() );
            }
        }

        return result;
    }

    /**
     * Prunes redundant edges from the workflow.
     * 
     * @param workflow
     * 
     * @return the workflow with non essential edges removed
     */
    public Graph prune( Graph workflow ) {
        //start a DFS for the graph at root. 
        mCurrentDepth = -1;

        //sanity intialization of all nodes depth
        for( Iterator it = workflow.nodeIterator(); it.hasNext(); ){
            GraphNode node = ( GraphNode )it.next();
            node.setDepth( mCurrentDepth );
            node.setColor( GraphNode.WHITE_COLOR );
        }
        
        //get all the roots of the workflow
        
        for( GraphNode root: workflow.getRoots() ){
            //mCurrentDepth = 0;
            root.setDepth( 0 );
            root.setColor( GraphNode.GRAY_COLOR );
            //System.out.println( "Traversing node " + root.getID() );
            
            //start an iterative DFS on the root
            Stack<GraphNode> stack = new Stack();
            stack.push(root);
            while( !stack.isEmpty() ){
                GraphNode top = stack.peek();
                int newDepth = top.getDepth() + 1;
                
                List<GraphNode> parentsForDeletion = new LinkedList();
                
                //deletion map maps children to list of their ancestors to delete
                //this is to prevent concurrent modification error in the loop below
                Map<GraphNode,Collection<GraphNode>> deletionMap = new HashMap();
                
                for( GraphNode child : top.getChildren() ){
                    //we always update the depth to max of current and new depth
                    child.setDepth( Math.max( child.getDepth(), newDepth) );
                    
                    if( child.isColor( GraphNode.GRAY_COLOR )){
                        //this is where the collision is happening
                        //find LCA of the top and child
                        Collection<GraphNode> ancestors = findLCA( top, child );
                        if( !ancestors.isEmpty() ){
                            deletionMap.put( child, ancestors );
                        }
                        
                        continue;
                    }
                    
                    if( child.isColor( GraphNode.WHITE_COLOR )){
                        child.setColor( GraphNode.GRAY_COLOR );
                        //System.out.println( "Traversing node " + child.getID() + " with depth " + child.getDepth());
     
                        stack.push( child );
                    }
                }
                
                //delete from the deletion map the edges
                for( Map.Entry<GraphNode,Collection<GraphNode>> entry : deletionMap.entrySet()){
                    GraphNode child = entry.getKey();
                    for( GraphNode ancestor: entry.getValue() ){
                        //System.out.println( "Deleting Edge " + ancestor.getID() + " -> " + child.getID() );
                        
                        ancestor.removeChild( child);
                        //remove from the child hte parent
                        child.removeParent(ancestor);
                    }
                }
                
                //set the color of the node to be black
                //top.setColor( GraphNode.BLACK_COLOR );
                stack.pop();
            }
            
            
        }
        
        return workflow;
    }

    /**
     * We find LCA of from and to.
     * 
     * @param from
     * @param to 
     * 
     * @return  the ancestors  for which the edge from ancestor to the "to" node that have to be deleted.
     */
    private Collection<GraphNode> findLCA(GraphNode from, GraphNode to) {
        Set<GraphNode> ancestors = new HashSet();
        
        Queue<GraphNode> parents = new LinkedList();
        parents.addAll( to.getParents() );
        parents.addAll( from.getParents() );
        
        if( from.getDepth() - to.getDepth() == 1 ){
            //just remove the from node from the parents if present.
            //important to work correctly
            parents.remove( from );
        }
        
        System.out.println( "Find LCA for " + from.getID() + " -> " + to.getID() );
        Set<GraphNode> deletedAncestors = new HashSet();
        
        while( !parents.isEmpty() ){
            GraphNode parent = parents.remove();
            //System.out.println( parent.getID() );
            if( !ancestors.add( parent )){
                //means the parent was already present
                //check if from this parent this is a direct edge to the "to" node
                if( parent.getChildren().contains(to) ){
                    //we need to delete edge parent to the to  
                    //System.out.println( "Scheduling deletion of Edge " + parent.getID() + " -> " + to.getID() );
                    deletedAncestors.add( parent );
                    
                    //parent.removeChild(to);
                    //remove from the child hte parent
                    //to.removeParent(parent);
                }
            } 
            parents.addAll( parent.getParents());
        }
        return deletedAncestors;
        
    }
    
}
