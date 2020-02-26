/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.isi.pegasus.planner.refiner;

import edu.isi.pegasus.planner.classes.ADag;
import edu.isi.pegasus.planner.partitioner.graph.Graph;
import edu.isi.pegasus.planner.partitioner.graph.GraphNode;
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
 * An algorithm to reduce remove edges in the workflow, based on a DFS of a graph and doing least
 * common ancestor tranversals to detect duplicate edges.
 *
 * @author Rajiv Mayani
 * @author Karan Vahi
 */
public class ReduceEdges {

    public ReduceEdges() {}

    /**
     * Prunes redundant edges from the workflow. For example if A->B->C and A->D exists, we can
     * delete edge A->D
     *
     * @param dag the workflow
     * @return
     */
    public ADag reduce(ADag dag) {
        // PM-747 no need for conversion as ADag now implements Graph interface
        Graph resultGraph = this.reduce((Graph) dag);

        return (ADag) resultGraph;
    }

    /**
     * Prunes redundant edges from the workflow.
     *
     * @param workflow
     * @return the workflow with non essential edges removed
     */
    public Graph reduce(Graph workflow) {
        // start a DFS for the graph at root.

        // get all the roots of the workflow
        for (GraphNode root : workflow.getRoots()) {
            // reset( workflow );
            this.assignLevels(workflow, root);

            // mCurrentDepth = 0;
            root.setDepth(0);
            root.setColor(GraphNode.GRAY_COLOR);
            // System.out.println( "Traversing node " + root.getID() );

            // start an iterative DFS on the root
            Stack<GraphNode> stack = new Stack();
            stack.push(root);
            while (!stack.isEmpty()) {
                GraphNode top = stack.peek();
                int newDepth = top.getDepth() + 1;

                List<GraphNode> parentsForDeletion = new LinkedList();

                // deletion map maps children to list of their ancestors to delete
                // this is to prevent concurrent modification error in the loop below
                Map<GraphNode, Collection<GraphNode>> deletionMap = new HashMap();

                List<GraphNode> children = new LinkedList();
                for (GraphNode child : top.getChildren()) {
                    children.add(child);
                }

                // for( GraphNode child : top.getChildren() ){
                for (GraphNode child : children) {
                    // we always update the depth to max of current and new depth
                    child.setDepth(Math.max(child.getDepth(), newDepth));

                    if (child.isColor(GraphNode.GRAY_COLOR)) {
                        // this is where the collision is happening
                        // find LCA of the top and child
                        // Collection<GraphNode> ancestors = findLCA( top, child );

                        // we now do LCA between child and all it's parents
                        // we need to clone to prevent concurrent modification exception
                        // as LCA can delete the edge between parent and child
                        List<GraphNode> clonedParents = new LinkedList();
                        for (GraphNode parent : child.getParents()) {
                            clonedParents.add(parent);
                        }
                        // for( GraphNode parent: child.getParents() ){
                        for (GraphNode parent : clonedParents) {
                            // the lca itself might have removed other parents of
                            // the child
                            if (!child.getParents().contains(parent)) {
                                // System.out.println( "Bypassing LCA for " + parent.getID() + " ->
                                // " + child.getID());
                                continue;
                            }

                            Collection<GraphNode> ancestors = findLCA(parent, child);
                            if (!ancestors.isEmpty()) {
                                if (deletionMap.containsKey(child)) {
                                    Collection<GraphNode> existing = deletionMap.get(child);

                                    System.out.println(
                                            "addeing to existing ancestors " + existing.size());
                                    existing.addAll(ancestors);
                                } else {
                                    deletionMap.put(child, ancestors);
                                }
                            }
                        }

                        // we set the child color to Black to ensure that if
                        // we visit child again, we don't attempt LCA procedure again
                        // one call to LCA is sufficient to remove all the redundant
                        // edges making up a cycle
                        child.setColor(GraphNode.BLACK_COLOR);

                        continue;
                    }

                    if (child.isColor(GraphNode.WHITE_COLOR)) {
                        child.setColor(GraphNode.GRAY_COLOR);
                        // System.out.println( "Traversing node " + child.getID() + " with depth " +
                        // child.getDepth());

                        stack.push(child);
                    }
                }

                // delete from the deletion map the edges
                for (Map.Entry<GraphNode, Collection<GraphNode>> entry : deletionMap.entrySet()) {
                    GraphNode child = entry.getKey();
                    for (GraphNode ancestor : entry.getValue()) {
                        // System.out.println( "\tDeleting Edge " + ancestor.getID() + " -> " +
                        // child.getID() );

                        ancestor.removeChild(child);
                        // remove from the child hte parent
                        child.removeParent(ancestor);
                    }
                }

                // set the color of the node to be black
                // top.setColor( GraphNode.BLACK_COLOR );
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
     * @return the ancestors for which the edge from ancestor to the "to" node that have to be
     *     deleted.
     */
    private Collection<GraphNode> findLCA(GraphNode from, GraphNode to) {
        Set<GraphNode> ancestors = new HashSet();

        Queue<GraphNode> parents = new LinkedList();
        parents.addAll(to.getParents());
        // the from node should never be considered initially
        // parents.remove( from );
        parents.addAll(from.getParents());

        // find min depth of all the parents of the to node
        int minDepth = Integer.MAX_VALUE;
        for (GraphNode parent : to.getParents()) {
            minDepth = Math.min(minDepth, parent.getDepth());
        }
        if (minDepth == Integer.MAX_VALUE) {
            throw new RuntimeException(
                    "Inconsistent state for LCA " + from.getID() + " -> " + to.getID());
        }

        // System.out.println( "Find LCA for " + from.getID() + " -> " + to.getID() );
        /*if( to.getID().equals( "stage_out_remote_blueridge_0_0")){
            System.out.println( "DEBUG" );
        }*/
        /*for( GraphNode parent: to.getParents() ){
            System.out.println( parent.getID() + " -> " + parent.getDepth() );
        }*/

        Set<GraphNode> deletedAncestors = new HashSet();
        Set<GraphNode> uniqueParentsInQueue = new HashSet();
        uniqueParentsInQueue.addAll(parents);

        while (!parents.isEmpty()) {
            // System.out.println( "Parents Size is " + parents.size() + "," +
            // uniqueParentsInQueue.size() );
            GraphNode parent = parents.remove();
            uniqueParentsInQueue.remove(parent); // rajiv

            // System.out.println( parent.getID() );
            if (parent.getDepth() < 0 || parent.getDepth() < minDepth) {
                // if the depth is -1 we don't do backtracking
                // as that is associated with a different root
                // also, we only want the lca search to go as far back as
                // the min depth of the parents of the to node.

                continue;
            }

            if (!ancestors.add(parent)) {
                // means the parent was already present
                // check if from this parent this is a direct edge to the "to" node
                if (parent.getChildren().contains(to)) {
                    // we need to delete edge parent to the to
                    // System.out.println( "Deleting Edge in LCA " + parent.getID() + " -> " +
                    // to.getID() );
                    // deletedAncestors.add( parent );

                    parent.removeChild(to);
                    // remove from the child hte parent
                    to.removeParent(parent);
                }
            } else {
                // traversedAncestors.add( parent );
                for (GraphNode ancestor : parent.getParents()) {
                    if (!uniqueParentsInQueue.contains(ancestor)) {
                        // if( !ancestors.contains( ancestor ) ){
                        parents.add(ancestor);
                        uniqueParentsInQueue.add(ancestor);
                    }
                }
            }
        }
        return deletedAncestors;
    }

    /**
     * Prunes redundant edges from the workflow.
     *
     * @param workflow
     * @param root the root from which to start to assign the levels
     * @return the workflow with non essential edges removed
     */
    public void assignLevels(Graph workflow, GraphNode root) {
        // start a DFS for the graph at root.

        reset(workflow);
        // mCurrentDepth = 0;
        root.setDepth(0);
        root.setColor(GraphNode.GRAY_COLOR);
        // System.out.println( "Traversing node " + root.getID() );

        // start an iterative DFS on the root
        Stack<GraphNode> stack = new Stack();
        stack.push(root);
        while (!stack.isEmpty()) {
            GraphNode top = stack.peek();
            int newDepth = top.getDepth() + 1;

            for (GraphNode child : top.getChildren()) {
                // we always update the depth to max of current and new depth
                child.setDepth(Math.max(child.getDepth(), newDepth));

                if (child.isColor(GraphNode.WHITE_COLOR)) {
                    child.setColor(GraphNode.GRAY_COLOR);
                    // System.out.println( "Traversing node " + child.getID() + " with depth " +
                    // child.getDepth());

                    stack.push(child);
                }
            }

            // set the color of the node to be black
            top.setColor(GraphNode.BLACK_COLOR);
            stack.pop();
        }

        // reset colors again to white
        // sanity intialization of all nodes depth
        for (Iterator it = workflow.nodeIterator(); it.hasNext(); ) {
            GraphNode node = (GraphNode) it.next();
            node.setColor(GraphNode.WHITE_COLOR);
        }
    }

    /**
     * Resets internal depth and color counters associated with the nodes in the workflow, before
     * doing any graph traversals.
     *
     * @param workflow the workflow
     */
    private void reset(Graph workflow) {
        int depth = -1;

        // sanity intialization of all nodes depth
        for (Iterator it = workflow.nodeIterator(); it.hasNext(); ) {
            GraphNode node = (GraphNode) it.next();
            node.setDepth(depth);
            node.setColor(GraphNode.WHITE_COLOR);
        }
    }
}
