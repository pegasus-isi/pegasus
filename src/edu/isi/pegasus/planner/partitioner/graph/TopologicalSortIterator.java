/**
 * Copyright 2007-2008 University Of Southern California
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.isi.pegasus.planner.partitioner.graph;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Does a topological sort on the Partition.
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public class TopologicalSortIterator implements Iterator {

    /** The partition that has to be sorted. */
    private Graph mGraph;

    /** An array that contains the number of incoming edges to a node. */
    private int[] mInDegree;

    /**
     * A Map that returns the index into mInDegree map for a particular node in graph. Maps a ID of
     * the node to an int value, which is the index to to the array containing the in degree for
     * each node.
     *
     * @see #mInDegree
     */
    private Map mIndexMap;

    /** The internal list of nodes that contains the nodes to be traversed. */
    private List<GraphNode> mQueue;

    /** The number of nodes in the graph. */
    private int mOrder;

    /**
     * The overloaded constructor.
     *
     * @param p the graph that has to be sorted.
     */
    public TopologicalSortIterator(Graph graph) {
        mGraph = graph;
        initialize();

        mOrder = mGraph.size();
        mQueue = new LinkedList();
        // add all the root nodes to queue first
        for (Iterator it = this.mGraph.getRoots().iterator(); it.hasNext(); ) {
            mQueue.add((GraphNode) it.next());
        }
    }

    /** Initializes the inDegree for each node of the partition. */
    public void initialize() {
        // build up a inDegree map for each node.
        int order = mGraph.size();
        mInDegree = new int[order];
        mIndexMap = new HashMap(order);

        int index = 0;
        // each of the root nodes have in degree of 0
        for (Iterator it = mGraph.getRoots().iterator(); it.hasNext(); ) {
            GraphNode root = (GraphNode) it.next();
            mIndexMap.put(root.getID(), new Integer(index));
            mInDegree[index++] = 0;
        }

        // determine inDegree for other nodes
        // in degree for a node is the number of incoming edges/parents of a node
        for (Iterator<GraphNode> it = mGraph.nodeIterator(); it.hasNext(); ) {
            GraphNode node = it.next();
            if (node.getParents().isEmpty()) {
                // node is a root. indegree already assigned
                continue;
            }

            mIndexMap.put(node.getID(), new Integer(index));
            mInDegree[index++] = node.getParents().size();
        }

        // sanity check
        if (index != order) {
            throw new RuntimeException("Index does not match order of partition ");
        }
    }

    /**
     * Returns whether there are more nodes to be traversed in the graph or not.
     *
     * @return boolean
     */
    public boolean hasNext() {
        return !mQueue.isEmpty();
    }

    /**
     * Returns the next node to be traversed
     *
     * @return
     */
    public Object next() {
        GraphNode node = mQueue.remove(0);
        String nodeID = node.getID();

        // traverse all the children of the node
        // GraphNode n = null;
        for (Iterator<GraphNode> it = node.getChildren().iterator(); it.hasNext(); ) {
            GraphNode child = it.next();
            String childID = child.getID();

            // remove the edge from node to child by decrementing inDegree
            int index = index(childID);
            mInDegree[index] -= 1;

            if (mInDegree[index] == 0) {
                // add the node to the queue
                mQueue.add(child);
            }
        }
        return node;
    }

    /** Removes a node from the graph. Operation not supported as yet. */
    public void remove() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    /**
     * Returns the index of a particular node. The index is used as an index into arrays.
     *
     * @param id the id of the node.
     * @return the index
     */
    private int index(String id) {
        return ((Integer) mIndexMap.get(id)).intValue();
    }
}
