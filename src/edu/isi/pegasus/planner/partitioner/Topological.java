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
package edu.isi.pegasus.planner.partitioner;

import edu.isi.pegasus.planner.partitioner.graph.GraphNode;
import java.util.ArrayList;
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
public class Topological // implements Iterator
 {

    /** The partition that has to be sorted. */
    private Partition mPartition;

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

    /**
     * The overloaded constructor.
     *
     * @param p the partition that has to be sorted.
     */
    public Topological(Partition p) {
        mPartition = p;
        initialize();
    }

    /** Initializes the inDegree for each node of the partition. */
    public void initialize() {
        // build up a inDegree map for each node.
        int order = mPartition.size();
        mInDegree = new int[order];
        mIndexMap = new HashMap(order);

        int index = 0;
        // each of the root nodes have in degree of 0
        for (Iterator it = mPartition.getRootNodes().iterator(); it.hasNext(); ) {
            GraphNode root = (GraphNode) it.next();
            mIndexMap.put(root.getID(), new Integer(index));
            mInDegree[index++] = 0;
        }

        // determine inDegree for other nodes
        for (Iterator it = mPartition.getRelations().entrySet().iterator(); it.hasNext(); ) {
            Map.Entry entry = (Map.Entry) it.next();

            mIndexMap.put(entry.getKey(), new Integer(index));
            mInDegree[index++] = ((List) entry.getValue()).size();
        }

        // sanity check
        if (index != order) {
            throw new RuntimeException(
                    "Index does not match order of partition " + mPartition.getID());
        }
    }

    /**
     * Topologically sorts the partition and returns a List of <code>GraphNode</code> elements. The
     * iterator of the list, returns the elements in the topological order.
     *
     * @return List of <code>GraphNode</code> objects
     */
    public List sort() {
        List l = new LinkedList();
        int order = mPartition.size();

        // get all the adjaceny list representation
        Map relations = this.childrenRepresentation();

        List queue = new LinkedList();

        // add all the root nodes to queue first
        for (Iterator it = this.mPartition.getRootNodes().iterator(); it.hasNext(); ) {
            queue.add(((GraphNode) it.next()).getID());
        }

        int index;
        while (!queue.isEmpty()) {
            String nodeID = (String) queue.remove(0);
            l.add(nodeID);

            // traverse all the children of the node
            if (relations.containsKey(nodeID)) {
                for (Iterator it = ((List) relations.get(nodeID)).iterator(); it.hasNext(); ) {
                    String childID = (String) it.next();
                    // remove the edge from node to child by decrementing inDegree
                    index = index(childID);
                    mInDegree[index] -= 1;

                    if (mInDegree[index] == 0) {
                        // add the node to the queue
                        queue.add(childID);
                    }
                }
            }
        }

        // sanity check
        if (l.size() != order) {
            throw new RuntimeException(" Partition  " + mPartition.getID() + " has a cycle");
        }

        return l;
    }

    /**
     * Returns a map that is index by GraphNode ID's and each value is the list of ID's of children
     * of that GraphNode.
     *
     * @return Map that contains adjacency list's for each node.
     */
    protected Map childrenRepresentation() {
        // adjacency list where List contains parents
        Map m = new HashMap(mPartition.size());

        for (Iterator it = mPartition.getRelations().entrySet().iterator(); it.hasNext(); ) {
            Map.Entry entry = (Map.Entry) it.next();
            Object node = entry.getKey();
            List parents = (List) entry.getValue();

            List children = null;
            for (Iterator pit = parents.iterator(); pit.hasNext(); ) {
                Object parent = pit.next();
                // the node should be in parents adjacency list
                if (m.containsKey(parent)) {
                    children = (List) m.get(parent);
                    children.add(node);
                } else {
                    children = new ArrayList(5);
                    children.add(node);
                    m.put(parent, children);
                }
            }
        }

        return m;
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
