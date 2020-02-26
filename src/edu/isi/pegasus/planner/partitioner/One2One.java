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

import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.planner.common.PegasusProperties;
import edu.isi.pegasus.planner.partitioner.graph.GraphNode;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * This partitioning technique considers each of the job in the dax as a separate partition. This is
 * used for Euryale style mode of operation in Pegasus.
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public class One2One extends Partitioner {

    /** A short description about the partitioner. */
    public static final String DESCRIPTION = "One to One Partitioning";

    /**
     * The overloaded constructor.
     *
     * @param root the dummy root node of the graph.
     * @param graph the map containing all the nodes of the graph keyed by the logical id of the
     *     nodes.
     * @param properties the properties passed to the planner.
     */
    public One2One(GraphNode root, Map graph, PegasusProperties properties) {
        super(root, graph, properties);
    }

    /**
     * This ends up writing out a partition for each job in the dax. It is a one 2 one mapping from
     * the jobs in the dax to the corresponding partitions in the pdax. The ids of the partitions in
     * pdax is same as the ids of the corresponding jobs in the dax.
     *
     * @param c the callback object to callout to while partitioning.
     */
    public void determinePartitions(Callback c) {
        // we just traverse the graph via an iterator, as we do not
        // need to any particular graph traversal for this mode.

        String key = null;
        GraphNode node = null;
        int currentIndex = 0;

        for (Iterator it = mGraph.keySet().iterator(); it.hasNext(); ) {
            // the key is the logical id of the node specified in the dax
            key = (String) it.next();
            node = (GraphNode) mGraph.get(key);
            // we have to ignore the dummy root node.
            if (node.getID().equals(mRoot.getID())) {
                // we go to next node
                mLogger.log("Ignoring node " + node.getID(), LogManager.DEBUG_MESSAGE_LEVEL);
                continue;
            }
            currentIndex++;

            // construct the partition for this node
            // the partition has just one node with id same as the id
            // of the corresponding id of the job in the dax
            List levelList = new ArrayList(1);
            levelList.add(node);
            Partition p = new Partition(levelList, node.getID());
            p.setIndex(currentIndex);
            //            p.setName(mDAXWriter.getPartitionName());
            p.constructPartition();

            mLogger.log("Partition is " + p.getNodeIDs(), LogManager.DEBUG_MESSAGE_LEVEL);

            c.cbPartition(p);
        }

        // writing out the relations between the partitions in the file
        mLogger.log("Building Relations between partitions ", LogManager.DEBUG_MESSAGE_LEVEL);
        for (Iterator it = mGraph.keySet().iterator(); it.hasNext(); ) {
            // the key is the logical id of the node specified in the dax
            key = (String) it.next();
            node = (GraphNode) mGraph.get(key);
            Collection<GraphNode> parents = node.getParents();

            // we have to ignore the dummy root node.
            // and the node with no parents
            if (node.getID().equals(mRoot.getID()) || parents == null) {
                // we go to next node
                mLogger.log("Ignoring node " + node.getID(), LogManager.DEBUG_MESSAGE_LEVEL);
                continue;
            }

            // get the parents of the node and write out to the pdax file.
            List partitionIDs = new java.util.ArrayList(parents.size());
            for (Iterator it1 = parents.iterator(); it1.hasNext(); ) {
                // the jobs in the dax have same id as corresponding paritions
                partitionIDs.add(((GraphNode) it1.next()).getID());
            }
            // write out to the pdax file
            c.cbParents(key, partitionIDs);
            partitionIDs = null;
        }
        mLogger.log("Building Relations between partitions - DONE", LogManager.DEBUG_MESSAGE_LEVEL);

        // we are done with the partitioning
        c.cbDone();
    }

    /**
     * Returns a textual description of the transfer implementation.
     *
     * @return a short textual description
     */
    public String description() {
        return this.DESCRIPTION;
    }
}
