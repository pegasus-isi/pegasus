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
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * This does a modified breadth first search of the graph to identify the levels. A node is put in a
 * level only if all the parents of that node are already assigned a level.
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public class BFS extends Partitioner {

    /** A short description about the partitioner. */
    public static final String DESCRIPTION = "Level Based Partitioning";

    /**
     * The first in first out queue, that manages the set of gray vertices in a breadth first
     * search.
     */
    private LinkedList mQueue;

    /** The current depth of the nodes that are being traversed in the BFS. */
    private int mCurrentDepth;

    /**
     * The overloaded constructor.
     *
     * @param root the dummy root node of the graph.
     * @param graph the map containing all the nodes of the graph keyed by the logical id of the
     *     nodes.
     * @param properties the properties passed to the planner.
     */
    public BFS(GraphNode root, Map graph, PegasusProperties properties) {
        super(root, graph, properties);
        mQueue = new LinkedList();
        mCurrentDepth = -1;
    }

    /**
     * Does a constrained breadth first search to identify the partitions, and calls out to write
     * out the partition graph.
     *
     * @param c the callback for the partitioner.
     */
    public void determinePartitions(Callback c) {
        mCurrentDepth = 0;
        GraphNode node;
        GraphNode child;
        int depth = 0;
        List levelList = new java.util.LinkedList();
        int i = 0;
        // they contain those nodes whose parents have not been traversed as yet
        // but the BFS did it.
        List orphans = new java.util.LinkedList();

        // set the depth of the dummy root as 0
        mRoot.setDepth(mCurrentDepth);

        mQueue.addLast(mRoot);

        while (!mQueue.isEmpty()) {
            node = (GraphNode) mQueue.getFirst();
            depth = node.getDepth();
            if (mCurrentDepth < depth) {

                if (mCurrentDepth > 0) {
                    // we are done with one level!
                    constructPartitions(c, levelList, mCurrentDepth);
                }

                // a new level starts
                mCurrentDepth++;
                levelList.clear();
            }
            mLogger.log(
                    "Adding to level " + mCurrentDepth + " " + node.getID(),
                    LogManager.DEBUG_MESSAGE_LEVEL);
            levelList.add(node);

            // look at the orphans first to see if any
            // of the dependency has changed or not.
            /*it = orphans.iterator();
            while(it.hasNext()){
                child = (GraphNode)it.next();
                if(child.parentsBlack()){
                    child.setDepth(depth + 1);
                    System.out.println("Set depth of " + child.getID() + " to " + child.getDepth());

                    child.traversed();
                    mQueue.addLast(child);
                }

                //remove the child from the orphan
                it.remove();
            }*/

            node.setColor(GraphNode.BLACK_COLOR);
            for (Iterator it = node.getChildren().iterator(); it.hasNext(); ) {
                child = (GraphNode) it.next();
                if (!child.isColor(GraphNode.GRAY_COLOR)
                        && child.parentsColored(GraphNode.BLACK_COLOR)) {
                    mLogger.log("Adding to queue " + child.getID(), LogManager.DEBUG_MESSAGE_LEVEL);
                    child.setDepth(depth + 1);
                    child.setColor(GraphNode.GRAY_COLOR);
                    mQueue.addLast(child);
                }
                /*else if(!child.isTraversed() && !child.parentsBlack()){
                    //we have to do the bumping effect
                    System.out.println("Bumping child " + child);
                    orphans.add(child);
                }*/
            }
            node = (GraphNode) mQueue.removeFirst();
            mLogger.log("Removed " + node.getID(), LogManager.DEBUG_MESSAGE_LEVEL);
        }

        // handle the last level of the BFS
        constructPartitions(c, levelList, mCurrentDepth);

        // all the partitions are dependant sequentially
        for (i = mCurrentDepth; i > 1; i--) {
            constructLevelRelations(c, i - 1, i);
        }

        done(c);
    }

    /**
     * Returns a textual description of the transfer implementation.
     *
     * @return a short textual description
     */
    public String description() {
        return this.DESCRIPTION;
    }

    /**
     * Given a list of jobs, constructs (one or more) partitions out of it. Calls out to the
     * partitioner callback, for each of the partitions constructed.
     *
     * @param c the parititoner callback
     * @param nodes the list of <code>GraphNode</code> objects on a particular level.
     * @param level the level as determined from the root of the workflow.
     */
    protected void constructPartitions(Callback c, List nodes, int level) {
        // we want to ignore the dummy node partition
        String id = getPartitionID(mCurrentDepth);
        Partition p = new Partition(nodes, id);
        p.setIndex(mCurrentDepth);

        p.constructPartition();
        mLogger.log(
                "Partition " + p.getID() + " is :" + p.getNodeIDs(),
                LogManager.DEBUG_MESSAGE_LEVEL);
        c.cbPartition(p);
    }

    /**
     * Calls out to the callback with appropriate relations between the partitions constructed for
     * the levels.
     *
     * @param c the parititoner callback
     * @param parent the parent level
     * @param child the child level.
     */
    protected void constructLevelRelations(Callback c, int parent, int child) {
        String childID = getPartitionID(child);
        String parentID = getPartitionID(parent);
        List parents = new ArrayList(1);
        parents.add(parentID);
        c.cbParents(childID, parents);
    }

    /**
     * Indicates that we are done with the partitioning. Calls out to the appropriate callback
     * function
     */
    protected void done(Callback c) {
        // done with the partitioning
        c.cbDone();
    }

    /**
     * Constructs the id for the partition.
     *
     * @param level the depth from the root of the graph.
     * @return the ID for the Partition.
     */
    private String getPartitionID(int level) {
        StringBuffer sb = new StringBuffer(5);
        sb.append("ID").append(level);
        return sb.toString();
    }
}
