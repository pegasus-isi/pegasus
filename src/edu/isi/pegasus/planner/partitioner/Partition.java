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

import edu.isi.pegasus.planner.classes.Data;
import edu.isi.pegasus.planner.partitioner.graph.GraphNode;
import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * This is an abstract container for a partition in the graph. This used for the generation of the
 * partition element in the partition graph, and identifies the relations between the jobs in the
 * partition if any.
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public class Partition extends Data {

    /** The set of node id's in the partition. */
    private Set<String> mNodeSet;

    /**
     * A map containing a node and it's parents ids in the partition. A node id's is the key and the
     * corresponding value is the list of String id's of it's parents. The map only contain those
     * nodes for which there is a parent.
     */
    private Map<String, List<String>> mParentsMap;

    /**
     * The list of <code>GraphNode<code> objects corresponding to the nodes
     * making the partiition.
     */
    private List<GraphNode> mNodeList;

    /** The partition id of the partition. */
    private String mID;

    /**
     * The index associated with the partition. In most cases the ID of the partition is constructed
     * using this index.
     */
    private int mIndex;

    /** The name of the partition. */
    private String mName;

    /** A pointer to the last added node to the partition. */
    private GraphNode mLastAddedNode;

    /**
     * A boolean indicating whether a label was associated with the jobs in the partition or not.
     */
    private boolean mHasAssociatedLabel;

    /** The default constructor. */
    public Partition() {
        mID = null;
        mName = "test";
        mIndex = -1;
        mNodeSet = new LinkedHashSet();
        mParentsMap = new HashMap();
        mNodeList = new java.util.LinkedList();
        mLastAddedNode = null;
        mHasAssociatedLabel = false;
    }

    /**
     * The overloaded constructor.
     *
     * @param nodeList list of <code>GraphNode</code> objects.
     * @param id the partition id of the partition.
     */
    public Partition(List nodeList, String id) {
        mNodeList = nodeList;
        mID = id;
        mParentsMap = new HashMap(nodeList.size());
        mNodeSet = new LinkedHashSet(nodeList.size());
        mIndex = -1;
        // default to test
        mName = "test";
        mHasAssociatedLabel = false;
        mLastAddedNode = null;
        for (Iterator it = mNodeList.iterator(); it.hasNext(); ) {
            mNodeSet.add(((GraphNode) it.next()).getID());
        }
    }

    /**
     * Adds a node to the partition. It ends up adding it to the underneath node list.
     *
     * @param node the <code>GraphNode</code> object corresponding to the job that is to be added.
     */
    public void addNode(GraphNode node) {
        mNodeList.add(node);
        // also add it to the underlying job set
        mNodeSet.add(node.getID());
        mLastAddedNode = node;
    }

    /**
     * Returns the last added node to the partition.
     *
     * @return the last added node, or null in case partition is empty
     */
    public GraphNode lastAddedNode() {
        return mLastAddedNode;
    }

    /**
     * Returns a list of nodes making up the partition.
     *
     * @return List of <code>GraphNode</code> objects.
     */
    public List<GraphNode> getNodes() {
        return this.mNodeList;
    }

    /**
     * Returns the root nodes in the partition. They can only be determined, after the
     * constructPartition() has been called.
     *
     * @return List of <code>GraphNode</code> objects that are the root.
     */
    public List getRootNodes() {
        List l = new ArrayList(10);
        Map m = this.getRelations();
        for (Iterator it = getNodes().iterator(); it.hasNext(); ) {
            GraphNode gn = (GraphNode) it.next();
            if (!m.containsKey(gn.getID())) {
                l.add(gn);
            }
        }
        return l;
    }

    /**
     * It while looking at the node list constructs the relations between the jobs in the partition,
     * that can be gotten through getRelationsInPartition().
     */
    public void constructPartition() {
        // traverse through all the nodes in the partition
        for (Iterator it = mNodeList.iterator(); it.hasNext(); ) {
            GraphNode node = (GraphNode) it.next();
            Collection<GraphNode> parents = node.getParents();
            if (parents == null) {
                continue;
            }

            // traverse through all the parents of the node, in
            // the original DAX/Graph,that may or maynot be in
            // this partition
            List partitionParents = new java.util.LinkedList();
            for (Iterator pIt = parents.iterator(); pIt.hasNext(); ) {
                GraphNode parent = (GraphNode) pIt.next();
                if (mNodeSet.contains(parent.getID())) {
                    // relation between 2 nodes in the same partition.
                    partitionParents.add(parent.getID());
                }
            }
            // only add if there are any parents
            if (!partitionParents.isEmpty()) {
                mParentsMap.put(node.getID(), partitionParents);
            }
        }
    }

    /**
     * It sets the partition name to the value passed.
     *
     * @param name the name to which the partition name needs to be set to.
     */
    public void setName(String name) {
        mName = name;
    }

    /** It returns the name of the partition. */
    public String getName() {
        return mName;
    }

    /**
     * Returns a boolean indicating if partition was associated with a label.
     *
     * @return boolean
     */
    public boolean hasAssociatedLabel() {
        return this.mHasAssociatedLabel;
    }

    /**
     * Set a boolean indicating if partition was associated with a label.
     *
     * @param value boolean value
     */
    public void doesHaveAssociatedLabel(boolean value) {
        this.mHasAssociatedLabel = value;
    }

    /**
     * Returns the number of nodes in the partition
     *
     * @return the size
     */
    public int getSize() {
        return this.mNodeList.size();
    }

    /**
     * It sets the index associated with this partition to the value passed.
     *
     * @param index the index value.
     */
    public void setIndex(int index) {
        mIndex = index;
    }

    /** It returns the index to number of the partition. */
    public int getIndex() {
        return mIndex;
    }

    /** It returns the unique id that is associated with the partition. */
    public String getID() {
        return mID;
    }

    /**
     * It sets the id of the partition.
     *
     * @param id the id of the partition.
     */
    public void setID(String id) {
        mID = id;
    }

    /**
     * Returns the number of nodes in the partition.
     *
     * @return the number of nodes.
     */
    public int size() {
        return mNodeList.size();
    }

    /**
     * Returns a list of id's of parents for a node in the partition. It lists parents that in the
     * partition itself
     *
     * @param id the node for which parents are required
     * @return List of parents else empty list
     */
    public List<String> getParents(String id) {
        List<String> parents = (List) mParentsMap.get(id);
        return (parents == null) ? new LinkedList() : parents;
    }

    /** Returns a String version of the object. */
    public String toString() {
        StringBuffer sb = new StringBuffer();
        sb.append("Partition ID ->").append(mID);

        for (Iterator it = this.mNodeList.iterator(); it.hasNext(); ) {
            GraphNode gn = (GraphNode) it.next();
            String id = (String) gn.getID();
            sb.append("\nJob ->").append(id);
            sb.append("\nBag ->").append(gn.getBag());

            List l = (List) mParentsMap.get(id);
            if (l == null) continue;

            Iterator it1 = l.iterator();
            sb.append(" Parents {");
            while (it1.hasNext()) {
                sb.append(it1.next()).append(',');
            }
            sb.append("}");
        }

        return sb.toString();
    }

    /**
     * Returns the xml description of the object. This is used for generating the partition graph.
     * That is no longer done.
     *
     * @param writer is a Writer opened and ready for writing. This can also be a StringWriter for
     *     efficient output.
     * @exception IOException if something fishy happens to the stream.
     */
    public void toXML(Writer writer) throws IOException {
        String newLine = System.getProperty("line.separator", "\r\n");
        String indent = "\t";

        // write out the partition xml element
        writer.write(indent);
        writer.write("<partition");
        writeAttribute(writer, "name", mName);
        writeAttribute(writer, "index", Integer.toString(mIndex));
        writeAttribute(writer, "id", mID);
        writer.write(">");
        writer.write(newLine);

        // write out all the jobs making up the partition
        String newIndent = indent + "\t";
        for (Iterator it = mNodeList.iterator(); it.hasNext(); ) {
            GraphNode gn = (GraphNode) it.next();
            writer.write(newIndent);
            writer.write("<job");
            writeAttribute(writer, "name", gn.getName());
            writeAttribute(writer, "id", gn.getID());
            writer.write("/>");
            writer.write(newLine);
        }

        // write out all the dependencies amongst the jobs.
        String id;
        for (Iterator it = mNodeSet.iterator(); it.hasNext(); ) {
            id = (String) it.next();

            List l = (List) mParentsMap.get(id);
            if (l == null || l.isEmpty()) continue;

            // write out the child
            writer.write(newIndent);
            writer.write("<child");
            writeAttribute(writer, "ref", id);
            writer.write(">");
            writer.write(newLine);

            // write out all the parents of the child
            String parentIndent = newIndent + "\t";
            for (Iterator it1 = l.iterator(); it1.hasNext(); ) {
                writer.write(parentIndent);
                writer.write("<parent");
                writeAttribute(writer, "ref", (String) it1.next());
                writer.write("/>");
                writer.write(newLine);
            }

            writer.write(newIndent);
            writer.write("</child>");
            writer.write(newLine);
        }

        writer.write(indent);
        writer.write("</partition>");
        writer.write(newLine);
    }

    /**
     * Returns the xml description of the object. This is used for generating the partition graph.
     * That is no longer done.
     *
     * @return String containing the Partition object in XML.
     * @exception IOException if something fishy happens to the stream.
     */
    public String toXML() throws IOException {
        Writer writer = new StringWriter(32);
        toXML(writer);
        return writer.toString();
    }

    /**
     * Writes an attribute to the stream. Wraps the value in quotes as required by XML.
     *
     * @param writer
     * @param key
     * @param value
     * @exception IOException if something fishy happens to the stream.
     */
    private void writeAttribute(Writer writer, String key, String value) throws IOException {
        writer.write(" ");
        writer.write(key);
        writer.write("=\"");
        writer.write(value);
        writer.write("\"");
    }

    /** It returns the set of the job ids making up the partition. */
    public Set getNodeIDs() {
        return mNodeSet;
    }

    /**
     * Ends up assigning the parents to a particular node. It does assign the parents to the node,
     * if the node is in the partition. It however does not check if the parents are in the
     * partition or not.
     *
     * @param node the id of the node for which you want to add the parents.
     * @param parents list of id's of the parents of the nodes.
     */
    public void addParents(String node, List parents) {
        // check if node is in the node set
        if (mNodeSet.contains(node)) {
            // add to the graph
            mParentsMap.put(node, parents);
        }
    }

    /**
     * A function to return the child-parent relations for the jobs making up the partition. The
     * child parent relations are only returned for the jobs that have parents in the partition.
     *
     * @return Map containing the job id's as the keys and the values as the list of the parent id's
     *     in the partition.
     */
    public Map getRelations() {
        return mParentsMap;
    }

    /** Returns a copy of the object */
    public Object clone() throws CloneNotSupportedException {
        throw new CloneNotSupportedException("Clone method not implemented in Partition");
    }
}
