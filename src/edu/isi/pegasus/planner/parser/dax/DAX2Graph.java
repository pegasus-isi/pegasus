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
package edu.isi.pegasus.planner.parser.dax;

import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.planner.catalog.transformation.TransformationCatalogEntry;
import edu.isi.pegasus.planner.classes.CompoundTransformation;
import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.classes.Profile;
import edu.isi.pegasus.planner.classes.ReplicaLocation;
import edu.isi.pegasus.planner.common.PegasusProperties;
import edu.isi.pegasus.planner.dax.Invoke;
import edu.isi.pegasus.planner.partitioner.graph.GraphNode;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * This callback implementation ends up building a detailed structure of the graph referred to by
 * the abstract plan in dax, that should make the graph traversals easier. Later on this graph
 * representation would be used uniformly in the Pegasus code base.
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public class DAX2Graph implements Callback {

    /**
     * The id of the dummy root node added on the top of the graph. Makes easier the starting of the
     * traversal.
     */
    public static final String DUMMY_NODE_ID = "dummy";

    /**
     * The map containing the a graph node for each of the jobs referred to in the dax. The key is
     * the logical id of the job.
     */
    protected Map mAbstractGraph;

    /** A flag to specify whether the graph has been generated for the partition or not. */
    protected boolean mDone;

    /** The label of the abstract dax as set by Chimera. */
    protected String mLabel;

    /** The root node for the graph that is constructed. */
    protected GraphNode mRoot;

    /** The handle to the properties object. */
    protected PegasusProperties mProps;

    /** The logging object. */
    protected LogManager mLogger;

    /**
     * The overloaded constructor.
     *
     * @param bag the bag of initialization objects containing the properties and the logger
     * @param dax the path to the DAX file.
     */
    public void initialize(PegasusBag bag, String dax) {
        mProps = bag.getPegasusProperties();
        mAbstractGraph = new java.util.HashMap();
        mLogger = bag.getLogger();
        mDone = false;
        mLabel = null;
        mRoot = null;
    }

    /**
     * Returns a Map indexed by the logical ID of the jobs, and each value being a GraphNode object.
     *
     * @return ADag object containing the abstract plan referred in the dax.
     */
    public Object getConstructedObject() {
        if (!mDone)
            throw new RuntimeException(
                    "Method called before the abstract dag "
                            + " for the partition was fully generated");

        return mAbstractGraph;
    }

    /**
     * Callback when the opening tag was parsed. This contains all attributes and their raw values
     * within a map. It ends up storing the attributes with the adag element in the internal memory
     * structure.
     *
     * @param attributes is a map of attribute key to attribute value
     */
    public void cbDocument(Map attributes) {
        /** @todo Implement this org.griphyn.cPlanner.parser.Callback method */
        if (attributes == null || (mLabel = (String) attributes.get("name")) == null) {
            mLabel = "test";
        }
    }

    /**
     * Callback when a invoke entry is encountered in the top level inside the adag element in the
     * DAX.
     *
     * @param invoke the invoke object
     */
    public void cbWfInvoke(Invoke invoke) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    /**
     * Callback when a metadata element is encountered in the adag element.
     *
     * @param profile profile element of namespace metadata
     */
    public void cbMetadata(Profile p) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    /**
     * This constructs a graph node for the job and ends up storing it in the internal map.
     *
     * @param job the job that was parsed.
     */
    public void cbJob(Job job) {
        GraphNode gn = new GraphNode(job.getLogicalID(), job.getTXName());
        mLogger.log("Adding job to graph " + job.getName(), LogManager.DEBUG_MESSAGE_LEVEL);
        put(job.logicalId, gn);
    }

    /**
     * This updates the internal graph nodes of child with references to it's parents referred to by
     * the list of parents passed. It gets the handle to the parents graph nodes from it's internal
     * map.
     *
     * @param child the logical id of the child node.
     * @param parents list containing the logical id's of the parents of the child nodes.
     */
    public void cbParents(String child, List parents) {
        GraphNode childNode = (GraphNode) get(child);
        Iterator it = parents.iterator();
        String parentId;
        ArrayList parentList = new ArrayList(parents.size());

        mLogger.log("Adding parents for child " + child, LogManager.DEBUG_MESSAGE_LEVEL);
        // construct the references to the parent nodes
        while (it.hasNext()) {
            parentId = (String) it.next();
            GraphNode parentNode = (GraphNode) get(parentId);
            parentList.add(parentNode);

            // add the child to the parent's child list
            parentNode.addChild(childNode);
        }
        childNode.setParents(parentList);
    }

    /**
     * Returns the name of the dax.
     *
     * @return name of dax
     */
    public String getNameOfDAX() {
        return mLabel;
    }

    /**
     * Callback to signal that traversal of the DAX is complete. At this point a dummy root node is
     * added to the graph, that is the parents to all the root nodes in the existing DAX.
     */
    public void cbDone() {
        // the abstract graph is fully generated
        mDone = true;

        // just print out the graph that is generated internally.
        // find the root nodes from where to start the breadth first
        // search
        Iterator it = mAbstractGraph.entrySet().iterator();
        List rootNodes = new LinkedList();

        while (it.hasNext()) {
            GraphNode gn = (GraphNode) ((java.util.Map.Entry) it.next()).getValue();
            if (gn.getParents() == null || gn.getParents().isEmpty()) {
                rootNodes.add(gn);
            }
            // System.out.println(gn);
        }

        // add a dummy node that is a root to all these nodes.
        String rootId = this.DUMMY_NODE_ID;
        mRoot = new GraphNode(rootId, rootId);
        mRoot.setChildren(rootNodes);
        put(rootId, mRoot);
        // System.out.println(dummyNode);

    }

    /**
     * It puts the key and the value in the internal map.
     *
     * @param key the key to the entry in the map.
     * @param value the entry in the map.
     */
    protected void put(Object key, Object value) {
        mAbstractGraph.put(key, value);
    }

    /**
     * It returns the value associated with the key in the map.
     *
     * @param key the key to the entry in the map.
     * @return the object
     */
    public Object get(Object key) {
        return mAbstractGraph.get(key);
    }

    /**
     * Callback when a compound transformation is encountered in the DAX
     *
     * @param compoundTransformation the compound transforamtion
     */
    public void cbCompoundTransformation(CompoundTransformation compoundTransformation) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    /**
     * Callback when a replica catalog entry is encountered in the DAX
     *
     * @param rl the ReplicaLocation object
     */
    public void cbFile(ReplicaLocation rl) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    /**
     * Callback when a transformation catalog entry is encountered in the DAX
     *
     * @param tce the transformation catalog entry object.
     */
    public void cbExecutable(TransformationCatalogEntry tce) {
        throw new UnsupportedOperationException("Not supported yet.");
    }
}
