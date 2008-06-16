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


package org.griphyn.cPlanner.parser.dax;

import org.griphyn.cPlanner.classes.SubInfo;

import org.griphyn.cPlanner.partitioner.graph.Graph;
import org.griphyn.cPlanner.partitioner.graph.MapGraph;
import org.griphyn.cPlanner.partitioner.graph.GraphNode;

import org.griphyn.cPlanner.common.PegasusProperties;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;


/**
 * An exploratory implementation that builds on the DAX2Graph.
 * There is a graph object created that is returned.
 *
 * @author Karan Vahi
 * @version $Revision$
 */

public class DAX2NewGraph implements Callback {

    /**
     * The Graph instance that stores the abstract workflow as a Graph.
     */
    protected Graph mWorkflow;

    /**
     * A flag to specify whether the graph has been generated for the partition
     * or not.
     */
    protected boolean mDone;

    /**
     * The label of the abstract dax.
     */
    protected String mLabel;

    /**
     * The handle to the properties object.
     */
    protected PegasusProperties mProps;

    /**
     * The overloaded constructor.
     *
     * @param properties  the properties passed to the planner.
     * @param dax         the path to the DAX file.
     */
    public DAX2NewGraph( PegasusProperties properties, String dax ){
        mProps = properties;
        mWorkflow = new MapGraph();
        mDone          = false;
        mLabel         = null;
    }

    /**
     * Returns the workflow represented in the <code>Graph</code> form.
     *
     *
     * @return  <code>Graph</code> containing the abstract workflow referred
     *          in the dax.
     */
    public Object getConstructedObject() {
        if(!mDone)
            throw new RuntimeException("Method called before the abstract dag " +
                                       " for the partition was fully generated");

        return mWorkflow;
    }

    /**
     * Callback when the opening tag was parsed. This contains all
     * attributes and their raw values within a map. It ends up storing
     * the attributes with the adag element in the internal memory structure.
     *
     * @param attributes is a map of attribute key to attribute value
     */
    public void cbDocument(Map attributes) {
        /**@todo Implement this org.griphyn.cPlanner.parser.Callback method*/
        if( attributes == null ||
            (mLabel = (String)attributes.get("name")) == null){
            mLabel = "test";
        }
    }

    /**
     * This constructs a graph node for the job and ends up storing it in the
     * internal map.
     *
     * @param job  the job that was parsed.
     */
    public void cbJob( SubInfo job ) {
        //populate the job as a node in the graph
        GraphNode node = new GraphNode( job.getID(), job );
        mWorkflow.addNode( node );
    }

    /**
     * This updates the internal graph nodes of child with references to it's
     * parents referred to by the list of parents passed. It gets the handle
     * to the parents graph nodes from it's internal map.
     *
     * @param child   the logical id of the child node.
     * @param parents list containing the logical id's of the parents of the
     *                child nodes.
     */
    public void cbParents( String child, List parents ) {
        mWorkflow.addEdges( child, parents );
    }


    /**
     * Returns the name of the dax.
     */
    public String getNameOfDAX(){
        return mLabel;
    }

    /**
     * Callback to signal that traversal of the DAX is complete. At this point a
     * dummy root node is added to the graph, that is the parents to all the root
     * nodes in the existing DAX.
     */
    public void cbDone() {
        //the abstract graph is fully generated
        mDone = true;
    }

    /**
     * Returns the <code>GraphNode</code> of the corresponding id.
     *
     * @param id   the id of the node.
     *
     * @return <code>GraphNode</code>.
     */
    public GraphNode get( String key ){
        return mWorkflow.getNode( key );
    }


}
