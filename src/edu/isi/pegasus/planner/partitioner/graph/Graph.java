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

package edu.isi.pegasus.planner.partitioner.graph;


import edu.isi.pegasus.planner.classes.NameValue;
import java.util.Iterator;
import java.util.List;

/**
 * The interface for the Graph Class. It implements the GraphNodeContent interface.
 * This allows us to associate Graphs with the nodes in a Graph i.e. graph of
 * graphs.
 *
 * @author Karan Vahi
 * @version $Revision$
 */

public interface Graph
       extends GraphNodeContent { //allows us to have graphs as nodes of a graph

    /**
     * The version number associated with this Graph API.
     */
    public static final String VERSION = "1.6";


    /**
     * Adds a node to the Graph. It overwrites an already existing node with the
     * same ID.
     *
     * @param node  the node to be added to the Graph.
     */
    public void addNode( GraphNode node );

    /**
     * Adds an edge between two already existing nodes in the graph.
     *
     * @param parent   the parent node ID.
     * @param child    the child node ID.
     */
    public void addEdge( String parent, String child );
    
    /**
     * Adds an edge between two already existing nodes in the graph.
     *
     * @param parent   the parent node .
     * @param child    the child node .
     */
    public void addEdge( GraphNode parent, GraphNode child );

    /**
     * A convenience method that allows for bulk addition of edges between
     * already existing nodes in the graph.
     *
     * @param child   the child node ID
     * @param parents list of parent identifiers as <code>String</code>.
     */
    public void addEdges( String child, List<String> parents );
    
    /**
     * Resets all the dependencies in the Graph, while preserving the nodes. 
     * The resulting Graph is a graph of independent nodes.
     */
    public void resetEdges();

    /**
     * Returns the node matching the id passed.
     *
     * @param identifier  the id of the node.
     *
     * @return the node matching the ID else null.
     */
    public GraphNode getNode( String identifier );

    /**
     * Adds a single root node to the Graph. All the exisitng roots of the
     * Graph become children of the root.
     *
     * @param root  the <code>GraphNode</code> to be added as a root.
     *
     * @throws RuntimeException if a node with the same id already exists.
     */
    public void addRoot( GraphNode root );

    /**
     * Removes a node from the Graph.
     *
     * @param identifier   the id of the node to be removed.
     *
     * @return boolean indicating whether the node was removed or not.
     */
    public boolean remove( String identifier );


    /**
     * Returns an iterator for the nodes in the Graph. These iterators are
     * fail safe.
     *
     * @return Iterator
     */
    public Iterator<GraphNode> nodeIterator();

    /**
     * Returns an iterator that traverses through the graph using a graph
     * traversal algorithm.
     *
     * @return Iterator through the nodes of the graph.
     */
    public Iterator<GraphNode> iterator();

    /**
     * Returns an iterator for the graph that traverses in topological sort
     * order.
     *
     * @return Iterator through the nodes of the graph.
     */
    public Iterator<GraphNode> topologicalSortIterator();

    /**
     * Returns an iterator that traverses the graph bottom up from the leaves.
     * At any one time, only one iterator can
     * iterate through the graph.
     *
     * @return Iterator through the nodes of the graph.
     */
    public Iterator<GraphNode> bottomUpIterator();
    
    /**
     * Returns the number of nodes in the graph.
     */
    public int size();

    /**
     * Returns the root nodes of the Graph.
     *
     * @return  a list containing <code>GraphNode</code> corressponding to the
     *          root nodes.
     */
    public List<GraphNode> getRoots();


    /**
     * Returns the leaf nodes of the Graph.
     *
     * @return  a list containing <code>GraphNode</code> corressponding to the
     *          leaf nodes.
     */
    public List<GraphNode> getLeaves();


    /**
     * Returns a boolean if there are no nodes in the graph.
     *
     * @return boolean
     */
    public boolean isEmpty();
    
    
    /**
     * Returns a boolean indicating whether a graph has cyclic edges or not.
     * 
     * @return boolean
     */
    public boolean hasCycles();
    
    /**
     * Returns the detected cyclic edge if , hasCycles returns true
     * 
     * @return 
     */
    public NameValue getCyclicEdge();

}
