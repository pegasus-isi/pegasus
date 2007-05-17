/*
 * This file or a portion of this file is licensed under the terms of
 * the Globus Toolkit Public License, found in file GTPL, or at
 * http://www.globus.org/toolkit/download/license.html. This notice must
 * appear in redistributions of this file, with or without modification.
 *
 * Redistributions of this Software, with or without modification, must
 * reproduce the GTPL in: (1) the Software, or (2) the Documentation or
 * some other similar material which is provided with the Software (if
 * any).
 *
 * Copyright 1999-2004 University of Chicago and The University of
 * Southern California. All rights reserved.
 */

package org.griphyn.cPlanner.partitioner.graph;


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
    public static final String VERSION = "1.1";


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
     * A convenience method that allows for bulk addition of edges between
     * already existing nodes in the graph.
     *
     * @param child   the child node ID
     * @param parents list of parent identifiers as <code>String</code>.
     */
    public void addEdges( String child, List parents );

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
    public Iterator nodeIterator();

    /**
     * Returns an iterator that traverses through the graph using a graph
     * traversal algorithm.
     *
     * @return Iterator through the nodes of the graph.
     */
    public Iterator iterator();

    /**
     * Returns the root nodes of the Graph.
     *
     * @return  a list containing <code>GraphNode</code> corressponding to the
     *          root nodes.
     */
    public List getRoots();


    /**
     * Returns the leaf nodes of the Graph.
     *
     * @return  a list containing <code>GraphNode</code> corressponding to the
     *          leaf nodes.
     */
    public List getLeaves();


}
