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

package org.griphyn.vdl.planner;

import java.io.*;
import java.util.*;

/**
 * This class is used to represent the graph form of a workflow. The graph is represented using
 * adjaceny lists. Each node is the name of a job in the workflow. The arcs represent dependencies
 * of the jobs on one another.
 *
 * @author Jens-S. Vöckler
 * @author Yong Zhao
 * @version $Revision $
 */
public class Graph implements Cloneable {
    /** The adjacency list representing a graph. */
    private Map m_adj;

    /** Initializes internal objects to hold the graph. */
    private void initialize() {
        m_adj = new HashMap();
    }

    /** Default constructor, call the initialize function */
    public Graph() {
        initialize();
    }

    /** Clones the graph using a deep copy. */
    public Object clone() {
        Graph result = new Graph();

        for (Iterator i = getVertices(); i.hasNext(); ) {
            String node = (String) i.next();
            result.m_adj.put(new String(node), new ArrayList(neighbors(node)));
        }

        return result;
    }

    /**
     * Reads a stream representing the graph. The format of the stream is:
     *
     * <pre>
     *   # of vertices
     *   vertex adjcency_list_of_the_vertex
     * </pre>
     *
     * @param reader is an input file open for reading.
     */
    public Graph(Reader reader) {
        String line = null;
        StringTokenizer token = null;

        try {
            LineNumberReader lnr = new LineNumberReader(reader);

            line = lnr.readLine();
            token = new StringTokenizer(line);
            if (token.countTokens() != 1)
                throw new Error("Bad format: number of vertices is first line");

            // number of nodes
            int n = Integer.parseInt(token.nextToken());
            initialize();

            // read rest of graph
            for (int u = 0; u < n; ++u) {
                line = lnr.readLine();
                token = new StringTokenizer(line);
                if (token.countTokens() < 1)
                    throw new Error(
                            "line "
                                    + lnr.getLineNumber()
                                    + ": Please specify the vertex and neighbor list.");

                // add node to graph
                String node = token.nextToken();
                addVertex(node);

                // add arcs to graph
                while (token.hasMoreTokens()) {
                    String w = token.nextToken();
                    ((List) m_adj.get(node)).add(new String(w));
                }
            }
        } catch (IOException x) {
            throw new Error("Bad input stream");
        }
    }

    /**
     * Provides an iterator over the vertices.
     *
     * @return an initialized iterator to walk all vertices.
     */
    public Iterator getVertices() {
        return m_adj.keySet().iterator();
    }

    /**
     * Adds a vertex to the adjacency list. The vortex's adjacency list is initialized to an empty
     * list. If the vortex already exists, nothing will be done.
     *
     * @param node is the name of the vortex to add.
     * @see #removeVertex( String )
     */
    public void addVertex(String node) {
        if (!m_adj.containsKey(node)) {
            m_adj.put(new String(node), new ArrayList());
        }
    }

    /**
     * Removes a vortex from the graph. This is an expensive function, because the vortex must also
     * be removed from all adjacency lists.
     *
     * @param node is the name of the vortex to remove.
     * @see #addVertex( String )
     */
    public void removeVertex(String node) {
        // remove entry for vortex from adjacency list
        m_adj.remove(node);

        // remove vortex from all other adjaceny lists
        for (Iterator i = getVertices(); i.hasNext(); ) {
            List v = (List) m_adj.get((String) i.next());
            v.remove(node);
        }
    }

    /**
     * Adds a directed edge between two nodes.
     *
     * @param v is the source node
     * @param w is the destination node
     * @see #removeArc( String, String )
     */
    public void addArc(String v, String w) {
        // skip, if the arc already exists
        if (isArc(v, w)) return;

        // add arc
        ((List) m_adj.get(v)).add(new String(w));
    }

    /**
     * Removes a directed edge between two nodes.
     *
     * @param v is the source node
     * @param w is the destination node
     * @see #addArc( String, String )
     */
    public void removeArc(String v, String w) {
        ((List) m_adj.get(v)).remove(w);
    }

    /**
     * Predicate to check the existence of a directed edge between from v to w.
     *
     * @param v is the source node
     * @param w is the destination node
     */
    public boolean isArc(String v, String w) {
        return ((List) m_adj.get(v)).contains(w);
    }

    /**
     * Counts the number of incoming arcs (edges) for a given node.
     *
     * @param v is the vortex name to count incoming edge for.
     * @return the number of incoming edges.
     * @see #outDegree( String )
     */
    public int inDegree(String v) {
        int result = 0;

        // for all nodes, see if they have an edge to v
        for (Iterator i = getVertices(); i.hasNext(); ) {
            String w = (String) i.next();
            if (isArc(w, v)) result++;
        }
        return result;
    }

    /**
     * Counts the number of outgoing arcs (edges) for a given node.
     *
     * @param v is the vortex name to count outgoing edge for.
     * @return the number of outgoing edges.
     * @see #inDegree( String )
     */
    public int outDegree(String v) {
        return ((List) m_adj.get(v)).size();
    }

    /**
     * Determines the neighbors of a given node. This is effectively a copy of the node's adjacency
     * list.
     *
     * @param v is the node to determine the neighbors for
     * @return a copy of the node's adjacency list.
     */
    public List neighbors(String v) {
        return new ArrayList((List) m_adj.get(v));
    }

    /**
     * Counts the number of nodes (vertices) in a graph.
     *
     * @return the number of vertices.
     */
    public int order() {
        return m_adj.size();
    }

    /**
     * Counts the number of directed edges (arcs) in the graph. Undirected edges are counted as two
     * directed edges.
     *
     * @return number of directed edges.
     */
    public int size() {
        int result = 0;
        for (Iterator i = m_adj.values().iterator(); i.hasNext(); )
            result += ((List) i.next()).size();
        return result;
    }

    /**
     * Constructs the reverse graph by inverting the direction of every arc.
     *
     * @return a new graph which is the reverse of the current one.
     */
    public Graph reverseGraph() {
        String v = null;
        Graph result = new Graph();

        // copy all nodes
        for (Iterator i = getVertices(); i.hasNext(); ) {
            v = (String) i.next();
            result.addVertex(v);
        }

        // copy all edges
        for (Iterator i = getVertices(); i.hasNext(); ) {
            v = (String) i.next();
            for (Iterator j = ((List) m_adj.get(v)).iterator(); j.hasNext(); ) {
                result.addArc((String) j.next(), v);
            }
        }

        return result;
    }

    /**
     * Generates a simple string representation of the graph. The format of the representation is
     * the same as it is read from a stream.
     *
     * @return a complete graph as a single string
     * @see #Graph( Reader )
     */
    public String toString() {
        String newline = System.getProperty("line.separator", "\r\n");
        StringBuffer result = new StringBuffer(256);

        // write order of graph (number of nodes)
        result.append(order()).append(newline);

        // write nodes
        for (Iterator i = getVertices(); i.hasNext(); ) {
            String v = (String) i.next();
            result.append(v);

            // write adjacency list for node v
            for (Iterator j = ((List) m_adj.get(v)).iterator(); j.hasNext(); ) {
                result.append(' ').append((String) j.next());
            }

            result.append(newline);
        }

        // done
        return result.toString();
    }
}
