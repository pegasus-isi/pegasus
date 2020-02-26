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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Class which implements a topological sort of a graph.
 *
 * @author Jens-S. Vöckler
 * @author Yong Zhao
 * @version $Revision$
 * @see Graph
 */
public class Topology {
    /** The graph representation. */
    private Graph m_graph;

    /** Hashtable to map vertex name to array index. */
    private Map m_vertexMap;

    /** Array to keep the name of each vertex. */
    private String[] m_vertices;

    /** Array to keep the in-degree of each vertex. */
    private int[] m_inDeg;

    /** Queue to keep 0 in-degree vertices. */
    private List m_inQueue;

    /**
     * Constructor, given a graph, construct internal objects needed for topological sort.
     *
     * @param graph is a graph representation.
     */
    public Topology(Graph graph) {
        m_graph = (Graph) graph.clone();
        init();
    }

    /** Initializes in-degree array and the corresponding 0 in-degree queue. */
    public void init() {
        // allocate memory for collections
        int n = m_graph.order();
        m_vertexMap = new HashMap();
        m_vertices = new String[n];
        m_inDeg = new int[n];
        m_inQueue = new ArrayList();

        // map nodes
        int i = 0;
        for (Iterator e = m_graph.getVertices(); e.hasNext(); ) {
            String v = (String) e.next();
            m_vertices[i] = new String(v);
            m_vertexMap.put(new String(v), new Integer(i));
            m_inDeg[i] = m_graph.inDegree(v);
            ++i;
        }

        // put 0 in-degree vertices in the queue
        for (i = 0; i < n; i++) {
            if (m_inDeg[i] == 0) m_inQueue.add(new Integer(i));
        }
    }

    /**
     * Sorts the graph topologically. It first invokes the {@link #init()} method to start the graph
     * over, and initialize data structures.
     *
     * @return A sorted list of vortex names.
     */
    public String[] sort() {
        int n = m_graph.order();
        String[] s = new String[n];

        int cnt = 0;
        int i;
        while (m_inQueue.size() > 0) {
            i = ((Integer) m_inQueue.remove(0)).intValue();
            s[cnt++] = new String(m_vertices[i]);

            List neighbors = m_graph.neighbors(m_vertices[i]);
            for (int j = 0; j < neighbors.size(); j++) {
                String w = (String) neighbors.get(j);
                int k = ((Integer) m_vertexMap.get(w)).intValue();
                m_inDeg[k] -= 1;
                if (m_inDeg[k] == 0) m_inQueue.add(new Integer(k));
            }
        }
        if (cnt != n) throw new Error("The graph is not acyclic");

        return s;
    }

    /**
     * Sorts the graph topologically, but also with regards to stages. It first invokes the {@link
     * #init()} method to start the graph over, and initialize data structures.
     *
     * @return a sorted list of zero in-degree vertices.
     */
    public String[] stageSort() {
        int n = m_inQueue.size();
        if (n == 0) return null;

        String[] s = new String[n];
        int cnt = 0;
        while (cnt < n) {
            int i = ((Integer) m_inQueue.remove(0)).intValue();
            s[cnt++] = new String(m_vertices[i]);

            List neighbors = m_graph.neighbors(m_vertices[i]);
            for (int j = 0; j < neighbors.size(); j++) {
                String w = (String) neighbors.get(j);
                int k = ((Integer) m_vertexMap.get(w)).intValue();
                m_inDeg[k] -= 1;
                if (m_inDeg[k] == 0) m_inQueue.add(new Integer(k));
            }
        }
        return s;
    }

    /**
     * The main method contains some simple tests for the sorting algorithms. The graph to test with
     * is read from stdin.
     *
     * @param args are commandline arguments.
     */
    public static void main(String[] args) {
        Graph g = new Graph(new BufferedReader(new InputStreamReader(System.in)));
        Topology t = new Topology(g);
        String[] r = t.sort();

        System.out.println("The topological sorting result:");
        for (int i = 0; i < r.length; i++) System.out.print(r[i] + " ");
        System.out.println();

        System.out.println("The stage-by-stage sorting result:");
        t.init();
        while ((r = t.stageSort()) != null) {
            for (int i = 0; i < r.length; i++) System.out.print(r[i] + " ");
            System.out.println();
        }

        System.out.println("Let's do it in reverse");
        t = new Topology(g.reverseGraph());
        r = t.sort();
        System.out.println("The topological sorting result:");
        for (int i = 0; i < r.length; i++) System.out.print(r[i] + " ");
        System.out.println();
    }
}
