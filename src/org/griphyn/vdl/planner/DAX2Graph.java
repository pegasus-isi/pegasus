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

import java.util.Iterator;
import org.griphyn.vdl.dax.*;

/**
 * This class converts a given DAX into the internal representation of a graph.
 *
 * @author Jens-S. Vöckler
 * @author Yong Zhao
 * @version $Revision$
 * @see org.griphyn.vdl.planner.Graph
 */
public class DAX2Graph {
    /**
     * Converts a DAX into the internal representation of a graph.
     *
     * @param adag is the parsed DAX's internal representation.
     * @return our internal representation of a graph that we can sort on.
     */
    public static Graph DAG2Graph(ADAG adag) {
        Graph result = new Graph();

        // add all nodes
        for (Iterator i = adag.iterateJob(); i.hasNext(); )
            result.addVertex(((Job) i.next()).getID());

        // add all edges
        for (Iterator i = adag.iterateChild(); i.hasNext(); ) {
            Child c = (Child) i.next();
            String child = c.getChild();
            for (Iterator j = c.iterateParent(); j.hasNext(); ) {
                String parent = (String) j.next();
                result.addArc(parent, child);
            }
        }

        return result;
    }

    /** Simple test program. */
    public static void main(String[] args) {
        // construct a fake diamond DAG as DAX w/o any real transformations.
        ADAG adag = new ADAG();
        Job A = new Job();
        Job B = new Job();
        Job C = new Job();
        Job D = new Job();
        A.setID("A");
        B.setID("B");
        C.setID("C");
        D.setID("D");
        adag.addJob(A);
        adag.addJob(B);
        adag.addJob(C);
        adag.addJob(D);
        adag.addChild("C", "A");
        adag.addChild("C", "B");
        adag.addChild("D", "C");

        // convert DAX into graph
        Graph g = DAG2Graph(adag);

        // show
        System.out.println(g.toString());
    }
}
