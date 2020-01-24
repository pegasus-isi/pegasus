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

package org.griphyn.vdl.util;

import java.io.*;
import java.util.Iterator;
import org.griphyn.vdl.dax.*;

/**
 * Convert a dag structure into the new CoG's XML format for visualization.
 *
 * @author Jens-S. Vöckler
 * @author Yong Zhao
 * @version $Revision$
 */
public class DAX2CoG {
    /**
     * Converts a given DAX into CoG visualizier XML.
     *
     * @param w is a writer onto which to dump the generated XML.
     * @param dax is the input ADAG.
     * @throws IOException if the stream write runs into trouble.
     */
    public static void toString(Writer w, ADAG dax) throws IOException {
        String newline = System.getProperty("line.separator", "\r\n");

        // start graph
        w.write("<graph>");
        w.write(newline);

        // start nodes
        for (Iterator i = dax.iterateJob(); i.hasNext(); ) {
            Job job = (Job) i.next();
            w.write("  <node nodeid=\"");
            w.write(job.getID());
            w.write("\" name=\"");
            w.write(job.getName());
            w.write("\"/>");
            w.write(newline);
        }

        // start edges
        for (Iterator c = dax.iterateChild(); c.hasNext(); ) {
            Child child = (Child) c.next();
            String name = child.getChild();
            for (Iterator p = child.iterateParent(); p.hasNext(); ) {
                w.write("  <edge to=\"");
                w.write(name);
                w.write("\" from=\"");
                w.write((String) p.next());
                w.write("\"/>");
                w.write(newline);
            }
        }

        // done
        w.write("</graph>");
        w.write(newline);
        w.flush();
    }

    /**
     * Converts a DAGMan <code>.dag</code> file into a Peudo-DAX.
     *
     * @param dag is a File pointing to the DAG file
     * @return a Pseudo DAX, or null in case of error.
     * @throws IOException if reading the DAGMan file fails.
     */
    public static ADAG DAGMan2DAX(File dag) {
        // sanity check
        if (dag == null) return null;

        ADAG result = new ADAG();
        return result;
    }
}
