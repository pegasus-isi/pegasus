/*
 * This file or a portion of this file is licensed under the terms of
 * the Globus Toolkit Public License, found in file ../GTPL, or at
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
package org.griphyn.vdl.euryale;

import java.io.*;
import java.util.*;
import org.griphyn.vdl.dax.*;
import org.griphyn.vdl.util.*;

/**
 * This class is used to test the <code>DAXParser</code> class and the input file index. It parses
 * all the DAX documents specified in the commandline, creates the corresponding java objects, and
 * generates an XML document from these objects. It also prints the input file list if the last
 * definition in the document is a derivation.
 *
 * @author Kavitha Ranganathan
 * @author Jens-S. Vöckler
 * @author Yong Zhao
 * @version $Revision$
 * @see DAXParser
 * @see org.griphyn.vdl.dax.ADAG
 */
public class DAXTest implements Callback {
    long m_callback[] = null;

    public DAXTest() {
        m_callback = new long[5];
    }

    public void cb_document(java.util.Map attributes) {
        m_callback[0]++;
        System.out.print("free=" + Runtime.getRuntime().freeMemory());
        System.out.println(" document callback: " + attributes);
    }

    public void cb_filename(Filename filename) {
        m_callback[1]++;
        System.out.print("free=" + Runtime.getRuntime().freeMemory());
        System.out.println(" filename callback: \"" + filename.getFilename() + "\"");
    }

    public void cb_job(Job job) {
        m_callback[2]++;
        System.out.print("free=" + Runtime.getRuntime().freeMemory());
        System.out.println(" job callback: " + job.getID());
    }

    public void cb_parents(String child, java.util.List parents) {
        m_callback[3]++;
        System.out.print("free=" + Runtime.getRuntime().freeMemory());
        System.out.println(" relationship callback: " + child + " " + parents);
    }

    public void cb_done() {
        m_callback[4]++;
        System.out.print("free=" + Runtime.getRuntime().freeMemory());
        System.out.println(" done callback");
    }

    private static String c_callback[] = {"documents", "filenames", "jobs", "children", "dones"};

    protected void finalize() throws Throwable {
        if (m_callback != null) {
            System.out.print("CALL STATS: ");
            for (int i = 0; i < m_callback.length; ++i) {
                if (i > 0) System.out.print(", ");
                System.out.print(c_callback[i] + "=" + m_callback[i]);
            }
            System.out.println();
        }
    }

    public static void main(String[] args) throws IOException {
        DAXTest me = new DAXTest();
        if (args.length == 0) {
            System.err.println("Usage: java " + me.getClass().getName() + " [dax] ...");
            return;
        }

        // connect debug stream
        // Logging.instance().register( "parser", System.err );
        Logging.instance().register("app", System.err);

        DAXParser parser = new DAXParser(System.getProperty("vds.schema.dax"));
        parser.setCallback(me);

        for (int i = 0; i < args.length; i++) {
            if (!parser.parse(args[i])) {
                System.err.println("Detected error while parsing XML, exiting\n");
                System.exit(1);
            }
        }

        // done -- release references
        parser = null;
        me = null;
        System.gc();
    }
}
