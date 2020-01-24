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

package edu.isi.pegasus.planner.parser;

import edu.isi.pegasus.planner.invocation.InvocationRecord;
import java.io.*;
import org.griphyn.vdl.util.*;

/**
 * This class is used to test the <code>InvocationParser</code> class. It parses an invocation
 * record, creates the corresponding java objects, and generates an XML document from these objects.
 *
 * @author Jens-S. Vöckler
 * @author Yong Zhao
 * @version $Revision$
 * @see InvocationParser
 * @see org.griphyn.vdl.invocation.Invocation
 */
public class IVPTest {
    public static void main(String[] args) throws IOException {
        if (args.length == 0) {
            System.err.println("Usage: java IVPTest [invocationfile] ...");
            return;
        }

        // connect debug stream
        Logging.instance().register("parser", System.err);
        Logging.instance().register("app", System.err);
        //    Logging.instance().register( "app", System.err );

        InvocationParser ip = new InvocationParser(InvocationRecord.SCHEMA_LOCATION);
        Writer stdout = new BufferedWriter(new OutputStreamWriter(System.out));
        for (int i = 0; i < args.length; i++) {
            InvocationRecord invocation = ip.parse(new FileInputStream(args[i]));
            System.err.println("\nNow convert back to XML\n");
            invocation.toXML(stdout, "", null);
            Logging.instance().log("app", 0, "done writing XML");
        }
    }
}
