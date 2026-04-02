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

package org.griphyn.vdl.parser;

import java.io.*;
import org.griphyn.vdl.dax.*;
import org.griphyn.vdl.util.*;

/**
 * This class is used to test the <code>DAXParser</code> class and the input file index. It parses
 * all the DAX documents specified in the commandline, creates the corresponding java objects, and
 * generates an XML document from these objects. It also prints the input file list if the last
 * definition in the document is a derivation.
 *
 * @author Jens-S. Vöckler
 * @author Yong Zhao
 * @version $Revision$
 * @see DAXParser
 * @see org.griphyn.vdl.dax.ADAG
 */
public class DAXTest {
    public static void main(String[] args) throws IOException {
        if (args.length == 0) {
            System.err.println("Usage: java Test [daxURI] ...");
            return;
        }
        // connect debug stream
        Logging.instance().register("DAXparser", System.err);
        Logging.instance().register("app", System.err);

        DAXParser daxParser = new DAXParser(System.getProperty("vds.schema.dax"));
        Writer stdout = new BufferedWriter(new OutputStreamWriter(System.out));
        for (int i = 0; i < args.length; i++) {
            ADAG adag = daxParser.parse(args[i]);
            adag.toXML(stdout, "", null);
            Logging.instance().log("app", 0, "done writing XML");
        }
    }
}
