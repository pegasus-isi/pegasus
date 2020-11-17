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
import org.griphyn.vdl.classes.*;
import org.griphyn.vdl.util.*;

/**
 * This class is used to test the <code>VDLxParser</code> class and the input file index. It parses
 * all the XML documents specified in the commandline, creates the corresponding java objects, and
 * generates an XML document from these objects. It also prints the input file list if the last
 * definition in the document is a derivation.
 *
 * @author Jens-S. Vöckler
 * @author Yong Zhao
 * @version $Revision$
 * @see VDLxParser
 * @see org.griphyn.vdl.classes.Definitions
 * @see org.griphyn.vdl.classes.Derivation
 */
public class VDLxTest {
    public static void main(String[] args) throws IOException {
        if (args.length == 0) {
            System.err.println("Usage: java VDLxTest [xmlURI] ...");
            return;
        }
        // connect debug stream
        Logging.instance().register("filler", System.out);
        Logging.instance().register("parser", System.out);
        Logging.instance().register("app", System.out);

        VDLxParser parser = new VDLxParser(System.getProperty("vds.schema.vdl"));
        for (int i = 0; i < args.length; i++) {
            Logging.instance().log("app", 1, "starting to read from " + args[i]);
            Definitions def = new Definitions();
            parser.parse(
                    new org.xml.sax.InputSource(
                            new BufferedInputStream(new FileInputStream(args[i]))),
                    new MemoryStorage(def, true, true));
            Logging.instance().log("app", 1, "read " + def.getDefinitionCount() + " definitions");

            String outfn = args[i] + ".xml";
            Logging.instance().log("app", 1, "dumping results to " + outfn);
            def.toXML(
                    new OutputStreamWriter(new BufferedOutputStream(new FileOutputStream(outfn))),
                    "");
            Logging.instance().log("app", 1, "done dumping results");
        }
    }
}
