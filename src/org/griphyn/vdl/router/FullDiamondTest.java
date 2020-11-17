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

package org.griphyn.vdl.router;

import java.io.*;
import java.lang.reflect.*;
import java.sql.SQLException;
import org.griphyn.vdl.classes.*;
import org.griphyn.vdl.dax.*;
import org.griphyn.vdl.dbschema.*;
import org.griphyn.vdl.util.Logging;

public class FullDiamondTest {
    public static void main(String[] args)
            throws IllegalArgumentException, IOException, ClassNotFoundException,
                    NoSuchMethodException, InstantiationException, SQLException,
                    IllegalAccessException, InvocationTargetException {
        // create debug output
        Logging.instance().register("dag", System.err);
        Logging.instance().register("state", System.err);
        Logging.instance().register("route", System.err);

        // create new route object, in memory classes
        Definitions diamond = org.griphyn.vdl.router.CreateFullDiamond.create();
        Route r = new Route(new InMemorySchema(diamond));

        // request known production
        BookKeeper bk = r.requestLfn("f.d");
        ADAG dax = bk.getDAX("testing");

        // show us the result(s)
        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(System.out));
        if (args.length > 0 && args[0] != null && args[0].startsWith("t")) {
            dax.toString(bw);
        } else {
            dax.toXML(bw, "");
        }
        bw.flush();

        System.exit(0);
    }
}
