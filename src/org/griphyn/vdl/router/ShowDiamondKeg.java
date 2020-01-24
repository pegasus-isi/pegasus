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
import org.griphyn.vdl.classes.*;
import org.griphyn.vdl.util.Logging;

public class ShowDiamondKeg {
    public static void main(String[] args) throws IllegalArgumentException, IOException {
        // create debug output
        Logging.instance().register("dag", System.err);
        Logging.instance().register("state", System.err);
        Logging.instance().register("route", System.err);

        // start timer
        long start = System.currentTimeMillis();

        // create the diamond
        boolean b = Boolean.getBoolean("diamond.condor");
        Definitions diamond = org.griphyn.vdl.router.CreateDiamondKeg.create(b);
        if (args.length > 0
                && args[0] != null
                && (args[0].equalsIgnoreCase("vdlt") || args[0].startsWith("t")))
            System.out.println(diamond.toString());
        else {
            BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(System.out));
            diamond.toXML(bw, "");
            bw.flush(); // IMPORTANT!
        }
        long diff = System.currentTimeMillis() - start;
        System.err.println("execution time: " + diff + " ms");
        System.exit(0);
    }
}
