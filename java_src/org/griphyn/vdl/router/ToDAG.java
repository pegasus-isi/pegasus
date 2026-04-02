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

import edu.isi.pegasus.common.util.Version;
import gnu.getopt.*;
import java.io.BufferedWriter;
import java.io.PrintWriter;
import java.io.Writer;
import org.griphyn.vdl.classes.*;
import org.griphyn.vdl.dbschema.*;
import org.griphyn.vdl.directive.*;
import org.griphyn.vdl.parser.*;
import org.griphyn.vdl.toolkit.*;
import org.griphyn.vdl.util.ChimeraProperties;
import org.griphyn.vdl.util.Logging;

public class ToDAG extends Toolkit {
    /** ctor: Constructs a new instance object with the given application name. */
    public ToDAG(String appName) {
        super(appName);
    }

    /** Shows the usage. */
    public void showUsage() {
        String m_usage = "[-d dbname] [-l t|x] -f lfn";

        String linefeed = System.getProperty("line.separator", "\r\n");

        System.out.println(
                "$Id$" + linefeed + "VDS version " + Version.instance().toString() + linefeed);

        System.out.println("Usage: " + this.m_application + " [-d db] [-l t|x] -f lfn" + linefeed);

        System.out.println(
                " -V|--version   print version information and exit."
                        + linefeed
                        + " -d|--dbase dbx associates the dbname with the database, unused."
                        + linefeed
                        + " -l|--list x|t  output format, textual or XML, default is XML."
                        + linefeed
                        + " -f|--file lfn  request a filename to be produced"
                        + linefeed);
    }

    /**
     * Creates a set of long options.
     *
     * @return the long option vector.
     */
    protected LongOpt[] generateValidOptions() {
        LongOpt[] lo = new LongOpt[6];

        lo[0] = new LongOpt("help", LongOpt.NO_ARGUMENT, null, 'h');
        lo[1] = new LongOpt("dbase", LongOpt.REQUIRED_ARGUMENT, null, 'd');
        lo[2] = new LongOpt("version", LongOpt.NO_ARGUMENT, null, 'V');

        lo[3] = new LongOpt("file", LongOpt.REQUIRED_ARGUMENT, null, 'f');
        lo[4] = new LongOpt("lfn", LongOpt.REQUIRED_ARGUMENT, null, 'f');
        lo[5] = new LongOpt("list", LongOpt.REQUIRED_ARGUMENT, null, 'l');

        return lo;
    }

    public static void main(String[] args) throws IllegalArgumentException {
        // create debug output
        Logging.instance().register("app", System.err);
        Logging.instance().register("dag", System.err);
        Logging.instance().register("state", System.err);
        Logging.instance().register("route", System.err);
        Logging.instance().register("trace", System.err);
        Logging.instance().register("stack", System.err);

        try {
            // new instance
            ToDAG me = new ToDAG("ToDAG");

            // get the commandline options
            Getopt opts = new Getopt(me.m_application, args, "hd:f:l:V", me.generateValidOptions());
            opts.setOpterr(false);

            String dbase = null;
            String lfn = null;
            String t = null;
            int option = 0;
            while ((option = opts.getopt()) != -1) {
                switch (option) {
                    case 'V':
                        System.out.println("$Id$");
                        System.out.println("VDS version " + Version.instance().toString());
                        return;

                    case 'd':
                        dbase = opts.getOptarg();
                        break;

                    case 'f':
                        lfn = opts.getOptarg();
                        break;

                    case 'l':
                        t = opts.getOptarg().toLowerCase();
                        break;

                    case 'h':
                    default:
                        me.showUsage();
                        return;
                }
            }

            // sanity check
            if (lfn == null || lfn.length() == 0) {
                me.showUsage();
                System.err.println("You must specify a value for -f");
                System.exit(1);
            }

            // user supplied database, set up me.m_dbschema
            String vdcSchemaName = ChimeraProperties.instance().getVDCSchemaName();
            Connect connect = new Connect();
            DatabaseSchema dbschema = connect.connectDatabase(vdcSchemaName);

            // output format, defaults to XML
            boolean wantText = (t != null && Character.toLowerCase(t.charAt(0)) == 't');

            Definitions defs = null;
            if (dbschema instanceof InMemorySchema) {
                // already everything in main memory, use backdoor (Yuck! Dirty!)
                // avoid duplicating DVs in main memory.
                defs = ((InMemorySchema) dbschema).backdoor();
            } else {
                // Load all Definitions into an in-memory database (uiuiui)
                me.m_logger.log("app", 1, "loading *all* definitions into memory");
                defs = new Definitions();
                defs.setDefinition(((VDC) dbschema).searchDefinition(null, null, null, -1));
            }

            if (defs == null) {
                System.err.println("No input data, nothing to route");
                return;
            }

            // create new route object, in memory classes
            Route r = new Route(new InMemorySchema(defs));

            // actually do some routing
            BookKeeper bk = r.requestLfn(lfn);

            // what is it to be, xml or text
            Writer bw = new BufferedWriter(new PrintWriter(System.out));
            if (wantText) {
                if (bk != null) bk.toString(bw);
                else bw.write("# no jobs generated" + System.getProperty("line.separator", "\r\n"));
            } else {
                if (bk != null) bk.toXML(bw, "");
                else
                    bw.write(
                            "<!-- no jobs generated -->"
                                    + System.getProperty("line.separator", "\r\n"));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
