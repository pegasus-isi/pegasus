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

package org.griphyn.vdl.toolkit;

import edu.isi.pegasus.common.util.Version;
import gnu.getopt.*;
import java.io.*;
import java.util.Iterator;
import org.griphyn.vdl.annotation.*;
import org.griphyn.vdl.classes.*;
import org.griphyn.vdl.dbschema.*;
import org.griphyn.vdl.directive.*;
import org.griphyn.vdl.util.ChimeraProperties;
import org.griphyn.vdl.util.Logging;

/**
 * This class searches definitions or LFNs which match a query.
 *
 * @author Jens-S. Vöckler
 * @author Yong Zhao
 * @version $Revision$
 * @see org.griphyn.vdl.parser.VDLxParser
 */
public class SearchMeta extends Toolkit {
    /** Constructor */
    public SearchMeta(String appName) {
        super(appName);
    }

    /** Prints the usage string. */
    public void showUsage() {
        String linefeed = System.getProperty("line.separator", "\r\n");

        System.out.println(
                "$Id$" + linefeed + "VDS version " + Version.instance().toString() + linefeed);

        System.out.println(
                "Usage: " + this.m_application + " [-d db] -t tr|dv|lfn [-a arg] query_file");

        System.out.println(
                linefeed
                        + " -V|--version    print version information and exit."
                        + linefeed
                        + " -d|--dbase db   associates the dbname with the database, unused."
                        + linefeed
                        + "    --verbose    increases the verbosity level."
                        + linefeed
                        + " -t|--type type  limits candidates to either TR, DV, or LFN."
                        + linefeed
                        + " -a|--arg  arg   limits to formal arguments, requires -t tr option?"
                        + linefeed);
    }

    /**
     * Creates a set of options.
     *
     * @return the assembled long option list
     */
    protected LongOpt[] generateValidOptions() {
        LongOpt[] lo = new LongOpt[7];

        lo[0] = new LongOpt("dbase", LongOpt.REQUIRED_ARGUMENT, null, 'd');
        lo[1] = new LongOpt("version", LongOpt.NO_ARGUMENT, null, 'V');
        lo[2] = new LongOpt("help", LongOpt.NO_ARGUMENT, null, 'h');
        lo[3] = new LongOpt("verbose", LongOpt.NO_ARGUMENT, null, 1);

        lo[4] = new LongOpt("arg", LongOpt.REQUIRED_ARGUMENT, null, 'a');
        lo[5] = new LongOpt("args", LongOpt.REQUIRED_ARGUMENT, null, 'a');
        lo[6] = new LongOpt("type", LongOpt.REQUIRED_ARGUMENT, null, 't');

        return lo;
    }

    /** Searches the database for specific TR's or DV's */
    public static void main(String[] args) {
        int result = 0;

        try {
            SearchMeta me = new SearchMeta("searchmeta");

            // obtain commandline options first -- we may need the database stuff
            Getopt opts = new Getopt(me.m_application, args, "hd:t:a:V", me.generateValidOptions());

            opts.setOpterr(false);
            String arg = null;
            String type = null;
            int option = 0;
            while ((option = opts.getopt()) != -1) {
                switch (option) {
                    case 1:
                        me.increaseVerbosity();
                        break;

                    case 'V':
                        System.out.println("$Id$");
                        System.out.println("VDS version " + Version.instance().toString());
                        return;

                    case 'a':
                        arg = opts.getOptarg();
                        break;

                    case 't':
                        type = opts.getOptarg();
                        break;

                    case 'h':
                    default:
                        me.showUsage();
                        return;
                }
            }

            // sanity check
            int classType = -1;
            if (type == null || type.length() == 0) {
                me.showUsage();
                throw new RuntimeException("You must specify a -t argument");
            } else {
                switch (Character.toUpperCase(type.charAt(0))) {
                    case 'D': // DV
                        classType = Annotation.CLASS_DERIVATION;
                        break;
                    case 'T': // TR
                        classType = Annotation.CLASS_TRANSFORMATION;
                        if (arg != null && arg.length() > 0) {
                            classType = Annotation.CLASS_DECLARE;
                        }
                        break;
                    case 'L': // LFN
                        classType = Annotation.CLASS_FILENAME;
                        break;
                    default:
                        me.showUsage();
                        throw new RuntimeException("Illegal value " + type + " for option -t");
                }
            }

            // Connect the database.
            String schemaName = ChimeraProperties.instance().getVDCSchemaName();
            Connect connect = new Connect();
            DatabaseSchema dbschema = connect.connectDatabase(schemaName);

            if (!(dbschema instanceof Annotation)) {
                dbschema.close();
                throw new RuntimeException("The database does not support metadata!");
            } else {
                // safe to cast now
                Annotation annotation = (Annotation) dbschema;

                // open query file -- or read from stdin
                String qfile = (opts.getOptind() < args.length ? args[opts.getOptind()] : null);
                LineNumberReader lnr = null;
                if (qfile == null || qfile.equals("-")) {
                    System.err.println("# reminder: reading from stdin");
                    lnr = new LineNumberReader(new InputStreamReader(System.in));
                } else {
                    lnr = new LineNumberReader(new FileReader(qfile));
                }

                QueryParser parser = new QueryParser(lnr);
                QueryTree tree = parser.parse();

                Logging.instance().log("app", 1, "searching the database");
                java.util.List list = annotation.searchAnnotation(classType, arg, tree);

                if (list != null && !list.isEmpty()) {
                    if (classType == Annotation.CLASS_FILENAME) {
                        for (Iterator i = list.iterator(); i.hasNext(); ) {
                            System.out.println((String) i.next());
                        }
                    } else {
                        for (Iterator i = list.iterator(); i.hasNext(); ) {
                            Definition def = (Definition) i.next();
                            System.out.println(def.identify());
                        }
                    }
                } else {
                    Logging.instance().log("app", 1, "no results");
                }
            }

            // done
            if (dbschema != null) dbschema.close();
        } catch (RuntimeException rte) {
            Logging.instance().log("default", 0, "runtime error: " + rte.getMessage());
            System.err.println("ERROR: " + rte.getMessage());
            result = 1;

        } catch (Exception e) {
            Logging.instance().log("default", 0, "FATAL: " + e.getMessage());
            e.printStackTrace();
            System.err.println("FATAL: " + e.getMessage());
            result = 2;
        }

        if (result != 0) System.exit(result);
    }
}
