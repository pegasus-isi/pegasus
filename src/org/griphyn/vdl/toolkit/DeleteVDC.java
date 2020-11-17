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

package org.griphyn.vdl.toolkit;

import edu.isi.pegasus.common.util.Version;
import gnu.getopt.*;
import java.io.*;
import java.sql.SQLException;
import java.util.*;
import org.griphyn.vdl.classes.*;
import org.griphyn.vdl.dbschema.*;
import org.griphyn.vdl.directive.*;
import org.griphyn.vdl.parser.*;
import org.griphyn.vdl.util.ChimeraProperties;
import org.griphyn.vdl.util.Logging;

/**
 * This class deletes definition's that match the namespace, identifier, version triple.
 *
 * @author Jens-S. Vöckler
 * @author Yong Zhao
 * @version $Revision$
 * @see org.griphyn.vdl.parser.VDLxParser
 */
public class DeleteVDC extends Toolkit {
    /** Constructor */
    public DeleteVDC(String appName) {
        super(appName);
    }

    /** Print the usage string */
    public void showUsage() {
        String linefeed = System.getProperty("line.separator", "\r\n");

        System.out.println(
                "$Id$" + linefeed + "VDS version " + Version.instance().toString() + linefeed);

        System.out.println(
                "Usage: "
                        + this.m_application
                        + " [-d db] [-t tr|dv] [-f] [-n ns] [-i id] [-v vs] [-c fn]");

        System.out.println(
                linefeed
                        + "Options: "
                        + linefeed
                        + " -V|--version    print version information and exit."
                        + linefeed
                        + " -d|--dbase db   associates the dbname with the database, unused."
                        + linefeed
                        + "    --verbose    increases the verbosity level."
                        + linefeed
                        + " -t|--type tr|dv limits candidates to either TR or DV, default is both."
                        + linefeed
                        + " -n|--vdlns ns   limits candidates to definition namespace matches."
                        + linefeed
                        + " -i|--vdlid id   limits candidates to definition name matches."
                        + linefeed
                        + " -v|--vdlvs vs   limits candidates to definition version matches."
                        + linefeed
                        + " -f|--force      permits removals in batch mode, assuming wildcards."
                        + linefeed
                        + "                 Beware, without any other options, everything will be removed!"
                        + linefeed
                        + " -c|--capture fn captures removed definitions into file fn."
                        + linefeed);
    }

    /** Creates a set of options. */
    protected LongOpt[] generateValidOptions() {
        LongOpt[] lo = new LongOpt[16];

        lo[0] = new LongOpt("dbase", LongOpt.REQUIRED_ARGUMENT, null, 'd');
        lo[1] = new LongOpt("version", LongOpt.NO_ARGUMENT, null, 'V');
        lo[2] = new LongOpt("help", LongOpt.NO_ARGUMENT, null, 'h');
        lo[3] = new LongOpt("verbose", LongOpt.NO_ARGUMENT, null, 1);

        lo[4] = new LongOpt("vdlns", LongOpt.REQUIRED_ARGUMENT, null, 'n');
        lo[5] = new LongOpt("namespace", LongOpt.REQUIRED_ARGUMENT, null, 'n');
        lo[6] = new LongOpt("ns", LongOpt.REQUIRED_ARGUMENT, null, 'n');

        lo[7] = new LongOpt("vdlid", LongOpt.REQUIRED_ARGUMENT, null, 'i');
        lo[8] = new LongOpt("name", LongOpt.REQUIRED_ARGUMENT, null, 'i');
        lo[9] = new LongOpt("identifier", LongOpt.REQUIRED_ARGUMENT, null, 'i');
        lo[10] = new LongOpt("id", LongOpt.REQUIRED_ARGUMENT, null, 'i');

        lo[11] = new LongOpt("vdlvs", LongOpt.REQUIRED_ARGUMENT, null, 'v');
        lo[12] = new LongOpt("vs", LongOpt.REQUIRED_ARGUMENT, null, 'v');

        lo[13] = new LongOpt("type", LongOpt.REQUIRED_ARGUMENT, null, 't');
        lo[14] = new LongOpt("force", LongOpt.NO_ARGUMENT, null, 'f');
        lo[15] = new LongOpt("capture", LongOpt.REQUIRED_ARGUMENT, null, 'c');

        return lo;
    }

    /** Delete definition's given the triple: namespace, id and version. */
    public static void main(String[] args) {
        String arg;
        Writer capfile = null;
        int classType = -1;
        String vdlns = null;
        String vdlid = null;
        String vdlvs = null;
        boolean force = false;

        DeleteVDC me = new DeleteVDC("deletevdc");

        try {
            // obtain commandline options first -- we may need the database stuff
            Getopt opts =
                    new Getopt(
                            me.m_application, args, "c:d:fhi:n:t:v:V", me.generateValidOptions());
            opts.setOpterr(false);
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

                    case 'c':
                        arg = opts.getOptarg();
                        if (arg != null && arg.length() != 0) {
                            // yes, risk IOException, will be caught at bottom
                            capfile = new BufferedWriter(new FileWriter(arg));
                        }
                        break;

                    case 'd':
                        // currently inactive option
                        opts.getOptarg();
                        break;

                    case 'f':
                        force = !force;
                        break;

                    case 'i':
                        vdlid = opts.getOptarg();
                        break;

                    case 'n':
                        vdlns = opts.getOptarg();
                        break;

                    case 't':
                        // type, must be TR or DV (or T or D)
                        arg = opts.getOptarg();
                        switch (Character.toUpperCase(arg.charAt(0))) {
                            case 'D': // derivation
                                classType = Definition.DERIVATION;
                                break;
                            case 'T': // transformation
                                classType = Definition.TRANSFORMATION;
                                break;
                            default:
                                me.showUsage();
                                throw new RuntimeException(
                                        "invalid argument \"" + arg + "\" for option t");
                        }
                        break;

                    case 'v':
                        vdlvs = opts.getOptarg();
                        break;

                    case '?':
                        System.out.println("Invalid option '" + (char) opts.getOptopt() + "'");
                    default:
                    case 'h':
                        me.showUsage();
                        return;
                }
            }

            // Connect the database.
            String schemaName = ChimeraProperties.instance().getVDCSchemaName();

            Connect connect = new Connect();
            DatabaseSchema dbschema = connect.connectDatabase(schemaName);

            // Search the database, delete the matched definitions
            me.m_logger.log("app", 1, "Searching the database");
            Delete delete = new Delete(dbschema);
            java.util.List defList =
                    delete.deleteDefinition(vdlns, vdlid, vdlvs, classType, capfile, force);

            if (defList.isEmpty()) {
                me.m_logger.log("app", 1, "no matching definitions in the database");
            } else {
                me.m_logger.log("app", 1, "removed " + defList.size() + " definitions");
            }

            try {
                if (capfile != null) {
                    capfile.flush();
                    capfile.close();
                }
            } catch (IOException e) {
                me.m_logger.log("default", 0, "I/O error: " + e.getMessage() + ", ignoring");
            }

            // done
            if (dbschema != null) dbschema.close();

        } catch (SQLException sql) {
            // database problems
            for (int i = 0; sql != null; ++i) {
                Logging.instance()
                        .log(
                                "default",
                                0,
                                "SQL error "
                                        + i
                                        + ": "
                                        + sql.getErrorCode()
                                        + ": "
                                        + sql.getMessage());
                sql = sql.getNextException();
            }
            System.exit(1);

        } catch (IOException e) {
            me.m_logger.log("default", 0, "I/O error");
            System.err.println(e.getMessage());
            System.exit(1);
        } catch (RuntimeException rte) {
            me.m_logger.log("default", 0, "runtime error");
            System.err.println(rte.getMessage());
            System.exit(1);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }
}
