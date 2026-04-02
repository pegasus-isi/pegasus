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
import java.util.Iterator;
import java.util.Set;
import org.griphyn.vdl.dbdriver.*;
import org.griphyn.vdl.dbschema.*;
import org.griphyn.vdl.directive.*;
import org.griphyn.vdl.parser.*;
import org.griphyn.vdl.util.ChimeraProperties;

/**
 * This class uses the <code>VDLxParser</code> to parse VDLx specifications and add them to the
 * database backend. If a definition already exists in the database, it is updated in overwrite
 * mode, or rejected in insert mode.
 *
 * @author Jens-S. Vöckler
 * @author Yong Zhao
 * @version $Revision$
 * @see org.griphyn.vdl.parser.VDLxParser
 */
public class UpdateVDC extends VDLHelper {
    /** writer to output rejected definitions */
    private Writer m_reject = null;

    /**
     * This initializies the class to handle inserts and updates, and optionally store the rejects
     * for later availability.
     *
     * @param appName is the true name of the application
     * @param reject is a writer to output rejected inserts.
     * @param level is the verbosity level for the "app" logging queue.
     */
    public UpdateVDC(String appName, Writer reject, int level) throws IOException {
        super(appName, level);
        m_reject = reject;
    }

    /** Closes the rejects file, if still open. */
    protected void finalize() throws Throwable {
        if (m_reject != null) m_reject.close();
        super.finalize();
    }

    /**
     * Prints the usage string.
     *
     * @param ow is the overwrite mode to distinguish between insert and update.
     */
    public static void showUsage(boolean ow) {
        String linefeed = System.getProperty("line.separator", "\r\n");

        System.out.println(
                "$Id$" + linefeed + "VDS version " + Version.instance().toString() + linefeed);

        System.out.println(
                "Usage: "
                        + (ow ? "update" : "insert")
                        + "vdc [general] [-r fn] vdlx|vdlt [vdlt|vdlx [..]]");

        System.out.println(
                linefeed
                        + " -V|--version      print version information and exit."
                        + linefeed
                        + " -d|--dbase dbx    associates the dbname with the database, unused."
                        + linefeed
                        + "    --verbose      increases the verbosity level."
                        + linefeed
                        + " -r|--rejects fn   gathers the rejected definitions into file fn."
                        + linefeed);
    }

    public void showUsage() {
        showUsage(m_application != null && m_application.charAt(0) == 'i');
    }

    /** Creates a set of options. */
    protected static LongOpt[] generateValidOptions() {
        LongOpt[] lo = new LongOpt[6];

        lo[0] = new LongOpt("help", LongOpt.NO_ARGUMENT, null, 'h');
        lo[1] = new LongOpt("dbase", LongOpt.REQUIRED_ARGUMENT, null, 'd');
        lo[2] = new LongOpt("version", LongOpt.NO_ARGUMENT, null, 'V');
        lo[3] = new LongOpt("verbose", LongOpt.NO_ARGUMENT, null, 1);

        lo[4] = new LongOpt("rejects", LongOpt.REQUIRED_ARGUMENT, null, 'r');
        lo[5] = new LongOpt("force", LongOpt.NO_ARGUMENT, null, 'f');
        return lo;
    }

    /** Add or update definition's from VDLx documents into the database. */
    public static void main(String[] args) {
        int result = 0;

        try {
            Getopt opts = new Getopt("(undef)", args, "Vhd:fr:", generateValidOptions());
            opts.setOpterr(false);

            Writer reject = null;
            String dbase = null; // unused option
            boolean overwrite = false; // --force
            String filename = null; // reject file

            int verbose = 0;
            int option = 0;
            while ((option = opts.getopt()) != -1) {
                switch (option) {
                    case 1:
                        ++verbose;
                        break;

                    case 'V':
                        System.out.println("$Id$");
                        System.out.println("VDS version " + Version.instance().toString());
                        return;

                    case 'd':
                        dbase = opts.getOptarg();
                        break;

                    case 'f':
                        // if force is not true, the program acts in insert mode,
                        // i.e. definition's already exist will not be overwritten.
                        overwrite = true;
                        break;

                    case 'r':
                        filename = opts.getOptarg();
                        break;

                    case 'h':
                    default:
                        showUsage(overwrite);
                        return;
                }
            }

            if (filename != null && filename.length() > 0)
                reject = new BufferedWriter(new FileWriter(filename));
            UpdateVDC me = new UpdateVDC(overwrite ? "updatevdc" : "insertvdc", reject, verbose);

            if (opts.getOptind() >= args.length) {
                // nothing to process -- what is this?
                showUsage(overwrite);
                throw new RuntimeException("You must specify at least one input file");
            }

            // Connect the database.
            String schemaName = ChimeraProperties.instance().getVDCSchemaName();
            Connect connect = new Connect();
            DatabaseSchema dbschema = connect.connectDatabase(schemaName);
            Define define = new Define(dbschema);
            // if ( derivations.isEmpty() ) define.setDerivationMemory( true );

            // Add definitions to the database backend while parsing
            Set badfile = me.addFilesToVDC(args, opts.getOptind(), define, reject, overwrite);
            int errcount = badfile.size();
            if (errcount > 0) {
                // previous section saw one or more errors, better give up.
                for (Iterator i = badfile.iterator(); i.hasNext(); ) {
                    System.err.println("errors in " + ((String) i.next()));
                }
                throw new RuntimeException(
                        "Detected "
                                + errcount
                                + " error"
                                + (errcount == 1 ? " " : "s ")
                                + "while parsing VDL");
            }

            if (reject != null) reject.close();
            int count = define.getNumberSaved();
            int rejected = define.getNumberRejected();

            me.m_logger.log(
                    "app", 1, "modified " + count + " definition" + (count == 1 ? "" : "s"));
            me.m_logger.log(
                    "app", 1, "rejected " + rejected + " definition" + (rejected == 1 ? "" : "s"));
            define.close();
        } catch (RuntimeException rte) {
            System.err.println("ERROR: " + rte.getMessage());
            result = 1;
        } catch (Exception e) {
            System.err.println("FATAL: " + e.getMessage());
            e.printStackTrace();
            result = 2;
        }

        // Java will return with 0 unless exit is used. Unfortunately, using
        // System.exit sometimes has some unwanted side-effects on d'tors,
        // thus avoid using it unless strictly necessary.
        if (result != 0) System.exit(result);
    }
}
