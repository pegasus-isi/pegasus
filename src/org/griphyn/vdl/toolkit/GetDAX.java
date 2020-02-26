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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.StringTokenizer;
import org.griphyn.vdl.classes.*;
import org.griphyn.vdl.dbschema.*;
import org.griphyn.vdl.directive.*;
import org.griphyn.vdl.router.*;
import org.griphyn.vdl.util.ChimeraProperties;

/**
 * This class generates the DAX per the request for an lfn or a derivation.
 *
 * @author Jens-S. Vöckler
 * @author Yong Zhao
 * @version $Revision$
 * @see org.griphyn.vdl.router.Route
 * @see org.griphyn.vdl.router.BookKeeper
 */
public class GetDAX extends Toolkit {
    /** ctor: Constructs a new instance object with the given application name. */
    public GetDAX(String appName) {
        super(appName);
    }

    /** Implements printing the usage string onto stdout. */
    public void showUsage() {
        String linefeed = System.getProperty("line.separator", "\r\n");

        System.out.println(
                "$Id$" + linefeed + "VDS version " + Version.instance().toString() + linefeed);

        System.out.println(
                "Usage: "
                        + this.m_application
                        + ' '
                        + "[-d dbx] [-l label] [-m max] [-o DAX] -n ns -i id -v ver"
                        + linefeed
                        + "   or: "
                        + this.m_application
                        + ' '
                        + "[-d dbx] [-l label] [-m max] [-o DAX] -f f1[,..] -F lof"
                        + linefeed
                        + "       -D d1[,..] -L lod");

        System.out.println(
                linefeed
                        + "Generic options: "
                        + linefeed
                        + " -V|--version      print version information and exit."
                        + linefeed
                        + " -d|--dbase dbx    associates the dbname with the database, unused."
                        + linefeed
                        + "    --verbose      increases the verbosity level."
                        + linefeed
                        + " -l|--label label  uses the specified label for the output DAX, default \"test\"."
                        + linefeed
                        + " -o|--output DAX   writes the generated results into outfn, default is stdout."
                        + linefeed
                        + " -m|--maxdepth max only search up to the specified depth, default is "
                        + Route.MAXIMUM_DEPTH
                        + "."
                        + linefeed
                        + "                   For complex or large graphs, you may want to increase this."
                        + linefeed
                        + " -X|--xmlns prefix uses an XML namespace prefix for the generated DAX document."
                        + linefeed
                        + linefeed
                        + "Group 1: At least one of these must be used, but none of group 2 below:"
                        + linefeed
                        + " -n|--namespace ns uses namespace ns to search for matching DVs, default null."
                        + linefeed
                        + " -i|--name name    uses the specified DV name to search for it, required."
                        + linefeed
                        + " -v|--ver vs       uses the specified version to narrow DVs, default null."
                        + linefeed
                        + linefeed
                        + "Group 2: All of these may be mixed, but not used with group 1 above:"
                        + linefeed
                        + " -f|--file fn[,..] requests LFN or a list of LFNs to be materialized."
                        + linefeed
                        + " -f|--lfn          is a synonym for the --file option."
                        + linefeed
                        + " -F|--filelist lof read the LFNs from file lolfn, one per line."
                        + linefeed
                        + " -D|--dv dv[,..]   requests the DV or list of DVs to be produced. Each argument"
                        + linefeed
                        + "                   is a fully-qualified derivation name namespace::name:version"
                        + linefeed
                        + "                   with the usual omission rules applying."
                        + linefeed
                        + " -D|--derivation   is a synonym for the --dv option."
                        + linefeed
                        + " -L|--dvlist lodvs read the DVs from file lodvs, one per line."
                        + linefeed);

        System.out.println(
                "The following exit codes are produced:"
                        + linefeed
                        + " 0  :-)  Success"
                        + linefeed
                        + " 1  :-|  Empty result while "
                        + m_application
                        + " still ran successfully."
                        + linefeed
                        + " 2  :-(  Runtime error detected by "
                        + m_application
                        + ", please read the message."
                        + linefeed
                        + " 3  8-O  Fatal error merits a program abortion. Please carefully check your"
                        + linefeed
                        + "         configuration files and setup before filing a bug report."
                        + linefeed);
    }

    /** Creates a set of options. */
    protected LongOpt[] generateValidOptions() {
        LongOpt[] lo = new LongOpt[20];

        lo[0] = new LongOpt("dbase", LongOpt.REQUIRED_ARGUMENT, null, 'd');
        lo[1] = new LongOpt("version", LongOpt.NO_ARGUMENT, null, 'V');
        lo[2] = new LongOpt("help", LongOpt.NO_ARGUMENT, null, 'h');
        lo[3] = new LongOpt("verbose", LongOpt.NO_ARGUMENT, null, 1);

        lo[4] = new LongOpt("label", LongOpt.REQUIRED_ARGUMENT, null, 'l');
        lo[5] = new LongOpt("output", LongOpt.REQUIRED_ARGUMENT, null, 'o');
        lo[6] = new LongOpt("maxdepth", LongOpt.REQUIRED_ARGUMENT, null, 'm');
        lo[7] = new LongOpt("xmlns", LongOpt.REQUIRED_ARGUMENT, null, 'X');

        lo[8] = new LongOpt("namespace", LongOpt.REQUIRED_ARGUMENT, null, 'n');
        lo[9] = new LongOpt("ns", LongOpt.REQUIRED_ARGUMENT, null, 'n');
        lo[10] = new LongOpt("name", LongOpt.REQUIRED_ARGUMENT, null, 'i');
        lo[11] = new LongOpt("id", LongOpt.REQUIRED_ARGUMENT, null, 'i');
        lo[12] = new LongOpt("ver", LongOpt.REQUIRED_ARGUMENT, null, 'v');
        lo[13] = new LongOpt("vs", LongOpt.REQUIRED_ARGUMENT, null, 'v');

        lo[14] = new LongOpt("file", LongOpt.REQUIRED_ARGUMENT, null, 'f');
        lo[15] = new LongOpt("lfn", LongOpt.REQUIRED_ARGUMENT, null, 'f');
        lo[16] = new LongOpt("filelist", LongOpt.REQUIRED_ARGUMENT, null, 'F');
        lo[17] = new LongOpt("derivation", LongOpt.REQUIRED_ARGUMENT, null, 'D');
        lo[18] = new LongOpt("dv", LongOpt.REQUIRED_ARGUMENT, null, 'D');
        lo[19] = new LongOpt("dvlist", LongOpt.REQUIRED_ARGUMENT, null, 'L');

        return lo;
    }

    /** Get the request for a dv or an lfn and generate the corresponding DAX. */
    public static void main(String[] args) {
        int result = 0;
        int verbose = 0;
        Writer bw = null;
        DatabaseSchema dbschema = null;
        GetDAX me = null;

        try {
            me = new GetDAX("gendax");
            if (args.length == 0) {
                me.showUsage();
                return;
            }

            // get the commandline options
            Getopt opts =
                    new Getopt(
                            me.m_application,
                            args,
                            "hd:m:n:i:v:o:f:l:D:F:L:VX:",
                            me.generateValidOptions());
            opts.setOpterr(false);

            boolean flag = false;
            String xmlns = null;
            String arg = null;
            String label = null;
            String dbase = null;
            String ns = null;
            String id = null;
            String vs = null;
            String fn = null;
            int maxdepth = -1;
            ArrayList derivations = new ArrayList();
            ArrayList filenames = new ArrayList();

            int option = 0;
            while ((option = opts.getopt()) != -1) {
                switch (option) {
                    case 1:
                        verbose++;
                        me.increaseVerbosity();
                        break;

                    case 'V':
                        System.out.println("$Id$");
                        System.out.println("VDS version " + Version.instance().toString());
                        return;

                    case 'X':
                        xmlns = opts.getOptarg();
                        break;

                    case 'D':
                        arg = opts.getOptarg();
                        if (arg != null) {
                            StringTokenizer st = new StringTokenizer(arg, ",", false);
                            while (st.hasMoreTokens()) derivations.add(st.nextToken());
                        }
                        break;

                    case 'd':
                        dbase = opts.getOptarg();
                        break;

                    case 'F':
                        arg = opts.getOptarg();
                        if (arg != null) {
                            int nr = me.readFile(arg, filenames);
                            me.m_logger.log("app", 1, "read " + nr + " LFNs from " + arg);
                        }
                        break;

                    case 'f':
                        arg = opts.getOptarg();
                        if (arg != null) {
                            StringTokenizer st = new StringTokenizer(arg, ",", false);
                            while (st.hasMoreTokens()) filenames.add(st.nextToken());
                        }
                        break;

                    case 'L':
                        arg = opts.getOptarg();
                        if (arg != null) {
                            int nr = me.readFile(arg, derivations);
                            me.m_logger.log("app", 1, "read " + nr + " DV names from " + arg);
                        }
                        break;

                    case 'l':
                        arg = opts.getOptarg();
                        if (arg != null) label = arg;
                        break;

                    case 'm':
                        arg = opts.getOptarg();
                        if (arg != null) maxdepth = Integer.valueOf(arg).intValue();
                        break;

                    case 'n':
                        arg = opts.getOptarg();
                        if (arg != null) {
                            ns = arg;
                            flag = true;
                        }
                        break;

                    case 'i':
                        arg = opts.getOptarg();
                        if (arg != null) {
                            id = arg;
                            flag = true;
                        }
                        break;

                    case 'v':
                        arg = opts.getOptarg();
                        if (arg != null) {
                            vs = arg;
                            flag = true;
                        }
                        break;

                    case 'o':
                        arg = opts.getOptarg();
                        if (arg != null) fn = arg;
                        break;

                    case 'h':
                        me.showUsage();
                        return;

                    default:
                        me.showUsage();
                        throw new RuntimeException("Incorrect option or option usage");
                }
            }

            // you can use either the pattern match, or -D and -f
            int group2 = derivations.size() + filenames.size();
            if (!flag && group2 == 0) {
                me.showUsage();
                throw new RuntimeException(
                        "You must specify either group 1 (-n -i -v) or group 2 (-f -F -L -D) options!");
            }

            // bug#90/91: warn about mixing options groups
            if (flag && group2 > 0) {
                me.m_logger.log(
                        "default", 0, "Warning: You are mixing group 1 and group 2 options!");
            }

            // bug#90/91: require name in group1 options.
            if (flag && (id == null || id.length() == 0)) {
                throw new RuntimeException("Group 1 options require -i|--name");
            }

            // Connect the database.
            String schemaName = ChimeraProperties.instance().getVDCSchemaName();

            Connect connect = new Connect();
            dbschema = connect.connectDatabase(schemaName);
            Explain explain = new Explain(dbschema);

            // constraint on search
            if (maxdepth >= 0) explain.setMaximumDepth(maxdepth);

            // Make the request.
            if (flag) explain.requestDerivation(ns, id, vs);
            if (filenames.size() > 0) explain.requestLFN(filenames);
            if (derivations.size() > 0) {
                int size = derivations.size();
                int count = 1;
                for (Iterator i = derivations.iterator(); i.hasNext(); count++) {
                    String what = (String) i.next();
                    me.m_logger.log(
                            "app", 2, "requesting " + what + " (" + count + "/" + size + ")");
                    explain.requestDerivation(what);
                }
            }

            // show the DAX
            if (fn != null) {
                // save to file
                me.m_logger.log("app", 1, "saving output to " + fn);
                bw = new BufferedWriter(new FileWriter(fn));
            } else {
                // somebody may have redirected stdout, thus buffer!
                bw = new BufferedWriter(new PrintWriter(System.out));
            }

            // write resulting DAX
            explain.writeDAX(bw, label, xmlns);
            bw.flush();
            bw.close();

            // fail on empty requested?
            if (explain.isEmpty()) {
                System.err.println("ERROR: The resulting DAX is empty");
                result = 1;
            }

            dbschema.close();
        } catch (IOException ioe) {
            System.err.println("IO ERROR: " + ioe.getMessage());
            if (verbose > 0) ioe.printStackTrace(System.err);
            result = 2;
        } catch (RuntimeException rte) {
            System.err.println("RT ERROR: " + rte.getMessage());
            if (verbose > 0) rte.printStackTrace(System.err);
            result = 2;
        } catch (Exception e) {
            e.printStackTrace(System.err);
            System.err.println("FATAL: " + e.getMessage());
            result = 3;
        }
        if (result != 0) System.exit(result);
    }
}
