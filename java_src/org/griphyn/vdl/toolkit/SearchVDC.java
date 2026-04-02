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

import edu.isi.pegasus.common.util.Separator;
import edu.isi.pegasus.common.util.Version;
import gnu.getopt.*;
import java.io.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.StringTokenizer;
import org.griphyn.vdl.classes.*;
import org.griphyn.vdl.dbschema.*;
import org.griphyn.vdl.directive.*;
import org.griphyn.vdl.util.ChimeraProperties;

/**
 * This class searches definitions that match the namespace, name, version triple, then prints the
 * search results in one of the formats: vdlx, vdlt, or name. Alternatively, it permits to search
 * from a list of inputs, either DVs or LFNs.
 *
 * @author Jens-S. Vöckler
 * @author Yong Zhao
 * @version $Revision$
 * @see org.griphyn.vdl.parser.VDLxParser
 */
public class SearchVDC extends Toolkit {
    /** Constructor */
    public SearchVDC(String appName) {
        super(appName);
    }

    /** Prints the usage string onto stdout. */
    public void showUsage() {
        String linefeed = System.getProperty("line.separator", "\r\n");

        System.out.println(
                "$Id$" + linefeed + "VDS version " + Version.instance().toString() + linefeed);

        System.out.println(
                "Usage: "
                        + this.m_application
                        + " [general] [-t tr|dv] [[-n ns] [-i id] [-v vs] | -D dv] [-L lod]"
                        + linefeed
                        + "   or: "
                        + this.m_application
                        + " [general] [-t i|o|io] [-f lfn | -F lof]");

        System.out.println(
                linefeed
                        + "General options:"
                        + linefeed
                        + " -V|--version       print version information and exit."
                        + linefeed
                        + " -d|--dbase dbx     associates the dbname with the database, unused."
                        + linefeed
                        + "    --verbose       increases the verbosity level."
                        + linefeed
                        + " -l|--list x|t|n    print x:VDLx, t:VDLt or just a table of n:names."
                        + linefeed
                        + " -e|--error         if present, return failure for a combined empty result set."
                        + linefeed
                        + " -E|--empty         if present and bundling, raise an error for any empty match."
                        + linefeed
                        + " -o|--output fn     put the output into the file fn, default is stdout."
                        + linefeed
                        + linefeed
                        + "Group 1: Search for definitions"
                        + linefeed
                        + " -t|--type tr|dv|all Search only for TR or DV, default is all (both)."
                        + linefeed
                        + " -n|--namespace ns  Search for matches with namespace ns, default wildcard."
                        + linefeed
                        + " -i|--name id       Search for matches with name id, default wildcard."
                        + linefeed
                        + " -v|--ver vs        Search for matches with version vs, default wildcard."
                        + linefeed
                        + " -D|--def dv[,..]   Search for matches of the complete FQDI (see manpage)."
                        + linefeed
                        + " -L|--deflist lodvs read definition FQDIs from file lodvs, one per line."
                        + linefeed
                        + linefeed
                        + "Group 2: Search for logical filenames"
                        + linefeed
                        + " -t|--type i|o|io   Limit search to (i)n, (o)ut or (io) filenames."
                        + linefeed
                        + " -f|--lfn lfn       Limit search to filename lfn, default wildcard."
                        + linefeed
                        + " -F|--filelist lof  read the LFNs from file lof, one per line."
                        + linefeed
                        + linefeed
                        + "The following exit codes are produced:"
                        + linefeed
                        + " 0  :-)  Success"
                        + linefeed
                        + " 1  :-|  Runtime error detected, please read the message."
                        + linefeed
                        + " 2  :-?  empty result detected, and escalation (-e, -E) requested."
                        + linefeed
                        + " 3  8-O  Fatal error merits a program abortion. Please carefully check your"
                        + linefeed
                        + "         configuration files and setup before filing a bug report."
                        + linefeed);
    }

    /** Creates a set of options. */
    protected LongOpt[] generateValidOptions() {
        LongOpt[] lo = new LongOpt[21];

        lo[0] = new LongOpt("dbase", LongOpt.REQUIRED_ARGUMENT, null, 'd');
        lo[1] = new LongOpt("version", LongOpt.NO_ARGUMENT, null, 'V');
        lo[2] = new LongOpt("help", LongOpt.NO_ARGUMENT, null, 'h');
        lo[3] = new LongOpt("verbose", LongOpt.NO_ARGUMENT, null, 1);

        lo[4] = new LongOpt("list", LongOpt.REQUIRED_ARGUMENT, null, 'l');
        lo[5] = new LongOpt("error", LongOpt.NO_ARGUMENT, null, 'e');
        lo[6] = new LongOpt("output", LongOpt.REQUIRED_ARGUMENT, null, 'o');

        lo[7] = new LongOpt("type", LongOpt.REQUIRED_ARGUMENT, null, 't');
        lo[8] = new LongOpt("namespace", LongOpt.REQUIRED_ARGUMENT, null, 'n');
        lo[9] = new LongOpt("ns", LongOpt.REQUIRED_ARGUMENT, null, 'n');
        lo[10] = new LongOpt("name", LongOpt.REQUIRED_ARGUMENT, null, 'i');
        lo[11] = new LongOpt("id", LongOpt.REQUIRED_ARGUMENT, null, 'i');
        lo[12] = new LongOpt("ver", LongOpt.REQUIRED_ARGUMENT, null, 'v');
        lo[13] = new LongOpt("vs", LongOpt.REQUIRED_ARGUMENT, null, 'v');

        lo[14] = new LongOpt("file", LongOpt.REQUIRED_ARGUMENT, null, 'f');
        lo[15] = new LongOpt("lfn", LongOpt.REQUIRED_ARGUMENT, null, 'f');

        lo[16] = new LongOpt("def", LongOpt.REQUIRED_ARGUMENT, null, 'D');
        lo[17] = new LongOpt("definition", LongOpt.REQUIRED_ARGUMENT, null, 'D');
        lo[18] = new LongOpt("deflist", LongOpt.REQUIRED_ARGUMENT, null, 'L');
        lo[19] = new LongOpt("filelist", LongOpt.REQUIRED_ARGUMENT, null, 'F');
        lo[20] = new LongOpt("empty", LongOpt.NO_ARGUMENT, null, 'E');

        return lo;
    }

    /** search the database for specific TR's or DV's */
    public static void main(String[] args) {
        String groupError = "different option groups cannot be combined";
        boolean emptyFailure = false;
        boolean emptyBundle = false;
        boolean seenResults = false;
        int result = 0;

        try {
            SearchVDC me = new SearchVDC("searchvdc");

            // get the commandline options
            Getopt opts =
                    new Getopt(
                            me.m_application,
                            args,
                            "D:EL:F:Vd:ef:hi:l:n:o:t:v:",
                            me.generateValidOptions());
            opts.setOpterr(false);

            int state = -1; // -1: undef, 0: dv(s), 1: lfn(s)
            String dbase = null;
            String ns = null;
            String id = null;
            String vs = null;
            String arg;

            String outfn = null;
            int classType = -1;
            int linkage = -1;
            int outputFormat = Search.FORMAT_FQDN;

            ArrayList definitions = new ArrayList();
            ArrayList filenames = new ArrayList();
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

                    case 'D':
                        if (state != -1 && state != 0) {
                            me.showUsage();
                            throw new RuntimeException(groupError);
                        } else {
                            arg = opts.getOptarg();
                            if (arg != null) {
                                state = 0;
                                StringTokenizer st = new StringTokenizer(arg, ",", false);
                                while (st.hasMoreTokens()) definitions.add(st.nextToken());
                            }
                        }
                        break;

                    case 'E':
                        emptyBundle = true;
                        break;

                    case 'F':
                        if (state != -1 && state != 1) {
                            me.showUsage();
                            throw new RuntimeException(groupError);
                        } else {
                            arg = opts.getOptarg();
                            if (arg != null) {
                                state = 1;
                                int nr = me.readFile(arg, filenames);
                                me.m_logger.log("app", 1, "read " + nr + " LFNs from " + arg);
                            }
                        }
                        break;

                    case 'L':
                        if (state != -1 && state != 0) {
                            me.showUsage();
                            throw new RuntimeException(groupError);
                        } else {
                            arg = opts.getOptarg();
                            if (arg != null) {
                                state = 0;
                                int nr = me.readFile(arg, definitions);
                                me.m_logger.log("app", 1, "read " + nr + " DEF names from " + arg);
                            }
                        }
                        break;

                    case 'd':
                        dbase = opts.getOptarg();
                        break;

                    case 'e':
                        emptyFailure = true;
                        break;

                    case 'f':
                        if (state != -1 && state != 1) {
                            me.showUsage();
                            throw new RuntimeException(groupError);
                        } else {
                            arg = opts.getOptarg();
                            if (arg != null) {
                                state = 1;
                                StringTokenizer st = new StringTokenizer(arg, ",", false);
                                while (st.hasMoreTokens()) filenames.add(st.nextToken());
                            }
                        }
                        break;

                    case 'i':
                        if (state != -1 && state != 0) {
                            me.showUsage();
                            throw new RuntimeException(groupError);
                        } else {
                            arg = opts.getOptarg();
                            if (arg != null) {
                                id = arg;
                                state = 0;
                            }
                        }
                        break;

                    case 'n':
                        if (state != -1 && state != 0) {
                            me.showUsage();
                            throw new RuntimeException(groupError);
                        } else {
                            arg = opts.getOptarg();
                            if (arg != null) {
                                ns = arg;
                                state = 0;
                            }
                        }
                        break;

                    case 'o':
                        arg = opts.getOptarg();
                        if (arg != null) outfn = arg;
                        break;

                    case 'v':
                        if (state != -1 && state != 0) {
                            me.showUsage();
                            throw new RuntimeException(groupError);
                        } else {
                            arg = opts.getOptarg();
                            if (arg != null) {
                                vs = arg;
                                state = 0;
                            }
                        }
                        break;

                    case 'l':
                        arg = opts.getOptarg().toLowerCase();
                        if (arg != null) {
                            switch (arg.charAt(0)) {
                                case 'x':
                                    outputFormat = Search.FORMAT_VDLX;
                                    break;
                                case 't':
                                    outputFormat = Search.FORMAT_VDLT;
                                    break;
                                default:
                                    outputFormat = Search.FORMAT_FQDN;
                                    break;
                            }
                        }
                        break;

                    case 't':
                        arg = opts.getOptarg().toLowerCase();
                        if (arg != null) {
                            switch (arg.charAt(0)) {
                                case 'a':
                                case 'b':
                                    if (state != -1 && state != 0) {
                                        me.showUsage();
                                        throw new RuntimeException(groupError);
                                    } else {
                                        state = 0;
                                        classType = -1; // both, all
                                    }
                                    break;
                                case 'd':
                                    if (state != -1 && state != 0) {
                                        me.showUsage();
                                        throw new RuntimeException(groupError);
                                    } else {
                                        state = 0;
                                        classType = Definition.DERIVATION;
                                    }
                                    break;
                                case 't':
                                    if (state != -1 && state != 0) {
                                        me.showUsage();
                                        throw new RuntimeException(groupError);
                                    } else {
                                        state = 0;
                                        classType = Definition.TRANSFORMATION;
                                    }
                                    break;
                                case 'i':
                                    if (state != -1 && state != 1) {
                                        me.showUsage();
                                        throw new RuntimeException(groupError);
                                    } else {
                                        state = 1;
                                        linkage = arg.equals("io") ? LFN.INOUT : LFN.INPUT;
                                    }
                                    break;
                                case 'o':
                                    if (state != -1 && state != 1) {
                                        me.showUsage();
                                        throw new RuntimeException(groupError);
                                    } else {
                                        state = 1;
                                        linkage = arg.equals("oi") ? LFN.INOUT : LFN.OUTPUT;
                                    }
                                    break;
                                default:
                                    me.showUsage();
                                    throw new RuntimeException(
                                            "Invalid value \"" + arg + "\" for option -t");
                            }
                        }
                        break;

                    case 'h':
                    default:
                        me.showUsage();
                        return;
                }
            }

            //      me.m_logger.log( "app", 3, filenames.size() + " LFNs and " +
            //		       definitions.size() + " DEFs, state=" + state );

            // Connect the database.
            String schemaName = ChimeraProperties.instance().getVDCSchemaName();

            Connect connect = new Connect();
            DatabaseSchema dbschema = connect.connectDatabase(schemaName);

            // Search the database.
            me.m_logger.log("app", 2, "searching the database");
            java.util.List resultList = new ArrayList();
            Search search = new Search(dbschema);

            java.util.List bundle = null;
            if (state == 1) {
                // looking for LFNs
                if (filenames.isEmpty()) {
                    // wildcard LFN search
                    me.m_logger.log("app", 3, "wildcard search requested");
                    bundle = search.searchDefinition(null, linkage);
                    resultList.addAll(bundle);
                } else {
                    for (Iterator i = filenames.iterator(); i.hasNext(); ) {
                        String lfn = (String) i.next();
                        bundle = search.searchDefinition(lfn, linkage);

                        if (bundle.isEmpty() && emptyBundle) {
                            dbschema.close();
                            throw new FriendlyNudge("no definitions found for \"" + lfn + "\"", 2);
                        } else {
                            //	      me.m_logger.log( "app", 3, "found " + bundle.size() +
                            //			       " matches for " + lfn );
                            resultList.addAll(bundle);
                        }
                    }
                }
            } else {
                // looking for definitions
                if (definitions.isEmpty()) {
                    // wildcard definition search
                    me.m_logger.log("app", 3, "wildcard search requested");
                    if (dbschema instanceof Advanced)
                        bundle = search.searchDefinitionEx(ns, id, vs, classType);
                    else bundle = search.searchDefinition(ns, id, vs, classType);
                    resultList.addAll(bundle);

                } else {
                    for (Iterator i = definitions.iterator(); i.hasNext(); ) {
                        String def = (String) i.next();
                        // String[] d = Separator.split( def );
                        String[] d = Separator.splitFQDI(def);
                        if (dbschema instanceof Advanced)
                            bundle =
                                    search.searchDefinitionEx(
                                            (d[0] == null ? ns : d[0]),
                                            (d[1] == null ? id : d[1]),
                                            (d[2] == null ? vs : d[2]),
                                            classType);
                        else
                            bundle =
                                    search.searchDefinition(
                                            (d[0] == null ? ns : d[0]),
                                            (d[1] == null ? id : d[1]),
                                            (d[2] == null ? vs : d[2]),
                                            classType);

                        if (bundle.isEmpty() && emptyBundle) {
                            dbschema.close();
                            throw new FriendlyNudge("no definitions found for \"" + def + "\"", 2);
                        } else {
                            //	      me.m_logger.log( "app", 3, "found " + bundle.size() +
                            //			       " matches for " + def );
                            resultList.addAll(bundle);
                        }
                    }
                }
            }
            me.m_logger.log("app", 1, "found " + resultList.size() + " matches total");

            if (!resultList.isEmpty()) {
                seenResults = true;
                Writer writer;
                if (outfn != null) {
                    // save to file
                    me.m_logger.log("app", 1, "Saving to the file " + outfn);
                    writer = new BufferedWriter(new FileWriter(outfn));
                } else {
                    writer = new PrintWriter(System.out);
                }

                search.printDefinitionList(writer, resultList, outputFormat);
                writer.close();
            } else {
                me.m_logger.log("app", 1, "no results");
            }

            // done
            dbschema.close();
        } catch (FriendlyNudge fn) {
            System.err.println(fn.getMessage());
            result = fn.getResult();
        } catch (IOException ioe) {
            System.err.println("ERROR: " + ioe.getMessage());
            result = 1;
        } catch (RuntimeException rte) {
            System.err.println("ERROR: " + rte.getMessage());
            result = 1;
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("FATAL: " + e.getMessage());
            result = 3;
        }

        // fail on empty result set, if requested
        if (emptyFailure & !seenResults) result = 2;

        if (result != 0) System.exit(result);
    }
}
