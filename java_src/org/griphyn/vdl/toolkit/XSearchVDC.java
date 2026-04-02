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
import org.griphyn.vdl.classes.*;
import org.griphyn.vdl.dbschema.*;
import org.griphyn.vdl.directive.*;
import org.griphyn.vdl.util.ChimeraProperties;

/**
 * This class searches definition's that match the namespace, name, version triple, then prints the
 * search results in one of the formats: vdlx, vdlt, or name.
 *
 * @author Jens-S. Vöckler
 * @author Yong Zhao
 * @version $Revision$
 * @see org.griphyn.vdl.parser.VDLxParser
 */
public class XSearchVDC extends Toolkit {
    /** Constructor */
    public XSearchVDC(String appName) {
        super(appName);
    }

    /** Prints the usage string. */
    public void showUsage() {
        String linefeed = System.getProperty("line.separator", "\r\n");

        System.out.println(
                "$Id$" + linefeed + "VDS version " + Version.instance().toString() + linefeed);

        System.out.println(
                "Usage: "
                        + this.m_application
                        + " [general] [-t tr|dv] [-n ns] [-i id] [-v vs]"
                        + linefeed
                        + "   or: "
                        + this.m_application
                        + " [general] [-t i|o|io] -f lfn"
                        + linefeed
                        + "   or: "
                        + this.m_application
                        + " [general] -q query qargs");

        System.out.println(
                linefeed
                        + "General options:"
                        + linefeed
                        + " -V|--version      print version information and exit."
                        + linefeed
                        + " -d|--dbase dbx    associates the dbname with the database, unused."
                        + linefeed
                        + "    --verbose      increases the verbosity level."
                        + linefeed
                        + " -l|--list x|t|n   print x:VDLx, t:VDLt or just a table of n:names."
                        + linefeed
                        + " -e|--error        if present, return failure for an empty search."
                        + linefeed
                        + " -o|--output fn    put the output into the file fn, default is stdout."
                        + linefeed
                        + linefeed
                        + "Group 1: Searching for definitions"
                        + linefeed
                        + " -t|--type tr|dv   Search only for TR or DV, default is both."
                        + linefeed
                        + " -n|--namespace ns Search for matches with namespace ns, default wildcard."
                        + linefeed
                        + " -i|--name id      Search for matches with name id, default wildcard."
                        + linefeed
                        + " -v|--ver vs       Search for matches with version vs, default wildcard."
                        + linefeed
                        + linefeed
                        + "Group 2: Searching for logical filenames"
                        + linefeed
                        + " -t|--type i|o|io  Limit search to (i)n, (o)ut or (io) filenames."
                        + linefeed
                        + " -f|--lfn lfn      Limit search to filename lfn, default wildcard."
                        + linefeed
                        + linefeed
                        + "Group 3: Searching by ... what? ... Yong?"
                        + linefeed
                        + " -q|--query query  ????"
                        + linefeed);
    }

    /** Creates a set of options. */
    protected LongOpt[] generateValidOptions() {
        LongOpt[] lo = new LongOpt[17];

        lo[0] = new LongOpt("help", LongOpt.NO_ARGUMENT, null, 'h');
        lo[1] = new LongOpt("dbase", LongOpt.REQUIRED_ARGUMENT, null, 'd');
        lo[2] = new LongOpt("version", LongOpt.NO_ARGUMENT, null, 'V');
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
        lo[16] = new LongOpt("query", LongOpt.REQUIRED_ARGUMENT, null, 'q');

        return lo;
    }

    /** search the database for specific TR's or DV's */
    public static void main(String[] args) {
        int result = 0;
        boolean emptyFailure = false;
        boolean seenResults = false;

        try {
            XSearchVDC me = new XSearchVDC("xsearchvdc");

            // get the commandline options
            Getopt opts =
                    new Getopt(
                            me.m_application,
                            args,
                            "hd:l:ef:q:o:t:n:i:v:V",
                            me.generateValidOptions());
            opts.setOpterr(false);

            String dbase = null;
            String ns = null;
            String name = null;
            String ver = null;
            String lfn = null;
            String outfn = null;
            String format = null;
            String t = null;
            String query = null;

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

                    case 'd':
                        dbase = opts.getOptarg();
                        break;

                    case 'e':
                        emptyFailure = true;
                        break;

                    case 'f':
                        lfn = opts.getOptarg();
                        break;

                    case 'i':
                        name = opts.getOptarg();
                        break;

                    case 'l':
                        format = opts.getOptarg().toLowerCase();
                        break;

                    case 'n':
                        ns = opts.getOptarg();
                        break;

                    case 'o':
                        outfn = opts.getOptarg();
                        break;

                    case 'q':
                        query = opts.getOptarg();
                        break;

                    case 't':
                        t = opts.getOptarg().toLowerCase();
                        break;

                    case 'v':
                        ver = opts.getOptarg();
                        break;

                    case 'h':
                    default:
                        me.showUsage();
                        return;
                }
            }

            boolean condition1 = (lfn != null);
            boolean condition2 = (ns != null || name != null || ver != null);
            boolean condition3 = (query != null);

            if ((condition1 && (condition2 || condition3)) || (condition2 && condition3)) {
                me.showUsage();
                throw new RuntimeException(
                        "ERROR: you must either specify the -n -i -v options, the -f option, or the -q option!");
            }

            int link = -1;
            int clsType = -1;
            if (condition1) {
                // to search for input/output/inout file?
                if (t != null) {
                    if (t.equals("i")) link = LFN.INPUT;
                    else if (t.equals("o")) link = LFN.OUTPUT;
                    else if (t.equals("io")) link = LFN.INOUT;
                    else {
                        System.err.println("Invalid value \"" + t + "\" for option -t");
                        me.showUsage();
                        System.exit(1);
                    }
                }
            } else if (condition2) {
                // to search for tr/dv, or both
                if (t != null) {
                    char c = t.charAt(0);
                    if (c == 'd') clsType = Definition.DERIVATION;
                    else if (c == 't') clsType = Definition.TRANSFORMATION;
                    else {
                        System.err.println("Invalid value \"" + t + "\" for option -t");
                        me.showUsage();
                        System.exit(1);
                    }
                }
            }

            // get the output format, default is "n"
            if (format == null) format = "n";
            int f = Search.FORMAT_FQDN;
            switch (format.charAt(0)) {
                case 'x':
                    f = Search.FORMAT_VDLX;
                    break;
                case 't':
                    f = Search.FORMAT_VDLT;
                    break;
                case 'n':
                    f = Search.FORMAT_FQDN;
                    break;
                default:
                    System.err.println("Invalid value \"" + format + "\" for option -l");
                    me.showUsage();
                    System.exit(1);
            }

            // Connect the database.
            String schemaName = ChimeraProperties.instance().getVDCSchemaName();

            Connect connect = new Connect();
            DatabaseSchema dbschema = connect.connectDatabase(schemaName);

            // Search the database.
            me.m_logger.log("app", 1, "searching the database");
            java.util.List defList = null;
            Search search = new Search(dbschema);

            if (condition1) {
                // looking for lfn
                defList = search.searchDefinition(lfn, link);
            } else if (condition3) {
                StringBuffer xquery = new StringBuffer(256);
                int start = opts.getOptind();
                int arg_len = args.length;
                if (dbschema instanceof XDC) {
                    if (query.equals("tr_dv")) {
                        xquery.append("for $t in //transformation[@name='");
                        xquery.append(args[start]);
                        xquery.append("'] return //derivation[@uses=$t/@name]");
                    } else if (query.equals("dv_tr")) {
                        xquery.append("for $t in //derivation[@name='");
                        xquery.append(args[start]);
                        xquery.append("'] return //transformation[@name=$t/@uses]");
                    } else if (query.equals("tr_call")) {
                        xquery.append("//transformation[call/@uses='");
                        xquery.append(args[start]);
                        xquery.append("']");
                    } else if (query.equals("tr_para")) {
                        xquery.append("//transformation");
                        int i = start;
                        while (i < arg_len) {
                            xquery.append("[declare[@name='").append(args[i]).append("']");
                            i++;
                            if (i < arg_len && !args[i].equals("any")) {
                                xquery.append("[@link='").append(args[i]).append("']");
                            }
                            i++;
                            xquery.append("]");
                        }
                    } else if (query.equals("tr_arg")) {
                        xquery.append("//transformation[contains(argument/*, '");
                        xquery.append(args[start]);
                        xquery.append("')]");
                    } else if (query.equals("dv_pass")) {
                        xquery.append("//derivation[contains(pass//*, '");
                        xquery.append(args[start]);
                        xquery.append("')]");
                    } else if (query.equals("tr_meta")) {
                        xquery.append("for $m in //metadata[@subject='tr'][attribute[@name='");
                        xquery.append(args[start]);
                        xquery.append("']");

                        xquery.append("='").append(args[start + 1]).append("']");
                        xquery.append(
                                " let $mn:=$m/@name, $n := substring-before($mn, '::'), $na := substring-after($mn, '::'), $iv := if ($na) then $na else $mn, $v := substring-after($iv, ':'), $ib := substring-before($iv, ':'), $i := if ($ib) then $ib else $iv,");
                        xquery.append(
                                " $t := if ($n) then if ($v) then //transformation[@namespace=$n][@name=$i][@version=$v] else //transformation[@namespace=$n][@name=$i][empty(@version)] else if ($v) then //transformation[empty(@namespace)][@name=$i][@version=$v] else //transformation[empty(@namespace)][@name=$i][empty(@version)]");
                        xquery.append(" return $t[declare[@link='");
                        xquery.append(args[start + 2]);
                        xquery.append("'][@name = $m/@select]]");
                    } else if (query.equals("dv_tree")) {
                        xquery.append("declare namespace v='http://www.griphyn.org/chimera';");
                        xquery.append(
                                "declare function v:dv_tree($lfn as xs:string) as element()* {");
                        xquery.append("let $d := //derivation[.//lfn[@file=$lfn][@link='input']]");
                        xquery.append("return ( $d,");
                        xquery.append(
                                "for $out in $d//lfn[@link='output']/@file  return v:dv_tree($out))");
                        xquery.append("};");
                        xquery.append("let $d := v:dv_tree('")
                                .append(args[start])
                                .append("') return $d");
                    } else if (query.equals("lfn_tree")) {
                        xquery.append("declare namespace v='http://www.griphyn.org/chimera';");
                        xquery.append(
                                "declare function v:lfn_tree($lfn as xs:string) as item()* {");
                        xquery.append("let $d := //derivation[.//lfn[@file=$lfn][@link='input']]");
                        xquery.append("return ( $lfn,");
                        xquery.append(
                                "for $out in $d//lfn[@link='output']/@file  return v:lfn_tree($out))");
                        xquery.append("};");
                        xquery.append("let $f := v:lfn_tree('").append(args[start]);
                        xquery.append("') return distinct-values($f)");
                    } else if (query.equals("lfn_meta")) {
                        xquery.append("for $m in //metadata[@subject='lfn']");
                        int i = start;
                        while (i < arg_len) {
                            xquery.append("[attribute[@name='").append(args[i]).append("']");
                            i++;
                            xquery.append("='").append(args[i]).append("']");
                            i++;
                        }
                        xquery.append(" return $m/@name");
                    } else {
                        xquery.append(query);
                    }

                    if (query.equals("lfn_meta") || query.equals("lfn_tree"))
                        defList = ((XDC) dbschema).searchElements(xquery.toString());
                    else defList = ((XDC) dbschema).searchDefinition(xquery.toString());
                }
            } else {
                // looking for definitions
                if (dbschema instanceof Advanced)
                    defList = search.searchDefinitionEx(ns, name, ver, clsType);
                else defList = search.searchDefinition(ns, name, ver, clsType);
            }

            if (defList != null && !defList.isEmpty()) {
                seenResults = true;
                Writer bw;
                if (outfn != null) {
                    // save to file
                    me.m_logger.log("app", 1, "Saving to the file " + outfn);
                    bw = new BufferedWriter(new FileWriter(outfn));
                } else {
                    bw = new PrintWriter(System.out);
                }

                if (query != null && (query.equals("lfn_meta") || query.equals("lfn_tree")))
                    for (Iterator i = defList.iterator(); i.hasNext(); )
                        bw.write((String) i.next() + "\n");
                else search.printDefinitionList(bw, defList, f);
                bw.close();
            } else {
                me.m_logger.log("app", 1, "no results");
            }

            // done
            dbschema.close();

        } catch (RuntimeException rte) {
            System.err.println("ERROR: " + rte.getMessage());
            result = 1;
        } catch (Exception e) {
            e.printStackTrace();
            result = 2;
        }

        // fail on empty result set, if requested
        if (emptyFailure & !seenResults) result = 2;
        if (result != 0) System.exit(result);
    }
}
