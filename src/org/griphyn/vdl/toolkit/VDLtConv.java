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
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;
import org.griphyn.vdl.classes.*;
import org.griphyn.vdl.directive.VDLtConvert;
import org.griphyn.vdl.parser.*;

/**
 * Test calls to parse the a given filename and produce XML from it.
 *
 * @see org.griphyn.vdl.parser.VDLtParser
 */
public class VDLtConv extends Toolkit {
    /**
     * module local constructor for a toolkit application.
     *
     * @param appName is the name of the application to be displayed
     */
    VDLtConv(String appName) {
        super(appName);
    }

    public void showUsage() {
        String linefeed = System.getProperty("line.separator", "\r\n");

        System.out.println(
                "$Id$" + linefeed + "VDS version " + Version.instance().toString() + linefeed);

        System.out.println(
                "Usage: "
                        + this.m_application
                        + " [-n vdlns] [-v vdlvs] VDLt VDLx"
                        + linefeed
                        + "   or: "
                        + this.m_application
                        + " [-n vdlns] [-v vdlvs] VDLt > VDLx"
                        + linefeed
                        + "   or: "
                        + this.m_application
                        + " [-n vdlns] [-v vdlvs] < VDLt > VDLx");

        System.out.println(
                linefeed
                        + "Generic options: "
                        + linefeed
                        + " -V|--version     print version information and exit."
                        + linefeed
                        + "    --verbose     increases the verbosity level."
                        + linefeed
                        + " -n|--vdlns ns    generates default namespace ns, default is none."
                        + linefeed
                        + " -v|--vdlvs vs    geneartes default version vs, default is none."
                        + linefeed);
    }

    /** Creates a set of long options. */
    protected LongOpt[] generateValidOptions() {
        LongOpt[] lo = new LongOpt[7];

        lo[0] = new LongOpt("version", LongOpt.NO_ARGUMENT, null, 'V');
        lo[1] = new LongOpt("help", LongOpt.NO_ARGUMENT, null, 'h');
        lo[2] = new LongOpt("verbose", LongOpt.NO_ARGUMENT, null, 1);

        lo[3] = new LongOpt("vdlvs", LongOpt.REQUIRED_ARGUMENT, null, 'v');
        lo[4] = new LongOpt("vdlns", LongOpt.REQUIRED_ARGUMENT, null, 'n');
        lo[5] = new LongOpt("namespace", LongOpt.REQUIRED_ARGUMENT, null, 'n');
        lo[6] = new LongOpt("ns", LongOpt.REQUIRED_ARGUMENT, null, 'n');

        return lo;
    }

    public static void main(String args[]) {
        String vdlns = null;
        String vdlvs = null;
        org.griphyn.vdl.toolkit.VDLtConv me = new org.griphyn.vdl.toolkit.VDLtConv("vdlt2vdlx");

        try {
            // obtain commandline options first -- we may need the database stuff
            Getopt opts = new Getopt(me.m_application, args, "hn:v:V", me.generateValidOptions());
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

                    case 'n':
                        // default namespace option
                        vdlns = opts.getOptarg();
                        break;

                    case 'v':
                        // default version option
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

            Reader rd = null;
            Writer wr = null;
            int where = opts.getOptind();
            switch (args.length - where) {
                case 2:
                    wr = new BufferedWriter(new FileWriter(args[where + 1]));
                    rd = new BufferedReader(new FileReader(args[where + 0]));
                    break;
                case 1:
                    wr = new OutputStreamWriter(System.out);
                    rd = new BufferedReader(new FileReader(args[where]));
                    break;
                case 0:
                    System.err.println("# reminder: reading from stdin");
                    wr = new OutputStreamWriter(System.out);
                    rd = new InputStreamReader(System.in);
                    break;
                default:
                    me.showUsage();
                    throw new RuntimeException("Illegal number of non-option arguments");
            }

            VDLtConvert convert = new VDLtConvert();
            convert.VDLt2VDLx(rd, wr, vdlns, vdlvs);
            rd.close();
            wr.flush();
            wr.close();

        } catch (VDLtParserException e) {
            me.m_logger.log("default", 0, "syntactical error");
            System.err.println(e.getMessage());
            System.exit(1);
        } catch (VDLtScannerException e) {
            me.m_logger.log("default", 0, "lexical error");
            System.err.println(e.getMessage());
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
            me.m_logger.log("default", 0, "unspecified error ");
            System.err.println(e.getMessage());
            e.printStackTrace(System.err);
            System.exit(1);
        }
    }
}
