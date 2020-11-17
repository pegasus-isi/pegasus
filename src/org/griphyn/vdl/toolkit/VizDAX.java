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
import java.util.*;
import org.griphyn.vdl.dax.*;
import org.griphyn.vdl.directive.Display;

/**
 * This class generates GraphViz dot format from a DAX
 *
 * @author Jens-S. Vöckler
 * @author Yong Zhao
 * @version $Revision$
 */
public class VizDAX extends Toolkit {
    /** ctor: Constructs a new instance object with the given application name. */
    public VizDAX(String appName) {
        super(appName);
    }

    /** Implements printing the usage string onto stdout. */
    public void showUsage() {
        String linefeed = System.getProperty("line.separator", "\r\n");

        System.out.println(
                "$Id$" + linefeed + "VDS version " + Version.instance().toString() + linefeed);

        System.out.println("Usage: " + this.m_application + " [general] [-f] [-o dot] [dax]");

        System.out.println(
                linefeed
                        + " -V|--version      print version information and exit."
                        + linefeed
                        + " -v|--verbose      increases the verbosity level."
                        + linefeed
                        + " -f|--files        if present, show input and output files in graph."
                        + linefeed
                        + " -d|--dv           if present, also show the DV names in graph."
                        + linefeed
                        + " -o|--output dot   put the output into the file dot, defaults to stdout."
                        + linefeed
                        + " dax               reads the specified dax file; if absent, uses stdin."
                        + linefeed);
    }

    /** Creates a set of options. */
    protected LongOpt[] generateValidOptions() {
        LongOpt[] lo = new LongOpt[6];

        lo[0] = new LongOpt("help", LongOpt.NO_ARGUMENT, null, 'h');
        lo[1] = new LongOpt("version", LongOpt.NO_ARGUMENT, null, 'V');
        lo[2] = new LongOpt("verbose", LongOpt.NO_ARGUMENT, null, 'v');

        lo[3] = new LongOpt("files", LongOpt.NO_ARGUMENT, null, 'f');
        lo[4] = new LongOpt("output", LongOpt.REQUIRED_ARGUMENT, null, 'o');
        lo[5] = new LongOpt("dv", LongOpt.NO_ARGUMENT, null, 'd');

        return lo;
    }

    /** Get the DAX and generate GraphViz dot representation. */
    public static void main(String[] args) {
        int result = 0;

        try {
            VizDAX me = new VizDAX("dax2dot");

            // get the commandline options
            Getopt opts = new Getopt(me.m_application, args, "hdfo:V", me.generateValidOptions());
            opts.setOpterr(false);
            boolean files = false;
            boolean showDV = false;
            String arg = null;
            String dot = null;

            int option = 0;
            while ((option = opts.getopt()) != -1) {
                switch (option) {
                    case 'V':
                        System.out.println("$Id$");
                        System.out.println("VDS version " + Version.instance().toString());
                        return;

                    case 'd':
                        showDV = true;
                        break;

                    case 'f':
                        files = true;
                        break;

                    case 'o':
                        arg = opts.getOptarg();
                        if (arg != null && arg.length() > 0) dot = arg;
                        break;

                    case 'v':
                        me.increaseVerbosity();
                        break;

                    case 'h':
                    default:
                        me.showUsage();
                        return;
                }
            }

            Display display = new Display();
            String daxfn = (opts.getOptind() < args.length ? args[opts.getOptind()] : null);

            BufferedInputStream input = null;
            if (daxfn == null || daxfn.equals("-")) {
                input = new BufferedInputStream(System.in);
                System.err.println("# reminder: reading from stdin");
            } else {
                input = new BufferedInputStream(new FileInputStream(daxfn));
            }

            BufferedWriter output =
                    new BufferedWriter(
                            (dot == null || dot.equals("-"))
                                    ? new OutputStreamWriter(System.out)
                                    : // convert stream to writer
                                    new FileWriter(dot));

            display.setShowDV(showDV);
            display.DAX2DOT(input, output, files);
            output.flush();
            if (dot != null) output.close();
        } catch (RuntimeException rte) {
            System.err.println("ERROR: " + rte.getMessage());
            result = 1;
        } catch (IOException ioe) {
            System.err.println("ERROR: " + ioe.getMessage());
            result = 1;
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("FATAL: " + e.getMessage());
            result = 2;
        }

        if (result != 0) System.exit(result);
    }
}
