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
import org.griphyn.vdl.parser.DAXParser;
import org.griphyn.vdl.util.ChimeraProperties;
import org.griphyn.vdl.util.DAX2CoG;

/**
 * This class generates CoG XML from a DAX.
 *
 * @author Jens-S. Vöckler
 * @author Yong Zhao
 * @version $Revision$
 */
public class VizDAX2 extends Toolkit {
    /** ctor: Constructs a new instance object with the given application name. */
    public VizDAX2(String appName) {
        super(appName);
    }

    /** Implements printing the usage string onto stdout. */
    public void showUsage() {
        String linefeed = System.getProperty("line.separator", "\r\n");

        System.out.println(
                "$Id$" + linefeed + "VDS version " + Version.instance().toString() + linefeed);

        System.out.println("Usage: " + this.m_application + " [general] [-o cog] [dax]");

        System.out.println(
                linefeed
                        + " -V|--version      print version information and exit."
                        + linefeed
                        + " -v|--verbose      increases the verbosity level."
                        + linefeed
                        + " -o|--output cog   put the output into the file cog, defaults to stdout."
                        + linefeed
                        + " dax               reads the specified dax file; if absent, uses stdin."
                        + linefeed);
    }

    /**
     * Creates a set of options.
     *
     * @return the long option set.
     */
    protected LongOpt[] generateValidOptions() {
        LongOpt[] lo = new LongOpt[4];

        lo[0] = new LongOpt("help", LongOpt.NO_ARGUMENT, null, 'h');
        lo[1] = new LongOpt("version", LongOpt.NO_ARGUMENT, null, 'V');
        lo[2] = new LongOpt("verbose", LongOpt.NO_ARGUMENT, null, 'v');

        lo[3] = new LongOpt("output", LongOpt.REQUIRED_ARGUMENT, null, 'o');

        return lo;
    }

    /** Get the DAX and generate shell scripts in the designated directory. */
    public static void main(String[] args) {
        int result = 0;

        try {
            VizDAX2 me = new VizDAX2("dax2cog");
            // get the commandline options
            Getopt opts = new Getopt(me.m_application, args, "ho:V", me.generateValidOptions());
            opts.setOpterr(false);
            String arg = null;
            String dot = null;

            int option = 0;
            while ((option = opts.getopt()) != -1) {
                switch (option) {
                    case 'V':
                        System.out.println("$Id$");
                        System.out.println("VDS version " + Version.instance().toString());
                        return;

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

            // there is exactly one mandatory argument
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
                            dot == null || dot.equals("-")
                                    ? new OutputStreamWriter(System.out)
                                    : // convert stream to writer
                                    new FileWriter(dot));

            // start the DAX parser
            me.m_logger.log("app", 2, "Initializing DAX parser");
            DAXParser daxparser =
                    new DAXParser(ChimeraProperties.instance().getDAXSchemaLocation());

            // parse the file
            me.m_logger.log("app", 1, "Parsing the DAX file " + daxfn);
            ADAG adag = daxparser.parse(input);
            if (adag == null) {
                // unable to parse
                throw new RuntimeException("Unable to parse the DAX file " + daxfn);
            }

            me.m_logger.log("app", 1, "dumping XML output");
            DAX2CoG.toString(output, adag);

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
