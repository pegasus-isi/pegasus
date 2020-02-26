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
import org.griphyn.vdl.directive.Derive;

/**
 * The shell planner. This class is the command-line tool for generating shell scripts from a DAX.
 *
 * @author Jens-S. Vöckler
 * @author Yong Zhao
 * @version $Revision$
 */
public class Planner extends Toolkit {
    /** Constructs a new instance object with the given application name. */
    public Planner(String appName) {
        super(appName);
    }

    /** Prints the usage string onto stdout. */
    public void showUsage() {
        String linefeed = System.getProperty("line.separator", "\r\n");

        System.out.println(
                "$Id$" + linefeed + "VDS version " + Version.instance().toString() + linefeed);

        System.out.println(
                "Usage: " + this.m_application + " [general] [-o dir] [-k fn] [-b] [-n] dax");

        System.out.println(
                linefeed
                        + " -V|--version      print version information and exit."
                        + linefeed
                        + " -v|--verbose      increases the verbosity level."
                        + linefeed
                        + " -o|--output dir   directory where scripts are created, defaults to test."
                        + linefeed
                        + " -b|--build        enters build mode, default is make mode."
                        + linefeed
                        + " -n|--no-register  does not register produced files at all."
                        + linefeed
                        + " -k|--kickstart fn uses kickstart from the specified location fn."
                        + linefeed);
    }

    /** Creates a set of commandline options. */
    protected LongOpt[] generateValidOptions() {
        LongOpt[] lo = new LongOpt[7];

        lo[0] = new LongOpt("help", LongOpt.NO_ARGUMENT, null, 'h');
        lo[1] = new LongOpt("version", LongOpt.NO_ARGUMENT, null, 'V');
        lo[2] = new LongOpt("verbose", LongOpt.NO_ARGUMENT, null, 'v');

        lo[3] = new LongOpt("build", LongOpt.NO_ARGUMENT, null, 'b');
        lo[4] = new LongOpt("output", LongOpt.REQUIRED_ARGUMENT, null, 'o');
        lo[5] = new LongOpt("no-register", LongOpt.NO_ARGUMENT, null, 'n');
        lo[6] = new LongOpt("kickstart", LongOpt.REQUIRED_ARGUMENT, null, 'k');

        return lo;
    }

    /**
     * Get the DAX and generate shell scripts in the designated directory. It also decides whether
     * to run in a 'make' or 'build' mode
     */
    public static void main(String[] args) {
        int result = 0;

        // register the logger for search
        try {
            Planner me = new Planner("shplanner");

            // get the commandline options
            Getopt opts =
                    new Getopt(me.m_application, args, "hbk:no:Vv", me.generateValidOptions());
            opts.setOpterr(false);
            String dir = "test"; // where to produce files, default is "test"
            boolean build = false;
            boolean register = true;
            String kickstart = null;
            int verbosity = 0;

            int option = 0;
            while ((option = opts.getopt()) != -1) {
                switch (option) {
                    case 'v':
                        verbosity = me.increaseVerbosity();
                        break;

                    case 'V':
                        System.out.println("$Id$");
                        System.out.println("Pegasus version " + Version.instance().toString());
                        return;

                    case 'b':
                        build = true;
                        break;

                    case 'k':
                        kickstart = opts.getOptarg();
                        break;

                    case 'o':
                        dir = opts.getOptarg();
                        break;

                    case 'n':
                        register = false;
                        break;

                    case 'h':
                    default:
                        me.showUsage();
                        return;
                }
            }

            // did the user specify exactly one DAX file?
            String daxfn = null;
            if (opts.getOptind() >= args.length || opts.getOptind() < args.length - 1)
                throw new RuntimeException("You must specify exactly one (1) input file");
            else daxfn = args[opts.getOptind()];

            // do something about verbosity
            if (verbosity > 0) me.m_logger.register("planner", System.out, verbosity);

            // the directory name to hold shell scripts
            if (dir == null || dir.length() == 0) {
                me.m_logger.log(
                        "planner", 0, "Output directory not specified, using default: test");
                dir = "test";
            }

            // check the dax file
            FileInputStream fs = new FileInputStream(daxfn);
            Derive derive = new Derive();

            if (!derive.genShellScripts(fs, dir, build, register, kickstart)) {
                me.m_logger.log("default", 0, "ERROR: Failed to generate shell scripts!");
                result = 1;
            }
        } catch (RuntimeException rte) {
            System.err.println(rte.getMessage());
            result = 1;
        } catch (Exception e) {
            e.printStackTrace();
            result = 2;
        }

        if (result != 0) System.exit(result);
    }
}
