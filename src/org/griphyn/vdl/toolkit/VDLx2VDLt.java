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
import org.griphyn.vdl.classes.*;
import org.griphyn.vdl.directive.VDLxConvert;
import org.griphyn.vdl.parser.*;

/**
 * This class uses the <code>VDLxParser</code> to parse VDL XML specification and output VDL textual
 * specification.
 *
 * @author Jens-S. Vöckler
 * @author Yong Zhao
 * @version $Revision$
 * @see org.griphyn.vdl.parser.VDLxParser
 */
public class VDLx2VDLt extends Toolkit {
    VDLx2VDLt(String appName) {
        super(appName);
    }

    public void showUsage() {
        String linefeed = System.getProperty("line.separator", "\r\n");
        System.out.println(
                "$Id$" + linefeed + "VDS version " + Version.instance().toString() + linefeed);

        System.out.println(
                "Usage: "
                        + this.m_application
                        + " [-v] VDLxFile VDLtFile"
                        + linefeed
                        + "   or: "
                        + this.m_application
                        + " [-v] VDLxFile > VDLtFile"
                        + linefeed
                        + "   or: "
                        + this.m_application
                        + " [-v] < VDLxFile > VDLtFile"
                        + linefeed
                        + "   or: "
                        + this.m_application
                        + " -V");

        System.out.println(
                linefeed
                        + "Generic options: "
                        + linefeed
                        + " -V|--version     print version information and exit."
                        + linefeed
                        + " -v|--verbose     verbose mode, print parser details on stdout."
                        + linefeed);
    }

    /** Creates a set of long options. */
    protected LongOpt[] generateValidOptions() {
        LongOpt[] lo = new LongOpt[3];

        lo[0] = new LongOpt("version", LongOpt.NO_ARGUMENT, null, 'V');
        lo[1] = new LongOpt("verbose", LongOpt.NO_ARGUMENT, null, 'v');
        lo[2] = new LongOpt("help", LongOpt.NO_ARGUMENT, null, 'h');

        return lo;
    }

    /** the works */
    public static void main(String[] args) {
        try {
            VDLx2VDLt me = new VDLx2VDLt("vdlx2vdlt");

            // obtain commandline options first -- we may need the database stuff
            Getopt opts = new Getopt(me.m_application, args, "hvV", me.generateValidOptions());
            opts.setOpterr(false);
            int option = 0;
            while ((option = opts.getopt()) != -1) {
                switch (option) {
                    case 'V':
                        System.out.println("$Id$");
                        System.out.println("VDS version " + Version.instance().toString());
                        return;

                    case 'v':
                        option = me.increaseVerbosity();
                        if (option == 1) me.m_logger.register("parser", System.out, option);
                        else me.m_logger.setLevel("parser", option);
                        break;

                    case '?':
                        System.out.println("Invalid option '" + (char) opts.getOptopt() + "'");
                    default:
                    case 'h':
                        me.showUsage();
                        return;
                }
            }

            // work
            Writer wr = null;
            Reader rd = null;
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

            VDLxConvert convert = new VDLxConvert();
            convert.VDLx2VDLt(rd, wr);
            rd.close();
            wr.flush();
            wr.close();

        } catch (RuntimeException rte) {
            System.err.println("Runtime error: " + rte.getMessage());
            System.exit(1);
        } catch (Exception e) {
            System.err.println("Stumbled over " + e.getMessage());
            e.printStackTrace(System.err);
            System.exit(1);
        }
    }
}
