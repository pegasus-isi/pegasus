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
import java.util.StringTokenizer;
import java.util.regex.*;
import org.griphyn.vdl.classes.*;
import org.griphyn.vdl.dbschema.*;
import org.griphyn.vdl.directive.*;
import org.griphyn.vdl.parser.*;
import org.griphyn.vdl.router.*;
import org.griphyn.vdl.util.ChimeraProperties;

/**
 * This class generates implements the pipeline of the usual suspect from vdlt2vdlx via insert- or
 * updatevdc to gendax.
 *
 * @author Jens-S. Vöckler
 * @author Yong Zhao
 * @version $Revision$
 * @see org.griphyn.vdl.parser.VDLtParser
 * @see org.griphyn.vdl.parser.VDLxParser
 * @see org.griphyn.vdl.router.Route
 * @see org.griphyn.vdl.router.BookKeeper
 */
public class VDLc extends VDLHelper {
    /** ctor: Constructs a new instance object with the given application name. */
    public VDLc(String appName) {
        super(appName);
    }

    /** Implements printing the usage string onto stdout. */
    public void showUsage() {
        String linefeed = System.getProperty("line.separator", "\r\n");

        System.out.println(
                "$Id$" + linefeed + "VDS version " + Version.instance().toString() + linefeed);

        System.out.println("Usage: " + this.m_application + ' ' + "[options] vdlt|vdlx [..]");

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
                        + " -n|--vdlns ns     generates default namespace ns, default is none."
                        + linefeed
                        + " -v|--vdlvs vs     generates default version vs, default is none."
                        + linefeed
                        + " -i|--insert       insert into the VDC instead of trying to update."
                        + linefeed
                        + " -r|--rejects fn   gathers the rejected definitions into file fn."
                        + linefeed
                        + " -l|--label label  uses the specified label for the output DAX."
                        + linefeed
                        + " -o|--output DAX   writes the generated results into outfn, use - for stdout."
                        + linefeed
                        + " -e|--empty        empty output DAX is not an error."
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
                        + "Request refining arguments: "
                        + linefeed
                        + " -D|--dv dv[,..]   requests the DV or list of DVs to be produced. Each argument"
                        + linefeed
                        + "                   is a fully-qualified derivation name namespace::name:version."
                        + linefeed
                        + "                   In absence of this argument, all DVs seen will be requested."
                        + linefeed
                        + " -D|--derivation   is a synonym for the --dv option."
                        + linefeed
                        + " -L|--dvlist lodvs read the DVs from file lodvs, one per line."
                        + linefeed
                        + " -f|--file fn[,..] requests LFN or a list of LFNs to be materialized."
                        + linefeed
                        + " -f|--lfn          is a synonym for the --file option."
                        + linefeed
                        + " -F|--filelist lof read the LFNs from file lof, one per line."
                        + linefeed
                        + linefeed
                        + "Mandatory argument(s): "
                        + linefeed
                        + " vdlt              One or more files containing VDLt or VDLx."
                        + linefeed);

        System.out.println(
                "The following exit codes are produced:"
                        + linefeed
                        + " 0  :-)  Success"
                        + linefeed
                        + " 1  :-|  Empty result while "
                        + m_application
                        + " still ran successfully (but see --empty)."
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
        LongOpt[] lo =
                new LongOpt[] {
                    new LongOpt("dbase", LongOpt.REQUIRED_ARGUMENT, null, 'd'),
                    new LongOpt("version", LongOpt.NO_ARGUMENT, null, 'V'),
                    new LongOpt("help", LongOpt.NO_ARGUMENT, null, 'h'),
                    new LongOpt("verbose", LongOpt.NO_ARGUMENT, null, 1),
                    new LongOpt("empty", LongOpt.NO_ARGUMENT, null, 'e'),
                    new LongOpt("vdlvs", LongOpt.REQUIRED_ARGUMENT, null, 'v'),
                    new LongOpt("vdlns", LongOpt.REQUIRED_ARGUMENT, null, 'n'),
                    new LongOpt("rejects", LongOpt.REQUIRED_ARGUMENT, null, 'r'),
                    new LongOpt("insert", LongOpt.NO_ARGUMENT, null, 'i'),
                    new LongOpt("label", LongOpt.REQUIRED_ARGUMENT, null, 'l'),
                    new LongOpt("output", LongOpt.REQUIRED_ARGUMENT, null, 'o'),
                    new LongOpt("maxdepth", LongOpt.REQUIRED_ARGUMENT, null, 'm'),
                    new LongOpt("xmlns", LongOpt.REQUIRED_ARGUMENT, null, 'X'),
                    new LongOpt("derivation", LongOpt.REQUIRED_ARGUMENT, null, 'D'),
                    new LongOpt("dv", LongOpt.REQUIRED_ARGUMENT, null, 'D'),
                    new LongOpt("dvlist", LongOpt.REQUIRED_ARGUMENT, null, 'L'),
                    new LongOpt("file", LongOpt.REQUIRED_ARGUMENT, null, 'f'),
                    new LongOpt("lfn", LongOpt.REQUIRED_ARGUMENT, null, 'f'),
                    new LongOpt("filelist", LongOpt.REQUIRED_ARGUMENT, null, 'F')
                };
        return lo;
    }

    /**
     * Tries a stab at the basename w/o suffix of the filename.
     *
     * @param fn is the filename from the command-line args
     * @return a basename or null.
     */
    public String guessBasename(String fn) {
        String base = (new File(fn)).getName();
        if (base.length() == 0) return null;

        int dot = base.lastIndexOf('.');
        return (dot == -1 ? base : base.substring(0, dot));
    }

    /**
     * Tries a stab at the basename w/o suffix of the filename.
     *
     * @param fn is the filename from the command-line args
     * @return a basename or null.
     */
    public String guessDaxname(String fn) {
        String base = (new File(fn)).getName();
        if (base.length() == 0) return null;

        int dot = base.lastIndexOf('.');
        return (dot < 0 ? base : base.substring(0, dot)) + ".dax";
    }

    /**
     * Processes the VDLt file(s) into DAX file by requesting each DV found in the VDLt.
     *
     * @param args commandline arguments
     */
    public static void main(String[] args) {
        int result = 0;
        VDLc me = null;

        try {
            me = new VDLc("vdlc");
            if (args.length == 0) {
                me.showUsage();
                return;
            }

            // get the commandline options
            Getopt opts =
                    new Getopt(
                            me.m_application,
                            args,
                            "D:F:L:VX:hd:f:m:n:o:r:l:uv:e",
                            me.generateValidOptions());
            opts.setOpterr(false);

            String vdlns = null;
            String vdlvs = null;
            String xmlns = null;
            String label = null;
            String dbase = null;
            String outputFilename = null;
            int maxdepth = -1;
            String rejectFilename = null; // reject file
            boolean overwrite = true; // --force
            java.util.Set derivations = new java.util.HashSet();
            java.util.Set filenames = new java.util.HashSet();
            boolean emptydaxok = false;

            String arg = null;
            int option = 0;
            while ((option = opts.getopt()) != -1) {
                switch (option) {
                    case 1:
                        me.increaseVerbosity();
                        break;

                    case 'D':
                        arg = opts.getOptarg();
                        if (arg != null) {
                            StringTokenizer st = new StringTokenizer(arg, ",", false);
                            while (st.hasMoreTokens()) derivations.add(st.nextToken());
                        }
                        break;

                    case 'F':
                        arg = opts.getOptarg();
                        if (arg != null) {
                            int nr = me.readFile(arg, filenames);
                            me.m_logger.log("app", 1, "read " + nr + " LFNs from " + arg);
                        }
                        break;

                    case 'L':
                        arg = opts.getOptarg();
                        if (arg != null) {
                            int nr = me.readFile(arg, derivations);
                            me.m_logger.log("app", 1, "read " + nr + " DV names from " + arg);
                        }
                        break;

                    case 'V':
                        System.out.println("$Id$");
                        System.out.println("VDS version " + Version.instance().toString());
                        return;

                    case 'X':
                        xmlns = opts.getOptarg();
                        break;

                    case 'd':
                        dbase = opts.getOptarg();
                        break;

                    case 'f':
                        arg = opts.getOptarg();
                        if (arg != null) {
                            StringTokenizer st = new StringTokenizer(arg, ",", false);
                            while (st.hasMoreTokens()) filenames.add(st.nextToken());
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
                        // default namespace option
                        vdlns = opts.getOptarg();
                        break;

                    case 'o':
                        arg = opts.getOptarg();
                        if (arg != null) outputFilename = arg;
                        break;

                    case 'r':
                        rejectFilename = opts.getOptarg();
                        break;

                    case 'i':
                        // if update is not true, the program acts in insert mode,
                        // i.e. definition's already exist will not be overwritten.
                        overwrite = false;
                        break;

                    case 'v':
                        // default version option
                        vdlvs = opts.getOptarg();
                        break;

                    case 'e':
                        emptydaxok = true;
                        break;

                    case 'h':
                    default:
                        me.showUsage();
                        return;
                }
            }

            if (opts.getOptind() >= args.length) {
                // nothing to process -- what is this?
                me.showUsage();
                throw new RuntimeException("You must specify at least one input file");
            }

            // guesstimate a label and/or output filename
            if (label == null || outputFilename == null) {
                for (int i = opts.getOptind(); i < args.length; ++i) {

                    // bug#83.3
                    if (label == null) {
                        label = me.guessBasename(args[i]);
                        if (label != null)
                            me.m_logger.log("app", 0, "using label \"" + label + '"');
                    }

                    // bug#83.2
                    if (outputFilename == null) {
                        outputFilename = me.guessDaxname(args[i]);
                        if (outputFilename != null)
                            me.m_logger.log(
                                    "app", 0, "using output file \"" + outputFilename + '"');
                    }
                }
            }

            // rejects
            Writer reject = null;
            if (rejectFilename != null && rejectFilename.length() > 0)
                reject = new BufferedWriter(new FileWriter(rejectFilename));

            // Connect the database.
            String schemaName = ChimeraProperties.instance().getVDCSchemaName();
            Connect connect = new Connect();
            DatabaseSchema dbschema = connect.connectDatabase(schemaName);
            Define define = new Define(dbschema);
            if (derivations.isEmpty() && filenames.isEmpty()) define.setDerivationMemory(true);

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

            // request all derivations we encountered.
            if (derivations.isEmpty() && filenames.isEmpty())
                derivations = define.getDerivationMemory();
            define = null; // free memory
            // Runtime.getRuntime().gc(); // really free memory

            Explain explain = new Explain(dbschema);
            if (maxdepth >= 0) explain.setMaximumDepth(maxdepth);

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
            Writer bw = null;
            if (outputFilename != null && !outputFilename.equals("-")) {
                // save to file
                me.m_logger.log("app", 1, "saving output to " + outputFilename);
                bw = new BufferedWriter(new FileWriter(outputFilename));
            } else {
                // somebody may have redirected stdout, thus buffer!
                bw = new BufferedWriter(new PrintWriter(System.out));
            }

            // write resulting DAX
            explain.writeDAX(bw, label, xmlns);
            bw.flush();
            bw.close();

            // fail on empty requested?
            if (explain.isEmpty() && !emptydaxok) {
                System.err.println("ERROR: The resulting DAX is empty");
                result = 1;
            }

            dbschema.close();
        } catch (IOException ioe) {
            System.err.println("ERROR: " + ioe.getMessage());
            result = 2;
        } catch (RuntimeException rte) {
            System.err.println("ERROR: " + rte.getMessage());
            result = 2;
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("FATAL: " + e.getMessage());
            result = 3;
        }
        if (result != 0) System.exit(result);
    }
}
