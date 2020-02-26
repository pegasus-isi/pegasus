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
package edu.isi.pegasus.planner.client;

import edu.isi.pegasus.common.util.Currently;
import edu.isi.pegasus.common.util.Version;
import gnu.getopt.*;
import java.io.*;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import org.griphyn.vdl.dbschema.*;
import org.griphyn.vdl.directive.*;
import org.griphyn.vdl.toolkit.*;
import org.griphyn.vdl.util.ChimeraProperties;

/**
 * This class gets the exit code of a job from invocation record.
 *
 * @author Jens-S. Vöckler
 * @author Yong Zhao
 * @version $Revision$
 * @see org.griphyn.vdl.parser.InvocationParser
 * @see org.griphyn.vdl.dbschema.DatabaseSchema
 */
public class ExitCode extends Toolkit {
    /** Just a string to denote the short usage. */
    public static final String m_usage1 =
            "[-d dbprefix | -n | -N] [-e] [-f] [-i] [-v] [-l tag -m ISO] file [..]";

    /** ctor: Constructs a new instance object with the given application name. */
    public ExitCode(String appName) {
        super(appName);
    }

    /** Implements printing the usage string onto stdout. */
    public void showUsage() {
        String linefeed = System.getProperty("line.separator", "\r\n");

        System.out.println(
                "$Id$" + linefeed + "VDS version " + Version.instance().toString() + linefeed);

        System.out.println("Usage: " + this.m_application + ' ' + m_usage1);
        System.out.println(
                linefeed
                        + "Generic options: "
                        + linefeed
                        + " -d|--dbase dbx  associates the dbname with the database, unused."
                        + linefeed
                        + " -V|--version    print version information and exit."
                        + linefeed
                        + " -v|--verbose    verbose mode, switches on property vds.log.app on stdout."
                        + linefeed
                        + " -i|--ignore     ignore exit code for failure, just add to database."
                        + linefeed
                        + " -n|--noadd      extract the exit code, don\'t add to any database."
                        + linefeed
                        + " -N|--nofail     extract and add, but do not fail on db errors."
                        + linefeed
                        + " -e|--emptyfail  takes empty files to mean failure instead of ok."
                        + linefeed
                        + " -f|--fail       stops parsing on the first error found instead of going on."
                        + linefeed
                        + " -l|--label tag  attaches the tag string (32 char max) to all records."
                        + linefeed
                        + " -m|--mtime ISO  uses the ISO 8601 timestamp as WF mtime for all records."
                        + linefeed
                        + "                 Options -l and -m must be used in conjunction!"
                        + linefeed
                        + linefeed
                        + "The following exit codes are returned (except in -i mode):"
                        + linefeed
                        + "  0  remote application ran to conclusion with exit code zero."
                        + linefeed
                        + "  1  remote application concluded with a non-zero exit code."
                        + linefeed
                        + "  2  kickstart failed to start the remote application."
                        + linefeed
                        + "  3  remote application died on a signal, check database."
                        + linefeed
                        + "  4  remote application was suspended, should not happen."
                        + linefeed
                        + "  5  invocation record has an invalid state, unable to parse."
                        + linefeed
                        + "  6  illegal state, inform vds-support@griphyn.org."
                        + linefeed
                        + "  7  illegal state, stumbled over an exception, try --verbose for details"
                        + linefeed
                        + "  8  multiple 0..5 failures during parsing of multiple records"
                        + linefeed);
    }

    /** Creates a set of options. */
    protected LongOpt[] generateValidOptions() {
        LongOpt[] lo = new LongOpt[11];

        lo[0] = new LongOpt("help", LongOpt.NO_ARGUMENT, null, 'h');
        lo[1] = new LongOpt("dbase", LongOpt.REQUIRED_ARGUMENT, null, 'd');
        lo[2] = new LongOpt("version", LongOpt.NO_ARGUMENT, null, 'V');
        lo[3] = new LongOpt("verbose", LongOpt.NO_ARGUMENT, null, 'v');

        lo[4] = new LongOpt("ignore", LongOpt.NO_ARGUMENT, null, 'i');
        lo[5] = new LongOpt("noadd", LongOpt.NO_ARGUMENT, null, 'n');
        lo[6] = new LongOpt("nofail", LongOpt.NO_ARGUMENT, null, 'N');

        lo[7] = new LongOpt("emptyfail", LongOpt.NO_ARGUMENT, null, 'e');
        lo[8] = new LongOpt("fail", LongOpt.NO_ARGUMENT, null, 'f');
        lo[9] = new LongOpt("label", LongOpt.REQUIRED_ARGUMENT, null, 'l');
        lo[10] = new LongOpt("mtime", LongOpt.REQUIRED_ARGUMENT, null, 'm');

        return lo;
    }

    public void showThreads() {
        ThreadGroup tg = Thread.currentThread().getThreadGroup();
        while (tg.getParent() != null) tg = tg.getParent();
        tg.list();
    }

    public static void main(String[] args) {
        int result = 0;
        int verbose = 0;
        ExitCode me = null;
        boolean noDBase = false;
        boolean ignoreDBFail = false;

        boolean failOver = true;
        boolean emptyFail = false;
        boolean earlyFail = false;
        ParseKickstart pks = null;

        String wf_label = null;
        Date wf_mtime = null;
        int wf_flag = 0;

        try {
            me = new ExitCode("exitcode");
            if (args.length == 0) {
                me.showUsage();
                return;
            }

            // get the commandline options
            Getopt opts =
                    new Getopt(me.m_application, args, "d:hefil:m:nNvV", me.generateValidOptions());
            opts.setOpterr(false);
            int option = 0;
            while ((option = opts.getopt()) != -1) {
                switch (option) {
                    case 'V':
                        System.out.println("$Id$");
                        System.out.println("VDS version " + Version.instance().toString());
                        return;

                    case 'd':
                        // currently inactive option
                        opts.getOptarg();
                        break;

                    case 'e':
                        emptyFail = true;
                        break;

                    case 'f':
                        earlyFail = true;
                        break;

                    case 'i':
                        failOver = false;
                        break;

                    case 'l':
                        if ((wf_label = opts.getOptarg()) != null) {
                            if (wf_label.length() > 0) wf_flag |= 1;
                            else wf_label = null;
                        }
                        break;

                    case 'm':
                        if ((wf_mtime = Currently.parse(opts.getOptarg())) != null) wf_flag |= 2;
                        break;

                    case 'n':
                        noDBase = true;
                        break;

                    case 'N':
                        ignoreDBFail = true;
                        break;

                    case 'v':
                        verbose = me.increaseVerbosity();
                        break;

                    case '?':
                        System.out.println("Invalid option '" + (char) opts.getOptopt() + "'");
                    default:
                    case 'h':
                        me.showUsage();
                        return;
                }
            }

            // print usage information
            String arg0 = null;
            if (opts.getOptind() >= args.length) {
                System.out.println("missing necessary file argument");
                me.showUsage();
                return;
            }

            // check for -m and -l
            if (wf_flag != 0 && wf_flag != 3) {
                me.m_logger.log(
                        "default", 0, "Warning: Options -m and -l should be used together!");
            }
            if (wf_label != null && wf_label.length() > 32) {
                wf_label = wf_label.substring(0, 32);
                me.m_logger.log(
                        "default", 0, "Warning: Truncating workflow label to \"" + wf_label + "\"");
            }

            ChimeraProperties props = ChimeraProperties.instance();
            DatabaseSchema dbschema = null;
            String ptcSchemaName = props.getPTCSchemaName();
            if (ptcSchemaName == null) noDBase = true;
            if (!noDBase) {
                try {
                    Connect connect = new Connect();
                    dbschema = connect.connectDatabase(ptcSchemaName);
                } catch (Exception e) {
                    if (ignoreDBFail) {
                        // if dbase errors are not fatal, just record the fact
                        String cls = e.getClass().getName();
                        String msg = e.getMessage();
                        if (msg == null) {
                            Throwable t = e.getCause();
                            if (t != null) {
                                cls = t.getClass().getName();
                                msg = t.getMessage();
                            }
                        }
                        me.m_logger.log(
                                "default",
                                0,
                                "While connecting to dbase: " + cls + ": " + msg + ", ignoring");
                        dbschema = null;
                    } else {
                        // re-throw, if dbase errors are fatal (default)
                        throw e;
                    }
                }

                // check for invocation record support
                if (dbschema == null || !(dbschema instanceof PTC)) {
                    me.m_logger.log(
                            "default",
                            0,
                            "Your database cannot store invocation records" + ", assuming -n mode");
                    noDBase = true;
                }
            }

            // instantiate parser
            pks = new ParseKickstart(dbschema, emptyFail);
            pks.setNoDBase(noDBase);
            pks.setIgnoreDBFail(ignoreDBFail);
            pks.setWorkflowLabel(wf_label); // null ok
            pks.setWorkflowTimestamp(wf_mtime); // null ok
            dbschema = null; // decrease reference counter

            // for all files specified
            for (int i = opts.getOptind(); i < args.length; ++i) {
                List l = pks.parseFile(args[i]);

                // determine result code
                if (failOver) {
                    for (Iterator j = l.iterator(); j.hasNext(); ) {
                        int status = ((Integer) j.next()).intValue();
                        me.m_logger.log("app", 1, "exit status = " + status);
                        if (status != 0) result = (result == 0 ? status : 8);
                    }
                }

                if (result != 0 && earlyFail) break;
            } // for
        } catch (FriendlyNudge fn) {
            me.m_logger.log("default", 0, fn.getMessage());
            if (failOver) result = fn.getResult();

        } catch (Exception e) {
            String cls = e.getClass().getName();
            String msg = e.getMessage();
            if (msg == null) {
                // another try
                Throwable t = e.getCause();
                if (t != null) {
                    msg = t.getMessage();
                    cls = t.getClass().getName();
                }
            }

            if (verbose > 0) e.printStackTrace();
            System.err.println(cls + ": " + msg);
            result = 7;

        } finally {
            try {
                if (pks != null) pks.close();
            } catch (Exception e) {
                me.m_logger.log("default", 0, "ERROR: " + e.getMessage());
            }
        }

        // Java will return with 0 unless exit is used. Unfortunately, using
        // System.exit sometimes has some unwanted side-effects on d'tors,
        // thus avoid using it unless strictly necessary.
        // me.showThreads();
        if (result != 0) System.exit(result);
    }
}
