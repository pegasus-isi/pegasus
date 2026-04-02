/*
 * This file or a portion of this file is licensed under the terms of
 * the Globus Toolkit Public License, found in file ../GTPL, or at
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
import java.util.*;
import org.griphyn.vdl.annotation.*;
import org.griphyn.vdl.dbschema.*;
import org.griphyn.vdl.directive.*;
import org.griphyn.vdl.util.ChimeraProperties;
import org.griphyn.vdl.util.Logging;

/**
 * This class inserts annotations into the backend database.
 *
 * @author Jens-S. Vöckler
 * @author Yong Zhao
 * @version $Revision$
 */
public class InsertMeta extends Toolkit {
    /** Constructor */
    public InsertMeta(String appName) {
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
                        + " [general] [-n ns] [-i id] [-v vs] [-a arg] [mdf]"
                        + linefeed
                        + "Or   : "
                        + this.m_application
                        + " [general] [-f lfn] [metadata_file]");

        System.out.println(
                linefeed
                        + "General options: "
                        + linefeed
                        + " -V|--version    print version information and exit."
                        + linefeed
                        + " -d|--dbase db   associates the dbname with the database, unused."
                        + linefeed
                        + "    --verbose    increases the verbosity level."
                        + linefeed
                        + " -t|--type tr|dv limits candidates to either TR or DV, default is both."
                        + linefeed
                        + linefeed
                        + "Option group 1: Arguments dealing with VDL definitions "
                        + linefeed
                        + " -n|--vdlns ns   limits candidates to definition namespace matches."
                        + linefeed
                        + " -i|--vdlid id   limits candidates to definition name matches."
                        + linefeed
                        + " -v|--vdlvs vs   limits candidates to definition version matches."
                        + linefeed
                        + " -a|--arg  arg   limits to formal arguments, requires -t tr."
                        + linefeed
                        + linefeed
                        + "Option group 2: Arguments dealing with VDL logical filenames"
                        + linefeed
                        + " -f|--file lfn   limits candidates to logical filename matches."
                        + linefeed
                        + linefeed
                        + "Option groups 1 and 2 are mutually exclusive. The metadata file is a texual"
                        + linefeed
                        + "file with at least three columns, the key, the type and the value(s)."
                        + linefeed);
    }

    /**
     * Creates a set of options.
     *
     * @return the assembled long option list
     */
    protected LongOpt[] generateValidOptions() {
        LongOpt[] lo = new LongOpt[17];

        lo[0] = new LongOpt("dbase", LongOpt.REQUIRED_ARGUMENT, null, 'd');
        lo[1] = new LongOpt("version", LongOpt.NO_ARGUMENT, null, 'V');
        lo[2] = new LongOpt("help", LongOpt.NO_ARGUMENT, null, 'h');
        lo[3] = new LongOpt("verbose", LongOpt.NO_ARGUMENT, null, 1);

        lo[4] = new LongOpt("vdlns", LongOpt.REQUIRED_ARGUMENT, null, 'n');
        lo[5] = new LongOpt("namespace", LongOpt.REQUIRED_ARGUMENT, null, 'n');
        lo[6] = new LongOpt("ns", LongOpt.REQUIRED_ARGUMENT, null, 'n');

        lo[7] = new LongOpt("vdlid", LongOpt.REQUIRED_ARGUMENT, null, 'i');
        lo[8] = new LongOpt("name", LongOpt.REQUIRED_ARGUMENT, null, 'i');
        lo[9] = new LongOpt("identifier", LongOpt.REQUIRED_ARGUMENT, null, 'i');
        lo[10] = new LongOpt("id", LongOpt.REQUIRED_ARGUMENT, null, 'i');

        lo[11] = new LongOpt("vdlvs", LongOpt.REQUIRED_ARGUMENT, null, 'v');
        lo[12] = new LongOpt("vs", LongOpt.REQUIRED_ARGUMENT, null, 'v');

        lo[13] = new LongOpt("arg", LongOpt.REQUIRED_ARGUMENT, null, 'a');
        lo[14] = new LongOpt("args", LongOpt.REQUIRED_ARGUMENT, null, 'a');
        lo[15] = new LongOpt("type", LongOpt.REQUIRED_ARGUMENT, null, 't');

        lo[16] = new LongOpt("file", LongOpt.REQUIRED_ARGUMENT, null, 'f');

        return lo;
    }

    /** Inserts metadata for TR, DV, LFN. */
    public static void main(String[] args) {
        int result = 0;

        try {
            InsertMeta me = new InsertMeta("insertmeta");

            // no arguments -- yikes
            if (args.length == 0) {
                me.showUsage();
                return;
            }

            // obtain commandline options first -- we may need the database stuff
            Getopt opts =
                    new Getopt(
                            me.m_application, args, "hd:t:n:i:v:a:f:V", me.generateValidOptions());

            opts.setOpterr(false);
            String secondary = null;
            String primary = new String();
            String type = null;
            String vdlns = null;
            String vdlid = null;
            String vdlvs = null;
            String lfn = null;
            String arg = null;

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

                    case 'a':
                        arg = opts.getOptarg();
                        break;

                    case 'd':
                        // currently inactive option
                        opts.getOptarg();
                        break;

                    case 'f':
                        lfn = opts.getOptarg();
                        break;

                    case 'i':
                        vdlid = opts.getOptarg();
                        break;

                    case 'n':
                        vdlns = opts.getOptarg();
                        break;

                    case 't':
                        type = opts.getOptarg();
                        break;

                    case 'v':
                        vdlvs = opts.getOptarg();
                        break;

                    case 'h':
                    default:
                        me.showUsage();
                        return;
                }
            }

            boolean condition1 = (lfn != null);
            boolean condition2 = (vdlns != null || vdlid != null || vdlvs != null);
            //  (a XOR b) <=> (a AND !b) OR (!a AND b)
            // !(a XOR b) <=> (a OR !b) AND (!a OR b)
            if ((condition1 || !condition2) && (!condition1 || condition2)) {
                me.showUsage();
                throw new RuntimeException(
                        "You must either specify the -n -i -v options, or\n"
                                + "\tuse the -f option!");
            }

            int classType = -1;
            if (condition2) {
                // metadata for TR or DV?
                switch (Character.toUpperCase(type.charAt(0))) {
                    case 'D': // DV
                        classType = Annotation.CLASS_DERIVATION;
                        break;

                    case 'T': // TR
                        classType = Annotation.CLASS_TRANSFORMATION;
                        if (arg != null) {
                            classType = Annotation.CLASS_DECLARE;
                            secondary = arg;
                        }
                        break;

                    default:
                        me.showUsage();
                        throw new RuntimeException(
                                "invalid argument \"" + type + "\" for option t");
                }

                // will be used for 'D' and 'T' only
                primary = Separator.combine(vdlns, vdlid, vdlvs);
            } else {
                // must be condition1
                classType = Annotation.CLASS_FILENAME;
                primary = lfn;
            }
            // postcondition: variable "type" can be recycled now

            // Connect the database.
            String schemaName = ChimeraProperties.instance().getVDCSchemaName();
            Connect connect = new Connect();
            DatabaseSchema dbschema = connect.connectDatabase(schemaName);

            if (!(dbschema instanceof Annotation)) {
                dbschema.close();
                throw new RuntimeException("The database does not support metadata!");
            } else {
                Annotation annotation = (Annotation) dbschema;

                // open metadata file -- or read from stdin
                String mdfile = (opts.getOptind() < args.length ? args[opts.getOptind()] : null);
                LineNumberReader lnr = null;
                if (mdfile == null || mdfile.equals("-")) {
                    System.err.println("# reminder: reading from stdin");
                    lnr = new LineNumberReader(new InputStreamReader(System.in));
                } else {
                    lnr = new LineNumberReader(new FileReader(mdfile));
                }

                // parsing name-value pairs from the file
                Logging.instance().log("app", 1, "parsing name type value tuple");

                StringTokenizer st = null;
                String token, key, line = null;
                while ((line = lnr.readLine()) != null) {
                    Logging.instance().log("app", 3, lnr.getLineNumber() + ": " + line);

                    st = new StringTokenizer(line, " ", false);
                    int column = -1;
                    key = type = null;
                    StringBuffer value = new StringBuffer(80);

                    while (st.hasMoreTokens()) {
                        token = st.nextToken();
                        column++;
                        switch (column) {
                            case 0: // key
                                key = token;
                                break;
                            case 1: // type
                                type = token;
                                break;
                            case 2: // value
                                value.append(token);
                                break;
                            default: // addon
                                value.append(' ').append(token);
                                break;
                        }
                    }

                    // invalid line
                    if (column < 2) {
                        // FIXME: Warn the user here?
                        continue;
                    }

                    // System.out.println("column=" + column );
                    // System.out.println("key=" + key );
                    // System.out.println("type=" + type );
                    // System.out.println("value=" + value );

                    Tuple tuple = null;

                    if (type.equalsIgnoreCase("int") || type.equalsIgnoreCase("i"))
                        tuple = new TupleInteger(key, Long.parseLong(value.toString()));
                    else if (type.equalsIgnoreCase("float") || type.equalsIgnoreCase("f"))
                        tuple = new TupleFloat(key, Float.parseFloat(value.toString()));
                    else if (type.equalsIgnoreCase("bool")
                            || type.equalsIgnoreCase("boolean")
                            || type.equalsIgnoreCase("b"))
                        tuple = new TupleBoolean(key, Boolean.getBoolean(value.toString()));
                    else if (type.equalsIgnoreCase("string") || type.equalsIgnoreCase("s"))
                        tuple = (Tuple) new TupleString(key, value.toString());
                    else if (type.equalsIgnoreCase("date") || type.equalsIgnoreCase("d"))
                        tuple = (Tuple) new TupleDate(key, 0); // FIXME
                    else {
                        // FIXME: What about this case?
                        System.out.println("ERROR: InsertMeta: invalid type: " + type);
                    }

                    if (tuple != null) {
                        System.out.println("Processing (" + tuple.toString() + ")");
                        annotation.saveAnnotation(primary, secondary, classType, tuple, false);
                    } else {
                        // FIXME: What about this case?
                        System.out.println("ERROR: InsertMeta: Tuple creation failed.");
                    }
                } // while
                System.out.println("Metadata loaded successfully!");
            }

            // done
            if (dbschema != null) dbschema.close();
        } catch (RuntimeException rte) {
            Logging.instance().log("default", 0, "runtime error: " + rte.getMessage());
            System.err.println("ERROR: " + rte.getMessage());
            result = 1;

        } catch (Exception e) {
            Logging.instance().log("default", 0, "FATAL: " + e.getMessage());
            e.printStackTrace();
            System.err.println("FATAL: " + e.getMessage());
            result = 2;
        }

        if (result != 0) System.exit(result);
    }
}
