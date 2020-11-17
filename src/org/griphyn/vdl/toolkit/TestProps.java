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

import edu.isi.pegasus.common.util.CommonProperties;
import edu.isi.pegasus.common.util.Version;
import gnu.getopt.*;
import java.util.*;

/**
 * This class deletes annotations for definition's and lfn's
 *
 * @author Jens-S. Vöckler
 * @author Yong Zhao
 * @version $Revision$
 */
public class TestProps extends Toolkit {
    /** String for usage */
    public static String m_usage = "";

    /** Constructor */
    public TestProps(String appName) {
        super(appName);
    }

    /** Print the usage string */
    public void showUsage() {
        String linefeed = System.getProperty("line.separator", "\r\n");

        System.out.println(
                "$Id$" + linefeed + "VDS version " + Version.instance().toString() + linefeed);

        System.out.println("Usage: " + this.m_application + " [-c] [-u] | [-V]");
        System.out.println(
                linefeed
                        + "Options: "
                        + linefeed
                        + " -V|--version    print version information and exit."
                        + linefeed
                        + " -h|--help       print this message."
                        + linefeed
                        + "    --verbose    increases the verbosity level."
                        + linefeed
                        + " -c|--concise    print in concise format instead of filling in blanks."
                        + linefeed
                        + " -u|--unsorted   print in no particular sorting order."
                        + linefeed);
    }

    /** Creates a set of options. */
    protected LongOpt[] generateValidOptions() {
        LongOpt[] lo = new LongOpt[5];

        lo[0] = new LongOpt("version", LongOpt.NO_ARGUMENT, null, 'V');
        lo[1] = new LongOpt("help", LongOpt.NO_ARGUMENT, null, 'h');
        lo[2] = new LongOpt("verbose", LongOpt.NO_ARGUMENT, null, 1);

        lo[3] = new LongOpt("unsorted", LongOpt.NO_ARGUMENT, null, 'u');
        lo[4] = new LongOpt("concise", LongOpt.NO_ARGUMENT, null, 'c');

        return lo;
    }

    /** dump the properties after CommonProperties had parsed them. */
    public static void main(String[] args) {
        int max = Integer.MIN_VALUE;
        int min = Integer.MAX_VALUE;
        boolean unsorted = false;
        boolean concise = false;

        try {
            // create an instance
            TestProps me = new TestProps("show-properties");

            // parse CLI options
            Getopt opts = new Getopt(me.m_application, args, "chuV", me.generateValidOptions());
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

                    case 'c':
                        concise = true;
                        break;

                    case 'u':
                        unsorted = true;
                        break;

                    default:
                    case 'h':
                        me.showUsage();
                        return;
                }
            }

            // create and read all properties
            CommonProperties v = CommonProperties.instance();

            // sort property keys into a tree set (sorted set)
            // while obtaining minimum and maximum key lengths
            Collection keys = null;
            if (unsorted) keys = new ArrayList();
            else keys = new TreeSet();

            for (Enumeration e = v.propertyNames(); e.hasMoreElements(); ) {
                String key = (String) e.nextElement();
                int len = key.length();
                if (len > max) max = len;
                if (len < min) min = len;
                keys.add(key);
            }

            // create sufficient spaces to accomodate the smallest key
            StringBuffer space = new StringBuffer(max - min + 1);
            for (int i = min; i <= max; ++i) space.append(' ');

            // print all keys
            for (Iterator i = keys.iterator(); i.hasNext(); ) {
                String key = (String) i.next();
                if (concise) System.out.println(key + "=" + v.getProperty(key));
                else
                    System.out.println(
                            key
                                    + space.substring(0, max - key.length())
                                    + " "
                                    + v.getProperty(key));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
