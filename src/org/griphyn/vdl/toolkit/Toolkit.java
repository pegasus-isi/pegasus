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

import java.io.*;
import java.util.Collection;
import java.util.MissingResourceException;
import org.griphyn.vdl.dbschema.*;
import org.griphyn.vdl.util.ChimeraProperties;
import org.griphyn.vdl.util.Logging;

/**
 * This class is the base class for other toolkits. It provides basical common functions, to be
 * extended, used or restricted by siblings as necessary.
 *
 * @author Jens-S. Vöckler
 * @author Yong Zhao
 * @version $Revision$
 * @see org.griphyn.vdl.util.Logging
 * @see org.griphyn.vdl.util.ChimeraProperties
 * @see org.griphyn.vdl.dbschema.DatabaseSchema
 */
public abstract class Toolkit {
    /** Maintains an application specific set of properties */
    protected ChimeraProperties m_props;

    /** Stores the name of the application as which this one was run */
    protected String m_application;

    /** Stores the name of the home directory for the Chimera system. */
    protected String m_home;

    /** Stores the logging instance */
    protected Logging m_logger;

    /** Stores the verbosity level of the app logging queue. */
    protected int m_verbosity;

    /**
     * Helper for the constructors
     *
     * @param self is the reference to self (the unit to be constructed)
     */
    private static void setup(Toolkit self) {
        // set up logger
        self.m_logger = Logging.instance();

        // setting up properties -- might throw things at us
        try {
            self.m_props = ChimeraProperties.instance();
        } catch (IOException e) {
            Logging.instance()
                    .log("default", 0, "error while reading properties file: " + e.getMessage());
        } catch (MissingResourceException e) {
            Logging.instance()
                    .log("default", 0, "You probably forgot to set -Dvds.home=$PEGASUS_HOME");
            System.exit(1);
        }

        self.m_props.setupLogging(self.m_logger);
    }

    /**
     * Default ctor: Sets up the system
     *
     * @param appName is the name of the shell wrapper with which to report.
     */
    public Toolkit(String appName) {
        m_application = appName;
        setup(this);

        // Register app by default, but only with level 0
        // jsv: 20050815, bugzilla#79
        if (m_logger.isUnset("app")) {
            m_verbosity = 0;
            m_logger.register("app", System.out, m_verbosity);
        } else {
            m_verbosity = m_logger.getLevel("app");
            if (m_verbosity == Integer.MAX_VALUE || m_verbosity < 0) {
                m_verbosity = 0;
                m_logger.setLevel("app", m_verbosity);
            }
        }
    }

    /**
     * Default ctor: Sets up the system
     *
     * @param appName is the name of the shell wrapper with which to report.
     * @param verbosity sets the verbosity level of the "app" logging queue.
     */
    public Toolkit(String appName, int verbosity) {
        m_application = appName;
        setup(this);

        // Register app by default, but only with level 0
        // jsv: 20050815, bugzilla#79
        if (m_logger.isUnset("app")) {
            m_verbosity = verbosity;
            m_logger.register("app", System.out, m_verbosity);
        } else {
            m_verbosity = m_logger.getLevel("app");
            if (m_verbosity == Integer.MAX_VALUE || m_verbosity < 0) {
                m_verbosity = 0;
                m_logger.setLevel("app", m_verbosity);
            }
        }
    }

    /**
     * Helper function to set the verbosity level of the app logging queue.
     *
     * @param verbosity is the new verbosity level
     * @see #getVerbosity()
     * @see #increaseVerbosity()
     */
    public void setVerbosity(int verbosity) {
        this.m_verbosity = verbosity;
        this.m_logger.setLevel("app", verbosity);
    }

    /**
     * Helper function to obtain the verbosity level of the app logging queue.
     *
     * @return the verbosity level as non-negative integer, or -1 for error.
     * @see #setVerbosity( int )
     * @see #increaseVerbosity()
     */
    public int getVerbosity() {
        int result = this.m_logger.getLevel("app");
        this.m_verbosity = result;
        return result;
    }

    /**
     * Helper function to increase the verbosity level of the app logging queue using the internal
     * verbosity state.
     *
     * @return the new verbosity level
     * @see #setVerbosity( int )
     * @see #getVerbosity()
     */
    public int increaseVerbosity() {
        this.m_logger.setLevel("app", ++this.m_verbosity);
        return this.m_verbosity;
    }

    /**
     * Prints the short usage string onto stdout. No exiting. The application name is maintained by
     * the c'tor.
     */
    public abstract void showUsage();

    /**
     * This helper method reads lines from a file and add non-empty, no-comment lines to a
     * collection of lines. Invoke with a set to prohibit duplicates, or with a list to permit
     * duplicates.
     *
     * @param fn is the name of the file to read.
     * @param list is the container to add lines to
     * @return number of items read.
     * @throws IOException if any file operation failed.
     */
    protected int readFile(String fn, Collection list) throws IOException {
        String line;
        int count = 0;
        LineNumberReader in = null;
        if (fn == null) return 0;

        in = new LineNumberReader(new FileReader(fn));
        while ((line = in.readLine()) != null) {
            // remove comments
            int p = line.indexOf('#');
            if (p != -1) line = line.substring(0, p);

            // remove superflous whitespace
            line = line.trim();

            // add anything non-empty
            if (line.length() >= 1) {
                list.add(line);
                count++;
            }
        }

        in.close();
        return count;
    }
}
