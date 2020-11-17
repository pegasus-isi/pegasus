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
package edu.isi.pegasus.planner.invocation;

import java.io.IOException;
import java.io.Writer;
import java.util.*;

/**
 * This class maintains the application that was run, and the arguments to the commandline that were
 * actually passed on to the application.
 *
 * @author Jens-S. Vöckler
 * @author Yong Zhao
 * @version $Revision$
 * @see Job
 */
public abstract class Arguments extends Invocation {
    /** This is the executable that was run. */
    protected String m_executable;

    /**
     * This abstract method is called by higher-level functions to obtain a single string
     * representation of the arguments.
     *
     * @return string representing arguments, or <code>null</code> if there is no such string. The
     *     empty string is also possible.
     */
    public abstract String getValue();

    /** Default c'tor: Construct a hollow shell and allow further information to be added later. */
    public Arguments() {
        m_executable = null;
    }

    /**
     * Constructs an applications without arguments.
     *
     * @param executable is the name of the application.
     */
    public Arguments(String executable) {
        m_executable = executable;
    }

    /**
     * Accessor
     *
     * @see #setExecutable(String)
     */
    public String getExecutable() {
        return this.m_executable;
    }

    /**
     * Accessor.
     *
     * @param executable
     * @see #getExecutable()
     */
    public void setExecutable(String executable) {
        this.m_executable = executable;
    }

    /**
     * Converts the active state into something meant for human consumption. The method will be
     * called when recursively traversing the instance tree.
     *
     * @param stream is a stream opened and ready for writing. This can also be a string stream for
     *     efficient output.
     */
    public void toString(Writer stream) throws IOException {
        throw new IOException("method not implemented, please contact vds-support@griphyn.org");
    }
}
