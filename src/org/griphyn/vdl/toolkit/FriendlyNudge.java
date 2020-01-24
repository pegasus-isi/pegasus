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

/**
 * This exception is a signal by the invocation record digestor.
 *
 * @author Jens-S. Vöckler
 * @author Yong Zhao
 * @version $Revision$
 * @see ExitCode
 */
public class FriendlyNudge extends java.lang.RuntimeException {
    protected int m_result;

    /** Constructs a <code>FriendlyNudge</code> with no detail message. */
    public FriendlyNudge() {
        super();
        this.m_result = 0;
    }

    /**
     * Constructs a <code>FriendlyNudge</code> with the specified detailed message.
     *
     * @param s is the detailled message.
     */
    public FriendlyNudge(String s) {
        super(s);
        this.m_result = 0;
    }

    /**
     * Constructs a <code>FriendlyNudge</code> with the specified detailed message and an exit code
     * to record.
     *
     * @param s is the detailled message.
     * @param e is the exit code to record.
     */
    public FriendlyNudge(String s, int e) {
        super(s);
        this.m_result = e;
    }

    /**
     * Accessor for the recorded exit code.
     *
     * @return the exit code that was constructed.
     */
    public int getResult() {
        return this.m_result;
    }
}
