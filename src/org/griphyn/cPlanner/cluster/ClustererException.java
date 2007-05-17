/**
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
package org.griphyn.cPlanner.cluster;

/**
 * The baseclass of the exception that is thrown by all Clusterers.
 * It is a checked exception.
 *
 * @author Karan Vahi
 * @version $Revision$
 */

public class ClustererException extends Exception {

    /**
     * Constructs a <code>ClustererException</code> with no detail
     * message.
     */
    public ClustererException() {
        super();
    }

    /**
     * Constructs a <code>ClustererException</code> with the specified detailed
     * message.
     *
     * @param message is the detailled message.
     */
    public ClustererException(String message) {
        super(message);
    }

    /**
     * Constructs a <code>ClustererException</code> with the specified detailed
     * message and a cause.
     *
     * @param message is the detailled message.
     * @param cause is the cause (which is saved for later retrieval by the
     * {@link java.lang.Throwable#getCause()} method). A <code>null</code>
     * value is permitted, and indicates that the cause is nonexistent or
     * unknown.
     */
    public ClustererException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * Constructs a <code>ClustererException</code> with the
     * specified just a cause.
     *
     * @param cause is the cause (which is saved for later retrieval by the
     * {@link java.lang.Throwable#getCause()} method). A <code>null</code>
     * value is permitted, and indicates that the cause is nonexistent or
     * unknown.
     */
    public ClustererException(Throwable cause) {
        super(cause);
    }

}
