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
package org.griphyn.vdl.euryale;

/**
 * This run-time error is thrown, if variables from the set a virtual constructor relies upon are
 * write-changed after the virtual constructor has already been used.
 *
 * @author Kavitha Ranganathan
 * @author Jens-S. Vöckler
 * @author Yong Zhao
 * @version $Revision$
 * @see HashedFileFactory
 */
public class VTorInUseException extends RuntimeException {
    /** Constructor: Assembles a standard exception without message. */
    public VTorInUseException() {
        super();
    }

    /**
     * Constructor: Assembles a standard exception with a message.
     *
     * @param message is the user notification.
     */
    public VTorInUseException(String message) {
        super(message);
    }
}
