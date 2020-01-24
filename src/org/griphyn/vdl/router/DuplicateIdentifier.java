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

package org.griphyn.vdl.router;

/**
 * This exception is thrown if a transformation uses the same name for a formal argument as for a
 * local variable.
 *
 * @author Jens-S. Vöckler
 * @author Yong Zhao
 * @version $Revision$
 * @see org.griphyn.vdl.classes.Transformation
 * @see org.griphyn.vdl.classes.Local
 */
public class DuplicateIdentifier extends java.lang.RuntimeException {
    /** Constructs a <code>DuplicateIdentifier</code> with no detail message. */
    public DuplicateIdentifier() {
        super();
    }

    /**
     * Constructs a <code>DuplicateIdentifier</code> with the specified detailed message.
     *
     * @param s is the detailled message.
     */
    public DuplicateIdentifier(String s) {
        super(s);
    }
}
