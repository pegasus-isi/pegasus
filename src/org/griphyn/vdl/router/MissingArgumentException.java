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
 * This exception is thrown if the job specification is incomplete. Any job specification must have
 * a supplied value for formal arguments. The value can come from one of two possible sources.
 *
 * <ol>
 *   <li>The formal argument may contain a default value. In the absence of any other supplied
 *       value, the default will be taken.
 *   <li>The actual argument may supply an overwriting value for a formal argument with a default
 *       value. An actual argument must be supplied for formal arguments without a default value.
 * </ol>
 *
 * @author Jens-S. Vöckler
 * @author Yong Zhao
 * @version $Revision$
 * @see org.griphyn.vdl.classes.Derivation
 * @see org.griphyn.vdl.classes.Transformation
 * @see org.griphyn.vdl.classes.Pass
 * @see org.griphyn.vdl.classes.Declare
 */
public class MissingArgumentException extends java.lang.RuntimeException {
    /** Constructs a <code>MissingArgumentException</code> with no detail message. */
    public MissingArgumentException() {
        super();
    }

    /**
     * Constructs a <code>MissingArgumentException</code> with the specified detailed message.
     *
     * @param s is the detailled message.
     */
    public MissingArgumentException(String s) {
        super(s);
    }
}
