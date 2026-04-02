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
package org.griphyn.vdl.classes;

/**
 * This exception is thrown if the matching between the linkage of a declared variable and the
 * combined linkage of a used variable in a transformation have incompatible values. An example for
 * such an incompatability is having declared a variable of type input, but using it as ouput. The
 * exception will also be thrown, if multiple usages of a bound variable use incompatible linkages,
 * e.g. the same variable is used with linkage NONE in one instance, and with any other linkage in
 * the same transformation at another place.
 *
 * <p>A method is not required to declare in its throws clause any subclasses of RuntimeException
 * that might be thrown during the execution of the method but not caught.
 *
 * @author Jens-S. Vöckler
 * @author Yong Zhao
 * @version $Revision$
 * @see Derivation
 * @see Transformation
 */
public class IncompatibleLinkageException extends java.lang.RuntimeException {
    /** Constructs a <code>IncompatibleLinkageException</code> with no detail message. */
    public IncompatibleLinkageException() {
        super();
    }

    /**
     * Constructs a <code>IncompatibleLinkageException</code> with the specified detailed message.
     *
     * @param s is the detailled message.
     */
    public IncompatibleLinkageException(String s) {
        super(s);
    }
}
