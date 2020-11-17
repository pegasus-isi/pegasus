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
 * This exception is thrown if a <code>Transformation</code> simultaneously displays child elements
 * for a simple transformation and child elements for a compound transformation. A method is not
 * required to declare in its throws clause any subclasses of RuntimeException that might be thrown
 * during the execution of the method but not caught.
 *
 * @author Jens-S. Vöckler
 * @author Yong Zhao
 * @version $Revision$
 * @see Transformation
 */
public class IllegalTransformationException extends java.lang.RuntimeException {
    /** Constructs a <code>IllegalTransformationException</code> with no detail message. */
    public IllegalTransformationException() {
        super();
    }

    /**
     * Constructs a <code>IllegalTransformationException</code> with the specified detailed message.
     *
     * @param s is the detailled message.
     */
    public IllegalTransformationException(String s) {
        super(s);
    }
}
