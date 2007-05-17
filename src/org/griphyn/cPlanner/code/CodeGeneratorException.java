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
package org.griphyn.cPlanner.code;

/**
 * The baseclass of the exception that is thrown by all Code Generators.
 * It is a checked exception.
 *
 * @author Karan Vahi
 * @version $Revision: 1.1 $
 */

public class CodeGeneratorException extends Exception {

    /**
     * Constructs a <code>CodeGeneratorException</code> with no detail
     * message.
     */
    public CodeGeneratorException() {
        super();
    }

    /**
     * Constructs a <code>CodeGeneratorException</code> with the specified detailed
     * message.
     *
     * @param message is the detailled message.
     */
    public CodeGeneratorException(String message) {
        super(message);
    }

    /**
     * Constructs a <code>CodeGeneratorException</code> with the specified detailed
     * message and a cause.
     *
     * @param message is the detailled message.
     * @param cause is the cause (which is saved for later retrieval by the
     * {@link java.lang.Throwable#getCause()} method). A <code>null</code>
     * value is permitted, and indicates that the cause is nonexistent or
     * unknown.
     */
    public CodeGeneratorException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * Constructs a <code>CodeGeneratorException</code> with the
     * specified just a cause.
     *
     * @param cause is the cause (which is saved for later retrieval by the
     * {@link java.lang.Throwable#getCause()} method). A <code>null</code>
     * value is permitted, and indicates that the cause is nonexistent or
     * unknown.
     */
    public CodeGeneratorException(Throwable cause) {
        super(cause);
    }

}