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
package org.griphyn.vdl.parser;

import java.io.*;

/**
 * This class is used to signal errors while scanning or parsing.
 *
 * @see VDLtScanner
 * @see VDLtParser
 */
public class VDLtException extends java.lang.RuntimeException {
    /** Contains the current line number when the exception was thrown. */
    private int m_lineno;

    /**
     * Constructs a message that contains a line number prefix.
     *
     * @param lineno is the line number to prefix
     * @param message is the message to attach when throwing.
     */
    public VDLtException(int lineno, String message) {
        super("line " + lineno + ": " + message);
        this.m_lineno = lineno;
    }

    /**
     * Obtains the current line number as of the throw.
     *
     * @return a line number.
     */
    public int getLineNumber() {
        return this.m_lineno;
    }
}
