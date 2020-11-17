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
package org.griphyn.vdl.annotation;

import java.io.*;

/**
 * This class is used to signal errors while scanning only.
 *
 * @see QueryScanner
 * @author Jens-S. Vöckler
 * @version $Revision$
 */
public class QueryScannerException extends java.lang.RuntimeException {
    /**
     * Constructs an exception that will contain the line number.
     *
     * @param stream is the lineno stream to obtain the line number from.
     * @param message is the message to print for the failed parse.
     */
    public QueryScannerException(LineNumberReader stream, String message) {
        super("line " + stream.getLineNumber() + ": " + message);
    }
}
