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

package org.griphyn.vdl.util;

import java.io.*;
import java.util.*;

/**
 * This class allows access to a set of files. With the constructor, the basename of the fileset is
 * specified. From this file, a set of 10 files can be accessed and constructed, with suffices ".0"
 * to ".9". A cursor file, suffix ".nr", points to the currently active file in the ring.
 *
 * <p>In read mode, the cursor file gets locked temporarily while the stream is opened. If no cursor
 * file exists, it is assumed that the basename is also the filename of the database. A lock file
 * for the opened database will be created.
 *
 * <p>In write mode, the cursor file gets locked until the writer is being closed again. Thus,
 * parallel access by other writers or readers are prohibited. The cursor is advanced at stream
 * close. The database stream points to the next file beyong the cursor. If no cursor file existed,
 * it will point to suffix ".0".
 *
 * <p>All access to the files must go through the respective open and close methods provided by this
 * class in order to guarantee proper locking!
 *
 * @author Jens-S. Vöckler
 * @author Yong Zhao
 * @version $Revision$
 * @see java.io.File
 * @see LockHelper
 */
public abstract class FileHelper {
    /** base name of the fileset to access. */
    protected String m_database;

    /** description of the cursor file for the given basename set. */
    protected File m_number;

    /**
     * Primary ctor: obtain access to a database cycle via basename.
     *
     * @param basename is the name of the database without digit suffix.
     */
    public FileHelper(String basename) {
        this.m_database = basename;
        this.m_number = new File(basename + ".nr");
    }

    /**
     * Reads the cursor file and obtains the current cycle position. The contract requires you to
     * hold the lock for the cursor file.
     *
     * @return the current cycle position, or -1 to indicate failure.
     */
    protected int readCount() {
        int result = -1;
        try {
            LineNumberReader lnr = new LineNumberReader(new FileReader(this.m_number));
            result = Integer.parseInt(lnr.readLine());
            lnr.close();
        } catch (IOException ioe) {
            Logging.instance()
                    .log(
                            "lock",
                            2,
                            "unable to process "
                                    + this.m_number.getPath()
                                    + ": "
                                    + ioe.getMessage());
            result = -1; // make extra sure
        } catch (NumberFormatException nfe) {
            result = -1; // make extra sure
        }
        return result;
    }

    /**
     * Updates the cursor file with a new cycle position. The contract requires you to hold the lock
     * for the cursor file.
     *
     * @param number is the new cursor position.
     * @return true, if the file was updated all right, false, if an error occured during update.
     */
    protected boolean writeCount(int number) {
        boolean result = false;
        try {
            FileWriter fw = new FileWriter(this.m_number);
            fw.write(Integer.toString(number) + System.getProperty("line.separator", "\r\n"));
            fw.close();
            result = true;
        } catch (IOException ioe) {
            Logging.instance()
                    .log(
                            "lock",
                            2,
                            "unable to update "
                                    + this.m_number.getPath()
                                    + ": "
                                    + ioe.getMessage());
            result = false;
        }
        return result;
    }

    /**
     * Opens a reader for the basename, adjusting the database cycles. The access can be shared with
     * other simultaneous readers.
     *
     * @return a reader opened to the basename, or null for failure.
     */
    public abstract File openReader();

    /**
     * Closes a previously obtained reader, and releases internal locking resources. Only if the
     * reader was found in the internal state, any closing of the stream will be attempted.
     *
     * @param r is the reader that was created by {@link #openReader()}.
     * @return true, if unlocking went smoothly, or false in the presence of an error. The only
     *     error that can happen it to use a File instance which was not returned by this instance.
     */
    public abstract boolean closeReader(File r);

    /**
     * Opens a writer for the basename, adjusting the database cycles The access is exclusive, and
     * cannot be shared with readers nor writers.
     *
     * @return a writer opened for the basename, or null for failure.
     */
    public abstract File openWriter();

    /**
     * Closes a previously obtained writer, and releases internal locking resources. Error
     * conditions can be either a missing instance that passed, or the inability to update the
     * cursor file.
     *
     * @param w is the instance that was returned by {@link #openWriter()}.
     * @return true, if the closing went smoothly, false in the presence of an error.
     */
    public abstract boolean closeWriter(File w);
}
