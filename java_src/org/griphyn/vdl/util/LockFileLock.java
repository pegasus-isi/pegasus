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
 * This class implements file locking using explicit lock files. Some effort was taken to permit
 * some NFS alleviation of problems. However, all locking is exclusive, and may result in
 * termination for failure to obtain a lock due to the presence of a lock file.
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
public class LockFileLock extends FileHelper {
    /** file locking helper (could be static for all I care) */
    private LockHelper m_lock;

    /**
     * maintainer of reads and writes for this instance. Parallel instances might still try to
     * access in parallel, the reason lock files are employed.
     */
    private int m_state = 0;

    private int m_refCount = 0;

    /** state collector of the streams that are currently open. */
    private HashMap m_streams;

    /**
     * Primary ctor: obtain access to a database cycle via basename.
     *
     * @param basename is the name of the database without digit suffix.
     */
    public LockFileLock(String basename) {
        super(basename);

        this.m_lock = new LockHelper();
        this.m_streams = new HashMap();
    }

    /**
     * Opens a reader for the basename, adjusting the database cycles. The access can be shared with
     * other simultaneous readers.
     *
     * @return a reader opened to the basename, or null for failure.
     * @see #closeReader( File )
     */
    public synchronized File openReader() {
        // check, if any writer is already open. Parallel readers are allowed.
        if (this.m_state > 1) return null;

        int number = -1;
        if (this.m_number.exists()) {
            // if the number file does not exist, DON'T create it
            // FIXME: we still create a lock file for this (nonexisting) file
            // read which database is the current one
            if (this.m_lock.lockFile(this.m_number.getPath())) {
                number = readCount();
                // keep locked until database is opened
            }
        } else {
            // if the number file does not exit, DO NOT create it (yet)
            Logging.instance()
                    .log(
                            "lock",
                            2,
                            "number file " + m_number.getPath() + " does not exist, ignoring lock");
        }
        // postcondition: number points to the original database to read from

        // database file
        File database =
                new File(number == -1 ? m_database : m_database + '.' + Integer.toString(number));

        // lock and open database
        File result = null;
        if (this.m_lock.lockFile(database.getPath())) {
            result = database;
            this.m_state |= 1; // mark reader as active
            this.m_refCount++; // and count readers
            this.m_streams.put(result, new Integer(number));
        }

        // release lock on number file in any case. Once it is opened,
        // assume that it is protected by the OS.
        this.m_lock.unlockFile(this.m_number.getPath());

        // exit condition: Only the database file is locked, or in case of
        // failure it is unlocked. The number file is always unlocked at this
        // stage.
        return result;
    }

    /**
     * Opens a writer for the basename, adjusting the database cycles The access is exclusive, and
     * cannot be shared with readers nor writers.
     *
     * @return a writer opened for the basename, or null for failure.
     * @see #closeWriter( File )
     */
    public synchronized File openWriter() {
        // check, if any reader or a writer is already open
        if (this.m_state > 0) return null;

        int number = -1;
        if (!this.m_number.exists()) {
            // if the number file does not exist, DO NOT create it (yet)
            // FIXME: we still create a lock file for this (nonexisting) file
            number = 0;
        } else {
            // read which database is the current one
            if (this.m_lock.lockFile(this.m_number.getPath())) {
                number = readCount();
                // keep file locked!
            }

            // generate next file
            number = (number + 1) % 10;
        }
        // postcondition: number is the new database to write to

        // database file
        File database = new File(this.m_database + '.' + Integer.toString(number));

        // lock and open database
        File result = null;
        if (this.m_lock.lockFile(database.getPath())) {
            result = database;
            this.m_state |= 2; // mark writer as active
            this.m_streams.put(result, new Integer(number));
        }

        if (result == null) {
            // failure, release lock on number file
            this.m_lock.unlockFile(this.m_number.getPath());
        }

        // exit condition: database file and number file are both locked, or
        // in case of failure: both unlocked.
        return result;
    }

    /**
     * Closes a previously obtained reader, and releases internal locking resources. Only if the
     * reader was found in the internal state, any closing of the stream will be attempted.
     *
     * @param r is the reader that was created by {@link #openReader()}.
     * @return true, if unlocking went smoothly, or false in the presence of an error. The only
     *     error that can happen it to use a File instance which was not returned by this instance.
     * @see #openReader()
     */
    public synchronized boolean closeReader(File r) {
        boolean result = false;
        Integer number = (Integer) this.m_streams.get(r);
        if (number != null) {
            // deactivate reader refcount
            if (--this.m_refCount == 0) this.m_state &= ~1;

            // remove lock from database file in any case
            this.m_streams.remove(r);
            this.m_lock.unlockFile(r.getPath());

            // everything is smooth
            result = true;
        }
        return result;
    }

    /**
     * Closes a previously obtained writer, and releases internal locking resources. Error
     * conditions can be either a missing instance that passed, or the inability to update the
     * cursor file.
     *
     * @param w is the instance that was returned by {@link #openWriter()}.
     * @return true, if the closing went smoothly, false in the presence of an error.
     * @see #openWriter()
     */
    public synchronized boolean closeWriter(File w) {
        boolean result = false;
        Integer number = (Integer) this.m_streams.get(w);
        if (number != null) {
            // Since the cursor could not be modified due to being locked,
            // we can update it now with the new version. NOW create it.
            result = writeCount(number.intValue());

            // deactivate writer
            this.m_state &= ~2;

            // remove locks from database and cursor file.
            this.m_streams.remove(w);
            this.m_lock.unlockFile(w.getPath());
            this.m_lock.unlockFile(this.m_number.getPath());
        }
        return result;
    }
}
