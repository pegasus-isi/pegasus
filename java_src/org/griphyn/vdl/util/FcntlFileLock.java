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
 * This class implements file locking using system-provided lock functions. On unix, these may most
 * likely include POSIX fcntl locking calls. While such lock may on some systems be NFS-safe,
 * networked file locking on Linux is notoriously broken, and cannot be assumed to work as expected.
 *
 * <p>All access to the files must go through the respective open and close methods provided by this
 * class in order to guarantee proper locking!
 *
 * @author Jens-S. Vöckler
 * @author Yong Zhao
 * @version $Revision$
 * @see FcntlLock
 * @see java.io.File
 */
public class FcntlFileLock extends FileHelper {
    /** File channel lock of database. */
    private FcntlLock m_lock_db;

    /** File channel lock of nr file. */
    private FcntlLock m_lock_nr;

    /** Number memory for updates on close. */
    private int m_current;

    /** Remembers, if this instance is already in use. */
    private boolean m_active;

    /**
     * Primary ctor: obtain access to a database cycle via database.
     *
     * @param database is the base name of the database without the digit suffix.
     */
    public FcntlFileLock(String database) {
        super(database);
        this.m_lock_db = this.m_lock_nr = null;
        this.m_current = -1;
        this.m_active = false;
    }

    /**
     * Opens a reader for the database, adjusting the database cycles. The access can be shared with
     * other simultaneous readers. The locks on number file and database are held shared. They are
     * released in the close operation.
     *
     * @return a reader opened to the database, or null for failure.
     * @see #closeReader( File )
     */
    public synchronized File openReader() {
        File result = null;

        // sanity check -- don't reopen without close
        if (m_active) return result;

        // lock number file shared
        // read contents of number file, if available
        int number = -1;
        if (m_number.exists()) {
            try {
                m_lock_nr = new FcntlLock(m_number, false, true);
            } catch (IOException e) {
                Logging.instance()
                        .log(
                                "lock",
                                0,
                                "while creating lock on "
                                        + m_number.getPath()
                                        + ": "
                                        + e.getMessage());
                return result;
            }

            // read number from number file
            number = readCount();

        } else {
            // if the number file does not exit, DO NOT create it (yet)
            Logging.instance()
                    .log(
                            "lock",
                            2,
                            "number file " + m_number.getPath() + " does not exist, ignoring lock");
        }

        // remember number
        m_current = number;

        // database file
        File database =
                new File(number == -1 ? m_database : m_database + '.' + Integer.toString(number));

        // lock and open database
        if (database.exists()) {
            try {
                m_lock_db = new FcntlLock(database, false, true);
            } catch (IOException e) {
                Logging.instance()
                        .log(
                                "lock",
                                0,
                                "while creating lock on "
                                        + database.getPath()
                                        + ": "
                                        + e.getMessage());
                return result;
            }
        } else {
            Logging.instance()
                    .log(
                            "lock",
                            1,
                            "database file " + database.getPath() + " does not exist, ignoring");
        }

        result = database;

        // exit condition: Both, number file and database are locked using
        // a shared (read) lock.
        m_active = (result != null);
        return result;
    }

    /**
     * Opens a writer for the database, adjusting the database cycles The access is exclusive, and
     * cannot be shared with readers nor writers. Both locks are exclusive, and held through the
     * close operation.
     *
     * @return a writer opened for the database, or null for failure.
     * @see #closeWriter( File )
     */
    public synchronized File openWriter() {
        File result = null;

        // sanity check -- don't reopen without close
        if (m_active) return result;

        // lock number file exclusively
        // read contents of number file, if available
        int number = -1;
        if (m_number.exists()) {
            try {
                m_lock_nr = new FcntlLock(m_number, true, false);
            } catch (IOException e) {
                Logging.instance()
                        .log(
                                "lock",
                                0,
                                "while creating lock on "
                                        + m_number.getPath()
                                        + ": "
                                        + e.getMessage());
                return result;
            }

            // read number from number file
            number = readCount();

        } else {
            // if the number file does not exit, DO NOT create it (yet)
            Logging.instance()
                    .log(
                            "lock",
                            2,
                            "number lock " + m_number.getPath() + " does not exist, ignoring");
        }

        // generate new file number to write to
        m_current = number = (number + 1) % 10;

        // generate database filename
        File database = new File(m_database + '.' + Integer.toString(number));

        // lock and open database for exclusive access
        try {
            m_lock_db = new FcntlLock(database, true, false);
        } catch (IOException e) {
            Logging.instance()
                    .log(
                            "lock",
                            0,
                            "while creating lock on " + database.getPath() + ": " + e.getMessage());
            return result;
        }

        // valid result
        result = database;
        m_active = true;

        // exit condition: Both, database and number file are locked, using
        // an exclusive lock.
        return result;
    }

    /**
     * Closes a previously obtained reader, and releases internal locking resources.
     *
     * @param r is the reader that was created by its open operation.
     * @return true, if unlocking went smoothly, or false in the presence of an error.
     * @see #openReader()
     */
    public synchronized boolean closeReader(File r) {
        boolean result = false;

        // sanity check
        if (!m_active) return false;
        result = true;

        // done with database
        if (m_lock_db != null) m_lock_db.done();

        // done with nr file; however, it may not have existed
        if (m_lock_nr != null) m_lock_nr.done();

        m_active = false;
        return result;
    }

    /**
     * Closes a previously obtained writer, and releases internal locking resources.
     *
     * @param w is the instance that was returned by its open operation.
     * @return true, if the closing went smoothly, false in the presence of an error.
     * @see #openWriter()
     */
    public synchronized boolean closeWriter(File w) {
        boolean result = false;

        // sanity check
        if (!m_active) return result;

        // done with database
        m_lock_db.done();

        // update number file
        result = writeCount(m_current);

        // release shared lock on number file
        if (m_lock_nr != null) m_lock_nr.done();

        m_active = false;
        return result;
    }
}
