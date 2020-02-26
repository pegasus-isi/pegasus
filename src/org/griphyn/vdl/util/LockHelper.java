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
package org.griphyn.vdl.util;

import java.io.*;
import java.util.*;

/**
 * @author Jens-S. Vöckler
 * @author Yong Zhao
 * @version $Revision$
 * @see java.io.File
 */
public class LockHelper {
    /** The suffix to use for the regular lock file. */
    public static final String LOCK_SUFFIX = ".lock";

    /** The initial timeout to use when retrying with exponential backoff. */
    private long m_timeout;

    /** The number of retries to attempt obtaining a lock. */
    private int m_retries;

    /** default ctor. */
    public LockHelper() {
        this.m_timeout = 250;
        this.m_retries = 10;
    }

    /** ctor. */
    public LockHelper(long timeout, int retries) {
        this.m_timeout = timeout;
        this.m_retries = retries;
    }

    /**
     * Creates a file-based lock file in an NFS secure fashion. Do not use this function for
     * locking, use {@link #lockFile(String)} instead. One exception is to use this method for
     * test-and-set locking.
     *
     * @param filename is the file to create a lockfile for
     * @return the representation of the lock file, or <code>null</code> in case of error.
     */
    public File createLock(String filename) {
        // create local names from basename
        File result = null;
        File lock = new File(filename + LockHelper.LOCK_SUFFIX);

        // exclusively create new file
        boolean created = false;
        try {
            created = lock.createNewFile();
        } catch (IOException ioe) {
            Logging.instance()
                    .log(
                            "lock",
                            0,
                            "while creating lock " + lock.getPath() + ": " + ioe.getMessage());
            created = false; // make extra sure
        }

        // if the lock was created, continue
        if (created) {
            // postcondition: file was created
            Logging.instance().log("lock", 2, "created lock " + lock.getPath());
            LockFileSet.instance().add(lock);
            lock.setLastModified(System.currentTimeMillis()); // force NFS
            result = lock;
        } else {
            // unable to rename file to lock file: lock exists
            Logging.instance().log("lock", 1, "lock " + lock.getPath() + " already exists");
        }

        // may be null
        return result;
    }

    /**
     * Locks a file using an empty lockfile. This method repeatedly retries to lock a file, and
     * gives up after a few seconds.
     *
     * @param filename is the basename of the file to lock.
     * @return true, if the file was locked successfully, false otherwise.
     */
    public boolean lockFile(String filename) {
        long timeout = this.m_timeout;

        // do five retries
        for (int i = 0; i < this.m_retries; ++i) {
            File lock = this.createLock(filename);
            if (lock == null) {
                // lock is busy, try again later
                Logging.instance()
                        .log(
                                "lock",
                                1,
                                "file "
                                        + filename
                                        + " is busy, "
                                        + "sleeping "
                                        + Long.toString(timeout)
                                        + " ms "
                                        + "(retry "
                                        + Integer.toString(i)
                                        + ')');
                try {
                    Thread.sleep(timeout);
                } catch (InterruptedException ie) {
                    // ignore, as usual
                }
                timeout <<= 1;
            } else {
                // we got the lock, go ahead, and write our pid into it
                // FIXME: we don't have a pid in Java (in Windows)
                return true;
            }
        }

        // postcondition: timeout out after retries
        Logging.instance()
                .log("default", 0, "unable to lock " + filename + ", still busy, giving up!");
        return false;
    }

    /**
     * Releases a lock file from its basename.
     *
     * @param filename is the name of the basename of the file to unlock. The locking extension will
     *     be added internally.
     * @return true, if the lock was removed successfully, false otherwise.
     */
    public boolean unlockFile(String filename) {
        File lock = new File(filename + LockHelper.LOCK_SUFFIX);
        LockFileSet.instance().remove(lock);

        boolean result = lock.delete();
        if (result) Logging.instance().log("lock", 2, "removed lock " + lock.getPath());
        return result;
    }
}
