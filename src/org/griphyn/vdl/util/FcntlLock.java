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
import java.nio.channels.*;
import java.util.*;

/**
 * This class encapsulates the instance variables necessary to lock a file. The NIO new locking
 * methods are required.
 *
 * @author Jens-S. Vöckler
 * @author Yong Zhao
 * @version $Revision$
 * @see java.io.RandomAccessFile
 * @see java.nio.channels.FileChannel
 * @see java.nio.channels.FileLock
 */
public class FcntlLock {
    /** For logging purposes, which file is it... */
    private File m_file;

    /** The random access file that remains open during lock operations. */
    private RandomAccessFile m_channel;

    /** One lock, covering the whole file, during the operation. */
    private FileLock m_lock;

    /**
     * Create a file lock on the given file.
     *
     * @param f is the file name to lock.
     * @param rw is true for read/write mode, which will create missing files. If false, read-only
     *     mode is used.
     * @param shared is true for shared locks, false for exclusive locks.
     * @throws FileNotFoundException if the file was not found
     * @throws IOException if the lock could not be acquired
     */
    public FcntlLock(File f, boolean rw, boolean shared) throws FileNotFoundException, IOException {
        Logging.instance()
                .log(
                        "lock",
                        2,
                        "requesting "
                                + (shared ? "shared" : "exclusive")
                                + " lock on "
                                + f.getPath());

        m_file = f;
        m_channel = new RandomAccessFile(f, rw ? "rw" : "r");
        m_lock = m_channel.getChannel().lock(0L, Long.MAX_VALUE, shared);
    }

    /** Release the file lock acquired with this instance. */
    public void done() {
        if (m_lock != null) {
            Logging.instance().log("lock", 2, "releasing lock on " + m_file.getPath());
            try {
                m_lock.release();
            } catch (IOException ioe) {
            }
            m_lock = null;
        }

        if (m_channel != null) {
            try {
                m_channel.close();
            } catch (IOException ioe) {
            }
            m_channel = null;
        }
    }

    /** Releases all resource, if they were still active. */
    protected void finalize() throws Throwable {
        done();
    }
}
