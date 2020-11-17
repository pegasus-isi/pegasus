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
 * This class is a helper for the <code>LockHelper</code>. It maintains the set of lock filenames in
 * status locked. These files need to be removed on exit. Thus, this class is implemented as a
 * Singleton.
 *
 * @author Jens-S. Vöckler
 * @author Yong Zhao
 * @version $Revision $
 * @see LockHelper
 */
public class LockFileSet extends Thread {
    /** This is the Singleton instance variable. */
    private static LockFileSet m_instance;

    /** The set of files that need to be erased on exit. */
    private HashSet m_fileSet;

    /** The c'tor is protected in order to enforce the Singleton interface. */
    protected LockFileSet() {
        m_fileSet = new HashSet();
        Runtime.getRuntime().addShutdownHook(this);
    }

    /**
     * This is the only access to any LockFileSet object. It is a kind of factory that produces just
     * one instance for all.
     *
     * @return the one and only instance of a LockFileSet. Always.
     */
    public static synchronized LockFileSet instance() {
        if (m_instance == null) m_instance = new LockFileSet();
        return m_instance;
    }

    /**
     * This method should not be called directly. It will be invoked on exit by the exit hook
     * handler.
     */
    public void run() {
        synchronized (this.m_fileSet) {
            for (Iterator i = this.m_fileSet.iterator(); i.hasNext(); ) {
                ((File) i.next()).delete();
            }
            this.m_fileSet = null;
        }
    }

    /**
     * Adds a file name to the set of lock filenames that need to be removed on exit.
     *
     * @param f is a File instance nameing the lock filename.
     */
    public void add(File f) {
        // Logging.instance().log( "lock", 2, "LFS add " + f.getPath() );
        synchronized (this.m_fileSet) {
            this.m_fileSet.add(f);
        }
    }

    /**
     * Removes a filename from the set of lock filenames that get removed on exit.
     *
     * @param f is a File instance of a filename to remove from the list of removable files (are you
     *     confused yet).
     */
    public void remove(File f) {
        // Logging.instance().log( "lock", 2, "LFS del " + f.getPath() );
        synchronized (this.m_fileSet) {
            this.m_fileSet.remove(f);
        }
    }
}
