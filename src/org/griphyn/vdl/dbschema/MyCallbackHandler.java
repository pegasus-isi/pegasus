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
package org.griphyn.vdl.dbschema;

import org.griphyn.vdl.classes.*;
import org.griphyn.vdl.parser.*;
import org.griphyn.vdl.util.Logging;

/**
 * This class adds a given Definition from the parser's callback into the fresh in-memory storage.
 * It is a streamlined version of the more versatile {@link MemoryStorage} handler. End-users should
 * not use this class.
 *
 * @author Jens-S. Vöckler
 * @author Yong Zhao
 * @version $Revision$
 */
class MyCallbackHandler implements FinalizerHandler {
    /** This is a reference to the already established in-memory storage. */
    private Definition m_memory;

    /** The c'tor initializes the references to a single Definition. */
    public MyCallbackHandler() {
        this.m_memory = null;
    }

    /**
     * Returns the value stored by the XML reader's callback function.
     *
     * @return a single Definition that was read, or <code>null</code>.
     */
    public Definition getDefinition() {
        return this.m_memory;
    }

    /**
     * This method adds the given Definition to whatever storage is implemented underneath.
     *
     * @param d is the Definition that is ready to be stored.
     * @return true, if new version was stored and database modified
     */
    public boolean store(VDL d) {
        if (d instanceof Definition) {
            this.m_memory = (Definition) d;
            Logging.instance().log("chunk", 0, "found " + m_memory.shortID());
            return true;
        } else {
            Logging.instance().log("chunk", 0, "not a definition: " + d.toString());
            return false;
        }
    }
}
