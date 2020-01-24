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

import java.util.ArrayList;
import org.griphyn.vdl.classes.*;
import org.griphyn.vdl.util.*;
import org.xml.sax.*;

/**
 * This class adds a given Definition from the parser's callback into the already established
 * in-memory storage.
 *
 * @author Jens-S. Vöckler
 * @author Yong Zhao
 * @version $Revision$
 */
public class MemoryStorage implements DefinitionHandler {
    /** This is a reference to the already established in-memory storage. */
    private Definitions m_memory;

    /** This determines the behavior: insert mode (false) or update mode (true) */
    private boolean m_overwrite;

    /** This variable determines whether to keep a list of rejects */
    private boolean m_dontcare;

    /** This is the list of rejects for insert, or old values for update. */
    private ArrayList m_rejects;

    /**
     * The c'tor initializes the references to modify the in-memory database of definitions.
     *
     * @param definitions is a reference to the in-memory database.
     * @param overwrite establishes insert or update mode.
     */
    public MemoryStorage(Definitions definitions, boolean overwrite, boolean dontcare) {
        if ((this.m_memory = definitions) == null) throw new NullPointerException();
        this.m_overwrite = overwrite;
        this.m_dontcare = dontcare;
        this.m_rejects = new ArrayList();
    }

    /**
     * Accessor: Provide an iterator to walk the rejects list.
     *
     * @return full access to the rejects list.
     */
    public java.util.List getRejects() {
        return this.m_rejects;
    }

    /**
     * This method adds the given Definition to whatever storage is implemented underneath.
     *
     * @param d is the Definition that is ready to be stored.
     * @return true, if new version was stored and database modified
     */
    public boolean store(Definition d) {
        int position = this.m_memory.positionOfDefinition(d);
        if (position != -1) {
            // definition already exists
            if (this.m_overwrite) {
                Logging.instance().log("app", 2, "Modifying " + d.shortID());
                Definition old = this.m_memory.setDefinition(position, d);
                if (!this.m_dontcare) this.m_rejects.add(old);
                return true;
            } else {
                Logging.instance().log("app", 2, "Rejecting " + d.shortID());
                if (!this.m_dontcare) this.m_rejects.add(d);
                return false;
            }
        } else {
            // definition does not exist
            Logging.instance().log("app", 2, "Adding " + d.shortID());
            this.m_memory.addDefinition(d);
            return true;
        }
    }
}
