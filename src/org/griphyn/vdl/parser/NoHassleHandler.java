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

import org.griphyn.vdl.classes.*;
import org.griphyn.vdl.util.*;
import org.xml.sax.*;

/**
 * This class adds a given Definition from the parser's callback into the fresh in-memory storage.
 * It is a streamlined version of the more versatile {@link MemoryStorage} handler. End-users should
 * not use this class, hence it is not public.
 *
 * @author Jens-S. Vöckler
 * @author Yong Zhao
 * @version $Revision$
 */
public class NoHassleHandler implements DefinitionHandler {
    /** This is a reference to the already established in-memory storage. */
    private Definitions m_memory;

    /**
     * The c'tor initializes the references to modify the in-memory database of definitions.
     *
     * @param definitions is a reference to the in-memory database.
     */
    public NoHassleHandler(Definitions definitions) {
        if ((this.m_memory = definitions) == null) throw new NullPointerException();
    }

    /**
     * This method adds the given Definition to whatever storage is implemented underneath. It is
     * assumed that a database will be read from file, and that there are no duplicates. Each new
     * definition will be added right away, skipping any safety net checks!
     *
     * @param d is the Definition that is ready to be stored.
     * @return true, if new version was stored and database modified
     */
    public boolean store(Definition d) {
        // definition does not exist
        this.m_memory.addDefinition(d);
        return true;
    }
}
