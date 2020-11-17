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
package org.griphyn.vdl.router;

import java.util.*;
import org.griphyn.vdl.dbschema.*;

/**
 * This class maintains each element in the nesting of Definitions. It turned out that the caching
 * for LFNs etc must also be kept in the stack instead of globally. In some cases, the caching may
 * be adverse to the performance, though.
 *
 * @author Jens-S. Vöckler
 * @author Yong Zhao
 * @version $Revision $
 */
public class StackElement {
    /** The database schema intermediary. */
    private DatabaseSchema m_dbschema;

    /** Temporarily saves filename lookups in a local cache. */
    private Cache m_lfnCache;

    /** Temporarily saves TR lookups in a local cache. */
    private Cache m_TRCache;

    /**
     * ctor: Initializes the local caches and the major data element.
     *
     * @param schema is a database backend manager
     */
    public StackElement(DatabaseSchema schema) {
        this.m_dbschema = schema;
        if (schema.cachingMakesSense()) {
            this.m_lfnCache = new Cache(600);
            this.m_TRCache = new Cache(600);
        } else {
            this.m_lfnCache = this.m_TRCache = null;
        }
    }

    /**
     * Obtains the reference to the database backend manager.
     *
     * @return a handle to the database backend manager.
     */
    public DatabaseSchema getDatabaseSchema() {
        return this.m_dbschema;
    }

    /**
     * Obtains the reference to the transient LFN cache.
     *
     * @return a handle to the transient LFN cache, or null for no caching.
     */
    public Cache getLFNCache() {
        return this.m_lfnCache;
    }

    /**
     * Obtains the reference to the transient TR cache.
     *
     * @return a handle to the transient TR cache, or null for no caching.
     */
    public Cache getTRCache() {
        return this.m_TRCache;
    }
}
