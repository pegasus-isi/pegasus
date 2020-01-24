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

package org.griphyn.vdl.planner;

import edu.isi.pegasus.common.util.CommonProperties;
import edu.isi.pegasus.planner.catalog.ReplicaCatalog;
import edu.isi.pegasus.planner.catalog.replica.ReplicaCatalogEntry;
import edu.isi.pegasus.planner.catalog.replica.ReplicaFactory;
import java.io.*;
import java.lang.reflect.*;
import java.util.*;

/**
 * This class wraps the shell planner's request into the new RC API.
 *
 * @author Jens-S. Vöckler
 * @author Yong Zhao
 * @version $Revision$
 */
public class RCWrapper implements Wrapper {
    /** replica catalog API reference. */
    private ReplicaCatalog m_rc;

    /**
     * Connects the interface with the replica catalog implementation. The choice of backend is
     * configured through properties.
     *
     * @exception ClassNotFoundException if the schema for the database cannot be loaded. You might
     *     want to check your CLASSPATH, too.
     * @exception NoSuchMethodException if the schema's constructor interface does not comply with
     *     the database driver API.
     * @exception InstantiationException if the schema class is an abstract class instead of a
     *     concrete implementation.
     * @exception IllegalAccessException if the constructor for the schema class it not publicly
     *     accessible to this package.
     * @exception InvocationTargetException if the constructor of the schema throws an exception
     *     while being dynamically loaded.
     */
    public RCWrapper()
            throws ClassNotFoundException, IOException, NoSuchMethodException,
                    InstantiationException, IllegalAccessException, InvocationTargetException,
                    MissingResourceException {
        m_rc = ReplicaFactory.loadInstance(CommonProperties.instance());
    }

    /**
     * Connects the interface with the replica catalog implementation. The choice of backend is
     * configured through properties.
     *
     * @param props is an already instantiated version of the properties.
     * @exception ClassNotFoundException if the schema for the database cannot be loaded. You might
     *     want to check your CLASSPATH, too.
     * @exception NoSuchMethodException if the schema's constructor interface does not comply with
     *     the database driver API.
     * @exception InstantiationException if the schema class is an abstract class instead of a
     *     concrete implementation.
     * @exception IllegalAccessException if the constructor for the schema class it not publicly
     *     accessible to this package.
     * @exception InvocationTargetException if the constructor of the schema throws an exception
     *     while being dynamically loaded.
     */
    public RCWrapper(CommonProperties props)
            throws ClassNotFoundException, IOException, NoSuchMethodException,
                    InstantiationException, IllegalAccessException, InvocationTargetException {
        m_rc = ReplicaFactory.loadInstance(props);
    }

    /**
     * Frees resources taken by the instance of the replica catalog. This method is safe to be
     * called on failed or already closed catalogs.
     */
    public void close() {
        if (m_rc != null) {
            m_rc.close();
            m_rc = null;
        }
    }

    /** garbage collection. */
    protected void finalize() {
        close();
    }

    /**
     * Find the (first) physical filename for the logical file and resource.
     *
     * @param pool is the pool, site or resource name.
     * @param lfn is the logical filename (LFN) to look up.
     * @return the physical entity, or <code>null</code> if not found.
     */
    public String lookup(String pool, String lfn) {
        // sanity check
        if (m_rc == null) return null;
        return m_rc.lookup(lfn, pool);
    }

    /**
     * Obtains the name of the class implementing the replica catalog.
     *
     * @return class name of the replica catalog implementor.
     */
    public String getName() {
        return (m_rc == null ? null : m_rc.getClass().getName());
    }

    /**
     * Shows the contents of the catalog as one string.
     *
     * @return the string with the complete catalog contents. Warning, this may be very large, slow,
     *     and memory expensive.
     */
    public String toString() {
        // sanity check
        if (m_rc == null) return null;

        Map query = new TreeMap();
        query.put(ReplicaCatalogEntry.RESOURCE_HANDLE, "local");
        Map reply = new TreeMap(m_rc.lookup(query));
        return reply.toString();
    }
}
