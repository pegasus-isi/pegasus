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

import edu.isi.pegasus.planner.catalog.TransformationCatalog;
import edu.isi.pegasus.planner.catalog.transformation.TCMode;
import edu.isi.pegasus.planner.catalog.transformation.TransformationCatalogEntry;
import edu.isi.pegasus.planner.catalog.transformation.classes.TCType;
import java.io.*;
import java.util.*;

/**
 * This class wraps the shell planner's request into the new TC API.
 *
 * @author Jens-S. Vöckler
 * @author Yong Zhao
 * @version $Revision$
 */
public class TCWrapper implements Wrapper {
    /** transformation catalog API reference. */
    private TransformationCatalog m_tc;

    /**
     * Connects the interface with the transformation catalog implementation. The choice of backend
     * is configured through properties.
     */
    public TCWrapper() {
        m_tc = TCMode.loadInstance();
    }

    /**
     * Frees resources taken by the instance of the replica catalog. This method is safe to be
     * called on failed or already closed catalogs.
     */
    public void close() {
        if (m_tc != null) {
            m_tc.close();
            m_tc = null;
        }
    }

    /** garbage collection. */
    protected void finalize() {
        close();
    }

    /**
     * Obtains all applications that bear the name and are installed at the resource.
     *
     * @param ns is the TR namespace
     * @param id is the TR identifier
     * @param vs is the TR version
     * @param site is the site handle, should be "local".
     * @return a possibly empty list of {@link org.griphyn.common.catalog.TransformationCatalogEntry
     *     TransformationCatalogEntry} with all matches.
     */
    public List lookup(String ns, String id, String vs, String site) {
        // sanity check
        if (m_tc == null) return null;

        List result = null;
        try {
            result = m_tc.lookup(ns, id, vs, site, TCType.INSTALLED);
        } catch (Exception e) {
            result = null;
        }

        return result;
    }

    /**
     * Extracts all profiles contained in the transformation catalog entry.
     *
     * @param tce is the transformation catalog entry
     * @return a map of maps. The outer map is indexed by the lower-cased namespace identifier. The
     *     inner map is indexed by the key within the particular namespace. An empty map is
     *     possible.
     */
    public static Map getProfiles(TransformationCatalogEntry tce) {
        Map result = new HashMap();
        List lop = tce.getProfiles();
        if (lop == null || lop.size() == 0) return result;

        Map submap = null;
        for (Iterator i = lop.iterator(); i.hasNext(); ) {
            edu.isi.pegasus.planner.classes.Profile p =
                    (edu.isi.pegasus.planner.classes.Profile) i.next();
            String ns = p.getProfileNamespace().trim().toLowerCase();
            String key = p.getProfileKey().trim();
            String value = p.getProfileValue();

            // insert at the right place into the result map
            if (result.containsKey(ns)) {
                submap = (Map) result.get(ns);
            } else {
                result.put(ns, (submap = new HashMap()));
            }
            submap.put(key, value);
        }

        return result;
    }

    /**
     * Obtains the name of the class implementing the replica catalog.
     *
     * @return class name of the replica catalog implementor.
     */
    public String getName() {
        return (m_tc == null ? null : m_tc.getClass().getName());
    }

    /**
     * Shows the contents of the catalog as one string.
     *
     * @return the string with the complete catalog contents. Warning, this may be very large, slow,
     *     and memory expensive.
     */
    public String toString() {
        return "(method not implemented)";
    }
}
