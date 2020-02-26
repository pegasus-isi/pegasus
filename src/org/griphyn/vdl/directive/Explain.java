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
package org.griphyn.vdl.directive;

import edu.isi.pegasus.common.util.Separator;
import java.io.*;
import java.util.ArrayList;
import java.util.Collection;
import java.util.MissingResourceException;
import org.griphyn.vdl.classes.*;
import org.griphyn.vdl.dax.ADAG;
import org.griphyn.vdl.dbschema.*;
import org.griphyn.vdl.router.*;

/**
 * This class generates the DAX per the request for LFNs or DVs
 *
 * @author Jens-S. Vöckler
 * @author Yong Zhao
 * @version $Revision$
 * @see org.griphyn.vdl.router.Route
 * @see org.griphyn.vdl.router.BookKeeper
 * @see org.griphyn.vdl.dbschema.VDC
 */
public class Explain extends Directive {
    /** Maintains the link to the VDC. */
    private DatabaseSchema m_dbschema = null;

    /** Provides a routing object to traverse dependencies in the VDC. */
    private Route m_route = null;

    /** Helpful object for routing requests. */
    private BookKeeper m_state = null;

    /** Constructor */
    public Explain() throws IOException, MissingResourceException {
        super();
    }

    /**
     * Constructor: Sets the database schema instance, and initializes the internal <code>Route
     * </code> accordingly.
     *
     * @param dbs the database schema instance
     * @see org.griphyn.vdl.router.Route
     * @see org.griphyn.vdl.router.BookKeeper
     */
    public Explain(DatabaseSchema dbs) throws IOException, MissingResourceException {
        m_dbschema = dbs;
        m_route = new Route(m_dbschema);
        m_state = new BookKeeper();
    }

    /**
     * Sets database schema, and initialzes the internal <code>Route</code> accordingly.
     *
     * @param dbs the database schema instance
     * @see org.griphyn.vdl.router.Route
     * @see org.griphyn.vdl.router.BookKeeper
     */
    public void setDatabaseSchema(DatabaseSchema dbs) {
        m_dbschema = dbs;
        m_route = new Route(m_dbschema);
        m_state = new BookKeeper();
    }

    /**
     * Allows to limit the maximum depth that the router is willing to go.
     *
     * @param depth is the maximum depth. Use Integer.MAX_VALUE for (virtually) unlimited depth.
     */
    public void setMaximumDepth(int depth) {
        m_route.setMaximumDepth(depth);
    }

    /**
     * Requests a data product logical filename. As a result, the complete build-style DAG for
     * producing the requested filename will be constructed.
     *
     * @param filename is the name of the requested LFN.
     * @return true if the request is successful
     * @see #requestLFN( java.util.Collection )
     * @see org.griphyn.vdl.router.BookKeeper
     */
    public boolean requestLFN(String filename) throws java.sql.SQLException {
        ArrayList al = new ArrayList(1);
        al.add(filename);
        return requestLFN(al);
    }

    /**
     * Requests a set of logical filenames. As a result, the complete build-style DAG for producing
     * the requested LFNs will be constructed.
     *
     * @param filenames is the list or set of logical filenames requested.
     * @return true if the request is successful
     * @see org.griphyn.vdl.router.Route#requestLfn( Collection, BookKeeper )
     * @see org.griphyn.vdl.router.BookKeeper
     */
    public boolean requestLFN(java.util.Collection filenames) throws java.sql.SQLException {
        // FIXME: What about previous results?
        m_route.requestLfn(filenames, m_state);
        return (m_state != null && !m_state.isEmpty());
    }

    /**
     * Requests for a specific derivation. As a result, a build-style DAG will be produced and
     * maintained in the book-keeping structure.
     *
     * @param namespace is the namespace of the derivation.
     * @param name is the name of the derivation.
     * @param version is the version of the derivation.
     * @return true if the request is successful
     * @see org.griphyn.vdl.router.Route#requestDerivation( String, String, String, BookKeeper )
     * @see org.griphyn.vdl.router.BookKeeper
     */
    public boolean requestDerivation(String namespace, String name, String version) {
        return m_route.requestDerivation(namespace, name, version, m_state);
    }

    /**
     * Requests for a specific derivation. As a result, a build-style DAG will be produced and
     * maintained in the book-keeping structure.
     *
     * @param fqdn is the fully qualified name of the derivation.
     * @return true if the request is successful
     * @see org.griphyn.common.util.Separator#splitFQDI( String )
     * @see org.griphyn.vdl.router.Route#requestDerivation( String, String, String, BookKeeper )
     * @see org.griphyn.vdl.router.BookKeeper
     */
    public boolean requestDerivation(String fqdn) throws IllegalArgumentException {
        String[] name = Separator.splitFQDI(fqdn);
        return m_route.requestDerivation(name[0], name[1], name[2], m_state);
    }

    /**
     * Requests a set of specific derivations. As a result, a build-style DAG will be produced and
     * maintained in the book-keeping structure.
     *
     * @param symbolicList is a collecting of symbolic FQDN derivation triples.
     * @return true if the request is successful
     * @see org.griphyn.common.util.Separator#splitFQDI( String )
     * @see org.griphyn.vdl.router.Route#requestDerivation( Collection, BookKeeper )
     * @see org.griphyn.vdl.router.BookKeeper
     */
    public boolean requestDerivation(java.util.Collection symbolicList)
            throws IllegalArgumentException {
        return m_route.requestDerivation(symbolicList, m_state);
    }

    /**
     * Writes the abstract DAX from the accumulated results.
     *
     * @param writer the output writer
     * @param label the label of the dax
     * @see org.griphyn.vdl.router.BookKeeper#getDAX
     */
    public void writeDAX(Writer writer, String label) throws IOException {
        this.writeDAX(writer, label, null);
    }

    /**
     * Writes the abstract DAX with a namespace prefix.
     *
     * @param writer the output writer
     * @param label the label of the dax
     * @param xmlns the xml namespace prefix
     * @see org.griphyn.vdl.router.BookKeeper#getDAX
     */
    public void writeDAX(Writer writer, String label, String xmlns) throws IOException {
        if (m_state == null || m_state.isEmpty()) {
            // whatever we did, there are no results for us
            m_logger.log("explain", 0, "WARNING: The requested DAX is empty!\n");
        } else {
            ADAG dax = m_state.getDAX(label == null ? "test" : label);
            m_logger.log(
                    "explain",
                    2,
                    "DAX has "
                            + dax.getFilenameCount()
                            + " LFNs, "
                            + dax.getJobCount()
                            + " jobs, "
                            + dax.getChildCount()
                            + " deps.");
            dax.toXML(writer, "", xmlns);
        }
    }

    /**
     * Checks if the result is empty or not.
     *
     * @return true, if the result is undefined or empty, false otherwise.
     */
    public boolean isEmpty() {
        return (m_state == null || m_state.isEmpty());
    }
}
