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

package org.griphyn.vdl.directive;

import java.io.*;
import java.sql.SQLException;
import java.util.MissingResourceException;
import java.util.Set;
import org.griphyn.vdl.classes.Definition;
import org.griphyn.vdl.classes.Derivation;
import org.griphyn.vdl.dbdriver.*;
import org.griphyn.vdl.dbschema.*;
import org.griphyn.vdl.parser.*;
import org.xml.sax.InputSource;

/**
 * This class parses VDL XML specifications and stores them to database backend.
 *
 * @author Jens-S. Vöckler
 * @author Yong Zhao
 * @version $Revision$
 * @see org.griphyn.vdl.parser.VDLxParser
 * @see org.griphyn.vdl.parser.DefinitionHandler
 */
public class Define extends Directive implements DefinitionHandler {
    /** This determines the behavior: insert mode (false) or update mode (true) */
    private boolean m_overwrite;

    /** This variable keeps the stream to print rejects onto, may be null. */
    private Writer m_rejects = null;

    /** Counts the number of successful database manipulations. */
    private int m_count = 0;

    /** Counts the rejected manipulations. */
    private int m_rejected = 0;

    /** database manipulator. */
    private DatabaseSchema m_dbschema = null;

    /** If enabled, collapses the names of DVs that were processed. */
    private java.util.Set m_derivations = null;

    /** Constructor */
    public Define() throws IOException, MissingResourceException {
        super();
    }

    /**
     * Constructor, set database schema instance
     *
     * @param dbs the database schema instance
     */
    public Define(DatabaseSchema dbs) throws IOException, MissingResourceException {
        m_dbschema = dbs;
    }

    /**
     * set database schema
     *
     * @param dbs the database schema instance
     */
    public void setDatabaseSchema(DatabaseSchema dbs) {
        m_dbschema = dbs;
    }

    /** Closes the associated database backend and invalidates the schema. */
    public void close() throws SQLException {
        if (m_dbschema != null) m_dbschema.close();
        m_dbschema = null;
    }

    /**
     * Returns the remembered derivations.
     *
     * @return all remembered derivations, an empty set if none were found, or <code>null</code> if
     *     remembering was off.
     * @see #setDerivationMemory( boolean )
     */
    public Set getDerivationMemory() {
        return m_derivations;
    }

    /**
     * Toggles the remembering of derivations that were processed.
     *
     * @param on is true to enable derivation memory
     * @see #getDerivationMemory()
     */
    public void setDerivationMemory(boolean on) {
        if (on) {
            // enable remembering derivations
            if (m_derivations == null) m_derivations = new java.util.HashSet();
        } else {
            // disable remembering derivations
            m_derivations = null;
        }
    }

    /**
     * Insert definitions into database, if a definition already exists in the database, then it is
     * rejected. This method does not keep track of rejected ones.
     *
     * @param reader the reader to vdlx source
     * @return true if insersion is successful
     */
    public boolean insertVDC(Reader reader) {
        return updateVDC(reader, null, false);
    }

    /**
     * Insert definitions into database, if a definition already exists in the database, then it is
     * rejected. This method keeps track of rejected ones.
     *
     * @param reader the reader to vdlx source
     * @param writer writer to output the rejected definitions
     * @return true if insersion is successful
     */
    public boolean insertVDC(Reader reader, Writer writer) {
        return updateVDC(reader, writer, false);
    }

    /**
     * Insert definitions into database, if a definition already exists in the database, then
     * overwrite it. This method does not keep track of overwritten ones.
     *
     * @param reader the reader to vdlx source
     * @return true if update is successful
     */
    public boolean updateVDC(Reader reader) {
        return updateVDC(reader, null, true);
    }

    /**
     * Insert definitions into database, if a definition already exists in the database, then
     * overwrite it. This method keeps track of overwritten ones.
     *
     * @param reader the reader to vdlx source
     * @param writer writer to output the overwritten definitions
     * @return true if update is successful
     */
    public boolean updateVDC(Reader reader, Writer writer) {
        return updateVDC(reader, writer, true);
    }

    /**
     * Insert definitions into database, if a definition already exists in the database, then either
     * update the definition or reject the definition.
     *
     * @param reader the reader to vdlx source
     * @param writer writer to output the overwritten/rejected definitions
     * @return true if update is successful
     */
    public boolean updateVDC(Reader reader, Writer writer, boolean overwrite) {
        m_rejects = writer;
        m_overwrite = overwrite;
        m_count = m_rejected = 0;

        org.griphyn.vdl.parser.VDLxParser parser =
                new org.griphyn.vdl.parser.VDLxParser(m_props.getVDLSchemaLocation());

        return parser.parse(new InputSource(reader), this);
    }

    /**
     * This method implements the interface defined in DefinitionHandler to save definition to
     * database backend.
     *
     * @param d is the Definition that is ready to be stored.
     * @return true, if new version was stored and database modified, false, if the definition was
     *     rejected for any reason.
     */
    public boolean store(Definition d) {
        boolean result = false;
        VDC vdc = (VDC) m_dbschema;

        // NEW: remember all DVs we came across
        if (m_derivations != null && d instanceof Derivation) m_derivations.add(d.shortID());

        try {
            if (m_rejects == null) {
                // rely on saveDefinition to do "the right thing"
                result = vdc.saveDefinition(d, m_overwrite);
            } else {
                // Is the Definition already in the database?
                if (vdc.containsDefinition(d)) {
                    if (m_overwrite) {
                        // this is time-consuming and ineffective
                        Definition old =
                                vdc.loadDefinition(
                                        d.getNamespace(), d.getName(), d.getVersion(), d.getType());
                        old.toXML(m_rejects, "  ");
                        result = vdc.saveDefinition(d, true);
                    } else {
                        // skip, if not forced to overwrite, but save rejects
                        d.toXML(m_rejects, "  ");
                    }
                } else {
                    // not found, insert unconditionally
                    result = vdc.saveDefinition(d, true);
                }
            }
        } catch (SQLException sql) {
            // database problems
            for (int i = 0; sql != null; ++i) {
                m_logger.log(
                        "database",
                        0,
                        "SQL error " + i + ": " + sql.getErrorCode() + ": " + sql.getMessage());
                sql = sql.getNextException();
            }
            m_logger.log("database", 0, "ignoring SQL exception(s)");
        } catch (Exception e) {
            m_logger.log("database", 0, "caught " + e + ", ignoring");
            result = false;
        }

        if (result) m_count++;
        else m_rejected++;
        return result;
    }

    /** return the number of successfully saved definitions */
    public int getNumberSaved() {
        return m_count;
    }

    /** return the number of rejected definitions */
    public int getNumberRejected() {
        return m_rejected;
    }
}
