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
package org.griphyn.vdl.dbdriver;

import java.sql.*;
import java.util.*;
import org.griphyn.vdl.util.*;

/**
 * This class implements the driver API for the MySQL 4.* database.
 *
 * @author Jens-S. Vöckler
 * @author Yong Zhao
 * @version $Revision$
 * @see org.griphyn.vdl.dbdriver.DatabaseDriver
 * @see org.griphyn.vdl.dbschema
 */
public class MySQL extends DatabaseDriver {
    /** Maintains the property, if locking of sequence table is required. */
    boolean m_lockSequenceTable;

    /**
     * Default constructor. As the constructor will do nothing, please use the connect method to
     * obtain a database connection.
     *
     * @see #connect( String, Properties, Set )
     */
    public MySQL() {
        super();
        m_lockSequenceTable = true;
    }

    /**
     * Establish a connection to your database. The parameters will often be ignored or abused for
     * different purposes on different backends. It is assumed that the connection is not in
     * auto-commit mode, and explicit commits must be issued.
     *
     * @param url the contact string to database, or schema location
     * @param info additional parameters, usually username and password
     * @param tables is a set of all table names in the schema. The existence of all tables will be
     *     checked to verify that the schema is active in the database.
     * @return true if the connection succeeded, false otherwise. Usually, false is returned, if the
     *     any of the tables or sequences is missing.
     * @exception SQLException if the driver is incapable of establishing a connection.
     */
    public boolean connect(String url, Properties info, Set tables)
            throws SQLException, ClassNotFoundException {
        // load MySQL driver class into memory
        boolean save = this.connect("com.mysql.jdbc.Driver", url, info, tables);

        // check for non-locking sequences
        /* DO NOT DO THIS,
         * because the PTC will always have several instances run in parallel.
         * If necessary, use an extra property that is specifically linked to
         * the combination of VDC and MySQL (argh, I want to avoid just that).
         *
         * String lock = info.getProperty( "lockSequenceTable", "true" );
         * m_lockSequenceTable = Boolean.valueOf(lock).booleanValue();
         */

        // add preparsed statement for sequence management
        this.addPreparedStatement("vds.sequence.0", "LOCK TABLE sequences WRITE");
        this.addPreparedStatement(
                "vds.sequence.1", "UPDATE sequences SET currval=currval+1 WHERE name=?");
        this.addPreparedStatement("vds.sequence.2", "SELECT currval FROM sequences where name=?");
        this.addPreparedStatement("vds.sequence.3", "UNLOCK TABLE");

        // done
        return save;
    }

    /**
     * Determines, if the backend is expensive, and results should be cached. Ideally, this will
     * move transparently into the backend itself.
     *
     * @return true if caching is advisable, false for no caching.
     */
    public boolean cachingMakesSense() {
        return true;
    }

    /**
     * Quotes a string that may contain special SQL characters.
     *
     * @param s is the raw string.
     * @return the quoted string, which may be just the input string.
     */
    public String quote(String s) {
        if (s.indexOf('\'') != -1) {
            StringBuffer result = new StringBuffer();
            for (int i = 0; i < s.length(); ++i) {
                char ch = s.charAt(i);
                result.append(ch);
                if (ch == '\'') result.append(ch);
            }
            return result.toString();
        } else {
            return s;
        }
    }

    /**
     * Obtains the next value from a sequence.
     *
     * @param name is the name of the sequence.
     * @return the next sequence number.
     * @exception SQLException if something goes wrong while fetching the new value.
     */
    public long sequence1(String name) throws SQLException {
        PreparedStatement ps = null;
        Logging.instance().log("sql", 2, "SELECT nextval(" + name + ")");
        Logging.instance().log("xaction", 1, "START sequence " + name);

        // phase 1: lock sequence table
        if (m_lockSequenceTable) {
            ps = this.getPreparedStatement("vds.sequence.0");
            ps.executeUpdate();
        }

        // phase 2: increment sequence
        ps = this.getPreparedStatement("vds.sequence.1");
        ps.setString(1, name);
        ps.executeUpdate();

        // phase 3: obtain new value
        ps = this.getPreparedStatement("vds.sequence.2");
        ps.setString(1, name);
        ResultSet rs = ps.executeQuery();
        rs.next();
        long result = rs.getLong(1);
        rs.close();

        // phase 4: unlock table
        if (m_lockSequenceTable) {
            ps = this.getPreparedStatement("vds.sequence.3");
            ps.executeUpdate();
        }

        // done
        // ??? commit();
        Logging.instance().log("xaction", 1, "FINAL sequence " + name + " = " + result);
        return result;
    }

    /**
     * Obtains the sequence value for the current statement. Sigh.
     *
     * @param s is a statment or prepared statement
     * @param name is the name of the sequence.
     * @param pos is the column number of the auto-increment column.
     * @return the next sequence number.
     * @exception SQLException if something goes wrong while fetching the new value.
     */
    public long sequence2(Statement s, String name, int pos) throws SQLException {
        return -1;
    }

    /**
     * Predicate to tell the schema, if using a string instead of number will result in the speedier
     * index scans instead of sequential scans. PostGreSQL has this problem, but using strings in
     * the place of integers may not be universally portable.
     *
     * @return true, if using strings instead of integers and bigints will yield better performance.
     */
    public boolean preferString() {
        return false;
    }
}
