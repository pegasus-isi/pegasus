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
 * This class implements the driver API for the PostGreSQL 7.3.* and 7.4.* series database. Please
 * note that at this point, we cannot recommend to use Postgres 8.0.*.
 *
 * @author Jens-S. Vöckler
 * @author Yong Zhao
 * @version $Revision$
 * @see org.griphyn.vdl.dbdriver.DatabaseDriver
 * @see org.griphyn.vdl.dbschema
 */
public class Postgres extends DatabaseDriver {
    /** Caches the driver version major, because this has side-effects on the behavior. */
    private int m_driver_major = -1;

    /**
     * Default constructor. As the constructor will do nothing, please use the connect method to
     * obtain a database connection.
     *
     * @see #connect( String, Properties, Set )
     */
    public Postgres() {
        super();
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
     * @exception if the driver is incapable of establishing a connection.
     */
    public boolean connect(String url, Properties info, Set tables)
            throws SQLException, ClassNotFoundException {
        // load PostGreSQL driver class into memory
        boolean save = this.connect("org.postgresql.Driver", url, info, tables);

        // add preparsed statement for sequence
        this.addPreparedStatement("vds.sequence", "SELECT nextval(?)");

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
     * Determines, if the JDBC driver is the right one for the database we talk to. Throws an
     * exception if not.
     */
    public void driverMatch() throws SQLException {
        DatabaseMetaData meta = m_connection.getMetaData();
        int driver_major = meta.getDriverMajorVersion();
        int driver_minor = meta.getDriverMinorVersion();
        String database = meta.getDatabaseProductVersion();

        boolean flag = false;
        int database_major = -1;
        int database_minor = -1;
        try {
            database_major = meta.getDatabaseMajorVersion();
            database_minor = meta.getDatabaseMinorVersion();
        } catch (SQLException e) {
            // check for "This method is not yet implemented"
            if (e.getErrorCode() == 0) flag = true;
            else throw e;
        }

        if (flag) {
            // use old-style check
            String jdbc = driver_major + "." + driver_minor;
            if (!database.startsWith(jdbc))
                throw new RuntimeException(
                        "JDBC driver " + jdbc + " does not match database version " + database);
        } else {
            // use new-style check - requires 7.4 JDBC driver
            if (driver_major < database_major
                    || (driver_major == database_major && driver_minor < database_minor))
                throw new RuntimeException(
                        "JDBC driver "
                                + driver_major
                                + "."
                                + driver_minor
                                + " does not match database version "
                                + database_major
                                + "."
                                + database_minor);
        }
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
     * Obtains the next value from a sequence. Postgres uses explicit sequence generators, so this
     * function will return the new id.
     *
     * @param name is the name of the sequence.
     * @return the next sequence number.
     * @exception if something goes wrong while fetching the new value.
     */
    public long sequence1(String name) throws SQLException {
        PreparedStatement ps = this.getPreparedStatement("vds.sequence");

        Logging.instance().log("sql", 2, "SELECT nextval(" + name + ")");
        Logging.instance().log("xaction", 1, "START sequence " + name);

        // obtain new sequence number
        ps.setString(1, name);
        ResultSet rs = ps.executeQuery();
        rs.next();
        long result = rs.getLong("nextval");
        rs.close();
        Logging.instance().log("xaction", 1, "FINAL sequence " + name + " = " + result);

        // done
        return result;
    }

    /**
     * Obtains the sequence value for the current statement. Postgres does not permit NULL-driven
     * auto-increment columns. Postgres uses explicit sequence generators, so this function always
     * returns -1.
     *
     * @param s is a statment or prepared statement
     * @param name is the name of the sequence.
     * @param pos is the column number of the auto-increment column.
     * @return the next sequence number.
     * @exception if something goes wrong while fetching the new value.
     */
    public long sequence2(Statement s, String name, int pos) throws SQLException {
        // should not be called here
        return -1;
    }

    /**
     * Predicate to tell the schema, if using a string instead of number will result in the speedier
     * index scans instead of sequential scans. PostGreSQL suffers from this problem.
     *
     * @return true, if using strings instead of integers and bigints will yield better performance.
     */
    public boolean preferString() {
        if (m_driver_major == -1) {
            try {
                DatabaseMetaData meta = m_connection.getMetaData();
                m_driver_major = meta.getDriverMajorVersion();
            } catch (SQLException e) {
                // cache failure
                m_driver_major = 0;
            }
        }
        return (m_driver_major < 8);
    }
}
