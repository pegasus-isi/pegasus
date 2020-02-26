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
 * This class implements the driver API for the Microsoft SQL Server 2000 database. Please note that
 * due to the limited capabilities of the Microsoft-provided free JDBC driver for SQL-Server, it
 * will <i>not</i> work as a database driver. However, if you have access to the commercial
 * JDBC3-compatible driver, you are encouraged to implement the missing functionality.
 *
 * @author Jens-S. Vöckler
 * @author Yong Zhao
 * @version $Revision$
 * @see org.griphyn.vdl.dbdriver.DatabaseDriver
 * @see org.griphyn.vdl.dbschema
 */
public class SQLServer2000 extends DatabaseDriver {
    /**
     * Default constructor. As the constructor will do nothing, please use the connect method to
     * obtain a database connection.
     *
     * @see #connect( String, Properties, Set )
     */
    public SQLServer2000() {
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
        return this.connect("com.microsoft.jdbc.sqlserver.SQLServerDriver", url, info, tables);
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
     * Obtains the next value from a sequence. JDBC drivers which allow explicit access to sequence
     * generator will return a valid value in this function. All other JDBC drivers should return
     * -1.
     *
     * @param name is the name of the sequence.
     * @return the next sequence number.
     * @exception if something goes wrong while fetching the new value.
     */
    public long sequence1(String name) throws SQLException {
        throw new SQLException(
                this.getClass().getName()
                        + ": Method not implemented, "
                        + "please notify vds-support@griphyn.org");
    }

    /**
     * Obtains the sequence value for the current statement. JDBC driver that permit insertion of
     * NULL into auto-increment value should use this method to return the inserted ID value via the
     * statements getGeneratedKeys(). Other JDBC drivers should treat return the parametric id.
     *
     * @param s is a statment or prepared statement
     * @param name is the name of the sequence.
     * @param pos is the column number of the auto-increment column.
     * @return the next sequence number.
     * @exception if something goes wrong while fetching the new value.
     */
    public long sequence2(Statement s, String name, int pos) throws SQLException {
        throw new SQLException(
                this.getClass().getName()
                        + ": Method not implemented, "
                        + "please notify vds-support@griphyn.org");
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
