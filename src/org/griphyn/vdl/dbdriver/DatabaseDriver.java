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
package org.griphyn.vdl.dbdriver;

import edu.isi.pegasus.common.util.DynamicLoader;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.reflect.*;
import java.sql.*;
import java.util.*;
import org.griphyn.vdl.util.ChimeraProperties;
import org.griphyn.vdl.util.Logging;

/**
 * This common database interface that defines basic functionalities for interacting with backend
 * SQL database. The implementation usually requires specific attention to the details between
 * different databases, as each does things slightly different. The API provides a functionality
 * that is independent of the schemas to be used.
 *
 * <p>The schema classes implement all their database access in terms of this database driver
 * classes.
 *
 * <p>The separation of database driver and schema lowers the implementation cost, as only N driver
 * and M schemas need to be implemented, instead of N x M schema-specific database-specific drivers.
 *
 * @author Jens-S. Vöckler
 * @author Yong Zhao
 * @version $Revision$
 * @see org.griphyn.vdl.dbschema
 */
public abstract class DatabaseDriver {
    /** This variable keeps the JDBC handle for the database connection. */
    protected Connection m_connection = null;

    /**
     * This list stores the prepared statements by their position. The constructor will initialize
     * it. Explicit destruction can be requested per statement.
     */
    protected Map m_prepared;

    //
    // class methods
    //

    /**
     * Instantiates the appropriate child according to property values. This method is a factory.
     * Currently, drivers may be instantiated multiple times.
     *
     * <p>
     *
     * @param dbDriverName is the name of the class that conforms to the DatabaseDriver API. This
     *     class will be dynamically loaded. The passed value should not be <code>null</code>.
     * @param propertyPrefix is the property prefix string to use.
     * @param arguments are arguments to the constructor of the driver to load. Please use "new
     *     Object[0]" for the argumentless default constructor.
     * @exception ClassNotFoundException if the driver for the database cannot be loaded. You might
     *     want to check your CLASSPATH, too.
     * @exception NoSuchMethodException if the driver's constructor interface does not comply with
     *     the database driver API.
     * @exception InstantiationException if the driver class is an abstract class instead of a
     *     concrete implementation.
     * @exception IllegalAccessException if the constructor for the driver class it not publicly
     *     accessible to this package.
     * @exception InvocationTargetException if the constructor of the driver throws an exception
     *     while being dynamically loaded.
     * @exception SQLException if the driver for the database can be loaded, but faults when
     *     initially accessing the database
     * @see org.griphyn.vdl.util.ChimeraProperties#getDatabaseDriverName
     */
    public static DatabaseDriver loadDriver(
            String dbDriverName, String propertyPrefix, Object[] arguments)
            throws ClassNotFoundException, IOException, NoSuchMethodException,
                    InstantiationException, IllegalAccessException, InvocationTargetException,
                    SQLException {
        Logging log = Logging.instance();
        log.log(
                "dbdriver",
                3,
                "accessing loadDriver( "
                        + (dbDriverName == null ? "(null)" : dbDriverName)
                        + ", "
                        + (propertyPrefix == null ? "(null)" : propertyPrefix)
                        + " )");

        // determine the database driver to load
        if (dbDriverName == null) {
            dbDriverName = ChimeraProperties.instance().getDatabaseDriverName(propertyPrefix);
            if (dbDriverName == null)
                throw new RuntimeException(
                        "You need to specify the " + propertyPrefix + " property");
        }

        // syntactic sugar adds absolute class prefix
        if (dbDriverName.indexOf('.') == -1) {
            // how about xxx.getClass().getPackage().getName()?
            dbDriverName = "org.griphyn.vdl.dbdriver." + dbDriverName;
        }

        // POSTCONDITION: we have now a fully-qualified class name
        log.log("dbdriver", 3, "trying to load " + dbDriverName);
        DynamicLoader dl = new DynamicLoader(dbDriverName);
        DatabaseDriver result = (DatabaseDriver) dl.instantiate(arguments);

        // done
        if (result == null) log.log("dbdriver", 0, "unable to load " + dbDriverName);
        else log.log("dbdriver", 3, "successfully loaded " + dbDriverName);
        return result;
    }

    /**
     * Convenience method instantiates the appropriate child according to property values.
     * Effectively, the following abbreviation is called:
     *
     * <pre>
     * loadDriver( null, propertyPrefix, new Object[0] );
     * </pre>
     *
     * @param propertyPrefix is the property prefix string to use.
     * @exception ClassNotFoundException if the driver for the database cannot be loaded. You might
     *     want to check your CLASSPATH, too.
     * @exception NoSuchMethodException if the driver's constructor interface does not comply with
     *     the database driver API.
     * @exception InstantiationException if the driver class is an abstract class instead of a
     *     concrete implementation.
     * @exception IllegalAccessException if the constructor for the driver class it not publicly
     *     accessible to this package.
     * @exception InvocationTargetException if the constructor of the driver throws an exception
     *     while being dynamically loaded.
     * @exception SQLException if the driver for the database can be loaded, but faults when
     *     initially accessing the database
     * @see #loadDriver( String, String, Object[] )
     */
    public static DatabaseDriver loadDriver(String propertyPrefix)
            throws ClassNotFoundException, IOException, NoSuchMethodException,
                    InstantiationException, IllegalAccessException, InvocationTargetException,
                    SQLException {
        return loadDriver(null, propertyPrefix, new Object[0]);
    }

    /**
     * Default constructor. As the constructor will do nothing, please use the connect method to
     * obtain a database connection. This is the constructor that will be invoked when dynamically
     * loading a driver.
     *
     * @see #connect( String, Properties, Set )
     */
    public DatabaseDriver() {
        this.m_connection = null;
        this.m_prepared = new TreeMap();
    }

    /**
     * Establishes a connection to the specified database. The parameters will often be ignored or
     * abused for different purposes on different backends. It is assumed that the connection is not
     * in auto-commit mode, and explicit commits must be issued.
     *
     * <p>Essentially, the deriving class will overwrite their connect method to fill in the
     * appropriate driver, and otherwise just call this method.
     *
     * @param driver is the Java class name of the database driver package
     * @param url the contact string to database, or schema location
     * @param info additional parameters, usually username and password
     * @param tables is a set of all table names in the schema. The existence of all tables will be
     *     checked to verify that the schema is active in the database.
     * @return true if the connection succeeded, false otherwise. Usually, false is returned, if the
     *     any of the tables or sequences is missing.
     * @see #connect( String, Properties, Set )
     * @see org.griphyn.vdl.util.ChimeraProperties#getDatabaseDriverName
     * @see org.griphyn.vdl.util.ChimeraProperties#getDatabaseURL
     * @see org.griphyn.vdl.util.ChimeraProperties#getDatabaseDriverProperties
     * @exception if the driver is incapable of establishing a connection.
     */
    protected boolean connect(String driver, String url, Properties info, Set tables)
            throws SQLException, ClassNotFoundException {
        // load specificed driver class into memory
        Class.forName(driver);

        Logging.instance().log("xaction", 1, "START connect to dbase");
        m_connection = DriverManager.getConnection(url, info);
        DriverManager.setLogWriter(new PrintWriter(System.err));
        Logging.instance().log("xaction", 1, "FINAL connected to dbase");

        // determine that database version and driver version match
        this.driverMatch();

        // disable auto commit, required for transaction (and speed).
        Logging.instance().log("xaction", 1, "START disable auto-commit");
        m_connection.setAutoCommit(false);
        Logging.instance().log("xaction", 1, "FINAL disabled auto-commit");

        // auto-disconnect, should we forget it, or die in an orderly fashion
        Runtime.getRuntime()
                .addShutdownHook(
                        new Thread() {
                            public void run() {
                                try {
                                    disconnect();
                                } catch (SQLException e) {
                                    e.printStackTrace();
                                }
                            }
                        });

        // final function return value
        boolean result = true;

        /* ### disabled
           // check for the presence of all tables in schema
           if ( result && tables != null && tables.size() > 0 ) {
             Logging.instance().log("xaction", 1, "START checking for tables" );

             List columns = new ArrayList();
             columns.add( "tablename" );

             Map where = new HashMap();
             where.put( "tableowner", info.get("username") );

             Set temp = new TreeSet();
             ResultSet rs = this.select( columns, "pg_tables", where, null );
             while ( rs.next() ) {
        temp.add( rs.getString() );
             }
             result = temp.containsAll( tables );
             Logging.instance().log("xaction", 1, "FINAL checking for tables" );
           }
           ### */

        return result;
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
    public abstract boolean connect(String url, Properties info, Set tables)
            throws SQLException, ClassNotFoundException;

    /**
     * Determines, if the JDBC driver is the right one for the database we talk to. Throws an
     * exception if not.
     */
    public void driverMatch() throws SQLException {
        // empty on purpose
    }

    /**
     * Close an established database connection.
     *
     * @exception if the driver threw up on the data.
     */
    public void disconnect() throws SQLException {
        if (this.m_connection != null) {
            this.m_connection.close();
            this.m_connection = null;
        }
    }

    /**
     * Closes an open connection to the database whenever this object is destroyed. This is still
     * not foolproof.
     */
    protected void finalize() throws Throwable {
        if (this.m_connection != null) {
            this.m_connection.close();
            this.m_connection = null;
        }

        super.finalize();
    }

    /**
     * Determines, if the backend is expensive, and results should be cached. Ideally, this will
     * move transparently into the backend itself.
     *
     * @return true if caching is advisable, false for no caching.
     */
    public abstract boolean cachingMakesSense();

    /**
     * Quotes a string that may contain special SQL characters.
     *
     * @param s is the raw string.
     * @return the quoted string, which may be just the input string.
     */
    public String quote(String s) {
        // not implemented.
        return s;
    }

    /**
     * Commits the latest changes to the database.
     *
     * @exception SQLException is propagated from the commit.
     */
    public void commit() throws SQLException {
        this.m_connection.commit();
    }

    /**
     * Rolls back the latest changes to the database. Some databases may be incapable of rolling
     * back.
     *
     * @exception SQLException is propagated from the rollback operation.
     */
    public void rollback() throws SQLException {
        this.m_connection.rollback();
    }

    /**
     * Clears all warnings reported for this database driver. After a call to this method, the
     * internal warnings are cleared until the next one occurs.
     */
    public void clearWarnings() throws SQLException {
        this.m_connection.clearWarnings();
    }

    /**
     * Retrieves the first warning reported by calls on this Connection object.
     *
     * @return the first SQLWarning object or null if there are none
     * @throws SQLException if a database access error occurs or this method is called on a closed
     *     connection
     * @see java.sql.Connection#getWarnings
     */
    public SQLWarning getWarnings() throws SQLException {
        return this.m_connection.getWarnings();
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
    public abstract long sequence1(String name) throws SQLException;

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
    public abstract long sequence2(Statement s, String name, int pos) throws SQLException;

    /**
     * Removes all rows that match the provided keyset from a table.
     *
     * @param table is the name of the table to remove values from
     * @param columns is a set of column names and their associated values to select the removed
     *     columns. The map may be null to remove all rows in a table.
     * @return the number of rows removed.
     * @exception if something goes wrong while removing the values.
     */
    public int delete(String table, Map columns) throws SQLException {
        StringBuffer request = new StringBuffer();

        request.append("DELETE FROM ").append(table);
        if (columns != null && columns.size() > 0) {
            request.append(" WHERE ");
            for (Iterator i = columns.entrySet().iterator(); i.hasNext(); ) {
                Map.Entry me = (Map.Entry) i.next();
                request.append((String) me.getKey()).append("='");
                request.append(this.quote((String) me.getValue())).append('\'');
                if (i.hasNext()) request.append(" AND ");
            }
        }

        String query = request.toString();
        Logging.instance().log("sql", 2, query);
        Logging.instance().log("xaction", 1, "START DELETE in " + table);
        int count = m_connection.createStatement().executeUpdate(query);
        Logging.instance().log("xaction", 1, "FINAL DELETE in " + table + ": " + count);

        return count;
    }

    /**
     * Inserts a row in one given database table.
     *
     * @param table is the name of the table to insert into.
     * @param keycolumns is a set of primary keys and their associated values. For special tables,
     *     the primary key set may be null or empty (e.g. a table without primary keys).
     * @param columns is a set of regular keys and their associated values.
     * @return the number of rows affected.
     * @exception if something goes wrong while inserting the values.
     */
    public long insert(String table, Map keycolumns, Map columns) throws SQLException {
        StringBuffer request = new StringBuffer();
        StringBuffer values = new StringBuffer();

        request.append("INSERT INTO ").append(table).append('(');
        values.append('(');

        // conditionally add primary key columns, if they exist
        if (keycolumns != null && keycolumns.size() > 0) {
            for (Iterator i = keycolumns.entrySet().iterator(); i.hasNext(); ) {
                Map.Entry me = (Map.Entry) i.next();
                request.append((String) me.getKey());
                values.append(quote((String) me.getValue()));
                if (i.hasNext()) {
                    request.append(", ");
                    values.append(", ");
                }
            }
        }

        // conditionally add all columns
        if (columns != null && columns.size() > 0) {
            for (Iterator i = columns.entrySet().iterator(); i.hasNext(); ) {
                Map.Entry me = (Map.Entry) i.next();
                request.append((String) me.getKey());
                values.append(quote((String) me.getValue()));
                if (i.hasNext()) {
                    request.append(", ");
                    values.append(", ");
                }
            }
        }

        String query = request.toString() + " VALUES " + values.toString();
        Logging.instance().log("sql", 2, query);
        Logging.instance().log("xaction", 1, "START INSERT in " + table);
        Statement st = m_connection.createStatement();
        long count = st.executeUpdate(query);
        Logging.instance().log("xaction", 1, "FINAL INSERT in " + table + ": " + count);
        return count;
    }

    /**
     * Updates matching rows in one given database table.
     *
     * @param table is the name of the table to insert into.
     * @param keycolumns is a set of primary keys and their associated values. For special tables,
     *     the primary key set may be null or empty (e.g. a table without primary keys).
     * @param columns is a set of regular keys and their associated values.
     * @return the number of rows affected
     * @exception if something goes wrong while updating the values.
     */
    public int update(String table, Map keycolumns, Map columns) throws SQLException {
        StringBuffer request = new StringBuffer();

        request.append("UPDATE ").append(table).append(' ');

        // unconditionally add all columns
        for (Iterator i = columns.entrySet().iterator(); i.hasNext(); ) {
            Map.Entry me = (Map.Entry) i.next();
            request.append("SET ").append((String) me.getKey()).append("='");
            request.append(quote((String) me.getValue())).append('\'');
            if (i.hasNext()) request.append(", ");
        }

        // conditionally add primary key columns, if they exist
        if (keycolumns != null && keycolumns.size() > 0) {
            request.append(" WHERE ");
            for (Iterator i = keycolumns.entrySet().iterator(); i.hasNext(); ) {
                Map.Entry me = (Map.Entry) i.next();
                request.append((String) me.getKey()).append("='");
                request.append(quote((String) me.getValue())).append('\'');
                if (i.hasNext()) request.append(" AND ");
            }
        }

        // so far, so good
        String query = request.toString();
        Logging.instance().log("sql", 2, query);
        Logging.instance().log("xaction", 1, "START UPDATE in " + table);
        int count = m_connection.createStatement().executeUpdate(query);
        Logging.instance().log("xaction", 1, "FINAL UPDATE in " + table + ": " + count);
        return count;
    }

    /**
     * Selects any rows in one or more colums from one or more tables restricted by some condition,
     * possibly ordered.
     *
     * @param select is the ordered set of column names to select, or simply a one-value list with
     *     an asterisk.
     * @param table is the name of the table to select from.
     * @param where is a collection of column names and values they must equal.
     * @param order is an optional ordering string.
     */
    public ResultSet select(List select, String table, Map where, String order)
            throws SQLException {
        StringBuffer request = new StringBuffer();

        request.append("SELECT ");
        for (Iterator i = select.iterator(); i.hasNext(); ) {
            request.append((String) i.next());
            if (i.hasNext()) request.append(',');
        }

        request.append(" FROM ").append(table);

        if (where != null && where.size() > 0) {
            request.append(" WHERE ");
            for (Iterator i = where.entrySet().iterator(); i.hasNext(); ) {
                Map.Entry me = (Map.Entry) i.next();
                request.append((String) me.getKey()).append("=\'");
                request.append(quote((String) me.getValue())).append('\'');
                if (i.hasNext()) request.append(" AND ");
            }
        }

        if (order != null && order.length() > 0) request.append(order);

        String query = request.toString();
        Logging.instance().log("sql", 2, query);
        Logging.instance().log("xaction", 1, "START SELECT FROM " + table);
        ResultSet result = m_connection.createStatement().executeQuery(query);
        Logging.instance().log("xaction", 1, "FINAL SELECT FROM " + table);
        return result;
    }

    /**
     * Selects any rows in one or more colums from one or more tables restricted by some condition
     * that allows operators. Permissable operators include =, &lt;&gt;, &gt;, &gt;=, &lt;, &lt;=,
     * like, etc. possibly ordered.
     *
     * @param select is the ordered set of column names to select, or simply a one-value list with
     *     an asterisk.
     * @param table is the name of the table to select from.
     * @param where is a collection of column names and values
     * @param operator is a collection of column names and operators if no entry is found for the
     *     name, then use '=' as default
     * @param order is an optional ordering string.
     */
    public ResultSet select(List select, String table, Map where, Map operator, String order)
            throws SQLException {
        StringBuffer request = new StringBuffer();
        Logging l = Logging.instance();

        request.append("SELECT ");
        for (Iterator i = select.iterator(); i.hasNext(); ) {
            request.append((String) i.next());
            if (i.hasNext()) request.append(',');
        }

        request.append(" FROM ").append(table);

        if (where != null && where.size() > 0) {
            l.log("xaction", 1, "adding WHERE clause");
            request.append(" WHERE ");
            for (Iterator i = where.keySet().iterator(); i.hasNext(); ) {
                String key = (String) i.next();
                request.append(key);
                String op = (String) operator.get(key);
                request.append(op == null ? "=\'" : (" " + op + " \'"));
                request.append(quote((String) where.get(key))).append('\'');
                if (i.hasNext()) request.append(" AND ");
            }
        } else {
            l.log("xaction", 1, "no WHERE clause");
        }

        if (order != null && order.length() > 0) request.append(order);

        String query = request.toString();
        l.log("sql", 2, query);
        l.log("xaction", 1, "START " + query);
        ResultSet result = m_connection.createStatement().executeQuery(query);
        l.log("xaction", 1, "FINAL " + query);
        return result;
    }

    /**
     * Drills a hole into the nice database driver abstraction to the JDBC3 level. Use with caution.
     *
     * @param query is an SQL query statement.
     * @exception SQLException if something goes wrong during the query
     */
    public ResultSet backdoor(String query) throws SQLException {
        Logging.instance().log("sql", 2, query);
        Logging.instance().log("xaction", 1, "START " + query);
        ResultSet result = m_connection.createStatement().executeQuery(query);
        Logging.instance().log("xaction", 1, "FINAL " + query);
        return result;
    }

    //
    // handle prepared statements
    //

    /**
     * Predicate to tell the schema, if using a string instead of number will result in the speedier
     * index scans instead of sequential scans. PostGreSQL has this problem, but using strings in
     * the place of integers may not be universally portable.
     *
     * @return true, if using strings instead of integers and bigints will yield better performance.
     */
    public abstract boolean preferString();

    /**
     * Inserts a new prepared statement into the list of prepared statements. If the id is already
     * taken, it will be rejected.
     *
     * @param id is the id into which to parse the statement
     * @param statement is the before-parsing statement string
     * @return true, if the statement was parsed and added at the id, false, if the id was already
     *     taken. The statement is not parsed in that case.
     * @exception SQLException may be thrown by parsing the statement.
     * @see #removePreparedStatement( String )
     * @see #getPreparedStatement( String )
     * @see #cancelPreparedStatement( String )
     */
    public boolean addPreparedStatement(String id, String statement) throws SQLException {
        // sanity check
        if (this.m_prepared == null)
            throw new RuntimeException("You forgot to initialize the prepared statement length");

        // duplicate check
        if (this.m_prepared.containsKey(id)) return false;

        // add to id
        Logging.instance().log("sql", 2, statement);
        Logging.instance().log("xaction", 1, "START prepare " + statement);

        try {
            this.m_prepared.put(id, this.m_connection.prepareStatement(statement));
        } catch (SQLException original) {
            SQLException sql = original;
            for (int i = 0; sql != null; ++i) {
                Logging.instance()
                        .log(
                                "sql",
                                0,
                                "SQL error "
                                        + i
                                        + ": "
                                        + sql.getErrorCode()
                                        + ": "
                                        + sql.getMessage());
                sql = sql.getNextException();
            }
            throw original;
        } catch (NullPointerException e) {
            Logging.instance().log("sql", 0, "stumbled over null");
            e.printStackTrace();
            System.exit(1);
        }

        Logging.instance().log("xaction", 1, "FINAL prepare " + statement);
        return true;
    }

    /**
     * Inserts a new prepared statement into the list of prepared statements. If the id is already
     * taken, it will be rejected. This method can only be used with JDBC drivers that support
     * auto-increment columns. It might fail with JDBC drivers that do not support auto-increment
     * columns, depending on the driver's implementation.
     *
     * @param id is the id into which to parse the statement
     * @param statement is the before-parsing statement string
     * @param autoGeneratedKeys is true, if the statement should reserve space to return autoinc
     *     columns, false, if the statement does not have any such keys.
     * @return true, if the statement was parsed and added at the id, false, if the id was already
     *     taken. The statement is not parsed in that case.
     * @exception SQLException may be thrown by parsing the statement.
     * @see #removePreparedStatement( String )
     * @see #getPreparedStatement( String )
     * @see #cancelPreparedStatement( String )
     */
    protected boolean addPreparedStatement(String id, String statement, boolean autoGeneratedKeys)
            throws SQLException {
        // sanity check
        if (this.m_prepared == null)
            throw new RuntimeException("You forgot to initialize the prepared statement length");

        // duplicate check
        if (this.m_prepared.containsKey(id)) return false;

        // add to id
        Logging.instance().log("sql", 2, statement);
        Logging.instance().log("xaction", 1, "START prepare " + statement);

        try {
            this.m_prepared.put(
                    id,
                    this.m_connection.prepareStatement(
                            statement,
                            autoGeneratedKeys
                                    ? Statement.RETURN_GENERATED_KEYS
                                    : Statement.NO_GENERATED_KEYS));
        } catch (SQLException original) {
            SQLException sql = original;
            for (int i = 0; sql != null; ++i) {
                Logging.instance()
                        .log(
                                "sql",
                                0,
                                "SQL error "
                                        + i
                                        + ": "
                                        + sql.getErrorCode()
                                        + ": "
                                        + sql.getMessage());
                sql = sql.getNextException();
            }
            throw original;
        } catch (NullPointerException e) {
            Logging.instance().log("sql", 0, "stumbled over null");
            e.printStackTrace();
            System.exit(1);
        }

        Logging.instance().log("xaction", 1, "FINAL prepare " + statement);
        return true;
    }

    /**
     * Inserts a new prepared statement into the list of prepared statements. If the id is already
     * taken, an error will be printed and execution aborted.
     *
     * @param id is the id into which to parse the statement
     * @param statement is the before-parsing statement string
     * @return true, if the statement was parsed and added at the id, false, if the id was already
     *     taken. The statement is not parsed in that case.
     * @exception SQLException may be thrown by parsing the statement.
     * @see #addPreparedStatement( String, String )
     * @see #getPreparedStatement( String )
     * @see #cancelPreparedStatement( String )
     */
    public boolean insertPreparedStatement(String id, String statement) throws SQLException {
        boolean result = addPreparedStatement(id, statement);
        if (result == false) {
            System.err.println("Duplicate key " + id);
            System.exit(1);
        }
        return result;
    }

    /**
     * Obtains a reference to a prepared statement to be used from the caller. This function will
     * also reset the input values in the prepared statement.
     *
     * @param id is the place of the statement to free up.
     * @exception SQLException if the database does not like the disconnect.
     * @see java.sql.PreparedStatement#clearParameters()
     * @see #addPreparedStatement( String, String )
     * @see #removePreparedStatement( String )
     * @see #cancelPreparedStatement( String )
     */
    public PreparedStatement getPreparedStatement(String id) throws SQLException {
        PreparedStatement result = (PreparedStatement) this.m_prepared.get(id);
        if (result != null) result.clearParameters();
        else throw new SQLException("unknown prepared statement " + id);
        return result;
    }

    /**
     * Explicitely requests a prepared id to be destroyed and its resources freed. Multiple
     * invocation for the same id are harmless.
     *
     * @param id is the place of the statement to free up.
     * @exception SQLException if the database does not like the disconnect.
     * @see #addPreparedStatement( String, String )
     * @see #getPreparedStatement( String )
     * @see #cancelPreparedStatement( String )
     */
    public void removePreparedStatement(String id) throws SQLException {
        PreparedStatement ps = (PreparedStatement) this.m_prepared.remove(id);
        if (ps != null) ps.close();
    }

    /**
     * Cancels and resets all previous values of a prepared statement.
     *
     * @param id is the id for which to obtain the previously prepared statement.
     * @exception SQLException if a database access error occurs while clearing the parameters.
     * @exception ArrayIndexOutOfBoundsException if a non-existing id is being requested.
     * @see java.sql.PreparedStatement#clearParameters()
     * @see #addPreparedStatement( String, String )
     * @see #getPreparedStatement( String )
     * @see #removePreparedStatement( String )
     */
    public void cancelPreparedStatement(String id)
            throws SQLException, ArrayIndexOutOfBoundsException {
        PreparedStatement ps = (PreparedStatement) this.m_prepared.get(id);
        if (ps != null) {
            ps.cancel();
            ps.clearParameters();
        }
    }
}
