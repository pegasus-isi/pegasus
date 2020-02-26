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
package org.griphyn.vdl.dbschema;

import edu.isi.pegasus.common.util.DynamicLoader;
import java.io.IOException;
import java.lang.reflect.*;
import java.sql.*;
import java.util.*;
import org.griphyn.vdl.dbdriver.*;
import org.griphyn.vdl.util.ChimeraProperties;
import org.griphyn.vdl.util.Logging;

/**
 * This common schema interface defines the schemas in which the abstraction layers access any given
 * database. It is independent of the implementing database, and does so by going via the database
 * driver class API.
 *
 * <p>The separation of database driver and schema lowers the implementation cost, as only N driver
 * and M schemas need to be implemented, instead of N x M schema-specific database-specific drivers.
 *
 * @author Jens-S. Vöckler
 * @author Yong Zhao
 * @version $Revision$
 * @see org.griphyn.vdl.dbdriver
 */
public abstract class DatabaseSchema implements Catalog {
    /** This is the variable that connect to the lower level database driver. */
    protected DatabaseDriver m_dbdriver;

    /** This stores properties specific to the schema. Currently unused. */
    protected Properties m_dbschemaprops;

    //
    // class methods
    //

    /**
     * Instantiates the appropriate leaf schema according to property values. This method is a
     * factory.
     *
     * @param dbSchemaName is the name of the class that conforms to the DatabaseSchema API. This
     *     class will be dynamically loaded. If the passed value is <code>null</code>, which should
     *     be the default, the value of property vds.db.schema is taken.
     * @param propertyPrefix is the property prefix string to use.
     * @param arguments are arguments to the constructor of the driver to load. Please use "new
     *     Object[0]" for the default constructor.
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
     * @see org.griphyn.vdl.util.ChimeraProperties
     */
    public static DatabaseSchema loadSchema(
            String dbSchemaName, String propertyPrefix, Object[] arguments)
            throws ClassNotFoundException, IOException, NoSuchMethodException,
                    InstantiationException, IllegalAccessException, InvocationTargetException {
        Logging log = Logging.instance();
        log.log(
                "dbschema",
                3,
                "accessing loadSchema( "
                        + (dbSchemaName == null ? "(null)" : dbSchemaName)
                        + ", "
                        + (propertyPrefix == null ? "(null)" : propertyPrefix)
                        + " )");

        // determine the database schema to load
        if (dbSchemaName == null) {
            // get it by property prefix
            dbSchemaName = ChimeraProperties.instance().getDatabaseSchemaName(propertyPrefix);
            if (dbSchemaName == null)
                throw new RuntimeException(
                        "You need to specify the " + propertyPrefix + " property");
        }

        // syntactic sugar adds absolute class prefix
        if (dbSchemaName.indexOf('.') == -1) {
            // how about xxx.getClass().getPackage().getName()?
            dbSchemaName = "org.griphyn.vdl.dbschema." + dbSchemaName;
        }

        // POSTCONDITION: we have now a fully-qualified class name
        log.log("dbschema", 3, "trying to load " + dbSchemaName);
        DynamicLoader dl = new DynamicLoader(dbSchemaName);
        DatabaseSchema result = (DatabaseSchema) dl.instantiate(arguments);

        // done
        if (result == null) log.log("dbschema", 0, "unable to load " + dbSchemaName);
        else log.log("dbschema", 3, "successfully loaded " + dbSchemaName);
        return result;
    }

    /**
     * Convenience method instantiates the appropriate child according to property values.
     * Effectively, the following is being called:
     *
     * <pre>
     * loadSchema( null, propertyPrefix, new Object[0] );
     * </pre>
     *
     * @param propertyPrefix is the property prefix string to use.
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
     * @see #loadSchema( String, String, Object[] )
     * @see org.griphyn.vdl.util.ChimeraProperties
     */
    public static DatabaseSchema loadSchema(String propertyPrefix)
            throws ClassNotFoundException, IOException, NoSuchMethodException,
                    InstantiationException, IllegalAccessException, InvocationTargetException {
        return loadSchema(null, propertyPrefix, new Object[0]);
    }

    //
    // instance methods
    //

    /**
     * Minimalistic default ctor. This constructor does nothing, and loads nothing. But it
     * initializes the empty schema props.
     */
    protected DatabaseSchema() {
        Logging.instance().log("dbschema", 3, "accessing DatabaseSchema()");
        this.m_dbdriver = null;
        this.m_dbschemaprops = new Properties();
    }

    /**
     * Connects to the database, this method does not rely on global property values, instead, each
     * property has to be provided explicitly.
     *
     * @param dbDriverName is the name of the class that conforms to the DatabaseDriver API. This
     *     class will be dynamically loaded.
     * @param url is the database url
     * @param dbDriverProperties holds properties specific to the database driver.
     * @param dbSchemaProperties holds properties specific to the database schema.
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
     */
    public DatabaseSchema(
            String dbDriverName,
            String url,
            Properties dbDriverProperties,
            Properties dbSchemaProperties)
            throws ClassNotFoundException, IOException, NoSuchMethodException,
                    InstantiationException, IllegalAccessException, InvocationTargetException,
                    SQLException {
        Logging.instance()
                .log(
                        "dbschema",
                        3,
                        "accessing DatabaseSchema(String,String, Properties, Properties)");

        // dynamically load the driver from its default constructor
        this.m_dbdriver = DatabaseDriver.loadDriver(dbDriverName, null, new Object[0]);
        this.m_dbschemaprops = dbSchemaProperties;

        // create a database connection right now, right here
        // mind, url may be null, which may be legal for some drivers!
        Logging.instance().log("dbschema", 3, "invoking connect( " + url + " )");
        this.m_dbdriver.connect(url, dbDriverProperties, null);

        Logging.instance().log("dbschema", 3, "connected to database backend");

        // prepare statements as necessary in the implementing classes!
    }

    /**
     * Guesses from the schema prefix the driver prefix.
     *
     * @param schemaPrefix is the property key prefix for the schema.
     * @return the guess for the driver's prefix, may be <code>null</code>
     */
    private static String driverFromSchema(String schemaPrefix) {
        String result = null;
        if (schemaPrefix != null && schemaPrefix.endsWith(".schema"))
            result = schemaPrefix.substring(0, schemaPrefix.length() - 7) + ".driver";
        Logging.instance()
                .log(
                        "dbschema",
                        4,
                        "dbdriver prefix guess " + (result == null ? "(null)" : result));
        return result;
    }

    /**
     * Guesses from the schema prefix the db prefix.
     *
     * @param schemaPrefix is the property key prefix for the schema.
     * @return the guess for the db properties prefix, may be <code>null</code>
     */
    private static String dbFromSchema(String schemaPrefix) {
        String result = null;
        if (schemaPrefix != null && schemaPrefix.endsWith(".schema"))
            result = schemaPrefix.substring(0, schemaPrefix.length() - 7);
        Logging.instance()
                .log(
                        "dbschema",
                        4,
                        "db propertiesr prefix guess " + (result == null ? "(null)" : result));
        return result;
    }

    /**
     * Connects to the database as specified by the properties, and checks the schema
     * implementation. Makes heavy use of global property values.
     *
     * @param dbDriverName is the name of the class that conforms to the DatabaseDriver API. This
     *     class will be dynamically loaded. If the passed value is <code>null</code>, which should
     *     be the default, the value of property vds.db.*.driver is taken.
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
     */
    public DatabaseSchema(String dbDriverName, String propertyPrefix)
            throws ClassNotFoundException, IOException, NoSuchMethodException,
                    InstantiationException, IllegalAccessException, InvocationTargetException,
                    SQLException {
        Logging.instance().log("dbschema", 3, "accessing DatabaseSchema(String,String)");

        // guess the db driver property prefix from schema prefix
        String driverPrefix = DatabaseSchema.driverFromSchema(propertyPrefix);

        // cache the properties - we may need a lot of them
        ChimeraProperties props = ChimeraProperties.instance();

        if (dbDriverName == null || dbDriverName.equals("")) {
            if (driverPrefix != null) dbDriverName = props.getDatabaseDriverName(driverPrefix);
            if (dbDriverName == null)
                throw new RuntimeException("You need to specify the database driver property");
        }
        Logging.instance().log("dbschema", 4, "dbdriver class " + dbDriverName);

        // dynamically load the driver from its default constructor
        this.m_dbdriver = DatabaseDriver.loadDriver(dbDriverName, driverPrefix, new Object[0]);
        this.m_dbschemaprops = props.getDatabaseSchemaProperties(propertyPrefix);

        // instead of the driverPrefix, use the DB prefix
        // This is because the DB properties are now gotten from example
        // pegasus.catalog.provenance.db.* instead of
        // pegasus.catalog.proveance.db.driver.*
        // Karan Oct 25, 2007. Pegasus Bug Number: 11
        // http://vtcpc.isi.edu/bugzilla/show_bug.cgi?id=11
        String dbPrefix = DatabaseSchema.dbFromSchema(propertyPrefix);

        //    Properties dbdriverprops = props.getDatabaseDriverProperties(driverPrefix);
        //    String url = props.getDatabaseURL(driverPrefix);

        // extract those properties specific to the database driver.
        // these properties are transparently passed through MINUS the url key.
        Properties dbdriverprops = props.getDatabaseDriverProperties(dbPrefix);
        String url = props.getDatabaseURL(dbPrefix);

        // create a database connection right now, right here
        // mind, url may be null, which may be legal for some drivers!
        Logging.instance().log("dbschema", 3, "invoking connect( " + url + " )");
        this.m_dbdriver.connect(url, dbdriverprops, null);

        Logging.instance().log("dbschema", 3, "connected to database backend");

        // prepare statements as necessary in the implementing classes!
    }

    /**
     * Associates a schema with a given database driver.
     *
     * @param driver is an instance conforming to the DatabaseDriver API.
     * @param propertyPrefix is the property prefix string to use.
     * @exception SQLException if the driver for the database can be loaded, but faults when
     *     initially accessing the database
     */
    public DatabaseSchema(DatabaseDriver driver, String propertyPrefix)
            throws SQLException, ClassNotFoundException, IOException {
        Logging.instance().log("dbschema", 3, "accessing DatabaseSchema(DatabaseDriver,String)");
        this.m_dbdriver = driver;

        // guess the db driver property prefix from schema prefix
        String driverPrefix = DatabaseSchema.driverFromSchema(propertyPrefix);

        // cache the properties - we may need a lot of them
        ChimeraProperties props = ChimeraProperties.instance();

        // get database schema properties
        this.m_dbschemaprops = props.getDatabaseSchemaProperties(propertyPrefix);

        // extract those properties specific to the database driver.
        // these properties are transparently passed through MINUS the url key.
        Properties dbdriverprops = props.getDatabaseDriverProperties(driverPrefix);
        String url = props.getDatabaseURL(driverPrefix);

        // create a database connection right now, right here
        // mind, url may be null, which may be legal for some drivers!
        Logging.instance().log("dbschema", 3, "invoking connect( " + url + " )");
        this.m_dbdriver.connect(url, dbdriverprops, null);

        Logging.instance().log("dbschema", 3, "connected to database backend");

        // prepare statements as necessary in the implementing classes!
    }

    /**
     * pass-thru to driver.
     *
     * @return true, if it is feasible to cache results from the driver false, if requerying the
     *     driver is sufficiently fast (e.g. driver is in main memory, or driver does caching
     *     itself).
     */
    public boolean cachingMakesSense() {
        return this.m_dbdriver.cachingMakesSense();
    }

    /**
     * Disassociate from the database driver before finishing. Mind that performing this action may
     * throw NullPointerException in later stages!
     */
    public void close() throws SQLException {
        if (this.m_dbdriver != null) {
            this.m_dbdriver.disconnect();
            this.m_dbdriver = null;
        }
    }

    /** Disassociate the database driver cleanly. */
    protected void finalize() throws Throwable {
        this.close();
        super.finalize();
    }

    //
    // papa's little helpers
    //

    /**
     * Adds a string or a SQL-NULL at the current prepared statement position, depending if the
     * String value is null or not.
     *
     * @param ps is the prepared statement to extend
     * @param pos is the position at which to insert the value
     * @param s is the String to use, which may be null.
     */
    protected void stringOrNull(PreparedStatement ps, int pos, String s) throws SQLException {
        if (s == null) ps.setNull(pos, Types.VARCHAR);
        else ps.setString(pos, s);
    }

    /**
     * Adds a BIGINT or a SQL-NULL at the current prepared statement position, depending if the
     * value is -1 or not. A value of -1 will lead to SQL-NULL.
     *
     * @param ps is the prepared statement to extend
     * @param pos is the position at which to insert the value
     * @param l is the long to use, which may be null.
     */
    protected void longOrNull(PreparedStatement ps, int pos, long l) throws SQLException {
        if (l == -1) ps.setNull(pos, Types.BIGINT);
        else {
            if (m_dbdriver.preferString()) ps.setString(pos, Long.toString(l));
            else ps.setLong(pos, l);
        }
    }

    /**
     * Converts any given string into a guaranteed non-null value. Especially the definition triples
     * use empty strings instead of null values.
     *
     * @param s is the string object to look at, which may be null.
     * @return a string that may be empty, but is not null.
     */
    protected String makeNotNull(String s) {
        return (s == null ? new String() : s);
    }
}
