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
package org.griphyn.vdl.util;

import edu.isi.pegasus.common.util.CommonProperties;
import edu.isi.pegasus.common.util.Currently;
import java.io.File;
import java.io.IOException;
import java.util.*;

/**
 * A Central Properties class that keeps track of all the properties used by Chimera. All other
 * classes access the methods in this class to get the value of the property. It access the
 * CommonProperties class to read the property file.
 *
 * @author Jens-S. Vöckler
 * @author Yong Zhao
 * @version $Revision$
 * @see org.griphyn.common.util.CommonProperties
 */
public class ChimeraProperties {
    /** Default values for schema locations. */
    public static final String VDL_SCHEMA_LOCATION = "http://www.griphyn.org/chimera/vdl-1.24.xsd";

    public static final String DAX_SCHEMA_LOCATION = "http://pegasus.isi.edu/schema/dax-4.0.xsd";

    public static final String IVR_SCHEMA_LOCATION = "http://pegasus.isi.edu/schema/iv-2.1.xsd";

    public static final String DB_ALL_PREFIX = "pegasus.catalog.*.db";

    public static final String DBDRIVER_ALL_PREFIX = "pegasus.catalog.*.db.driver";

    /** Implements the Singleton access. */
    private static ChimeraProperties m_instance = null;

    /** The value of the PEGASUS_HOME environment variable. */
    private String m_home;

    /** The object holding all the properties pertaining to the VDS system. */
    private CommonProperties m_props;

    /** To get a reference to the the object. */
    public static ChimeraProperties instance() throws IOException, MissingResourceException {
        if (m_instance == null) {
            m_instance = new ChimeraProperties();
        }
        return m_instance;
    }

    /** Constructor that is called only once, when creating the Singleton instance. */
    private ChimeraProperties() throws IOException, MissingResourceException {
        m_props = getVDSPropertiesInstance();
        m_home = m_props.getBinDir() + "/..";
    }

    /** Gets the handle to the property file. */
    private CommonProperties getVDSPropertiesInstance()
            throws IOException, MissingResourceException {
        return CommonProperties.instance();
    }

    /** Set up logging */
    public void setupLogging(Logging logger) {
        for (Enumeration e = m_props.propertyNames(); e.hasMoreElements(); ) {
            String key = (String) e.nextElement();
            if (key.equals("pegasus.timestamp.format")) {
                Currently c = new Currently(this.m_props.getProperty(key));
                logger.setDateFormat(c);
            } else if (key.startsWith("pegasus.log.")) {
                String subkey = key.substring(8);
                logger.log("default", 2, "found \"" + key + "\" -> " + subkey);
                logger.register(subkey, this.m_props.getProperty(key));
            } else if (key.startsWith("pegasus.verbose")) {
                int verbose = Integer.parseInt(this.m_props.getProperty(key));
                logger.log("default", 2, "verbosity mode = " + verbose);
                logger.setVerbose(verbose);
            }
        }
    }

    /**
     * Accessor to $PEGASUS_HOME/etc. The files in this directory have a low change frequency, are
     * effectively read-only, they reside on a per-machine basis, and they are valid usually for a
     * single user.
     *
     * @return the "etc" directory of the VDS runtime system.
     */
    public File getSysConfDir() {
        return m_props.getSysConfDir();
    }

    /**
     * Accessor: Obtains the root directory of the VDS/Chimera runtime system.
     *
     * @return the root directory of the VDS runtime system, as initially set from the system
     *     properties.
     */
    public String getVDSHome() {
        return m_home;
    }

    /**
     * Accessor to $PEGASUS_HOME/share. The files in this directory have a low change frequency, are
     * effectively read-only, can be shared via a networked FS, and they are valid for multiple
     * users.
     *
     * @return the "share" directory of the VDS runtime system.
     */
    public File getDataDir() {
        return m_props.getSharedStateDir();
    }

    /**
     * Get the fully qualified class name of the VDC-implementing database schema. If no properties
     * are configured, it returns the file-based VDC-schema implementation.
     *
     * @return the fully-qualified name of the class which implements the VDC according to
     *     properties.
     * @see org.griphyn.vdl.dbschema.SingleFileSchema
     */
    public String getVDCSchemaName() {
        // load the default schema name - default is to use the file based
        // schema.
        String schemaName = m_props.getProperty("pegasus.db.vdc.schema", "SingleFileSchema");
        if (schemaName.indexOf('.') == -1) schemaName = "org.griphyn.vdl.dbschema." + schemaName;

        // always returns something
        return schemaName;
    }

    /**
     * Obtains the fully qualified class name of the PTC-implementing database schema.
     *
     * @return the fully-qualified name of the class which implements the PTC according to
     *     properties, or <code>null</code>, if no such class exists.
     */
    public String getPTCSchemaName() {
        // load the default schema name - default is to use the file based
        // schema.
        // this should not have a default value because if this property is not set
        // the invocation records should not be populated to DB.
        String schemaName = m_props.getProperty("pegasus.catalog.provenance");
        if (schemaName != null && schemaName.indexOf('.') == -1)
            schemaName = "org.griphyn.vdl.dbschema." + schemaName;

        // may return null
        return schemaName;
    }

    /**
     * Obtains the fully qualified class name of the WF-implementing database schema.
     *
     * @return the fully-qualified name of the class which implements the WF according to
     *     properties, or <code>null</code>, if no such class exists.
     */
    public String getWFSchemaName() {
        // load the default schema name
        String schemaName = m_props.getProperty("pegasus.db.wf.schema");
        if (schemaName != null && schemaName.indexOf('.') == -1)
            schemaName = "org.griphyn.vdl.dbschema." + schemaName;

        // may return null
        return schemaName;
    }

    /**
     * Gets the location the VDLx XML schema from properties, if available. Please note that the
     * schema location URL in the instance document is only a hint, and may be overriden by the
     * findings of this method.
     *
     * @return a location pointing to a definition document of the XML schema that can read VDLx.
     *     Result may be null, if such a document is unknown or unspecified.
     * @see org.griphyn.vdl.parser.VDLxParser#VDLxParser( String )
     */
    public String getVDLSchemaLocation() {
        // treat URI as File, yes, I know - I need the basename
        File uri = new File(VDL_SCHEMA_LOCATION);
        File vdlx = // create a pointer to the default local position
                new File(this.m_props.getSysConfDir(), uri.getName());

        // Nota bene: pegasus.schema.vdl may be a networked URI...
        return m_props.getProperty("pegasus.schema.vdl", vdlx.getAbsolutePath());
    }

    /**
     * Gets the location of the DAX XML schema from properties, if available. Please note that the
     * schema location URL in the instance document is only a hint, and may be overriden by the
     * findings of this method.
     *
     * @return a location pointing to a definition document of the XML schema that can read DAX.
     *     Result may be null, if such a document is unknown or unspecified.
     * @see org.griphyn.vdl.parser.DAXParser#DAXParser( String )
     */
    public String getDAXSchemaLocation() {
        // treat URI as File, yes, I know - I need the basename
        File uri = new File(DAX_SCHEMA_LOCATION);
        File dax = // create a pointer to the default local position
                new File(m_props.getSysConfDir(), uri.getName());

        // Nota bene: pegasus.schema.dax may be a networked URI...
        return m_props.getProperty("pegasus.schema.dax", dax.getAbsolutePath());
    }

    /**
     * Helps the load database to locate the invocation record XML schema, if available. Please note
     * that the schema location URL in the instance document is only a hint, and may be overriden by
     * the findings of this method.
     *
     * @return a location pointing to a definition document of the XML schema that can read DAX.
     *     Result may be null, if such a document is unknown or unspecified.
     * @see org.griphyn.vdl.parser.InvocationParser#InvocationParser( String )
     */
    public String getPTCSchemaLocation() {
        // treat URI as File, yes, I know - I need the basename
        File uri = new File(IVR_SCHEMA_LOCATION);
        File ptc = // create a pointer to the default local position
                new File(m_props.getSysConfDir(), uri.getName());

        // Nota bene: pegasus.schema.ptc may be a networked URI...
        return m_props.getProperty("pegasus.catalog.provenance", ptc.getAbsolutePath());
    }

    /** Get the rc.data file location, which is used by shell planner */
    public String getRCLocation() {
        File rcFile = new File(m_props.getSysConfDir(), "rc.data");
        return m_props.getProperty("pegasus.db.rc", rcFile.getAbsolutePath());
    }

    /** Get the tc.data file location, which is used by shell planner */
    public String getTCLocation() {
        File tcFile = new File(m_props.getSysConfDir(), "tc.data");
        return m_props.getProperty("pegasus.db.tc", tcFile.getAbsolutePath());
    }

    /**
     * Gets the name of the database schema name from the properties.
     *
     * @param dbSchemaPrefix is the database schema key name in the properties file, which happens
     *     to be the pointer to the class to load.
     * @return the database schema name, result may be null, if such property is not specified.
     */
    public String getDatabaseSchemaName(String dbSchemaPrefix) {
        return m_props.getProperty(dbSchemaPrefix);
    }

    /**
     * Gets then name of the database driver from the properties. A specific match is preferred over
     * the any match.
     *
     * @param dbDriverPrefix is the database schema key name in the properties file, which happens
     *     to be the pointer to the class to load.
     * @return the database driver name, result may be null, if such property is not specified.
     */
    public String getDatabaseDriverName(String dbDriverPrefix) {
        return (dbDriverPrefix == null
                ? m_props.getProperty(DBDRIVER_ALL_PREFIX)
                : m_props.getProperty(dbDriverPrefix, m_props.getProperty(DBDRIVER_ALL_PREFIX)));
    }

    /**
     * Gets the Database URL from Properties file, the URL is a contact string to the database. The
     * URL contact string is removed from the regular properties which are passed to the JDBC
     * driver.
     *
     * @param dbDriverPrefix is the database schema key name.
     * @return the database url, result may be <code>null</code>, if the driver URL is not
     *     specified.
     * @see #getDatabaseDriverProperties( String )
     */
    public String getDatabaseURL(String dbDriverPrefix) {
        return (dbDriverPrefix == null
                ?
                // pick pegasus.catalog.*.db.url
                m_props.getProperty(DB_ALL_PREFIX + ".url")
                : m_props.getProperty(
                        dbDriverPrefix + ".url",
                        // default value pegasus.catalog.*.db.url
                        m_props.getProperty(DB_ALL_PREFIX + ".url")));
    }

    /**
     * Extracts a specific property key subset from the known properties. The prefix is removed from
     * the keys in the resulting dictionary.
     *
     * @param prefix is the key prefix to filter the properties by.
     * @return a property dictionary matching the filter key. May be an empty dictionary, if no
     *     prefix matches were found.
     */
    public Properties matchingSubset(String prefix) {
        return m_props.matchingSubset(prefix, false);
    }

    /**
     * Obtains database driver specific properties.
     *
     * @param dbDriverPrefix is the database driver property key prefix for which to obtain
     *     properties.
     * @return a property set to be filled with driver specific properties. May be null if no such
     *     properties specified.
     */
    public Properties getDatabaseDriverProperties(String dbDriverPrefix) {
        Properties result = new Properties(matchingSubset(DB_ALL_PREFIX));
        if (dbDriverPrefix != null) result.putAll(matchingSubset(dbDriverPrefix));
        result.remove("url"); // must not be passed to the JDBC driver
        return result;
    }

    /**
     * Obtains the database schema specific properties.
     *
     * @param dbSchemaPrefix is the database schema key name in the properties file
     * @return a property set to be filled with schema specific properties. May be null if no such
     *     properties specified.
     */
    public Properties getDatabaseSchemaProperties(String dbSchemaPrefix) {
        return matchingSubset(dbSchemaPrefix);
    }

    /**
     * Gets the name of the replica catalog implementating class from the properties.
     *
     * @param dbReplicaPrefix is the replica catalog class name in the properties file.
     * @return the replica catalog implementing class name, result may be null, if such property is
     *     not specified.
     */
    public String getReplicaCatalogName(String dbReplicaPrefix) {
        return m_props.getProperty(dbReplicaPrefix);
    }

    /**
     * Obtains all properties to handle the experimental replica catalog interface.
     *
     * @param dbReplicaPrefix is the prefix for the replica catalog's implementation configuration.
     * @return all properties, excluding the prefix itself, for the RC.
     */
    public Properties getReplicaCatalogProperties(String dbReplicaPrefix) {
        return matchingSubset(dbReplicaPrefix);
    }
}
