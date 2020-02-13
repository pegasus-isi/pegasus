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

package edu.isi.pegasus.planner.common;

import edu.isi.pegasus.common.util.Boolean;
import edu.isi.pegasus.common.util.CommonProperties;
import edu.isi.pegasus.planner.catalog.classes.Profiles;
import edu.isi.pegasus.planner.classes.NameValue;
import edu.isi.pegasus.planner.namespace.Dagman;
import edu.isi.pegasus.planner.namespace.Namespace;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.MissingResourceException;
import java.util.Properties;
import java.util.Set;

/**
 * A Central Properties class that keeps track of all the properties used by Pegasus. All other
 * classes access the methods in this class to get the value of the property. It access the
 * CommonProperties class to read the property file.
 *
 * @author Karan Vahi
 * @author Gaurang Mehta
 * @version $Revision$
 * @see org.griphyn.common.util.CommonProperties
 */
public class PegasusProperties implements Cloneable {

    /** the name of the property to disable invoke functionality */
    public static final String DISABLE_INVOKE_PROPERTY = "pegasus.gridstart.invoke.disable";

    public static final String PEGASUS_KICKSTART_STAT_PROPERTY = "pegasus.gridstart.kickstart.stat";

    public static final String PEGASUS_WORKER_NODE_EXECUTION_PROPERTY =
            "pegasus.execute.*.filesystem.local";

    public static final String PEGASUS_TRANSFER_WORKER_PACKAGE_PROPERTY =
            "pegasus.transfer.worker.package";

    public static final String PEGASUS_TRANSFORMATION_CATALOG_PROPERTY =
            "pegasus.catalog.transformation";

    public static final String PEGASUS_TRANSFORMATION_CATALOG_FILE_PROPERTY =
            "pegasus.catalog.transformation.file";

    public static final String PEGASUS_REPLICA_CATALOG_PROPERTY = "pegasus.catalog.replica";

    public static final String PEGASUS_REPLICA_CATALOG_FILE_PROPERTY =
            "pegasus.catalog.replica.file";

    public static final String PEGASUS_SITE_CATALOG_PROPERTY = "pegasus.catalog.site";

    public static final String PEGASUS_SITE_CATALOG_FILE_PROPERTY = "pegasus.catalog.site.file";

    public static final String PEGASUS_LOG_METRICS_PROPERTY = "pegasus.log.metrics";

    public static final String PEGASUS_LOG_METRICS_PROPERTY_FILE = "pegasus.log.metrics.file";

    public static final String PEGASUS_APP_METRICS_PREFIX = "pegasus.metrics.app";

    // Replica Catalog Constants
    public static final String DEFAULT_RC_COLLECTION = "GriphynData";

    public static final String DEFAULT_RLI_URL = null;

    public static final String DEFAULT_RLS_QUERY_MODE = "bulk";

    public static final String DEFAULT_RLS_EXIT_MODE = "error";

    // public static final String DEFAULT_REPLICA_MODE = "rls";

    public static final String DEFAULT_RLS_QUERY_ATTRIB = "false";

    public static final String DEFAULT_LRC_IGNORE_URL = null;

    public static final String DEFAULT_RLS_TIMEOUT = "30";

    public static final String DEFAULT_EXEC_DIR = "";

    public static final String DEFAULT_STORAGE_DIR = "";

    public static final String DEFAULT_TC_MODE = "Text";

    public static final String TC_TEXT_FILE = "tc.txt";

    public static final String DEFAULT_SITE_CATALOG_IMPLEMENTOR = "YAML";

    public static final String DEFAULT_CONDOR_BIN_DIR = "";

    public static final String DEFAULT_CONDOR_CONFIG_DIR = "";

    public static final String CONDOR_KICKSTART = "kickstart-condor";

    // transfer constants
    public static final String DEFAULT_STAGING_DELIMITER = "-";

    public static final String DEFAULT_TRANSFER_PROCESSES = "4";

    public static final String DEFAULT_TRANSFER_STREAMS = "1";

    // grid start constants

    public static final String DEFAULT_INVOKE_LENGTH = "4000";

    // site selector constants
    public static final String DEFAULT_SITE_SELECTOR = "Random";

    public static final String DEFAULT_SITE_SELECTOR_TIMEOUT = "300";

    public static final String DEFAULT_SITE_SELECTOR_KEEP = "onerror";

    /// some simulator constants that are used
    public static final String DEFAULT_DATA_MULTIPLICATION_FACTOR = "1";

    public static final String DEFAULT_COMP_MULTIPLICATION_FACTOR = "1";

    public static final String DEFAULT_COMP_ERROR_PERCENTAGE = "0";

    public static final String DEFAULT_COMP_VARIANCE_PERCENTAGE = "0";

    // collapsing constants
    public static final String DEFAULT_JOB_AGGREGATOR = "SeqExec";

    // some tranformation catalog constants
    public static final String DEFAULT_TC_MAPPER_MODE = "All";

    public static final String DEFAULT_TX_SELECTOR_MODE = "Random";

    // logging constants
    public static final String DEFAULT_LOGGING_FILE = "stdout";

    /** Default properties that applies priorities to all kinds of transfer jobs. */
    public static final String ALL_TRANSFER_PRIORITY_PROPERTY_KEY = "pegasus.transfer.*.priority";

    /** The property key designated the root workflow uuid. */
    public static final String ROOT_WORKFLOW_UUID_PROPERTY_KEY = "pegasus.workflow.root.uuid";

    /** The default value to be assigned for dagman.maxpre . */
    public static final String DEFAULT_DAGMAN_MAX_PRE_VALUE = "1";

    /** An enum defining The dial for cleanup algorithm */
    public enum CLEANUP_SCOPE {
        fullahead,
        deferred
    };

    /** An enum defining the dial for integrity checking */
    public enum INTEGRITY_DIAL {
        none,
        nosymlink,
        full
    };

    /** The default DAXCallback that is loaded, if none is specified by the user. */
    private static final String DEFAULT_DAX_CALLBACK = "DAX2Graph";

    /** The value of the PEGASUS_HOME environment variable. */
    private String mPegasusHome;

    /** The object holding all the properties pertaining to the VDS system. */
    private CommonProperties mProps;

    /** The default path to the transformation catalog. */
    private String mDefaultTC;

    /** The default transfer priority that needs to be applied to the transfer jobs. */
    private String mDefaultTransferPriority;

    /** The set containing the deprecated properties specified by the user. */
    private Set mDeprecatedProperties;

    /** The pointer to the properties file that is written out in the submit directory. */
    private String mPropsInSubmitDir;

    /** Profiles that are specified in the properties */
    private Profiles mProfiles;

    private static Map<Profiles.NAMESPACES, String> mNamepsaceToPropertiesPrefix;

    public Map<Profiles.NAMESPACES, String> namespaceToPropertiesPrefix() {
        if (mNamepsaceToPropertiesPrefix == null) {
            mNamepsaceToPropertiesPrefix = new HashMap<Profiles.NAMESPACES, String>();
            mNamepsaceToPropertiesPrefix.put(Profiles.NAMESPACES.condor, "condor");
            mNamepsaceToPropertiesPrefix.put(Profiles.NAMESPACES.dagman, "dagman");
            mNamepsaceToPropertiesPrefix.put(Profiles.NAMESPACES.globus, "globus");
            mNamepsaceToPropertiesPrefix.put(Profiles.NAMESPACES.env, "env");
            mNamepsaceToPropertiesPrefix.put(Profiles.NAMESPACES.hints, "hints");
            mNamepsaceToPropertiesPrefix.put(Profiles.NAMESPACES.pegasus, "pegasus");
            mNamepsaceToPropertiesPrefix.put(Profiles.NAMESPACES.selector, "selector");
            mNamepsaceToPropertiesPrefix.put(Profiles.NAMESPACES.stat, "stat");
        }

        return mNamepsaceToPropertiesPrefix;
    }

    /**
     * Returns an instance to this properties object.
     *
     * @return a handle to the Properties class.
     */
    public static PegasusProperties getInstance() {
        return getInstance(null);
    }

    /**
     * Returns an instance to this properties object.
     *
     * @param confProperties the path to conf properties, that supersede the loading of properties
     *     from $PEGASUS_HOME/.pegasusrc
     * @return a handle to the Properties class.
     */
    public static PegasusProperties getInstance(String confProperties) {
        return nonSingletonInstance(confProperties);
    }

    /**
     * To get a reference to the the object. The properties file that is loaded is from the path
     * specified in the argument. This is *not implemented* as singleton. However the invocation of
     * this does modify the internally held singleton object.
     *
     * @param confProperties the path to conf properties, that supersede the loading of properties
     *     from $PEGASUS_HOME/.pegasusrc
     * @return a handle to the Properties class.
     */
    protected static PegasusProperties nonSingletonInstance(String confProperties) {
        return new PegasusProperties(confProperties);
    }

    /**
     * To get a reference to the the object. The properties file that is loaded is from the path
     * specified in the argument.
     *
     * <p>This is *not implemented* as singleton. However the invocation of this does modify the
     * internally held singleton object.
     *
     * @return a handle to the Properties class.
     */
    public static PegasusProperties nonSingletonInstance() {
        // return nonSingletonInstance( CommonProperties.PROPERTY_FILENAME );
        return nonSingletonInstance(null);
    }

    /**
     * The constructor that constructs the default paths to the various configuration files, and
     * populates the singleton instance as required. If the properties file passed is null, then the
     * singleton instance is invoked, else the non singleton instance is invoked.
     *
     * @param confProperties the path to conf properties, that supersede the loading of properties
     *     from $PEGASUS_HOME/.pegasusrc
     */
    private PegasusProperties(String confProperties) {
        //        mLogger = LogManager.getInstance();

        mDeprecatedProperties = new HashSet(5);
        initializePropertyFile(confProperties);

        mDefaultTC = getDefaultPathToTC();
        mDefaultTransferPriority = getDefaultTransferPriority();
    }

    /**
     * Retrieves profiles from the properties
     *
     * @param properties the common properties so far
     * @return profiles object.
     */
    public Profiles retrieveProfilesFromProperties() {
        // retrieve up all the profiles that are specified in
        // the properties
        if (mProfiles == null) {
            mProfiles = retrieveProfilesFromProperties(mProps);
            // System.out.println( mProfiles );
        }
        return mProfiles;
    }

    /**
     * Retrieves profiles from the properties
     *
     * @param properties the common properties so far
     * @return profiles object.
     */
    protected Profiles retrieveProfilesFromProperties(CommonProperties properties) {
        Profiles profiles = new Profiles();

        // retrieve some matching properties first
        // traverse through all the enum keys
        for (Profiles.NAMESPACES n : Profiles.NAMESPACES.values()) {
            Properties p = properties.matchingSubset(namespaceToPropertiesPrefix().get(n), false);
            for (Map.Entry<Object, Object> entry : p.entrySet()) {
                profiles.addProfile(n, (String) entry.getKey(), (String) entry.getValue());
            }
        }
        return profiles;
    }

    /**
     * Returns the clone of the object.
     *
     * @return the clone
     */
    public Object clone() {
        PegasusProperties props;
        try {
            // this will do a shallow clone for all member variables
            // that is fine for the string variables
            props = (PegasusProperties) super.clone();

            // clone the CommonProperties
            props.mProfiles = (this.mProfiles == null) ? null : (Profiles) this.mProfiles.clone();
            props.mProps = (this.mProps == null) ? null : (CommonProperties) this.mProps.clone();
        } catch (CloneNotSupportedException e) {
            // somewhere in the hierarch chain clone is not implemented
            throw new RuntimeException(
                    "Clone not implemented in the base class of " + this.getClass().getName(), e);
        }
        return props;
    }

    /**
     * Accessor to the bin directory of the Pegasus install
     *
     * @return the "etc" directory of the VDS runtime system.
     */
    public File getBinDir() {
        return mProps.getBinDir();
    }

    /**
     * Accessor to the schema directory of the Pegasus install
     *
     * @return the "etc" directory of the VDS runtime system.
     */
    public File getSchemaDir() {
        return mProps.getSchemaDir();
    }

    /**
     * Accessor to the bin directory of the Pegasus install
     *
     * @return the "etc" directory of the VDS runtime system.
     */
    public File getSharedDir() {
        return mProps.getSharedStateDir();
    }

    /**
     * Returns all the profiles relevant to a particular namespace
     *
     * @param ns the namespace corresponding to which you need the profiles
     */
    public Namespace getProfiles(Profiles.NAMESPACES ns) {
        return this.retrieveProfilesFromProperties().get(ns);
    }

    /**
     * Returns the default path to the transformation catalog.
     *
     * @return tc.txt in the current working directory
     */
    public String getDefaultPathToTC() {
        File f = new File(".", PegasusProperties.TC_TEXT_FILE);
        // System.err.println("Default Path to SC is " + f.getAbsolutePath());
        return f.getAbsolutePath();
    }

    /**
     * Returns the default path to the condor kickstart. Currently the path defaults to
     * $PEGASUS_HOME/bin/kickstart-condor.
     *
     * @return default path to kickstart condor.
     */
    public String getDefaultPathToCondorKickstart() {
        StringBuffer sb = new StringBuffer(50);
        sb.append(mPegasusHome);
        sb.append(File.separator);
        sb.append("bin");
        sb.append(File.separator);
        sb.append(CONDOR_KICKSTART);
        return sb.toString();
    }

    /**
     * Gets the handle to the properties file. The singleton instance is invoked if the properties
     * file is null (partly due to the way CommonProperties is implemented ), else the non singleton
     * is invoked.
     *
     * @param confProperties the path to conf properties, that supersede the loading of properties
     *     from $PEGASUS_HOME/.pegasusrc
     */
    private void initializePropertyFile(String confProperties) {
        try {
            /*
            mProps = ( confProperties == null ) ?
                //invoke the singleton instance
                CommonProperties.instance() :
                //invoke the non singleton instance
                CommonProperties.nonSingletonInstance( confProperties );
             */
            // we always load non singleton instance?
            // Karan April 27, 2011
            mProps = CommonProperties.nonSingletonInstance(confProperties);
        } catch (IOException e) {
            System.err.println("unable to read property file: " + e.getMessage());
            System.exit(1);
        } catch (MissingResourceException e) {
            System.err.println("A required property is missing: " + e.getMessage());
            System.exit(1);
        }
    }

    /**
     * It allows you to get any property from the property file without going through the
     * corresponding accesor function in this class. For coding and clarity purposes, the function
     * should be used judiciously, and the accessor function should be used as far as possible.
     *
     * @param key the property whose value is desired.
     * @return String
     */
    public String getProperty(String key) {
        return mProps.getProperty(key);
    }

    /**
     * Returns the CommonProperties that this object encapsulates. Use only when absolutely
     * necessary. Use accessor methods whereever possible.
     *
     * @return CommonProperties
     */
    public CommonProperties getVDSProperties() {
        return this.mProps;
    }

    /**
     * Accessor: Overwrite any properties from within the program.
     *
     * @param key is the key to look up
     * @param value is the new property value to place in the system.
     * @return the old value, or null if it didn't exist before.
     */
    public Object setProperty(String key, String value) {
        return mProps.setProperty(key, value);
    }

    /**
     * Extracts a specific property key subset from the known properties. The prefix may be removed
     * from the keys in the resulting dictionary, or it may be kept. In the latter case, exact
     * matches on the prefix will also be copied into the resulting dictionary.
     *
     * @param prefix is the key prefix to filter the properties by.
     * @param keepPrefix if true, the key prefix is kept in the resulting dictionary. As
     *     side-effect, a key that matches the prefix exactly will also be copied. If false, the
     *     resulting dictionary's keys are shortened by the prefix. An exact prefix match will not
     *     be copied, as it would result in an empty string key.
     * @return a property dictionary matching the filter key. May be an empty dictionary, if no
     *     prefix matches were found.
     * @see #getProperty( String ) is used to assemble matches
     */
    public Properties matchingSubset(String prefix, boolean keepPrefix) {
        return mProps.matchingSubset(prefix, keepPrefix);
    }

    /**
     * Returns the properties matching a particular prefix as a list of sorted name value pairs,
     * where name is the full name of the matching property (including the prefix) and value is it's
     * value in the properties file.
     *
     * @param prefix the prefix for the property names.
     * @param system boolean indicating whether to match only System properties or all including the
     *     ones in the property file.
     * @return list of <code>NameValue</code> objects corresponding to the matched properties sorted
     *     by keys. null if no matching property is found.
     */
    public List getMatchingProperties(String prefix, boolean system) {
        // sanity check
        if (prefix == null) {
            return null;
        }
        Properties p = (system) ? System.getProperties() : matchingSubset(prefix, true);

        java.util.Enumeration e = p.propertyNames();
        List l = (e.hasMoreElements()) ? new java.util.ArrayList() : null;

        while (e.hasMoreElements()) {
            String key = (String) e.nextElement();
            NameValue nv = new NameValue(key, p.getProperty(key));
            l.add(nv);
        }

        Collections.sort(l);
        return (l.isEmpty()) ? null : l;
    }

    /**
     * Accessor to $PEGASUS_HOME/etc. The files in this directory have a low change frequency, are
     * effectively read-only, they reside on a per-machine basis, and they are valid usually for a
     * single user.
     *
     * @return the "etc" directory of the VDS runtime system.
     */
    public File getSysConfDir() {
        return mProps.getSysConfDir();
    }

    /**
     * Removes a property from the soft state.
     *
     * @param key the key
     * @return the corresponding value if key exits, else null
     */
    public String removeProperty(String key) {
        return mProps.removeProperty(key);
    }

    // PROPERTIES RELATED TO SCHEMAS
    /**
     * Returns the location of the schema for the DAX.
     *
     * <p>Referred to by the "pegasus.schema.dax" property.
     *
     * @return location to the DAX schema.
     */
    public String getDAXSchemaLocation() {
        return this.getDAXSchemaLocation(null);
    }

    /**
     * Returns the location of the schema for the DAX.
     *
     * <p>Referred to by the "pegasus.schema.dax" property.
     *
     * @param defaultLocation the default location to the schema.
     * @return location to the DAX schema specified in the properties file, else the default
     *     location if no value specified.
     */
    public String getDAXSchemaLocation(String defaultLocation) {
        return mProps.getProperty("pegasus.schema.dax", defaultLocation);
    }

    /**
     * Returns the location of the schema for the PDAX.
     *
     * <p>Referred to by the "pegasus.schema.pdax" property
     *
     * @param defaultLocation the default location to the schema.
     * @return location to the PDAX schema specified in the properties file, else the default
     *     location if no value specified.
     */
    public String getPDAXSchemaLocation(String defaultLocation) {
        return mProps.getProperty("pegasus.schema.pdax", defaultLocation);
    }

    // DIRECTORY CREATION PROPERTIES
    /**
     * Returns the name of the class that the user wants, to insert the create directory jobs in the
     * graph in case of creating random directories.
     *
     * <p>Referred to by the "pegasus.dir.create.strategy" property.
     *
     * @return the create dir classname if specified in the properties file, else Minimal.
     */
    public String getCreateDirClass() {

        return getProperty("pegasus.dir.create.strategy", "pegasus.dir.create", "Minimal");
    }

    /**
     * Returns the name of the class that the user wants, to render the directory creation jobs. It
     * dictates what mechanism is used to create the directory for a workflow.
     *
     * <p>Referred to by the "pegasus.dir.create.impl" property.
     *
     * @return the create dir classname if specified in the properties file, else
     *     DefaultImplementation.
     */
    public String getCreateDirImplementation() {

        return mProps.getProperty("pegasus.dir.create.impl", "DefaultImplementation");
    }
    /**
     * It specifies whether to use the extended timestamp format for generation of timestamps that
     * are used to create the random directory name, and for the classads generation.
     *
     * <p>Referred to by the "pegasus.dir.timestamp.extended" property.
     *
     * @return the value specified in the properties file if valid boolean, else false.
     */
    public boolean useExtendedTimeStamp() {
        return Boolean.parse(mProps.getProperty("pegasus.dir.timestamp.extended"), false);
    }

    /**
     * Returns a boolean indicating whether to use timestamp for directory name creation or not.
     *
     * <p>Referred to by "pegasus.dir.useTimestamp" property.
     *
     * @return the boolean value specified in the properties files, else false.
     */
    public boolean useTimestampForDirectoryStructure() {
        return Boolean.parse(mProps.getProperty("pegasus.dir.useTimestamp"), false);
    }

    /**
     * Returns the execution directory suffix or absolute specified that is appended/replaced to the
     * exec-mount-point specified in the pool catalog for the various pools.
     *
     * <p>Referred to by the "pegasus.dir.exec" property
     *
     * @return the value specified in the properties file, else the default suffix.
     * @see #DEFAULT_EXEC_DIR
     */
    public String getExecDirectory() {
        return mProps.getProperty("pegasus.dir.exec", DEFAULT_EXEC_DIR);
    }

    /**
     * Returns the the path to the logs directory on the submit host. This is the directory where
     * the condor logs for the workflows are created. The logs directory should be on the local
     * filesystem else condor may complain
     *
     * <p>Referred to by the "pegasus.dir.submit.logs" property
     *
     * @return the value in the properties file, else null
     */
    public String getSubmitLogsDirectory() {
        return mProps.getProperty("pegasus.dir.submit.logs");
    }

    /**
     * Returns a boolean indicating whether the submit directory for the sub workflows should
     * include the label of the sub workflow or not.
     *
     * <p>Referred to by the "pegasus.dir.submit.subwf.labelbased" property
     *
     * @return the value in the properties file, else false
     */
    public boolean labelBasedSubmitDirectoryForSubWorkflows() {
        return Boolean.parse(mProps.getProperty("pegasus.dir.submit.subwf.labelbased"), false);
    }

    /**
     * Returns the storage directory suffix or absolute specified that is appended/replaced to the
     * storage-mount-point specified in the pool catalog for the various pools.
     *
     * <p>Referred to by the "pegasus.dir.storage" property.
     *
     * @return the value specified in the properties file, else the default suffix.
     * @see #DEFAULT_STORAGE_DIR
     */
    public String getStorageDirectory() {
        return mProps.getProperty("pegasus.dir.storage", DEFAULT_STORAGE_DIR);
    }

    /**
     * Returns a boolean indicating whether to have a deep storage directory structure or not while
     * staging out data to the output site.
     *
     * <p>Referred to by the "pegasus.dir.storage.deep" property.
     *
     * @return the boolean value specified in the properties files, else false.
     */
    public boolean useDeepStorageDirectoryStructure() {
        return Boolean.parse(mProps.getProperty("pegasus.dir.storage.deep"), false);
    }

    // PROPERTIES RELATED TO CLEANUP
    /**
     * Returns the name of the Strategy class that the user wants, to insert the cleanup jobs in the
     * graph.
     *
     * <p>Referred to by the "pegasus.file.cleanup.strategy" property.
     *
     * @return the create dir classname if specified in the properties file, else InPlace.
     */
    public String getCleanupStrategy() {
        return mProps.getProperty("pegasus.file.cleanup.strategy", "InPlace");
    }

    /**
     * Returns the name of the class that the user wants, to render the cleanup jobs. It dictates
     * what mechanism is used to remove the files on a remote system.
     *
     * <p>Referred to by the "pegasus.file.cleanup.impl" property.
     *
     * @return the cleanup implementation classname if specified in the properties file, else
     *     Cleanup.
     */
    public String getCleanupImplementation() {

        return mProps.getProperty("pegasus.file.cleanup.impl", "Cleanup");
    }

    /**
     * Returns the maximum number of clean up jobs created per level of the workflow in case of
     * InPlace cleanup.
     *
     * <p>Referred to by the "pegasus.file.cleanup.clusters.num" property
     *
     * @return the value in the property file , else null
     */
    public String getMaximumCleanupJobsPerLevel() {
        return mProps.getProperty("pegasus.file.cleanup.clusters.num");
    }

    /**
     * Returns the fraction of cleanup jobs clustered into a single clustered cleanup job.
     *
     * <p>Referred to by the "pegasus.file.cleanup.clusters.size" property
     *
     * @return the value in the property file , else null
     */
    public String getClusterSizeCleanupJobsPerLevel() {
        return mProps.getProperty("pegasus.file.cleanup.clusters.size");
    }

    /**
     * Returns the maximum available space per site.
     *
     * <p>Referred to by the "pegasus.file.cleanup.constraint.maxspace" property
     *
     * @return the value in the property file , else null
     */
    public String getCleanupConstraintMaxSpace() {
        return mProps.getProperty("pegasus.file.cleanup.constraint.maxspace");
    }

    /**
     * Returns the scope for file cleanup. It is used to trigger cleanup in case of deferred
     * planning. The vaild property values accepted are - fullahead - deferred
     *
     * <p>Referred to by the property "pegasus.file.cleanup.scope"
     *
     * @return the value in property file if specified, else fullahead
     */
    public CLEANUP_SCOPE getCleanupScope() {
        CLEANUP_SCOPE scope = CLEANUP_SCOPE.fullahead;
        String value = mProps.getProperty("pegasus.file.cleanup.scope");
        if (value == null) {
            return scope;
        }

        // try to assign a cleanup value
        try {
            scope = CLEANUP_SCOPE.valueOf(value);
        } catch (IllegalArgumentException iae) {
            // ignore do nothing.
        }

        return scope;
    }

    // PROPERTIES RELATED TO THE TRANSFORMATION CATALOG
    /**
     * Returns the mode to be used for accessing the Transformation Catalog.
     *
     * <p>Referred to by the "pegasus.catalog.transformation" property.
     *
     * @return the value specified in properties file, else DEFAULT_TC_MODE.
     * @see #DEFAULT_TC_MODE
     */
    public String getTCMode() {
        return mProps.getProperty(
                PegasusProperties.PEGASUS_TRANSFORMATION_CATALOG_PROPERTY, DEFAULT_TC_MODE);
    }

    /**
     * Returns the location of the transformation catalog.
     *
     * <p>Referred to by "pegasus.catalog.transformation.file" property.
     *
     * @return the value specified in the properties file, else default path specified by
     *     mDefaultTC.
     * @see #mDefaultTC
     */
    public String getTCPath() {
        return mProps.getProperty(
                PegasusProperties.PEGASUS_TRANSFORMATION_CATALOG_FILE_PROPERTY, mDefaultTC);
    }

    /**
     * Returns the mode for loading the transformation mapper that sits in front of the
     * transformation catalog.
     *
     * <p>Referred to by the "pegasus.catalog.transformation.mapper" property.
     *
     * @return the value specified in the properties file, else default tc mapper mode.
     * @see #DEFAULT_TC_MAPPER_MODE
     */
    public String getTCMapperMode() {
        return mProps.getProperty("pegasus.catalog.transformation.mapper", DEFAULT_TC_MAPPER_MODE);
    }

    // REPLICA CATALOG PROPERTIES
    /**
     * Returns the replica mode. It identifies the ReplicaMechanism being used by Pegasus to
     * determine logical file locations.
     *
     * <p>Referred to by the "pegasus.catalog.replica" property.
     *
     * @return the replica mode, that is used to load the appropriate implementing class if property
     *     is specified, else null
     */
    public String getReplicaMode() {
        return mProps.getProperty(PEGASUS_REPLICA_CATALOG_PROPERTY);
    }

    /**
     * Returns the url to the RLI of the RLS.
     *
     * <p>Referred to by the "pegasus.rls.url" property.
     *
     * @return the value specified in properties file, else DEFAULT_RLI_URL.
     * @see #DEFAULT_RLI_URL
     */
    public String getRLIURL() {
        return mProps.getProperty("pegasus.catalog.replica.url", DEFAULT_RLI_URL);
    }

    /**
     * It returns the timeout value in seconds after which to timeout in case of no activity from
     * the RLS.
     *
     * <p>Referred to by the "pegasus.rc.rls.timeout" property.
     *
     * @return the timeout value if specified else, DEFAULT_RLS_TIMEOUT.
     * @see #DEFAULT_RLS_TIMEOUT
     */
    public int getRLSTimeout() {
        String prop =
                mProps.getProperty("pegasus.catalog.replica.rls.timeout", DEFAULT_RLS_TIMEOUT);
        int val;
        try {
            val = Integer.parseInt(prop);
        } catch (Exception e) {
            return Integer.parseInt(DEFAULT_RLS_TIMEOUT);
        }
        return val;
    }

    // PROPERTIES RELATED TO SITE CATALOG
    /**
     * Returns the mode to be used for accessing the pool information.
     *
     * <p>Referred to by the "pegasus.catalog.site" property.
     *
     * @return the pool mode, that is used to load the appropriate implementing class if the
     property is specified, else default pool mode specified by DEFAULT_SITE_CATALOG_IMPLEMENTOR
     * @see #DEFAULT_SITE_CATALOG_IMPLEMENTOR
     */
    public String getPoolMode() {
        return mProps.getProperty(PegasusProperties.PEGASUS_SITE_CATALOG_PROPERTY, DEFAULT_SITE_CATALOG_IMPLEMENTOR);
    }

    /**
     * Returns the location of the schema for the DAX.
     *
     * <p>Referred to by the "pegasus.schema.sc" property.
     *
     * @return the location of pool schema if specified in properties file, else null.
     */
    public String getPoolSchemaLocation() {
        return this.getPoolSchemaLocation(null);
    }

    /**
     * Returns the location of the schema for the site catalog file.
     *
     * <p>Referred to by the "pegasus.schema.sc" property
     *
     * @param defaultLocation the default location where the schema should be if no other location
     *     is specified.
     * @return the location specified by the property, else defaultLocation.
     */
    public String getPoolSchemaLocation(String defaultLocation) {
        return mProps.getProperty("pegasus.schema.sc", defaultLocation);
    }

    // PROVENANCE CATALOG PROPERTIES
    /**
     * Returns the provenance store to use to log the refiner actions.
     *
     * <p>Referred to by the "pegasus.catalog.provenance.refinement" property.
     *
     * @return the value set in the properties, else null if not set.
     */
    public String getRefinementProvenanceStore() {
        return mProps.getProperty("pegasus.catalog.provenance.refinement");
    }

    // TRANSFER MECHANISM PROPERTIES

    /**
     * Returns the transfer implementation that is to be used for constructing the transfer jobs.
     *
     * <p>Referred to by the "pegasus.transfer.*.impl" property.
     *
     * @return the transfer implementation
     */
    public String getTransferImplementation() {
        return getTransferImplementation("pegasus.transfer.*.impl");
    }

    /**
     * Returns the sls transfer implementation that is to be used for constructing the transfer
     * jobs.
     *
     * <p>Referred to by the "pegasus.transfer.lite.*.impl" property.
     *
     * @return the transfer implementation
     */
    /* PM-810 done away.
    public String getSLSTransferImplementation(){
        return getTransferImplementation( "pegasus.transfer.lite.*.impl" );
    }
    */

    /**
     * Returns the transfer implementation.
     *
     * @param property property name.
     * @return the transfer implementation, else the one specified by "pegasus.transfer.*.impl",
     */
    public String getTransferImplementation(String property) {
        return mProps.getProperty(property, getDefaultTransferImplementation());
    }

    /**
     * Returns a boolean indicating whether to stage sls files via Pegasus First Level Staging or
     * let Condor do it.
     *
     * <p>Referred to by the property "pegasus.transfer.stage.lite.file"
     *
     * @return boolean value mentioned in the properties or else the default value which is true.
     */
    public boolean stageSLSFilesViaFirstLevelStaging() {
        return Boolean.parse(mProps.getProperty("pegasus.transfer.stage.lite.file"), false);
    }

    /**
     * Returns the default list of third party sites.
     *
     * <p>Referred to by the "pegasus.transfer.*.thirdparty.sites" property.
     *
     * @return the value specified in the properties file, else null.
     */
    private String getDefaultThirdPartySites() {
        return mProps.getProperty("pegasus.transfer.*.thirdparty.sites");
    }

    /**
     * Returns the default transfer implementation to be picked up for constructing transfer jobs.
     *
     * <p>Referred to by the "pegasus.transfer.*.impl" property.
     *
     * @return the value specified in the properties file, else null.
     */
    private String getDefaultTransferImplementation() {
        return mProps.getProperty("pegasus.transfer.*.impl");
    }

    /**
     * Returns a boolean indicating whether to bypass first level staging of inputs. Useful in case
     * of PegasusLite setup
     *
     * <p>Referred to by the "pegasus.transfer.bypass.input.staging" property.
     *
     * @return boolean value specified , else false
     */
    public boolean bypassFirstLevelStagingForInputs() {
        return Boolean.parse(mProps.getProperty("pegasus.transfer.bypass.input.staging"), false);
    }

    /**
     * Returns the default priority for the transfer jobs if specified in the properties file.
     *
     * @return the value specified in the properties file, else null if non integer value or no
     *     value specified.
     */
    private String getDefaultTransferPriority() {
        String prop = mProps.getProperty(this.ALL_TRANSFER_PRIORITY_PROPERTY_KEY);
        int val = -1;

        try {
            val = Integer.parseInt(prop);
        } catch (Exception e) {
            return null;
        }
        return Integer.toString(val);
    }

    /**
     * Returns the base source URL where pointing to the directory where the worker package
     * executables for pegasus releases are kept.
     *
     * <p>Referred to by the "pegasus.transfer.setup.source.base.url
     *
     * @return the value in the property file, else null
     */
    public String getBaseSourceURLForSetupTransfers() {
        return mProps.getProperty("pegasus.transfer.setup.source.base.url");
    }

    /**
     * Returns the transfer refiner that is to be used for adding in the transfer jobs in the
     * workflow
     *
     * <p>Referred to by the "pegasus.transfer.refiner" property.
     *
     * @return the transfer refiner, else null
     */
    public String getTransferRefiner() {
        return mProps.getProperty("pegasus.transfer.refiner");
    }

    /**
     * Returns whether to introduce quotes around url's before handing to g-u-c and condor.
     *
     * <p>Referred to by "pegasus.transfer.single.quote" property.
     *
     * @return boolean value specified in the properties file, else true in case of non boolean
     *     value being specified or property not being set.
     */
    public boolean quoteTransferURL() {
        return Boolean.parse(mProps.getProperty("pegasus.transfer.single.quote"), true);
    }

    /**
     * It returns the number of processes of g-u-c that the transfer script needs to spawn to do the
     * transfers. This is applicable only in the case where the transfer executable has the
     * capability of spawning processes. It should not be confused with the number of streams that
     * each process opens. By default it is set to 4. In case a non integer value is specified in
     * the properties file it returns the default value.
     *
     * <p>Referred to by "pegasus.transfer.throttle.processes" property.
     *
     * @return the number of processes specified in properties file, else DEFAULT_TRANSFER_PROCESSES
     * @see #DEFAULT_TRANSFER_PROCESSES
     */
    public String getNumOfTransferProcesses() {
        String prop =
                mProps.getProperty(
                        "pegasus.transfer.throttle.processes", DEFAULT_TRANSFER_PROCESSES);
        int val = -1;

        try {
            val = Integer.parseInt(prop);
        } catch (Exception e) {
            return DEFAULT_TRANSFER_PROCESSES;
        }

        return Integer.toString(val);
    }

    /**
     * It returns the number of streams that each transfer process uses to do the ftp transfer. By
     * default it is set to 1.In case a non integer value is specified in the properties file it
     * returns the default value.
     *
     * <p>Referred to by "pegasus.transfer.throttle.streams" property.
     *
     * @return the number of streams specified in the properties file, else
     *     DEFAULT_TRANSFER_STREAMS.
     * @see #DEFAULT_TRANSFER_STREAMS
     */
    public String getNumOfTransferStreams() {

        String prop =
                mProps.getProperty("pegasus.transfer.throttle.streams", DEFAULT_TRANSFER_STREAMS);
        int val = -1;

        try {
            val = Integer.parseInt(prop);
        } catch (Exception e) {
            return DEFAULT_TRANSFER_STREAMS;
        }

        return Integer.toString(val);
    }

    /**
     * It specifies whether the underlying transfer mechanism being used should use the force option
     * if available to transfer the files.
     *
     * <p>Referred to by "pegasus.transfer.force" property.
     *
     * @return boolean value specified in the properties file,else false in case of non boolean
     *     value being specified or property not being set.
     */
    public boolean useForceInTransfer() {
        return Boolean.parse(mProps.getProperty("pegasus.transfer.force"), false);
    }

    /**
     * It returns whether the use of symbolic links in case where the source and destination files
     * happen to be on the same file system.
     *
     * <p>Referred to by "pegasus.transfer.links" property.
     *
     * @return boolean value specified in the properties file, else false in case of non boolean
     *     value being specified or property not being set.
     */
    public boolean getUseOfSymbolicLinks() {
        String value = mProps.getProperty("pegasus.transfer.links");
        return Boolean.parse(value, false);
    }

    /**
     * Returns the comma separated list of third party sites, specified in the properties.
     *
     * @param property property name.
     * @return the comma separated list of sites.
     */
    public String getThirdPartySites(String property) {
        String value = mProps.getProperty(property);

        return value;
    }

    /**
     * Returns the comma separated list of third party sites for which the third party transfers are
     * executed on the remote sites.
     *
     * @param property property name.
     * @return the comma separated list of sites.
     */
    public String getThirdPartySitesRemote(String property) {
        return mProps.getProperty(property);
    }

    /**
     * Returns the delimiter to be used for constructing the staged executable name, during transfer
     * of executables to remote sites.
     *
     * <p>Referred to by the "pegasus.transfer.staging.delimiter" property.
     *
     * @return the value specified in the properties file, else DEFAULT_STAGING_DELIMITER
     * @see #DEFAULT_STAGING_DELIMITER
     */
    public String getStagingDelimiter() {
        return mProps.getProperty("pegasus.transfer.staging.delimiter", DEFAULT_STAGING_DELIMITER);
    }

    /**
     * Returns the list of sites for which the chmod job creation has to be disabled for executable
     * staging.
     *
     * <p>Referred to by the "pegasus.transfer.disable.chmod" property.
     *
     * @return a comma separated list of site names.
     */
    public String getChmodDisabledSites() {
        return mProps.getProperty("pegasus.transfer.disable.chmod.sites");
    }

    /**
     * It specifies if the worker package needs to be staged to the remote site or not.
     *
     * <p>Referred to by "pegasus.transfer.worker.package" property.
     *
     * @return boolean value specified in the properties file,else false in case of non boolean
     *     value being specified or property not being set.
     */
    public boolean transferWorkerPackage() {
        return Boolean.parse(mProps.getProperty(PEGASUS_TRANSFER_WORKER_PACKAGE_PROPERTY), false);
    }

    /**
     * A Boolean property to indicate whether to enforce strict checks against provided worker
     * package for jobs in PegasusLite mode. if a job comes with worker package and it does not
     * match fully with worker node architecture , it will revert to Pegasus download website.
     * Default value is true.
     *
     * <p>Referred to by "pegasus.transfer.worker.package.strict" property.
     *
     * @return boolean value specified in the properties file,else true in case of non boolean value
     *     being specified or property not being set.
     */
    public boolean enforceStrictChecksForWorkerPackage() {
        return Boolean.parse(mProps.getProperty("pegasus.transfer.worker.package.strict"), true);
    }

    /**
     * A Boolean property to indicate whether a pegasus lite job is allowed to download from Pegasus
     * website.
     *
     * <p>Referred to by "pegasus.transfer.worker.package.autodownload" property.
     *
     * @return boolean value specified in the properties file,else true in case of non boolean value
     *     being specified or property not being set.
     */
    public boolean allowDownloadOfWorkerPackageFromPegasusWebsite() {
        return Boolean.parse(
                mProps.getProperty("pegasus.transfer.worker.package.autodownload"), true);
    }

    /**
     * Returns the arguments with which the transfer executable needs to be invoked.
     *
     * <p>Referred to by "pegasus.transfer.arguments" property.
     *
     * @return the arguments specified in the properties file, else null if property is not
     *     specified.
     */
    public String getTransferArguments() {
        return mProps.getProperty("pegasus.transfer.arguments");
    }

    /**
     * Returns the extra arguments with which the transfer executable used in PegasusLite needs to
     * be invoked.
     *
     * <p>Referred to by "pegasus.transfer.lite.arguments" property.
     *
     * @return the arguments specified in the properties file, else null if property is not
     *     specified.
     */
    public String getSLSTransferArguments() {
        return mProps.getProperty("pegasus.transfer.lite.arguments");
    }

    /**
     * Returns the priority to be set for the stage in transfer job.
     *
     * <p>Referred to by "pegasus.transfer.stagein.priority" property if set, else by
     * "pegasus.transfer.*.priority" property.
     *
     * @return the priority as String if a valid integer specified in the properties, else null.
     */
    public String getTransferStageInPriority() {
        return getTransferPriority("pegasus.transfer.stagein.priority");
    }

    /**
     * Returns the priority to be set for the stage out transfer job.
     *
     * <p>Referred to by "pegasus.transfer.stageout.priority" property if set, else by
     * "pegasus.transfer.*.priority" property.
     *
     * @return the priority as String if a valid integer specified in the properties, else null.
     */
    public String getTransferStageOutPriority() {
        return getTransferPriority("pegasus.transfer.stageout.priority");
    }

    /**
     * Returns the priority to be set for the interpool transfer job.
     *
     * <p>Referred to by "pegasus.transfer.inter.priority" property if set, else by
     * "pegasus.transfer.*.priority" property.
     *
     * @return the priority as String if a valid integer specified in the properties, else null.
     */
    public String getTransferInterPriority() {
        return getTransferPriority("pegasus.transfer.inter.priority");
    }

    /**
     * Returns the transfer priority.
     *
     * @param property property name.
     * @return the priority as String if a valid integer specified in the properties as value to
     *     property, else null.
     */
    private String getTransferPriority(String property) {
        String value = mProps.getProperty(property, mDefaultTransferPriority);
        int val = -1;
        try {
            val = Integer.parseInt(value);
        } catch (Exception e) {

        }
        // if value in properties file is corrupted
        // again use the default transfer priority
        return (val < 0) ? mDefaultTransferPriority : Integer.toString(val);
    }

    // REPLICA SELECTOR FUNCTIONS

    /**
     * Returns the mode for loading the transformation selector that selects amongst the various
     * candidate transformation catalog entry objects.
     *
     * <p>Referred to by the "pegasus.selector.transformation" property.
     *
     * @return the value specified in the properties file, else default transformation selector.
     * @see #DEFAULT_TC_MAPPER_MODE
     */
    public String getTXSelectorMode() {
        return mProps.getProperty("pegasus.selector.transformation", DEFAULT_TX_SELECTOR_MODE);
    }

    /**
     * Returns the name of the selector to be used for selection amongst the various replicas of a
     * single lfn.
     *
     * <p>Referred to by the "pegasus.selector.replica" property.
     *
     * @return the name of the selector if the property is specified, else null
     */
    public String getReplicaSelector() {
        return mProps.getProperty("pegasus.selector.replica");
    }

    /**
     * Returns a comma separated list of sites, that are restricted in terms of data movement from
     * the site.
     *
     * <p>Referred to by the "pegasus.rc.restricted.sites" property.
     *
     * @return comma separated list of sites.
     */
    //    public String getRestrictedSites(){
    //        return mProps.getProperty("pegasus.rc.restricted.sites","");
    //    }

    /**
     * Returns a comma separated list of sites, from which to prefer data transfers for all sites.
     *
     * <p>Referred to by the "pegasus.selector.replica.*.prefer.stagein.sites" property.
     *
     * @return comma separated list of sites.
     */
    public String getAllPreferredSites() {
        return mProps.getProperty("pegasus.selector.replica.*.prefer.stagein.sites", "");
    }

    /**
     * Returns a comma separated list of sites, from which to ignore data transfers for all sites.
     * Replaces the old pegasus.rc.restricted.sites property.
     *
     * <p>Referred to by the "pegasus.selector.ignore.*.prefer.stagein.sites" property.
     *
     * @return comma separated list of sites.
     */
    public String getAllIgnoredSites() {
        return mProps.getProperty("pegasus.selector.replica.*.ignore.stagein.sites", "");
    }

    // SITE SELECTOR PROPERTIES
    /**
     * Returns the class name of the site selector, that needs to be invoked to do the site
     * selection.
     *
     * <p>Referred to by the "pegasus.selector.site" property.
     *
     * @return the classname corresponding to the site selector that needs to be invoked if
     *     specified in the properties file, else the default selector specified by
     *     DEFAULT_SITE_SELECTOR.
     * @see #DEFAULT_SITE_SELECTOR
     */
    public String getSiteSelectorMode() {
        return mProps.getProperty("pegasus.selector.site", DEFAULT_SITE_SELECTOR);
    }

    /**
     * Returns the path to the external site selector that needs to be called out to make the
     * decision of site selection.
     *
     * <p>Referred to by the "pegasus.selector.site.path" property.
     *
     * @return the path to the external site selector if specified in the properties file, else
     *     null.
     */
    public String getSiteSelectorPath() {
        return mProps.getProperty("pegasus.selector.site.path");
    }

    /**
     * It returns the timeout value in seconds after which to timeout in case of no activity from
     * the external site selector.
     *
     * <p>Referred to by the "pegasus.selector.site.timeout" property.
     *
     * @return the timeout value if specified else, DEFAULT_SITE_SELECTOR_TIMEOUT.
     * @see #DEFAULT_SITE_SELECTOR_TIMEOUT
     */
    public int getSiteSelectorTimeout() {
        String prop =
                mProps.getProperty("pegasus.selector.site.timeout", DEFAULT_SITE_SELECTOR_TIMEOUT);
        int val;
        try {
            val = Integer.parseInt(prop);
        } catch (Exception e) {
            return Integer.parseInt(DEFAULT_SITE_SELECTOR_TIMEOUT);
        }
        return val;
    }

    /**
     * Returns a value designating whether we need to keep the temporary files that are passed to
     * the external site selectors. The check for the valid tristate value should be done at the
     * calling function end. This just passes on the value user specified in the properties file.
     *
     * <p>Referred to by the "pegasus.selector.site.keep.tmp" property.
     *
     * @return the value of the property is specified, else DEFAULT_SITE_SELECTOR_KEEP
     * @see #DEFAULT_SITE_SELECTOR_KEEP
     */
    public String getSiteSelectorKeep() {
        return mProps.getProperty("pegasus.selector.site.keep.tmp", DEFAULT_SITE_SELECTOR_KEEP);
    }

    // PROPERTIES RELATED TO KICKSTART AND EXITCODE

    /**
     * Returns the GRIDSTART that is to be used to launch the jobs on the grid.
     *
     * <p>Referred to by the "pegasus.gridstart" property.
     *
     * @return the value specified in the property file, else null
     */
    public String getGridStart() {
        return mProps.getProperty("pegasus.gridstart");
    }

    /**
     * Returns a boolean indicating whether kickstart should set x bit on staged executables before
     * launching them.
     *
     * <p>Referred to by the "pegasus.gridstart.kickstart.set.xbit" property.
     *
     * @return the value specified in the property file, else false
     */
    public boolean setXBitWithKickstart() {
        return Boolean.parse(mProps.getProperty("pegasus.gridstart.kickstart.set.xbit"), false);
    }

    /**
     * Return a boolean indicating whether to turn the stat option for kickstart on or not. By
     * default it is turned on.
     *
     * <p>Referred to by the "pegasus.gridstart.kickstart.stat" property.
     *
     * @return value specified in the property file, else null.
     */
    public String doStatWithKickstart() {
        return mProps.getProperty(PEGASUS_KICKSTART_STAT_PROPERTY);
    }

    /**
     * Return a boolean indicating whether to generate the LOF files for the jobs or not. This is
     * used to generate LOF files, but not trigger the stat option
     *
     * <p>Referred to by the "pegasus.gridstart.kickstart.generate.loft" property.
     *
     * @return the boolean value specified in the property file, else false if not specified or non
     *     boolean specified.
     */
    public boolean generateLOFFiles() {
        return Boolean.parse(mProps.getProperty("pegasus.gridstart.generate.lof"), false);
    }

    /**
     * Returns a boolean indicating whether to use invoke in kickstart always or not.
     *
     * <p>Referred to by the "pegasus.gridstart.invoke.always" property.
     *
     * @return the boolean value specified in the property file, else false if not specified or non
     *     boolean specified.
     */
    public boolean useInvokeInGridStart() {
        return Boolean.parse(mProps.getProperty("pegasus.gridstart.invoke.always"), false);
    }

    /**
     * Returns a boolean indicating whether to disable use of invoke or not.
     *
     * <p>Referred to by the "pegasus.gridstart.invoke.disable" property.
     *
     * @return the boolean value specified in the property file, else false if not specified or non
     *     boolean specified.
     */
    public boolean disableInvokeInGridStart() {
        return Boolean.parse(mProps.getProperty(PegasusProperties.DISABLE_INVOKE_PROPERTY), false);
    }

    /**
     * Returns the trigger value for invoking an application through kickstart using kickstart. If
     * the arguments value being constructed in the condor submit file is more than this value, then
     * invoke is used to pass the arguments to the remote end. Helps in bypassing the Condor 4K
     * limit.
     *
     * <p>Referred to by "pegasus.gridstart.invoke.length" property.
     *
     * @return the long value specified in the properties files, else DEFAULT_INVOKE_LENGTH
     * @see #DEFAULT_INVOKE_LENGTH
     */
    public long getGridStartInvokeLength() {
        long value = new Long(this.DEFAULT_INVOKE_LENGTH).longValue();

        String st =
                mProps.getProperty("pegasus.gridstart.invoke.length", this.DEFAULT_INVOKE_LENGTH);
        try {
            value = new Long(st).longValue();
        } catch (Exception e) {
            // ignore malformed values from
            // the property file
        }

        return value;
    }

    /**
     * Returns a boolean indicating whehter to pass extra options to kickstart or not. The extra
     * options have appeared only in VDS version 1.4.2 (like -L and -T).
     *
     * <p>Referred to by "pegasus.gridstart.label" property.
     *
     * @return the boolean value specified in the property file, else true if not specified or non
     *     boolean specified.
     */
    public boolean generateKickstartExtraOptions() {
        return Boolean.parse(mProps.getProperty("pegasus.gridstart.label"), true);
    }

    /**
     * Returns the mode adding the postscripts for the jobs. At present takes in only two values all
     * or none default being none.
     *
     * <p>Referred to by the "pegasus.exitcode.scope" property.
     *
     * @return the mode specified by the property, else DEFAULT_POSTSCRIPT_MODE
     * @see #DEFAULT_POSTSCRIPT_MODE
     */

    /*    public String getPOSTScriptScope() {
            return mProps.getProperty( "pegasus.exitcode.dial",
                                       DEFAULT_POSTSCRIPT_MODE );
        }
    */

    /**
     * Returns the postscript to use with the jobs in the workflow. They maybe overriden by values
     * specified in the profiles.
     *
     * <p>Referred to by the "pegasus.exitcode.impl" property.
     *
     * @return the postscript to use for the workflow, else null if not specified in the properties.
     */
    /*    public String getPOSTScript(){
            return mProps.getProperty( "pegasus.exitcode.impl" );
        }
    */
    /**
     * Returns the path to the exitcode executable to be used.
     *
     * <p>Referred to by the "pegasus.exitcode.path.[value]" property, where [value] is replaced by
     * the value passed an input to this function.
     *
     * @param value the short name of the postscript whose path we want.
     * @return the path to the postscript if specified in properties file.
     */
    /*    public String getPOSTScriptPath( String value ){
            value = ( value == null ) ? "*" : value;
            StringBuffer key = new StringBuffer();
            key.append( "pegasus.exitcode.path." ).append( value );

            return mProps.getProperty( key.toString() );
        }
    */

    /**
     * Returns the argument string containing the arguments by which exitcode is invoked.
     *
     * <p>Referred to by the "pegasus.exitcode.arguments" property.
     *
     * @return String containing the arguments,else empty string.
     */
    /*
        public String getPOSTScriptArguments() {
            return mProps.getProperty( "pegasus.exitcode.arguments", "");
        }
    */
    /**
     * Returns a boolean indicating whether to turn debug on or not for exitcode. By default false
     * is returned.
     *
     * <p>Referred to by the "pegasus.exitcode.debug" property.
     *
     * @return boolean value.
     */
    public boolean setPostSCRIPTDebugON() {
        return Boolean.parse(mProps.getProperty("pegasus.exitcode.debug"), false);
    }

    /**
     * Returns the argument string containing the arguments by which prescript is invoked.
     *
     * <p>Referred to by the "pegasus.prescript.arguments" property.
     *
     * @return String containing the arguments. null if not specified.
     */
    /*
        public String getPrescriptArguments() {
            return mProps.getProperty( "pegasus.prescript.arguments","" );
        }
    */

    // PROPERTIES RELATED TO REMOTE SCHEDULERS
    /**
     * Returns the project names that need to be appended to the RSL String while creating the
     * submit files. Referred to by pegasus.remote.projects property. If present, Pegasus ends up
     * inserting an RSL string (project = value) in the submit file.
     *
     * @return a comma separated list of key value pairs if property specified, else null.
     */
    //    public String getRemoteSchedulerProjects() {
    //        return mProps.getProperty( "pegasus.remote.scheduler.projects" );
    //    }

    /**
     * Returns the queue names that need to be appended to the RSL String while creating the submit
     * files. Referred to by the pegasus.remote.queues property. If present, Pegasus ends up
     * inserting an RSL string (project = value) in the submit file.
     *
     * @return a comma separated list of key value pairs if property specified, else null.
     */
    //    public String getRemoteSchedulerQueues() {
    //        return mProps.getProperty( "pegasus.remote.scheduler.queues" );
    //    }

    /**
     * Returns the maxwalltimes for the various pools that need to be appended to the RSL String
     * while creating the submit files. Referred to by the pegasus.scheduler.remote.queues property.
     * If present, Pegasus ends up inserting an RSL string (project = value) in the submit file.
     *
     * @return a comma separated list of key value pairs if property specified, else null.
     */
    //    public String getRemoteSchedulerMaxWallTimes() {
    //        return mProps.getProperty( "pegasus.remote.scheduler.min.maxwalltime" );
    //    }

    /**
     * Returns the minimum walltimes that need to be enforced.
     *
     * <p>Referred to by "pegasus.scheduler.remote.min.[key]" property.
     *
     * @param key the appropriate globus RSL key. Generally are maxtime|maxwalltime|maxcputime
     * @return the integer value as specified, -1 in case of no value being specified.
     */
    //    public int getMinimumRemoteSchedulerTime( String key ){
    //        StringBuffer property = new StringBuffer();
    //        property.append( "pegasus.remote.scheduler.min." ).append( key );
    //
    //        int val = -1;
    //
    //        try {
    //            val = Integer.parseInt( mProps.getProperty( property.toString() ) );
    //        } catch ( Exception e ) {
    //        }
    //        return val;
    //    }

    // PROPERTIES RELATED TO CONDOR

    /**
     * Completely disable placing a symlink for Condor common log (indiscriminately).
     *
     * <p>Starting 4.2.1 this defaults to "false" .
     *
     * <p>Referred to by the "pegasus.condor.logs.symlink" property.
     *
     * @return value specified by the property. Defaults to false.
     */
    public boolean symlinkCommonLog() {
        return Boolean.parse(mProps.getProperty("pegasus.condor.logs.symlink"), false);
    }

    /**
     * Whether Pegasus should associate condor concurrency limits or not
     *
     * <p>Referred to by the "pegasus.condor.concurrency.limits" property.
     *
     * @return value specified by the property. Defaults to false.
     */
    public boolean associateCondorConcurrencyLimits() {
        return Boolean.parse(mProps.getProperty("pegasus.condor.concurrency.limits"), false);
    }

    /**
     * Returns a boolean indicating whether we want to Condor Quote the arguments of the job or not.
     *
     * <p>Referred to by the "pegasus.condor.arguments.quote" property.
     *
     * @return boolean
     */
    public boolean useCondorQuotingForArguments() {
        return Boolean.parse(mProps.getProperty("pegasus.condor.arguments.quote"), true);
    }

    /**
     * Returns the number of times Condor should retry running a job in case of failure. The retry
     * ends up reinvoking the prescript, that can change the site selection decision in case of
     * failure.
     *
     * <p>Referred to by the "pegasus.dagman.retry" property.
     *
     * @return an int denoting the number of times to retry. null if not specified or invalid entry.
     */
    /*    public String getCondorRetryValue() {
            String prop = mProps.getProperty( "pegasus.dagman.retry" );
            int val = -1;

            try {
                val = Integer.parseInt( prop );
            } catch ( Exception e ) {
                return null;
            }

            return Integer.toString( val );
        }
    */

    /**
     * Tells whether to stream condor output or not. By default it is true , meaning condor streams
     * the output from the remote hosts back to the submit hosts, instead of staging it. This helps
     * in saving filedescriptors at the jobmanager end.
     *
     * <p>If it is set to false, output is not streamed back. The line "stream_output = false"
     * should be added in the submit files for kickstart jobs.
     *
     * <p>Referred to by the "pegasus.condor.output.stream" property.
     *
     * @return the boolean value specified by the property, else false in case of invalid value or
     *     property not being specified.
     */
    /*    public boolean streamCondorOutput() {
            return Boolean.parse(mProps.getProperty( "pegasus.condor.output.stream"),
                                 false );
        }
    */
    /**
     * Tells whether to stream condor error or not. By default it is true , meaning condor streams
     * the error from the remote hosts back to the submit hosts instead of staging it in. This helps
     * in saving filedescriptors at the jobmanager end.
     *
     * <p>Referred to by the "pegasus.condor.error.stream" property.
     *
     * <p>If it is set to false, output is not streamed back. The line "stream_output = false"
     * should be added in the submit files for kickstart jobs.
     *
     * @return the boolean value specified by the property, else false in case of invalid value or
     *     property not being specified.
     */
    /*    public boolean streamCondorError() {
            return Boolean.parse(mProps.getProperty( "pegasus.condor.error.stream"),
                                 false );
        }
    */
    // PROPERTIES RELATED TO STORK
    /**
     * Returns the credential name to be used for the stork transfer jobs.
     *
     * <p>Referred to by the "pegasus.transfer.stork.cred" property.
     *
     * @return the credential name if specified by the property, else null.
     */
    public String getCredName() {
        return mProps.getProperty("pegasus.transfer.stork.cred");
    }

    // SOME LOGGING PROPERTIES

    /**
     * Returns the log manager to use.
     *
     * <p>Referred to by the "pegasus.log.manager" property.
     *
     * @return the value in the properties file, else Default
     */
    public String getLogManager() {
        return mProps.getProperty("pegasus.log.manager", "Default");
    }

    /**
     * Returns the log formatter to use.
     *
     * <p>Referred to by the "pegasus.log.formatter" property.
     *
     * @return the value in the properties file, else Simple
     */
    public String getLogFormatter() {
        return mProps.getProperty("pegasus.log.formatter", "Simple");
    }

    /**
     * Returns the http url for log4j properties for windward project.
     *
     * <p>Referred to by the "log4j.configuration" property.
     *
     * @return the value in the properties file, else null
     */
    public String getHttpLog4jURL() {
        // return mProps.getProperty( "pegasus.log.windward.log4j.http.url" );
        return mProps.getProperty("log4j.configuration");
    }
    /**
     * Returns the file to which all the logging needs to be directed to.
     *
     * <p>Referred to by the "pegasus.log.*" property.
     *
     * @return the value of the property that is specified, else null
     */
    public String getLoggingFile() {
        return mProps.getProperty("pegasus.log.*");
    }

    /**
     * Returns the location of the local log file where you want the messages to be logged. Not used
     * for the moment.
     *
     * <p>Referred to by the "pegasus.log4j.log" property.
     *
     * @return the value specified in the property file,else null.
     */
    public String getLog4JLogFile() {
        return mProps.getProperty("pegasus.log4j.log");
    }

    /**
     * Returns a boolean indicating whether to write out the planner metrics or not.
     *
     * <p>Referred to by the "pegasus.log.metrics" property.
     *
     * @return boolean in the properties, else true
     */
    public boolean writeOutMetrics() {
        return Boolean.parse(
                        mProps.getProperty(PegasusProperties.PEGASUS_LOG_METRICS_PROPERTY), true)
                && (this.getMetricsLogFile() != null);
    }

    /**
     * Returns the path to the file that is used to be logging metrics
     *
     * <p>Referred to by the "pegasus.log.metrics.file" property.
     *
     * @return path to the metrics file if specified, else rundir/pegasus.metrics
     */
    public String getMetricsLogFile() {
        String file = mProps.getProperty(PegasusProperties.PEGASUS_LOG_METRICS_PROPERTY_FILE);
        return file;
    }

    /**
     * Returns a boolean indicating whether to log JVM memory usage or not.
     *
     * <p>Referred to by the "pegasus.log.memory.usage" property.
     *
     * @return boolean value specified in properties else false.
     */
    public boolean logMemoryUsage() {
        return Boolean.parse(mProps.getProperty("pegasus.log.memory.usage"), false);
    }

    // SOME MISCELLANEOUS PROPERTIES

    /**
     * Returns a boolean indicating whether we assign job priorities or not to the jobs
     *
     * <p>Referred to by the "pegasus.job.priority.assign" property.
     *
     * @return boolean value specified in properties else true.
     */
    public boolean assignDefaultJobPriorities() {
        return Boolean.parse(mProps.getProperty("pegasus.job.priority.assign"), true);
    }

    /**
     * Returns a boolean indicating whether we create registration jobs or not.
     *
     * <p>Referred to by the "pegasus.register" property.
     *
     * @return boolean value specified in properties else true.
     */
    public boolean createRegistrationJobs() {
        return Boolean.parse(mProps.getProperty("pegasus.register"), true);
    }

    /**
     * Returns a boolean indicating whether to register a deep LFN or not.
     *
     * <p>Referred to by the "pegasus.register.deep" property.
     *
     * @return boolean value specified in properties else true.
     */
    public boolean registerDeepLFN() {
        return Boolean.parse(mProps.getProperty("pegasus.register.deep"), true);
    }

    /**
     * Returns a boolean indicating whether to have jobs executing on worker node tmp or not.
     *
     * <p>Referred to by the "pegasus.execute.*.filesystem.local" property.
     *
     * @return boolean value in the properties file, else false if not specified or an invalid value
     *     specified.
     */
    public boolean executeOnWorkerNode() {
        return Boolean.parse(
                mProps.getProperty(PegasusProperties.PEGASUS_WORKER_NODE_EXECUTION_PROPERTY),
                false);
    }

    /**
     * Returns a boolean indicating whether to treat the entries in the cache files as a replica
     * catalog or not.
     *
     * @return boolean
     */
    public boolean treatCacheAsRC() {
        return Boolean.parse(mProps.getProperty("pegasus.catalog.replica.cache.asrc"), false);
    }

    /**
     * Returns a boolean indicating whether to treat the file locations in the DAX as a replica
     * catalog or not.
     *
     * <p>Referred to by the "pegasus.catalog.replica.dax.asrc" property.
     *
     * @return boolean value in the properties file, else false if not specified or an invalid value
     *     specified.
     */
    public boolean treatDAXLocationsAsRC() {
        return Boolean.parse(mProps.getProperty("pegasus.catalog.replica.dax.asrc"), false);
    }

    /**
     * Returns a boolean indicating whether to preserver line breaks.
     *
     * <p>Referred to by the "pegasus.parser.dax.preserve.linebreaks" property.
     *
     * @return boolean value in the properties file, else false if not specified or an invalid value
     *     specified.
     */
    public boolean preserveParserLineBreaks() {
        return Boolean.parse(mProps.getProperty("pegasus.parser.dax.preserve.linebreaks"), false);
    }

    /**
     * Returns a boolean indicating whether to automatically add edges as a result of underlying
     * data dependecnies between jobs.
     *
     * <p>Referred to by the "pegasus.parser.dax.data.dependencies" property.
     *
     * @return boolean value in the properties file, else true if not specified or an invalid value
     *     specified.
     */
    public boolean addDataDependencies() {
        return Boolean.parse(mProps.getProperty("pegasus.parser.dax.data.dependencies"), true);
    }

    /**
     * Returns the path to the wings properties file.
     *
     * <p>Referred to by the "pegasus.wings.properties" property.
     *
     * @return value in the properties file, else null.
     */
    public String getWingsPropertiesFile() {
        return mProps.getProperty("pegasus.wings.properties");
    }

    /**
     * Returns the request id.
     *
     * <p>Referred to by the "pegasus.wings.request-id" property.
     *
     * @return value in the properties file, else null.
     */
    public String getWingsRequestID() {
        return mProps.getProperty("pegasus.wings.request.id");
    }

    /**
     * Returns the timeout value in seconds after which to timeout in case of opening sockets to
     * grid ftp server.
     *
     * <p>Referred to by the "pegasus.auth.gridftp.timeout" property.
     *
     * @return the timeout value if specified else, null.
     * @see #DEFAULT_SITE_SELECTOR_TIMEOUT
     */
    public String getGridFTPTimeout() {
        return mProps.getProperty("pegasus.auth.gridftp.timeout");
    }

    /**
     * Returns which submit mode to be used to submit the jobs on to the grid.
     *
     * <p>Referred to by the "pegasus.code.generator" property.
     *
     * @return the submit mode specified in the property file, else the default i.e condor.
     */
    public String getSubmitMode() {
        return mProps.getProperty("pegasus.code.generator", "condor");
    }

    /**
     * Returns the mode for parsing the dax while writing out the partitioned daxes.
     *
     * <p>Referred to by the "pegasus.partition.parser.load" property.
     *
     * @return the value specified in the properties file, else the default value i.e single.
     */
    public String getPartitionParsingMode() {
        return mProps.getProperty("pegasus.partition.parser.load", "single");
    }

    /**
     * Returns the scope for the data reusue module.
     *
     * <p>Referred to by the "pegasus.data.reuse.scope" property.
     *
     * @return the value specified in the properties file, else null
     */
    public String getDataReuseScope() {
        return mProps.getProperty("pegasus.data.reuse.scope");
    }

    // JOB COLLAPSING PROPERTIES

    /**
     * Returns a comma separated list for the node collapsing criteria for the execution pools. This
     * determines how many jobs one fat node gobbles up.
     *
     * <p>Referred to by the "pegasus.clusterer.nodes" property.
     *
     * @return the value specified in the properties file, else null.
     */
    public String getCollapseFactors() {
        return mProps.getProperty("pegasus.clusterer.nodes");
    }

    /**
     * Returns the users horizontal clustering preference. This property determines how to cluster
     * horizontal jobs. If this property is set with a value value of runtime, the jobs will be
     * grouped into into clusters according to their runtimes as specified by <code>job.runtime
     * </code> property. For all other cases the default horizontal clustering approach will be
     * used.
     *
     * @return the value specified in the properties file, else null.
     */
    public String getHorizontalClusterPreference() {
        return mProps.getProperty("pegasus.clusterer.preference");
    }

    /**
     * Returns what job aggregator is to be used to aggregate multiple compute jobs into a single
     * condor job.
     *
     * <p>Referred to by the "pegasus.cluster.job.aggregator" property.
     *
     * @return the value specified in the properties file, else DEFAULT_JOB_AGGREGATOR
     * @see #DEFAULT_JOB_AGGREGATOR
     */
    public String getJobAggregator() {
        return mProps.getProperty("pegasus.clusterer.job.aggregator", DEFAULT_JOB_AGGREGATOR);
    }

    /**
     * Returns whether the seqexec job aggregator should log progress to a log or not.
     *
     * <p>Referred to by the "pegasus.clusterer.job.aggregator.seqexec.log" property.
     *
     * @return the value specified in the properties file, else false
     */
    public boolean logJobAggregatorProgress() {
        return Boolean.parse(getProperty("pegasus.clusterer.job.aggregator.seqexec.log"), false);
    }

    /**
     * Returns whether the seqexec job aggregator should write to a global log or not. This comes
     * into play only if "pegasus.clusterer.job.aggregator.seqexec.log" is set to true.
     *
     * <p>Referred to by the "pegasus.clusterer.job.aggregator.seqexec.log.global" property.
     *
     * @return the value specified in the properties file, else true
     */
    public boolean logJobAggregatorProgressToGlobal() {
        return Boolean.parse(
                getProperty(
                        "pegasus.clusterer.job.aggregator.seqexec.log.global",
                        "pegasus.clusterer.job.aggregator.seqexec.hasgloballog"),
                true);
    }

    /**
     * Returns a boolean indicating whether seqexec trips on the first job failure.
     *
     * <p>Referred to by the "pegasus.clusterer.job.aggregator.seqexec.firstjobfail" property.
     *
     * @return the value specified in the properties file, else true
     */
    public boolean abortOnFirstJobFailure() {
        return Boolean.parse(
                mProps.getProperty("pegasus.clusterer.job.aggregator.seqexec.firstjobfail"), true);
    }

    /**
     * Returns a boolean indicating whether clustering should be allowed for single jobs or not
     *
     * <p>Referred to by the "pegasus.clusterer.allow.single" property.
     *
     * @return the value specified in the properties file, else false
     */
    public boolean allowClusteringOfSingleJobs() {
        return Boolean.parse(mProps.getProperty("pegasus.clusterer.allow.single"), false);
    }

    /**
     * Returns a boolean indicating whether to enable integrity checking or not.
     *
     * @return false if set explicitly to none, else true
     */
    public boolean doIntegrityChecking() {
        return this.getIntegrityDial() != INTEGRITY_DIAL.none;
    }

    /**
     * Returns the integrity dial enum
     *
     * <p>Referred to by the "pegasus.integrity.checking" property.
     *
     * @return the value specified in the properties file, else INTEGRITY_DIAL.full
     * @see INTEGRITY_DIAL
     */
    public INTEGRITY_DIAL getIntegrityDial() {
        INTEGRITY_DIAL dial = INTEGRITY_DIAL.full;
        String value = mProps.getProperty("pegasus.integrity.checking");
        if (value == null) {
            return dial;
        }

        // try to assign a dial value
        try {
            dial = INTEGRITY_DIAL.valueOf(value);
        } catch (IllegalArgumentException iae) {
            throw new IllegalArgumentException(
                    "Invalid value specified for integrity checking " + value, iae);
        }

        return dial;
    }

    // DEFERRED PLANNING PROPERTIES

    /**
     * Returns the root workflow UUID if defined in the properties, else null
     *
     * <p>Referred to by the "pegasus.workflow.root.uuid" property.
     *
     * @return the value in the properties file else, null
     */
    public String getRootWorkflowUUID() {
        return mProps.getProperty(ROOT_WORKFLOW_UUID_PROPERTY_KEY, null);
    }

    /**
     * Returns the DAXCallback that is to be used while parsing the DAX.
     *
     * <p>Referred to by the "pegasus.partitioner.parser.dax.callback" property.
     *
     * @return the value specified in the properties file, else DEFAULT_DAX_CALLBACK
     * @see #DEFAULT_DAX_CALLBACK
     */
    public String getPartitionerDAXCallback() {
        return mProps.getProperty("pegasus.partitioner.parser.dax.callback", DEFAULT_DAX_CALLBACK);
    }

    /**
     * Returns the key that is to be used as a label key, for labelled partitioning.
     *
     * <p>Referred to by the "pegasus.partitioner.label.key" property.
     *
     * @return the value specified in the properties file.
     */
    public String getPartitionerLabelKey() {
        return mProps.getProperty("pegasus.partitioner.label.key");
    }

    /**
     * Returns the bundle value for a particular transformation.
     *
     * <p>Referred to by the "pegasus.partitioner.horziontal.bundle.[txname]" property, where
     * [txname] is replaced by the name passed an input to this function.
     *
     * @param name the logical name of the transformation.
     * @return the path to the postscript if specified in properties file, else null.
     */
    public String getHorizontalPartitionerBundleValue(String name) {
        StringBuffer key = new StringBuffer();
        key.append("pegasus.partitioner.horizontal.bundle.").append(name);
        return mProps.getProperty(key.toString());
    }

    /**
     * Returns the collapse value for a particular transformation.
     *
     * <p>Referred to by the "pegasus.partitioner.horziontal.collapse.[txname]" property, where
     * [txname] is replaced by the name passed an input to this function.
     *
     * @param name the logical name of the transformation.
     * @return the path to the postscript if specified in properties file, else null.
     */
    public String getHorizontalPartitionerCollapseValue(String name) {
        StringBuffer key = new StringBuffer();
        key.append("pegasus.partitioner.horizontal.collapse.").append(name);
        return mProps.getProperty(key.toString());
    }

    /**
     * Returns the key that is to be used as a label key, for labelled clustering.
     *
     * <p>Referred to by the "pegasus.clusterer.label.key" property.
     *
     * @return the value specified in the properties file.
     */
    public String getClustererLabelKey() {
        return mProps.getProperty("pegasus.clusterer.label.key");
    }

    /**
     * Returns the estimator to be used
     *
     * <p>Referred to by the "pegasus.estimator" property
     *
     * @return value specified else null
     */
    public String getEstimator() {

        return mProps.getProperty("pegasus.estimator");
    }

    /**
     * Returns the path to the property file that has been writting out in the submit directory.
     *
     * @return path to the property file
     * @exception RuntimeException in case of file not being generated.
     */
    public String getPropertiesInSubmitDirectory() {
        if (mPropsInSubmitDir == null || mPropsInSubmitDir.length() == 0) {
            throw new RuntimeException("Properties file does not exist in directory ");
        }
        return mPropsInSubmitDir;
    }

    /**
     * Writes out the properties to a temporary file in the directory passed.
     *
     * @param directory the directory in which the properties file needs to be written to.
     * @return the absolute path to the properties file written in the directory.
     * @throws IOException in case of error while writing out file.
     */
    public String writeOutProperties(String directory) throws IOException {
        return this.writeOutProperties(directory, true);
    }

    /**
     * Writes out the properties to a temporary file in the directory passed.
     *
     * @param directory the directory in which the properties file needs to be written to.
     * @param sanitizePath boolean indicating whether to sanitize paths for certain properties or
     *     not.
     * @return the absolute path to the properties file written in the directory.
     * @throws IOException in case of error while writing out file.
     */
    public String writeOutProperties(String directory, boolean sanitizePath) throws IOException {
        return this.writeOutProperties(directory, sanitizePath, true);
    }

    /**
     * Writes out the properties to a temporary file in the directory passed.
     *
     * @param directory the directory in which the properties file needs to be written to.
     * @param sanitizePath boolean indicating whether to sanitize paths for certain properties or
     *     not.
     * @param setInternalVariable whether to set the internal variable that stores the path to the
     *     properties file.
     * @return the absolute path to the properties file written in the directory.
     * @throws IOException in case of error while writing out file.
     */
    public String writeOutProperties(
            String directory, boolean sanitizePath, boolean setInternalVariable)
            throws IOException {
        File dir = new File(directory);

        // sanity check on the directory
        sanityCheck(dir);

        // we only want to write out the Pegasus properties for time being
        // and any profiles that were mentioned in the properties.
        Properties properties = new Properties();
        for (Profiles.NAMESPACES n : Profiles.NAMESPACES.values()) {
            Properties p = this.mProps.matchingSubset(namespaceToPropertiesPrefix().get(n), true);
            properties.putAll(p);
        }

        // check if we need to sanitize paths for certain properties or not
        if (sanitizePath) {
            sanitizePathForProperty(properties, "pegasus.catalog.site.file");
            sanitizePathForProperty(properties, "pegasus.catalog.replica.file");
            sanitizePathForProperty(properties, "pegasus.catalog.transformation.file");
        }

        // put in a sensible default for dagman maxpre for pegasus-run to
        // pick up if not specified beforehand
        StringBuffer buffer = new StringBuffer();
        buffer.append(Dagman.NAMESPACE_NAME).append(".").append(Dagman.MAXPRE_KEY.toLowerCase());
        String key = buffer.toString();
        if (!properties.containsKey(key)) {
            // add defautl value
            properties.put(key, DEFAULT_DAGMAN_MAX_PRE_VALUE);
        }

        // create a temporary file in directory
        File f = File.createTempFile("pegasus.", ".properties", dir);

        // the header of the file
        StringBuffer header = new StringBuffer(64);
        header.append("Pegasus USER PROPERTIES AT RUNTIME \n")
                .append("#ESCAPES IN VALUES ARE INTRODUCED");

        // create an output stream to this file and write out the properties
        OutputStream os = new FileOutputStream(f);
        properties.store(os, header.toString());
        os.close();

        // also set it to the internal variable
        if (setInternalVariable) {
            mPropsInSubmitDir = f.getAbsolutePath();
            return mPropsInSubmitDir;
        } else {
            return f.getAbsolutePath();
        }
    }

    /**
     * Santizes the value in the properties . Ensures that the path is absolute.
     *
     * @param properties the properties
     * @param key the key whose value needs to be sanitized
     */
    private void sanitizePathForProperty(Properties properties, String key) {
        if (properties.containsKey(key)) {
            String value = properties.getProperty(key);
            if (value != null) {
                properties.setProperty(key, new File(value).getAbsolutePath());
            }
        }
    }

    /**
     * Checks the destination location for existence, if it can be created, if it is writable etc.
     *
     * @param dir is the new base directory to optionally create.
     * @throws IOException in case of error while writing out files.
     */
    protected static void sanityCheck(File dir) throws IOException {
        if (dir.exists()) {
            // location exists
            if (dir.isDirectory()) {
                // ok, isa directory
                if (dir.canWrite()) {
                    // can write, all is well
                    return;
                } else {
                    // all is there, but I cannot write to dir
                    throw new IOException("Cannot write to existing directory " + dir.getPath());
                }
            } else {
                // exists but not a directory
                throw new IOException(
                        "Destination "
                                + dir.getPath()
                                + " already "
                                + "exists, but is not a directory.");
            }
        } else {
            // does not exist, try to make it
            if (!dir.mkdirs()) {
                // try to get around JVM bug. JIRA PM-91
                if (dir.getPath().endsWith(".")) {
                    // just try to create the parent directory
                    if (!dir.getParentFile().mkdirs()) {
                        throw new IOException("Unable to create  directory " + dir.getPath());
                    }
                    return;
                }

                throw new IOException("Unable to create directory destination " + dir.getPath());
            }
        }
    }

    /**
     * This function is used to check whether a deprecated property is used or not. If a deprecated
     * property is used,it logs a warning message specifying the new property. If both properties
     * are not set by the user, the function returns the default property. If no default property
     * then null.
     *
     * @param newProperty the new property that should be used.
     * @param deprecatedProperty the deprecated property that needs to be replaced.
     * @return the appropriate value.
     */
    private String getProperty(String newProperty, String deprecatedProperty) {
        return this.getProperty(newProperty, deprecatedProperty, null);
    }

    /**
     * This function is used to check whether a deprecated property is used or not. If a deprecated
     * property is used,it logs a warning message specifying the new property. If both properties
     * are not set by the user, the function returns the default property. If no default property
     * then null.
     *
     * @param newProperty the new property that should be used.
     * @param deprecatedProperty the deprecated property that needs to be replaced.
     * @param defaultValue the default value that should be returned.
     * @return the appropriate value.
     */
    private String getProperty(String newProperty, String deprecatedProperty, String defaultValue) {
        String value = null;

        // try for the new property
        // first
        value = mProps.getProperty(newProperty);
        if (value == null) {
            // try the deprecated property if set
            value = mProps.getProperty(deprecatedProperty);

            // if the value is not null
            if (value != null) {
                // print the warning message
                logDeprecatedWarning(deprecatedProperty, newProperty);
                return value;
            } else { // else return the default value
                return defaultValue;
            }
        }

        return value;
    }

    /**
     * Logs a warning about the deprecated property. Logs a warning only if it has not been
     * displayed before.
     *
     * @param deprecatedProperty the deprecated property that needs to be replaced.
     * @param newProperty the new property that should be used.
     */
    private void logDeprecatedWarning(String deprecatedProperty, String newProperty) {

        if (!mDeprecatedProperties.contains(deprecatedProperty)) {
            // log only if it had already not been logged
            StringBuffer sb = new StringBuffer();
            sb.append("The property ")
                    .append(deprecatedProperty)
                    .append(" has been deprecated. Use ")
                    .append(newProperty)
                    .append(" instead.");
            //            mLogger.log(sb.toString(),LogManager.WARNING_MESSAGE_LEVEL );
            System.err.println("[WARNING] " + sb.toString());

            // push the property in to indicate it has already been
            // warned about
            mDeprecatedProperties.add(deprecatedProperty);
        }
    }

    /**
     * Returns a boolean indicating whether to use third party transfers for all types of transfers
     * or not.
     *
     * <p>Referred to by the "pegasus.transfer.*.thirdparty" property.
     *
     * @return the boolean value in the properties files, else false if no value specified, or non
     *     boolean specified.
     */
    //     private boolean useThirdPartyForAll(){
    //         return Boolean.parse("pegasus.transfer.*.thirdparty",
    //                              false);
    //     }

    /**
     * Gets the reference to the internal singleton object. This method is invoked with the
     * assumption that the singleton method has been invoked once and has been populated. Also that
     * it has not been disposed by the garbage collector. Can be potentially a buggy way to invoke.
     *
     * @return a handle to the Properties class.
     */
    //    public static PegasusProperties singletonInstance() {
    //        return singletonInstance( null );
    //    }

    /**
     * Gets a reference to the internal singleton object.
     *
     * @param propFileName name of the properties file to picked from $PEGASUS_HOME/etc/ directory.
     * @return a handle to the Properties class.
     */
    //    public static PegasusProperties singletonInstance( String propFileName ) {
    //        if ( pegProperties == null ) {
    //            //only the default properties file
    //            //can be picked up due to the way
    //            //Singleton implemented in CommonProperties.???
    //            pegProperties = new PegasusProperties( null );
    //        }
    //        return pegProperties;
    //    }

}
