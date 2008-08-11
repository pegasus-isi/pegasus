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

package org.griphyn.cPlanner.common;

import org.griphyn.cPlanner.classes.NameValue;

import org.griphyn.cPlanner.poolinfo.PoolMode;

import org.griphyn.common.catalog.transformation.TCMode;

import org.griphyn.common.util.VDSProperties;
import org.griphyn.common.util.Boolean;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.FileOutputStream;

import java.util.Collections;
import java.util.List;
import java.util.MissingResourceException;
import java.util.Properties;
import java.util.Set;
import java.util.HashSet;

/**
 * A Central Properties class that keeps track of all the properties used by
 * Pegasus. All other classes access the methods in this class to get the value
 * of the property. It access the VDSProperties class to read the property file.
 *
 * @author Karan Vahi
 * @author Gaurang Mehta
 *
 * @version $Revision$
 *
 * @see org.griphyn.common.util.VDSProperties
 */
public class PegasusProperties {

    //Replica Catalog Constants
    public static final String DEFAULT_RC_COLLECTION = "GriphynData";

    public static final String DEFAULT_RLI_URL = null;

    public static final String DEFAULT_RLS_QUERY_MODE = "bulk";

    public static final String DEFAULT_RLS_EXIT_MODE = "error";

    public static final String DEFAULT_REPLICA_MODE = "rls";

    public static final String DEFAULT_RLS_QUERY_ATTRIB = "false";

    public static final String DEFAULT_LRC_IGNORE_URL = null;

    public static final String DEFAULT_RLS_TIMEOUT = "30";


    public static final String DEFAULT_EXEC_DIR = "";

    public static final String DEFAULT_STORAGE_DIR = "";

    public static final String DEFAULT_TC_MODE = TCMode.DEFAULT_TC_CLASS;

    public static final String DEFAULT_POOL_MODE = "XML";

    public static final String DEFAULT_CONDOR_BIN_DIR = "";

    public static final String DEFAULT_CONDOR_CONFIG_DIR = "";

    public static final String DEFAULT_POSTSCRIPT_MODE = "none";

    public static final String POOL_CONFIG_FILE = "sites.";

    public static final String CONDOR_KICKSTART = "kickstart-condor";

    //transfer constants

    public static final String DEFAULT_TRANSFER_IMPLEMENTATION = "Transfer";
    
    public static final String DEFAULT_SETUP_TRANSFER_IMPLEMENTATION = "GUC";

    public static final String DEFAULT_TRANSFER_REFINER = "Default";

    public static final String DEFAULT_STAGING_DELIMITER = "-";

    public static final String DEFAULT_TRANSFER_PROCESSES = "4";

    public static final String DEFAULT_TRANSFER_STREAMS = "1";

    //grid start constants
    public static final String DEFAULT_GRIDSTART_MODE = "Kickstart";

    public static final String DEFAULT_INVOKE_LENGTH = "4000";

    //site selector constants
    public static final String DEFAULT_SITE_SELECTOR = "Random";

    public static final String DEFAULT_SITE_SELECTOR_TIMEOUT = "300";

    public static final String DEFAULT_SITE_SELECTOR_KEEP = "onerror";

    ///some simulator constants that are used
    public static final String DEFAULT_DATA_MULTIPLICATION_FACTOR = "1";

    public static final String DEFAULT_COMP_MULTIPLICATION_FACTOR = "1";

    public static final String DEFAULT_COMP_ERROR_PERCENTAGE = "0";

    public static final String DEFAULT_COMP_VARIANCE_PERCENTAGE = "0";

    //collapsing constants
    public static final String DEFAULT_JOB_AGGREGATOR = "SeqExec";

    //some tranformation catalog constants
    public static final String DEFAULT_TC_MAPPER_MODE = "All";

    public static final String DEFAULT_TX_SELECTOR_MODE = "Random";

    public static final String TC_DATA_FILE = "tc.data";

    //logging constants
    public static final String DEFAULT_LOGGING_FILE = "stdout";

    /**
     * Default properties that applies priorities to all kinds of transfer
     * jobs.
     */
    public static final String ALL_TRANSFER_PRIORITY_PROPERTY =
                                                      "pegasus.transfer.*.priority";



    /**
     * The default DAXCallback that is loaded, if none is specified by the user.
     */
    private static final String DEFAULT_DAX_CALLBACK = "DAX2Graph";

    /**
     * Ensures only one object is created always. Implements the Singleton.
     */
    private static PegasusProperties pegProperties = null;


    /**
     * The value of the PEGASUS_HOME environment variable.
     */
    private String mPegasusHome;

    /**
     * The object holding all the properties pertaining to the VDS system.
     */
    private VDSProperties mProps;

    /**
     * The Logger object.
     */
    private LogManager mLogger;

    /**
     * The String containing the messages to be logged.
     */
    private String mLogMsg;

    /**
     * The default path to the transformation catalog.
     */
    private String mDefaultTC;

    /**
     * The default path to the pool file.
     */
    private String mDefaultPoolFile;

    /**
     * The default path to the kickstart condor script, that allows the user to
     * submit the concrete DAG directly to the underlying CondorG.
     */
    private String mDefaultCondorKickStart;

    /**
     * The default transfer priority that needs to be applied to the transfer
     * jobs.
     */
    private String mDefaultTransferPriority;

    /**
     * The set containing the deprecated properties specified by the user.
     */
    private Set mDeprecatedProperties;

    /**
     * The pointer to the properties file that is written out in the submit directory.
     */
    private String mPropsInSubmitDir;




    /**
     * Returns an instance to this properties object.
     *
     * @return a handle to the Properties class.
     */
    public static PegasusProperties getInstance( ){
        return nonSingletonInstance( null );
    }


    /**
     * Returns an instance to this properties object.
     *
     * @param propFileName  name of the properties file to picked from
     *                      $PEGASUS_HOME/etc/ directory.
     *
     * @return a handle to the Properties class.
     */
    public static PegasusProperties getInstance( String propFileName ){
        return nonSingletonInstance( propFileName );
    }

    /**
     * To get a reference to the the object. The properties file that is loaded is
     * from the path specified in the argument.
     * This is *not implemented* as singleton. However the invocation of this
     * does modify the internally held singleton object.
     *
     * @param propFileName  name of the properties file to picked from
     *                      $PEGASUS_HOME/etc/ directory.
     *
     * @return a handle to the Properties class.
     */
    protected static PegasusProperties nonSingletonInstance( String propFileName ) {
        return new PegasusProperties( propFileName );
    }

    /**
     * To get a reference to the the object. The properties file that is loaded is
     * from the path specified in the argument.
     *
     * This is *not implemented* as singleton. However the invocation of this
     * does modify the internally held singleton object.
     *
     *
     * @return a handle to the Properties class.
     */
    public static PegasusProperties nonSingletonInstance() {
        return nonSingletonInstance( VDSProperties.PROPERTY_FILENAME );
    }




    /**
     * The constructor that constructs the default paths to the various
     * configuration files, and populates the singleton instance as required. If
     * the properties file passed is null, then the singleton instance is
     * invoked, else the non singleton instance is invoked.
     *
     * @param propertiesFile name of the properties file to picked
     *                       from $PEGASUS_HOME/etc/ directory.
     */
    private PegasusProperties( String propertiesFile ) {
        mLogger = LogManager.getInstance();
        mDeprecatedProperties   = new HashSet(5);
        initializePropertyFile( propertiesFile );
        mPegasusHome = mProps.getPegasusHome();

        mDefaultCondorKickStart = getDefaultPathToCondorKickstart();
        mDefaultPoolFile        = getDefaultPathToSC();
        mDefaultTC              = getDefaultPathToTC();
        mDefaultTransferPriority= getDefaultTransferPriority();

    }

    /**
     * Returns the default path to the transformation catalog. Currently the
     * default path defaults to  $PEGASUS_HOME/var/tc.data.
     *
     * @return the default path to tc.data.
     */
    public String getDefaultPathToTC() {
        StringBuffer sb = new StringBuffer( 50 );
        sb.append( mPegasusHome );
        sb.append( File.separator );
        sb.append( "var" );
        File f = new File( sb.toString(), TC_DATA_FILE );

        return f.getAbsolutePath();
    }

    /**
     * Returns the default path to the site catalog file.
     * The default path is constructed on the basis of the mode set by
     * the user.
     *
     * @return $PEGASUS_HOME/etc/sites.txt if the pool mode is Text, else
     *         $PEGASUS_HOME/etc/sites.xml
     *
     * @see #getPoolMode()
     */
    public String getDefaultPathToSC() {
        String name = POOL_CONFIG_FILE;
        name += (getPoolMode().equalsIgnoreCase("Text"))?
                 "txt":
                 "xml";
        File f = new File( mProps.getSysConfDir(),name);
        //System.err.println("Default Path to SC is " + f.getAbsolutePath());
        return f.getAbsolutePath();
    }

    /**
     * Returns the default path to the condor kickstart. Currently the path
     * defaults to $PEGASUS_HOME/bin/kickstart-condor.
     *
     * @return default path to kickstart condor.
     */
    public String getDefaultPathToCondorKickstart() {
        StringBuffer sb = new StringBuffer( 50 );
        sb.append( mPegasusHome );
        sb.append( File.separator );
        sb.append( "bin" );
        sb.append( File.separator );
        sb.append( CONDOR_KICKSTART );
        return sb.toString();
    }

    /**
     * Gets the handle to the properties file. The singleton instance is
     * invoked if the properties file is null (partly due to the way VDSProperties
     * is implemented ), else the non singleton is invoked. If you want to pick
     * up the default properties file in a non singleton manner, specify
     * VDSProperties.PROPERTY_FILENAME as a parameter.
     *
     * @param propertiesFile name of the properties file to picked
     *                       from $PEGASUS_HOME/etc/ directory.
     */
    private void initializePropertyFile( String propertiesFile ) {
        try {
            mProps = ( propertiesFile == null ) ?
                //invoke the singleton instance
                VDSProperties.instance() :
                //invoke the non singleton instance
                VDSProperties.nonSingletonInstance( propertiesFile );
        } catch ( IOException e ) {
            mLogMsg = "unable to read property file: " + e.getMessage();
            mLogger.log( mLogMsg , LogManager.FATAL_MESSAGE_LEVEL);
            System.exit( 1 );
        } catch ( MissingResourceException e ) {
            mLogMsg = "You forgot to set -Dpegasus.home=$PEGASUS_HOME!";
            mLogger.log( mLogMsg , LogManager.FATAL_MESSAGE_LEVEL);
            System.exit( 1 );
        }

    }


    /**
     * It allows you to get any property from the property file without going
     * through the corresponding accesor function in this class. For coding
     * and clarity purposes, the function should be used judiciously, and the
     * accessor function should be used as far as possible.
     *
     * @param key  the property whose value is desired.
     * @return String
     */
    public String getProperty( String key ) {
        return mProps.getProperty( key );
    }

    /**
     * Returns the VDSProperties that this object encapsulates. Use only when
     * absolutely necessary. Use accessor methods whereever possible.
     *
     * @return VDSProperties
     */
    public VDSProperties getVDSProperties(){
        return this.mProps;
    }

    /**
     * Accessor: Overwrite any properties from within the program.
     *
     * @param key is the key to look up
     * @param value is the new property value to place in the system.
     * @return the old value, or null if it didn't exist before.
     */
    public Object setProperty( String key, String value ) {
        return mProps.setProperty( key, value );
    }

    /**
     * Extracts a specific property key subset from the known properties.
     * The prefix may be removed from the keys in the resulting dictionary,
     * or it may be kept. In the latter case, exact matches on the prefix
     * will also be copied into the resulting dictionary.
     *
     * @param prefix is the key prefix to filter the properties by.
     * @param keepPrefix if true, the key prefix is kept in the resulting
     * dictionary. As side-effect, a key that matches the prefix exactly
     * will also be copied. If false, the resulting dictionary's keys are
     * shortened by the prefix. An exact prefix match will not be copied,
     * as it would result in an empty string key.
     *
     * @return a property dictionary matching the filter key. May be
     * an empty dictionary, if no prefix matches were found.
     *
     * @see #getProperty( String ) is used to assemble matches
     */
    public Properties matchingSubset( String prefix, boolean keepPrefix ) {
        return mProps.matchingSubset( prefix, keepPrefix );
    }

    /**
     * Returns the properties matching a particular prefix as a list of
     * sorted name value pairs, where name is the full name of the matching
     * property (including the prefix) and value is it's value in the properties
     * file.
     *
     * @param prefix   the prefix for the property names.
     * @param system   boolean indicating whether to match only System properties
     *                 or all including the ones in the property file.
     *
     * @return  list of <code>NameValue</code> objects corresponding to the matched
     *          properties sorted by keys.
     *          null if no matching property is found.
     */
    public List getMatchingProperties( String prefix, boolean system ) {
        //sanity check
        if ( prefix == null ) {
            return null;
        }
        Properties p = (system)?
                       System.getProperties():
                       matchingSubset(prefix,true);

        java.util.Enumeration e = p.propertyNames();
        List l = ( e.hasMoreElements() ) ? new java.util.ArrayList() : null;

        while ( e.hasMoreElements() ) {
            String key = ( String ) e.nextElement();
                NameValue nv = new NameValue( key, p.getProperty( key ) );
                l.add( nv );
        }

        Collections.sort(l);
        return ( l.isEmpty() ) ? null : l;
    }

    /**
     * Accessor to $PEGASUS_HOME/etc. The files in this directory have a low
     * change frequency, are effectively read-only, they reside on a
     * per-machine basis, and they are valid usually for a single user.
     *
     * @return the "etc" directory of the VDS runtime system.
     */
    public File getSysConfDir() {
        return mProps.getSysConfDir();
    }

    /**
     * Accessor: Obtains the root directory of the Pegasus runtime
     * system.
     *
     * @return the root directory of the Pegasus runtime system, as initially
     * set from the system properties.
     */
    public String getPegasusHome() {
        return mProps.getPegasusHome();
    }

    //PROPERTIES RELATED TO SCHEMAS
    /**
     * Returns the location of the schema for the DAX.
     *
     * Referred to by the "pegasus.schema.dax" property.
     *
     * @return location to the DAX schema.
     */
    public String getDAXSchemaLocation() {
        return this.getDAXSchemaLocation( null );
    }

    /**
     * Returns the location of the schema for the DAX.
     *
     * Referred to by the "pegasus.schema.dax" property.
     *
     * @param defaultLocation the default location to the schema.
     *
     * @return location to the DAX schema specified in the properties file,
     *         else the default location if no value specified.
     */
    public String getDAXSchemaLocation( String defaultLocation ) {
        return mProps.getProperty( "pegasus.schema.dax", defaultLocation );
    }

    /**
     * Returns the location of the schema for the PDAX.
     *
     * Referred to by the "pegasus.schema.pdax" property
     *
     * @param defaultLocation the default location to the schema.
     *
     * @return location to the PDAX schema specified in the properties file,
     *         else the default location if no value specified.
     */
    public String getPDAXSchemaLocation( String defaultLocation ) {
        return mProps.getProperty( "pegasus.schema.pdax", defaultLocation );
    }

    //DIRECTORY CREATION PROPERTIES
    /**
     * Returns the name of the class that the user wants, to insert the
     * create directory jobs in the graph in case of creating random
     * directories.
     *
     * Referred to by the "pegasus.dir.create" property.
     *
     * @return  the create dir classname if specified in the properties file,
     *          else HourGlass.
     */
    public String getCreateDirClass() {
        return mProps.getProperty( "pegasus.dir.create",
                                   "HourGlass" );
    }

    /**
     * It specifies whether to use the extended timestamp format for generation
     * of timestamps that are used to create the random directory name, and for
     * the classads generation.
     *
     * Referred to by the "pegasus.dir.timestamp.extended" property.
     *
     * @return the value specified in the properties file if valid boolean, else
     *         false.
     */
    public boolean useExtendedTimeStamp() {
        return Boolean.parse(mProps.getProperty( "pegasus.dir.timestamp.extended"),
                             false );
    }


    /**
     * Returns a boolean indicating whether to use timestamp for directory
     * name creation or not.
     *
     * Referred to by "pegasus.dir.useTimestamp" property.
     *
     * @return  the boolean value specified in the properties files, else false.
     */
    public boolean useTimestampForDirectoryStructure(){
        return Boolean.parse( mProps.getProperty( "pegasus.dir.useTimestamp" ),
                              false );
    }

    /**
     * Returns the execution directory suffix or absolute specified
     * that is appended/replaced to the exec-mount-point specified in the
     * pool catalog for the various pools.
     *
     * Referred to by the "pegasus.dir.exec" property
     *
     * @return the value specified in the properties file,
     *         else the default suffix.
     *
     * @see #DEFAULT_EXEC_DIR
     */
    public String getExecDirectory() {
        return mProps.getProperty( "pegasus.dir.exec", DEFAULT_EXEC_DIR );
    }

    /**
     * Returns the storage directory suffix or absolute specified
     * that is appended/replaced to the storage-mount-point specified in the
     * pool catalog for the various pools.
     *
     * Referred to by the "pegasus.dir.storage" property.
     *
     * @return the value specified in the properties file,
     *         else the default suffix.
     *
     * @see #DEFAULT_STORAGE_DIR
     */
    public String getStorageDirectory() {
        return mProps.getProperty( "pegasus.dir.storage", DEFAULT_STORAGE_DIR );
    }

    /**
     * Returns a boolean indicating whether to have a deep storage directory
     * structure or not while staging out data to the output site.
     *
     * Referred to by the "pegasus.dir.storage.deep" property.
     *
     * @return  the boolean value specified in the properties files, else false.
     */
    public boolean useDeepStorageDirectoryStructure(){
        return Boolean.parse( mProps.getProperty( "pegasus.dir.storage.deep" ),
                              false );
    }



    //PROPERTIES RELATED TO THE TRANSFORMATION CATALOG
    /**
     * Returns the mode to be used for accessing the Transformation Catalog.
     *
     * Referred to by the "pegasus.catalog.transformation" property.
     *
     * @return  the value specified in properties file,
     *          else DEFAULT_TC_MODE.
     *
     * @see #DEFAULT_TC_MODE
     */
    public String getTCMode() {
        return mProps.getProperty( "pegasus.catalog.transformation", DEFAULT_TC_MODE );
    }

    /**
     * Returns the location of the transformation catalog.
     *
     * Referred to by "pegasus.catalog.transformation.file" property.
     *
     * @return the value specified in the properties file,
     *         else default path specified by mDefaultTC.
     *
     * @see #mDefaultTC
     */
    public String getTCPath() {
        return mProps.getProperty( "pegasus.catalog.transformation.file", mDefaultTC );
    }

    /**
     * Returns the mode for loading the transformation mapper that sits in
     * front of the transformation catalog.
     *
     * Referred to by the "pegasus.catalog.transformation.mapper" property.
     *
     * @return the value specified in the properties file,
     *         else default tc mapper mode.
     *
     * @see #DEFAULT_TC_MAPPER_MODE
     */
    public String getTCMapperMode() {
        return mProps.getProperty( "pegasus.catalog.transformation.mapper", DEFAULT_TC_MAPPER_MODE );
    }

    //REPLICA CATALOG PROPERTIES
    /**
     * Returns the replica mode. It identifies the ReplicaMechanism being used
     * by Pegasus to determine logical file locations.
     *
     * Referred to by the "pegasus.catalog.replica" property.
     *
     * @return the replica mode, that is used to load the appropriate
     *         implementing class if property is specified,
     *         else the DEFAULT_REPLICA_MODE
     *
     * @see #DEFAULT_REPLICA_MODE
     */
    public String getReplicaMode() {
        return mProps.getProperty( "pegasus.catalog.replica", DEFAULT_REPLICA_MODE);
    }

    /**
     * Returns the url to the RLI of the RLS.
     *
     * Referred to by the "pegasus.rls.url" property.
     *
     * @return the value specified in properties file,
     *          else DEFAULT_RLI_URL.
     *
     * @see #DEFAULT_RLI_URL
     */
    public String getRLIURL() {
        return mProps.getProperty( "pegasus.catalog.replica.url", DEFAULT_RLI_URL );
    }


    /**
     * It returns the timeout value in seconds after which to timeout in case of
     * no activity from the RLS.
     *
     * Referred to by the "pegasus.rc.rls.timeout" property.
     *
     * @return the timeout value if specified else,
     *         DEFAULT_RLS_TIMEOUT.
     *
     * @see #DEFAULT_RLS_TIMEOUT
     */
    public int getRLSTimeout() {
        String prop = mProps.getProperty( "pegasus.catalog.replica.rls.timeout",
                                          DEFAULT_RLS_TIMEOUT );
        int val;
        try {
            val = Integer.parseInt( prop );
        } catch ( Exception e ) {
            return Integer.parseInt( DEFAULT_RLS_TIMEOUT );
        }
        return val;

    }

    //PROPERTIES RELATED TO SITE CATALOG
   /**
    * Returns the mode to be used for accessing the pool information.
    *
    * Referred to by the "pegasus.catalog.site" property.
    *
    * @return the pool mode, that is used to load the appropriate
    *         implementing class if the property is specified,
    *         else default pool mode specified by DEFAULT_POOL_MODE
    *
    * @see #DEFAULT_POOL_MODE
    */
   public String getPoolMode() {
       return mProps.getProperty( "pegasus.catalog.site", DEFAULT_POOL_MODE );
   }

   /**
    * Returns the path to the pool file.
    *
    * Referred to by the "pegasus.catalog.site.file" property.
    *
    * @return the path to the pool file specified in the properties file,
    *         else the default path specified by mDefaultPoolFile.
    *
    * @see #mDefaultPoolFile
    */
   public String getPoolFile() {
       return mProps.getProperty( "pegasus.catalog.site.file",
                                  mDefaultPoolFile );
   }

   /**
    * Returns the location of the schema for the DAX.
    *
    * Referred to by the "pegasus.schema.sc" property.
    *
    * @return the location of pool schema if specified in properties file,
    *         else null.
    */
   public String getPoolSchemaLocation() {
       return this.getPoolSchemaLocation( null );
   }

   /**
    * Returns the location of the schema for the site catalog file.
    *
    * Referred to by the "pegasus.schema.sc" property
    *
    * @param defaultLocation  the default location where the schema should be
    *                         if no other location is specified.
    *
    * @return the location specified by the property,
    *         else defaultLocation.
    */
   public String getPoolSchemaLocation( String defaultLocation ) {
       return mProps.getProperty("pegasus.schema.sc",
                                 defaultLocation );
   }

    //PROVENANCE CATALOG PROPERTIES
    /**
     * Returns the provenance store to use to log the refiner actions.
     *
     * Referred to by the "pegasus.catalog.provenance.refinement" property.
     *
     * @return the value set in the properties, else null if not set.
     */
    public String getRefinementProvenanceStore( ){
        return mProps.getProperty( "pegasus.catalog.provenance.refinement" );
    }

    //TRANSFER MECHANISM PROPERTIES

    /**
     * Returns the transfer implementation that is to be used for constructing
     * the transfer jobs.
     *
     * Referred to by the "pegasus.transfer.*.impl" property.
     *
     * @return the transfer implementation, else the
     *         DEFAULT_TRANSFER_IMPLEMENTATION.
     *
     * @see #DEFAULT_TRANSFER_IMPLEMENTATION
     */
    public String getTransferImplementation(){
        return getTransferImplementation( "pegasus.transfer.*.impl" );
    }

    /**
     * Returns the sls transfer implementation that is to be used for constructing
     * the transfer jobs.
     *
     * Referred to by the "pegasus.transfer.sls.*.impl" property.
     *
     * @return the transfer implementation, else the
     *         DEFAULT_TRANSFER_IMPLEMENTATION.
     *
     * @see #DEFAULT_TRANSFER_IMPLEMENTATION
     */
    public String getSLSTransferImplementation(){
        return getTransferImplementation( "pegasus.transfer.sls.*.impl" );
    }


    /**
     * Returns the transfer implementation.
     *
     * @param property property name.
     *
     * @return the transfer implementation,
     *         else the one specified by "pegasus.transfer.*.impl",
     *         else the DEFAULT_TRANSFER_IMPLEMENTATION.
     */
    public String getTransferImplementation(String property){
        String value = mProps.getProperty(property,
                                          getDefaultTransferImplementation());

        String dflt = property.equals( "pegasus.transfer.setup.impl" ) ?
                            DEFAULT_SETUP_TRANSFER_IMPLEMENTATION:
                            DEFAULT_TRANSFER_IMPLEMENTATION ;
                
        return (value == null)?
               dflt :
               value;
    }


    /**
     * Returns the transfer refiner that is to be used for adding in the
     * transfer jobs in the workflow
     *
     * Referred to by the "pegasus.transfer.refiner" property.
     *
     * @return the transfer refiner, else the DEFAULT_TRANSFER_REFINER.
     *
     * @see #DEFAULT_TRANSFER_REFINER
     */
    public String getTransferRefiner(){
        String value = mProps.getProperty("pegasus.transfer.refiner");

        //put in default if still we have a non null
        return (value == null)?
                DEFAULT_TRANSFER_REFINER:
                value;

    }


    /**
     * Returns whether to introduce quotes around url's before handing to
     * g-u-c and condor.
     *
     * Referred to by "pegasus.transfer.single.quote" property.
     *
     * @return boolean value specified in the properties file, else
     *         true in case of non boolean value being specified or property
     *         not being set.
     */
    public boolean quoteTransferURL() {
        return Boolean.parse(mProps.getProperty( "pegasus.transfer.single.quote"),
                             true);
    }

    /**
     * It returns the number of processes of g-u-c that the transfer script needs to
     * spawn to do the transfers. This is applicable only in the case where the
     * transfer executable has the capability of spawning processes. It should
     * not be confused with the number of streams that each process opens.
     * By default it is set to 4. In case a non integer value is specified in
     * the properties file it returns the default value.
     *
     * Referred to by "pegasus.transfer.throttle.processes" property.
     *
     * @return the number of processes specified in properties file, else
     *         DEFAULT_TRANSFER_PROCESSES
     *
     * @see #DEFAULT_TRANSFER_PROCESSES
     */
    public String getNumOfTransferProcesses() {
        String prop = mProps.getProperty( "pegasus.transfer.throttle.processes",
            DEFAULT_TRANSFER_PROCESSES );
        int val = -1;

        try {
            val = Integer.parseInt( prop );
        } catch ( Exception e ) {
            return DEFAULT_TRANSFER_PROCESSES;
        }

        return Integer.toString( val );

    }

    /**
     * It returns the number of streams that each transfer process uses to do the
     * ftp transfer. By default it is set to 1.In case a non integer
     * value is specified in the properties file it returns the default value.
     *
     * Referred to by "pegasus.transfer.throttle.streams" property.
     *
     * @return the number of streams specified in the properties file, else
     *         DEFAULT_TRANSFER_STREAMS.
     *
     * @see #DEFAULT_TRANSFER_STREAMS
     */
    public String getNumOfTransferStreams() {

        String prop = mProps.getProperty( "pegasus.transfer.throttle.streams",
            DEFAULT_TRANSFER_STREAMS );
        int val = -1;

        try {
            val = Integer.parseInt( prop );
        } catch ( Exception e ) {
            return DEFAULT_TRANSFER_STREAMS;
        }

        return Integer.toString( val );

    }

    /**
     * It specifies whether the underlying transfer mechanism being used should
     * use the force option if available to transfer the files.
     *
     * Referred to by "pegasus.transfer.force" property.
     *
     * @return boolean value specified in the properties file,else
     *         false in case of non boolean value being specified or
     *         property not being set.
     */
    public boolean useForceInTransfer() {
        return Boolean.parse(mProps.getProperty( "pegasus.transfer.force"),
                             false);
    }

    /**
     * It returns whether the use of symbolic links in case where the source
     * and destination files happen to be on the same file system.
     *
     * Referred to by "pegasus.transfer.links" property.
     *
     * @return boolean value specified in the properties file, else
     *         false in case of non boolean value being specified or
     *         property not being set.
     */
    public boolean getUseOfSymbolicLinks() {
        String value = mProps.getProperty( "pegasus.transfer.links" );
        return Boolean.parse(value,false);
    }


    /**
     * Returns the comma separated list of third party sites, specified in the
     * properties.
     *
     * @param property property name.
     *
     * @return the comma separated list of sites.
     */
    public String getThirdPartySites(String property){
        String value = mProps.getProperty(property);

        return value;


    }

    /**
     * Returns the comma separated list of third party sites for which
     * the third party transfers are executed on the remote sites.
     *
     *
     * @param property property name.
     *
     * @return the comma separated list of sites.
     */
    public String getThirdPartySitesRemote(String property){
        return mProps.getProperty(property);
    }



    /**
     * Returns the delimiter to be used for constructing the staged executable
     * name, during transfer of executables to remote sites.
     *
     * Referred to by the "pegasus.transfer.staging.delimiter" property.
     *
     * @return  the value specified in the properties file, else
     *          DEFAULT_STAGING_DELIMITER
     *
     * @see #DEFAULT_STAGING_DELIMITER
     */
    public String getStagingDelimiter(){
        return mProps.getProperty("pegasus.transfer.staging.delimiter",
                                  DEFAULT_STAGING_DELIMITER);
    }

    /**
     * Returns the list of sites for which the chmod job creation has to be
     * disabled for executable staging.
     *
     * Referred to by the "pegasus.transfer.disable.chmod" property.
     *
     * @return a comma separated list of site names.
     */
    public String getChmodDisabledSites() {
        return mProps.getProperty( "pegasus.transfer.disable.chmod.sites" );
    }


    /**
     * It specifies if for a job execution the proxy is to be transferred
     * from the submit host or not.
     *
     * Referred to by "pegasus.transfer.proxy" property.
     *
     * @return boolean value specified in the properties file,else
     *         false in case of non boolean value being specified or
     *         property not being set.
     */
    public boolean transferProxy() {
        return Boolean.parse(mProps.getProperty( "pegasus.transfer.proxy"),
                             false);
    }

    /**
     * Returns the arguments with which the transfer executable needs
     * to be invoked.
     *
     * Referred to by "pegasus.transfer.arguments" property.
     *
     * @return the arguments specified in the properties file,
     *         else null if property is not specified.
     */
    public String getTransferArguments() {
	return mProps.getProperty("pegasus.transfer.arguments");
    }

    /**
     * Returns the priority to be set for the stage in transfer job.
     *
     * Referred to by "pegasus.transfer.stagein.priority" property if set,
     * else by "pegasus.transfer.*.priority" property.
     *
     * @return the priority as String if a valid integer specified in the
     *         properties, else null.
     */
    public String getTransferStageInPriority(){
        return getTransferPriority("pegasus.transfer.stagein.priority");
    }

    /**
     * Returns the priority to be set for the stage out transfer job.
     *
     * Referred to by "pegasus.transfer.stageout.priority" property if set,
     * else by "pegasus.transfer.*.priority" property.
     *
     * @return the priority as String if a valid integer specified in the
     *         properties, else null.
     */
    public String getTransferStageOutPriority(){
        return getTransferPriority("pegasus.transfer.stageout.priority");
    }

    /**
     * Returns the priority to be set for the interpool transfer job.
     *
     * Referred to by "pegasus.transfer.inter.priority" property if set,
     * else by "pegasus.transfer.*.priority" property.
     *
     * @return the priority as String if a valid integer specified in the
     *         properties, else null.
     */
    public String getTransferInterPriority(){
        return getTransferPriority("pegasus.transfer.inter.priority");
    }


    /**
     * Returns the transfer priority.
     *
     * @param property property name.
     *
     * @return the priority as String if a valid integer specified in the
     *         properties as value to property, else null.
     */
    private String getTransferPriority(String property){
        String value = mProps.getProperty(property, mDefaultTransferPriority);
        int val = -1;
        try {
            val = Integer.parseInt( value );
        } catch ( Exception e ) {

        }
        //if value in properties file is corrupted
        //again use the default transfer priority
        return ( val < 0 ) ? mDefaultTransferPriority : Integer.toString( val );

    }





    //REPLICA SELECTOR FUNCTIONS

    /**
     * Returns the mode for loading the transformation selector that selects
     * amongst the various candidate transformation catalog entry objects.
     *
     * Referred to by the "pegasus.selector.transformation" property.
     *
     * @return the value specified in the properties file,
     *         else default transformation selector.
     *
     * @see #DEFAULT_TC_MAPPER_MODE
     */
    public String getTXSelectorMode() {
        return mProps.getProperty( "pegasus.selector.transformation",
                                   DEFAULT_TX_SELECTOR_MODE );
    }


    /**
     * Returns the name of the selector to be used for selection amongst the
     * various replicas of a single lfn.
     *
     * Referred to by the "pegasus.selector.replica" property.
     *
     * @return the name of the selector if the property is specified,
     *         else null
     */
    public String getReplicaSelector(){
        return mProps.getProperty( "pegasus.selector.replica" );
    }

    /**
     * Returns a comma separated list of sites, that are restricted in terms of
     * data movement from the site.
     *
     * Referred to by the "pegasus.rc.restricted.sites" property.
     *
     * @return comma separated list of sites.
     */
//    public String getRestrictedSites(){
//        return mProps.getProperty("pegasus.rc.restricted.sites","");
//    }

    /**
     * Returns a comma separated list of sites, from which to prefer data
     * transfers for all sites.
     *
     * Referred to by the "pegasus.selector.replica.*.prefer.stagein.sites" property.
     *
     * @return comma separated list of sites.
     */
    public String getAllPreferredSites(){
        return mProps.getProperty( "pegasus.selector.replica.*.prefer.stagein.sites","");
    }

    /**
     * Returns a comma separated list of sites, from which to ignore data
     * transfers for all sites. Replaces the old pegasus.rc.restricted.sites
     * property.
     *
     * Referred to by the "pegasus.selector.ignore.*.prefer.stagein.sites" property.
     *
     * @return comma separated list of sites.
     */
    public String getAllIgnoredSites(){
        return mProps.getProperty("pegasus.selector.replica.*.ignore.stagein.sites",
                                  "");
    }


    //SITE SELECTOR PROPERTIES
    /**
     * Returns the class name of the site selector, that needs to be invoked to do
     * the site selection.
     *
     * Referred to by the "pegasus.selector.site" property.
     *
     * @return the classname corresponding to the site selector that needs to be
     *         invoked if specified in the properties file, else the default
     *         selector specified by DEFAULT_SITE_SELECTOR.
     *
     * @see #DEFAULT_SITE_SELECTOR
     */
    public String getSiteSelectorMode() {
        return mProps.getProperty( "pegasus.selector.site",
                                   DEFAULT_SITE_SELECTOR );
    }

    /**
     * Returns the path to the external site selector that needs to be called
     * out to make the decision of site selection.
     *
     * Referred to by the "pegasus.selector.site.path" property.
     *
     * @return the path to the external site selector if specified in the
     *         properties file, else null.
     */
    public String getSiteSelectorPath() {
        return mProps.getProperty( "pegasus.selector.site.path" );
    }

    /**
     * It returns the timeout value in seconds after which to timeout in case of
     * no activity from the external site selector.
     *
     * Referred to by the "pegasus.selector.site.timeout" property.
     *
     * @return the timeout value if specified else,
     *         DEFAULT_SITE_SELECTOR_TIMEOUT.
     *
     * @see #DEFAULT_SITE_SELECTOR_TIMEOUT
     */
    public int getSiteSelectorTimeout() {
        String prop = mProps.getProperty( "pegasus.selector.site.timeout",
                                          DEFAULT_SITE_SELECTOR_TIMEOUT );
        int val;
        try {
            val = Integer.parseInt( prop );
        } catch ( Exception e ) {
            return Integer.parseInt( DEFAULT_SITE_SELECTOR_TIMEOUT );
        }
        return val;

    }

    /**
     * Returns a value designating whether we need to keep the temporary files
     * that are passed to the external site selectors. The check for the valid
     * tristate value should be done at the calling function end. This just
     * passes on the value user specified in the properties file.
     *
     * Referred to by the "pegasus.selector.site.keep.tmp" property.
     *
     * @return the value of the property is specified, else
     *         DEFAULT_SITE_SELECTOR_KEEP
     *
     * @see #DEFAULT_SITE_SELECTOR_KEEP
     */
    public String getSiteSelectorKeep() {
        return mProps.getProperty( "pegasus.selector.site.keep.tmp",
                                   DEFAULT_SITE_SELECTOR_KEEP );
    }



    //PROPERTIES RELATED TO KICKSTART AND EXITCODE

    /**
     * Returns the GRIDSTART that is to be used to launch the jobs on the grid.
     *
     * Referred to by the "pegasus.gridstart" property.
     *
     * @return the value specified in the property file,
     *         else DEFAULT_GRIDSTART_MODE
     *
     * @see #DEFAULT_GRIDSTART_MODE
     */
    public String getGridStart(){
        return mProps.getProperty("pegasus.gridstart",DEFAULT_GRIDSTART_MODE);
    }


    /**
     * Return a boolean indicating whether to turn the stat option for kickstart
     * on or not. By default it is turned on.
     *
     * Referred to by the "pegasus.gridstart.kickstart.stat" property.
     *
     * @return the boolean value specified in the property file,
     *         else false if not specified or non boolean specified.
     */
    public boolean doStatWithKickstart(){
        return Boolean.parse( mProps.getProperty( "pegasus.gridstart.kickstart.stat"),
                              false );
    }

    /**
     * Return a boolean indicating whether to generate the LOF files for the jobs
     * or not. This is used to generate LOF files, but not trigger the stat option
     *
     * Referred to by the "pegasus.gridstart.kickstart.generate.loft" property.
     *
     * @return the boolean value specified in the property file,
     *         else false if not specified or non boolean specified.
     */
    public boolean generateLOFFiles(){
        return Boolean.parse( mProps.getProperty( "pegasus.gridstart.generate.lof"),
                                                   false );
    }


    /**
     * Returns a boolean indicating whether to use invoke in kickstart always
     * or not.
     *
     * Referred to by the "pegasus.gridstart.invoke.always" property.
     *
     * @return the boolean value specified in the property file,
     *         else false if not specified or non boolean specified.
     */
    public boolean useInvokeInGridStart(){
        return Boolean.parse( mProps.getProperty( "pegasus.gridstart.invoke.always"),
                              false );
    }

    /**
     * Returns the trigger value for invoking an application through kickstart
     * using kickstart. If the arguments value being constructed in the condor
     * submit file is more than this value, then invoke is used to pass the
     * arguments to the remote end. Helps in bypassing the Condor 4K limit.
     *
     * Referred to by "pegasus.gridstart.invoke.length" property.
     *
     * @return  the long value specified in the properties files, else
     *          DEFAULT_INVOKE_LENGTH
     *
     * @see #DEFAULT_INVOKE_LENGTH
     */
    public long getGridStartInvokeLength(){
        long value = new Long(this.DEFAULT_INVOKE_LENGTH).longValue();

        String st = mProps.getProperty( "pegasus.gridstart.invoke.length",
                                        this.DEFAULT_INVOKE_LENGTH );
        try {
            value = new Long( st ).longValue();
        } catch ( Exception e ) {
            //ignore malformed values from
            //the property file
        }

        return value;
    }

    /**
     * Returns a boolean indicating whehter to pass extra options to kickstart
     * or not. The extra options have appeared only in VDS version 1.4.2 (like -L
     * and -T).
     *
     * Referred to by "pegasus.gridstart.label" property.
     *
     * @return the boolean value specified in the property file,
     *         else false if not specified or non boolean specified.
     */
    public boolean generateKickstartExtraOptions(){
        return Boolean.parse( mProps.getProperty( "pegasus.gridstart.label"),
                              false );
    }

    /**
     * Returns the mode adding the postscripts for the jobs. At present takes in
     * only two values all or none default being none.
     *
     * Referred to by the "pegasus.exitcode.scope" property.
     *
     * @return the mode specified by the property, else
     *         DEFAULT_POSTSCRIPT_MODE
     *
     * @see #DEFAULT_POSTSCRIPT_MODE
     */
    public String getPOSTScriptScope() {
        return mProps.getProperty( "pegasus.exitcode.scope",
                                   DEFAULT_POSTSCRIPT_MODE );
    }

    /**
     * Returns the postscript to use with the jobs in the workflow. They
     * maybe overriden by values specified in the profiles.
     *
     * Referred to by the "pegasus.exitcode.impl" property.
     *
     * @return the postscript to use for the workflow, else null if not
     *         specified in the properties.
     */
    public String getPOSTScript(){
        return mProps.getProperty( "pegasus.exitcode.impl" );
    }

    /**
     * Returns the path to the exitcode executable to be used.
     *
     * Referred to by the "pegasus.exitcode.path.[value]" property, where [value]
     * is replaced by the value passed an input to this function.
     *
     * @param value   the short name of the postscript whose path we want.
     *
     * @return the path to the postscript if specified in properties file.
     */
    public String getPOSTScriptPath( String value ){
        value = ( value == null ) ? "*" : value;
        StringBuffer key = new StringBuffer();
        key.append( "pegasus.exitcode.path." ).append( value );

        return mProps.getProperty( key.toString() );
    }


    /**
     * Returns the argument string containing the arguments by which exitcode is
     * invoked.
     *
     * Referred to by the "pegasus.exitcode.arguments" property.
     *
     * @return String containing the arguments,else empty string.
     */
    public String getPOSTScriptArguments() {
        return mProps.getProperty( "pegasus.exitcode.arguments", "");
    }

    /**
     * Returns a boolean indicating whether to turn debug on or not for exitcode.
     * By default false is returned.
     *
     * Referred to by the "pegasus.exitcode.debug" property.
     *
     * @return boolean value.
     */
    public boolean setPostSCRIPTDebugON(){
        return Boolean.parse( mProps.getProperty( "pegasus.exitcode.debug"), false );
    }

    /**
     * Returns the argument string containing the arguments by which prescript is
     * invoked.
     *
     * Referred to by the "pegasus.prescript.arguments" property.
     *
     * @return String containing the arguments.
     *         null if not specified.
     */
    public String getPrescriptArguments() {
        return mProps.getProperty( "pegasus.prescript.arguments","" );
    }


    //PROPERTIES RELATED TO REMOTE SCHEDULERS
    /**
     * Returns the project names that need to be  appended to the RSL String
     * while creating the submit files. Referred to by
     * pegasus.remote.projects property. If present, Pegasus ends up
     * inserting an RSL string (project = value) in the submit file.
     *
     * @return a comma separated list of key value pairs if property specified,
     *         else null.
     */
    public String getRemoteSchedulerProjects() {
        return mProps.getProperty( "pegasus.remote.scheduler.projects" );
    }

    /**
     * Returns the queue names that need to be  appended to the RSL String while
     * creating the submit files. Referred to by the pegasus.remote.queues
     * property. If present, Pegasus ends up inserting an RSL string
     * (project = value) in the submit file.
     *
     * @return a comma separated list of key value pairs if property specified,
     *         else null.
     */
    public String getRemoteSchedulerQueues() {
        return mProps.getProperty( "pegasus.remote.scheduler.queues" );
    }

    /**
     * Returns the maxwalltimes for the various pools that need to be  appended
     * to the RSL String while creating the submit files. Referred to by the
     * pegasus.scheduler.remote.queues property. If present, Pegasus ends up
     * inserting an RSL string (project = value) in the submit file.
     *
     *
     * @return a comma separated list of key value pairs if property specified,
     *         else null.
     */
    public String getRemoteSchedulerMaxWallTimes() {
        return mProps.getProperty( "pegasus.remote.scheduler.min.maxwalltime" );
    }

    /**
     * Returns the  minimum walltimes that need to be enforced.
     *
     * Referred to by "pegasus.scheduler.remote.min.[key]" property.
     *
     * @param key  the appropriate globus RSL key. Generally are
     *              maxtime|maxwalltime|maxcputime
     *
     * @return the integer value as specified, -1 in case of no value being specified.
     */
    public int getMinimumRemoteSchedulerTime( String key ){
        StringBuffer property = new StringBuffer();
        property.append( "pegasus.remote.scheduler.min." ).append( key );

        int val = -1;

        try {
            val = Integer.parseInt( mProps.getProperty( property.toString() ) );
        } catch ( Exception e ) {
        }
        return val;
    }

    //PROPERTIES RELATED TO CONDOR

    /**
     * Returns a boolean indicating whether we want to Condor Quote the
     * arguments of the job or not.
     *
     * Referred to by the "pegasus.condor.arguments.quote" property.
     *
     * @return boolean
     */
    public boolean useCondorQuotingForArguments(){
        return Boolean.parse( mProps.getProperty("pegasus.condor.arguments.quote"),
                              true);
    }

    /**
     * Returns the number of release attempts that are written into the condor
     * submit files. Condor holds jobs on certain kind of failures, which many
     * a time are transient, and if a job is released it usually progresses.
     *
     * Referred to by the "pegasus.condor.release" property.
     *
     * @return an int denoting the number of times to release.
     *         null if not  specified or invalid entry.
     */
    public String getCondorPeriodicReleaseValue() {
        String prop = mProps.getProperty( "pegasus.condor.release" );
        int val = -1;

        try {
            val = Integer.parseInt( prop );
        } catch ( Exception e ) {
            return null;
        }

        return ( val < 0 ) ? null : Integer.toString( val );
    }

    /**
     * Returns the number of release attempts that are attempted before
     * Condor removes the job from the queue and marks it as failed.
     *
     * Referred to by the "pegasus.condor.remove" property.
     *
     * @return an int denoting the number of times to release.
     *         null if not  specified or invalid entry.
     */
    public String getCondorPeriodicRemoveValue() {
        String prop = mProps.getProperty( "pegasus.condor.remove" );
        int val = -1;

        try {
            val = Integer.parseInt( prop );
        } catch ( Exception e ) {
            return null;
        }

        return ( val < 0 ) ? null : Integer.toString( val );
    }

    /**
     * Returns the number of times Condor should retry running a job in case
     * of failure. The retry ends up reinvoking the prescript, that can change
     * the site selection decision in case of failure.
     *
     * Referred to by the "pegasus.dagman.retry" property.
     *
     * @return an int denoting the number of times to retry.
     *         null if not specified or invalid entry.
     */
    public String getCondorRetryValue() {
        String prop = mProps.getProperty( "pegasus.dagman.retry" );
        int val = -1;

        try {
            val = Integer.parseInt( prop );
        } catch ( Exception e ) {
            return null;
        }

        return Integer.toString( val );
    }

    /**
     * Tells whether to stream condor output or not. By default it is true ,
     * meaning condor streams the output from the remote hosts back to the submit
     * hosts, instead of staging it. This helps in saving filedescriptors at the
     * jobmanager end.
     *
     * If it is set to false, output is not streamed back. The line
     * "stream_output = false" should be added in the submit files for kickstart
     *  jobs.
     *
     * Referred to by the "pegasus.condor.output.stream" property.
     *
     * @return the boolean value specified by the property, else
     *         true in case of invalid value or property not being specified.
     *
     */
    public boolean streamCondorOutput() {
        return Boolean.parse(mProps.getProperty( "pegasus.condor.output.stream"),
                             true );
    }

    /**
     * Tells whether to stream condor error or not. By default it is true ,
     * meaning condor streams the error from the remote hosts back to the submit
     * hosts instead of staging it in. This helps in saving filedescriptors at
     * the jobmanager end.
     *
     * Referred to by the "pegasus.condor.error.stream" property.
     *
     * If it is set to false, output is not streamed back. The line
     * "stream_output = false" should be added in the submit files for kickstart
     *  jobs.
     *
     * @return the boolean value specified by the property, else
     *         true in case of invalid value or property not being specified.
     */
    public boolean streamCondorError() {
        return Boolean.parse(mProps.getProperty( "pegasus.condor.error.stream"),
                             true);
    }

    //PROPERTIES RELATED TO STORK
    /**
     * Returns the credential name to be used for the stork transfer jobs.
     *
     * Referred to by the "pegasus.transfer.stork.cred" property.
     *
     * @return the credential name if specified by the property,
     *         else null.
     */
    public String getCredName() {
        return mProps.getProperty( "pegasus.transfer.stork.cred" );
    }

    //SOME LOGGING PROPERTIES
    /**
     * Returns the file to which all the logging needs to be directed to.
     *
     * Referred to by the "pegasus.log.*" property.
     *
     * @return the value of the property that is specified, else
     *         null
     */
    public String getLoggingFile(){
        return mProps.getProperty("pegasus.log.*");
    }


    /**
     * Returns the location of the local log file where you want the messages to
     * be logged. Not used for the moment.
     *
     * Referred to by the "pegasus.log4j.log" property.
     *
     * @return the value specified in the property file,else null.
     */
    public String getLog4JLogFile() {
        return mProps.getProperty( "pegasus.log4j.log" );
    }


    /**
     * Return returns the environment string specified for the local pool. If
     * specified the registration jobs are set with these environment variables.
     *
     * Referred to by the "pegasus.local.env" property
     *
     * @return the environment string for local pool in properties file if
     *         defined, else null.
     */
    public String getLocalPoolEnvVar() {
        return mProps.getProperty( "pegasus.local.env" );
    }

    /**
     * Returns a boolean indicating whether to write out the planner metrics
     * or not.
     *
     * Referred to by the "pegasus.log.metrics" property.
     *
     * @return boolean in the properties, else true
     */
    public boolean writeOutMetrics(){
        return Boolean.parse( mProps.getProperty( "pegasus.log.metrics" ), true );
    }

    /**
     * Returns the path to the file that is used to be logging metrics
     *
     * Referred to by the "pegasus.log.metrics.file" property.
     *
     * @return path to the metrics file if specified, else $PEGASUS_HOME/var/pegasus.log
     */
    public String getMetricsLogFile(){
        String file = mProps.getProperty( "pegasus.log.metrics.file" );
        if( file == null || file.length() == 0 ){
            //construct the default path
            File dir = new File( this.getPegasusHome(), "var" );
            file = new File( dir, "pegasus.log" ).getAbsolutePath();
        }
        return file;
    }


    //SOME MISCELLANEOUS PROPERTIES


    /**
     * Returns a boolean indicating whether to have jobs executing on worker
     * node tmp or not.
     *
     * Referred to by the "pegasus.execute.*.filesystem.local" property.
     *
     * @return boolean value in the properties file, else false if not specified
     *         or an invalid value specified.
     */
    public boolean executeOnWorkerNode( ){
        return Boolean.parse( mProps.getProperty( "pegasus.execute.*.filesystem.local" ) ,
                              false  );
    }

    /**
     * Returns a boolean indicating whether to treat the entries in the cache
     * files as a replica catalog or not.
     *
     * @return boolean
     */
    public boolean treatCacheAsRC(){
        return Boolean.parse(mProps.getProperty( "pegasus.catalog.replica.cache.asrc" ),
                             false);
    }

    /**
     * Returns a boolean indicating whether to preserver line breaks.
     * 
     * Referred to by the "pegasus.parser.dax.preserve.linebreaks" property.
     * 
     * @return boolean value in the properties file, else false if not specified
     *         or an invalid value specified.
     */
    public boolean preserveParserLineBreaks( ){
        return Boolean.parse( mProps.getProperty( "pegasus.parser.dax.preserve.linebreaks" ),
                              false) ;
    }

    /**
     * Returns the path to the wings properties file.
     * 
     * Referred to by the "pegasus.wings.properties" property.
     * 
     * @return value in the properties file, else null.
     */
    public String getWingsPropertiesFile( ){
        return mProps.getProperty( "pegasus.wings.properties" ) ;
    }
    
    /**
     * Returns the request id.
     * 
     * Referred to by the "pegasus.wings.request-id" property.
     * 
     * @return value in the properties file, else null.
     */
    public String getWingsRequestID( ){
        return mProps.getProperty( "pegasus.wings.request.id" ) ;
    }
    
    /**
     * Returns the timeout value in seconds after which to timeout in case of
     * opening sockets to grid ftp server.
     *
     * Referred to by the "pegasus.auth.gridftp.timeout" property.
     *
     * @return the timeout value if specified else,
     *         null.
     *
     * @see #DEFAULT_SITE_SELECTOR_TIMEOUT
     */
    public String getGridFTPTimeout(){
        return mProps.getProperty("pegasus.auth.gridftp.timeout");
    }

    /**
     * Returns which submit mode to be used to submit the jobs on to the grid.
     *
     * Referred to by the "pegasus.submit" property.
     *
     * @return the submit mode specified in the property file,
     *         else the default i.e condor.
     */
    public String getSubmitMode() {
        return mProps.getProperty( "pegasus.submit", "condor" );
    }

    /**
     * Returns the mode for parsing the dax while writing out the partitioned
     * daxes.
     *
     * Referred to by the "pegasus.partition.parser.load" property.
     *
     * @return the value specified in the properties file, else
     *         the default value i.e single.
     */
    public String getPartitionParsingMode() {
        return mProps.getProperty( "pegasus.partition.parser.load", "single" );
    }

    /**
     * Returns the default priority that needs to be applied to all job.
     *
     * Referred to by the "pegasus.job.priority" property.
     *
     * @return the value specified in the properties file, null if a non
     *         integer value is passed.
     */
    public String getJobPriority(){
        String prop = mProps.getProperty( "pegasus.job.priority" );
        int val = -1;

        try {
            val = Integer.parseInt( prop );
        } catch ( Exception e ) {
            return null;
        }

        return ( val < 0 ) ? null : Integer.toString( val );

    }

    //JOB COLLAPSING PROPERTIES

    /**
     * Returns a comma separated list for the node collapsing criteria for the
     * execution pools. This determines how many jobs one fat node gobbles up.
     *
     * Referred to by the "pegasus.cluster.nodes" property.
     *
     * @return  the value specified in the properties file, else null.
     */
    public String getCollapseFactors() {
        return mProps.getProperty( "pegasus.clusterer.nodes" );
    }

    /**
     * Returns what job aggregator is to be used to aggregate multiple
     * compute jobs into a single condor job.
     *
     * Referred to by the "pegasus.cluster.job.aggregator" property.
     *
     * @return the value specified in the properties file, else
     *         DEFAULT_JOB_AGGREGATOR
     *
     * @see #DEFAULT_JOB_AGGREGATOR
     */
    public String getJobAggregator(){
        return mProps.getProperty("pegasus.clusterer.job.aggregator",DEFAULT_JOB_AGGREGATOR);
    }


    /**
     * Returns what job aggregator is to be used to aggregate multiple
     * compute jobs into a single condor job.
     *
     * Referred to by the "pegasus.cluster.job.aggregator.seqexec.log.global" property.
     *
     * @return the value specified in the properties file, else true
     *
     */
    public boolean jobAggregatorLogGlobal(){
        return Boolean.parse( mProps.getProperty( "pegasus.clusterer.job.aggregator.seqexec.hasgloballog" ),
                              true );
    }

    /**
     * Returns a boolean indicating whether seqexec trips on the first job failure.
     *
     * Referred to by the "pegasus.cluster.job.aggregator.seqexec.firstjobfail" property.
     *
     * @return the value specified in the properties file, else false
     *
     */
    public boolean abortOnFirstJobFailure(){
        return Boolean.parse( mProps.getProperty( "pegasus.clusterer.job.aggregator.seqexec.firstjobfail" ),
                              false );
    }



    //DEFERRED PLANNING PROPERTIES
    /**
     * Returns the DAXCallback that is to be used while parsing the DAX.
     *
     * Referred to by the "pegasus.parser.dax.callback" property.
     *
     * @return the value specified in the properties file, else
     *         DEFAULT_DAX_CALLBACK
     *
     * @see #DEFAULT_DAX_CALLBACK
     */
    public String getDAXCallback(){
        return mProps.getProperty("pegasus.parser.dax.callback",DEFAULT_DAX_CALLBACK);
    }

    /**
     * Returns the key that is to be used as a label key, for labelled
     * partitioning.
     *
     * Referred to by the "pegasus.partitioner.label.key" property.
     *
     * @return the value specified in the properties file.
     */
    public String getPartitionerLabelKey(){
        return mProps.getProperty( "pegasus.partitioner.label.key" );
    }

    /**
     * Returns the bundle value for a particular transformation.
     *
     * Referred to by the "pegasus.partitioner.horziontal.bundle.[txname]" property,
     * where [txname] is replaced by the name passed an input to this function.
     *
     * @param name  the logical name of the transformation.
     *
     * @return the path to the postscript if specified in properties file,
     *         else null.
     */
    public String getHorizontalPartitionerBundleValue( String name ){
        StringBuffer key = new StringBuffer();
        key.append( "pegasus.partitioner.horizontal.bundle." ).append( name );
        return mProps.getProperty( key.toString() );
    }

    /**
     * Returns the collapse value for a particular transformation.
     *
     * Referred to by the "pegasus.partitioner.horziontal.collapse.[txname]" property,
     * where [txname] is replaced by the name passed an input to this function.
     *
     * @param name  the logical name of the transformation.
     *
     * @return the path to the postscript if specified in properties file,
     *         else null.
     */
    public String getHorizontalPartitionerCollapseValue( String name ){
        StringBuffer key = new StringBuffer();
        key.append( "pegasus.partitioner.horizontal.collapse." ).append( name );
        return mProps.getProperty( key.toString() );
    }


    /**
     * Returns the key that is to be used as a label key, for labelled
     * clustering.
     *
     * Referred to by the "pegasus.clusterer.label.key" property.
     *
     * @return the value specified in the properties file.
     */
    public String getClustererLabelKey(){
        return mProps.getProperty( "pegasus.clusterer.label.key");
    }


    /**
     * Returns the path to the property file that has been writting out in
     * the submit directory.
     *
     * @return path to the property file
     *
     * @exception RuntimeException in case of file not being generated.
     */
    public String getPropertiesInSubmitDirectory( ){
        if ( mPropsInSubmitDir == null || mPropsInSubmitDir.length() == 0 ){
            throw new RuntimeException( "Properties file does not exist in directory " );
        }
        return mPropsInSubmitDir;
    }


    /**
     * Writes out the properties to a temporary file in the directory passed.
     *
     * @param directory   the directory in which the properties file needs to
     *                    be written to.
     *
     * @return the absolute path to the properties file written in the directory.
     *
     * @throws IOException in case of error while writing out file.
     */
    public String writeOutProperties( String directory ) throws IOException{
        File dir = new File(directory);

        //sanity check on the directory
        sanityCheck( dir );

        //we only want to write out the VDS properties for time being
        Properties properties = mProps.matchingSubset( "pegasus", true );

        //create a temporary file in directory
        File f = File.createTempFile( "pegasus.", ".properties", dir );

        //the header of the file
        StringBuffer header = new StringBuffer(64);
        header.append("Pegasus USER PROPERTIES AT RUNTIME \n")
              .append("#ESCAPES IN VALUES ARE INTRODUCED");

        //create an output stream to this file and write out the properties
        OutputStream os = new FileOutputStream(f);
        properties.store( os, header.toString() );
        os.close();

        //also set it to the internal variable
        mPropsInSubmitDir  = f.getAbsolutePath();
        return mPropsInSubmitDir;
    }


    /**
     * Checks the destination location for existence, if it can
     * be created, if it is writable etc.
     *
     * @param dir is the new base directory to optionally create.
     *
     * @throws IOException in case of error while writing out files.
     */
    protected static void sanityCheck( File dir ) throws IOException{
        if ( dir.exists() ) {
            // location exists
            if ( dir.isDirectory() ) {
                // ok, isa directory
                if ( dir.canWrite() ) {
                    // can write, all is well
                    return;
                } else {
                    // all is there, but I cannot write to dir
                    throw new IOException( "Cannot write to existing directory " +
                                           dir.getPath() );
                }
            } else {
                // exists but not a directory
                throw new IOException( "Destination " + dir.getPath() + " already " +
                                       "exists, but is not a directory." );
            }
        } else {
            // does not exist, try to make it
            if ( ! dir.mkdirs() ) {
                throw new IOException( "Unable to create directory destination " +
                                       dir.getPath() );
            }
        }
    }






    /**
     * Logs a warning about the deprecated property. Logs a warning only if
     * it has not been displayed before.
     *
     * @param deprecatedProperty   the deprecated property that needs to be
     *                             replaced.
     * @param newProperty          the new property that should be used.
     */
    private void logDeprecatedWarning(String deprecatedProperty,
                                      String newProperty){

        if(!mDeprecatedProperties.contains(deprecatedProperty)){
            //log only if it had already not been logged
            StringBuffer sb = new StringBuffer();
            sb.append( "The property " ).append( deprecatedProperty ).
                append( " has been deprecated. Use " ).append( newProperty ).
                append( " instead." );
            mLogger.log(sb.toString(),LogManager.WARNING_MESSAGE_LEVEL );

            //push the property in to indicate it has already been
            //warned about
            mDeprecatedProperties.add(deprecatedProperty);
        }
     }

     /**
      * Returns a boolean indicating whether to use third party transfers for
      * all types of transfers or not.
      *
      * Referred to by the "pegasus.transfer.*.thirdparty" property.
      *
      * @return the boolean value in the properties files,
      *         else false if no value specified, or non boolean specified.
      */
//     private boolean useThirdPartyForAll(){
//         return Boolean.parse("pegasus.transfer.*.thirdparty",
//                              false);
//     }


     /**
      * Returns the default list of third party sites.
      *
      * Referred to by the "pegasus.transfer.*.thirdparty.sites" property.
      *
      * @return the value specified in the properties file, else
      *         null.
      */
     private String getDefaultThirdPartySites(){
         return mProps.getProperty("pegasus.transfer.*.thirdparty.sites");
     }

     /**
      * Returns the default transfer implementation to be picked up for
      * constructing transfer jobs.
      *
      * Referred to by the "pegasus.transfer.*.impl" property.
      *
      * @return the value specified in the properties file, else
      *         null.
      */
     private String getDefaultTransferImplementation(){
         return mProps.getProperty("pegasus.transfer.*.impl");
     }

     /**
      * Returns the default priority for the transfer jobs if specified in
      * the properties file.
      *
      * @return the value specified in the properties file, else null if
      *         non integer value or no value specified.
      */
     private String getDefaultTransferPriority(){
         String prop = mProps.getProperty( this.ALL_TRANSFER_PRIORITY_PROPERTY);
         int val = -1;

         try {
             val = Integer.parseInt( prop );
         } catch ( Exception e ) {
             return null;
         }
         return  Integer.toString( val );
     }


     /**
      * Gets the reference to the internal singleton object. This method is
      * invoked with the assumption that the singleton method has been invoked once
      * and has been populated. Also that it has not been disposed by the garbage
      * collector. Can be potentially a buggy way to invoke.
      *
      * @return a handle to the Properties class.
      */
//    public static PegasusProperties singletonInstance() {
//        return singletonInstance( null );
//    }

     /**
      * Gets a reference to the internal singleton object.
      *
      * @param propFileName  name of the properties file to picked
      *                      from $PEGASUS_HOME/etc/ directory.
      *
      * @return a handle to the Properties class.
      */
//    public static PegasusProperties singletonInstance( String propFileName ) {
//        if ( pegProperties == null ) {
//            //only the default properties file
//            //can be picked up due to the way
//            //Singleton implemented in VDSProperties.???
//            pegProperties = new PegasusProperties( null );
//        }
//        return pegProperties;
//    }

}
