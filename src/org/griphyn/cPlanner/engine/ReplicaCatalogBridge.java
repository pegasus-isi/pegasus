/**
 *  Copyright 2007-2008 University Of Southern California
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.griphyn.cPlanner.engine;


import edu.isi.pegasus.common.logging.LoggingKeys;
import edu.isi.pegasus.planner.catalog.classes.Profiles;

import edu.isi.pegasus.planner.catalog.site.classes.GridGateway;
import edu.isi.pegasus.planner.catalog.site.classes.SiteCatalogEntry;

import org.griphyn.cPlanner.classes.FileTransfer;
import org.griphyn.cPlanner.classes.LRC;
import org.griphyn.cPlanner.classes.NameValue;
import org.griphyn.cPlanner.classes.SiteInfo;
import org.griphyn.cPlanner.classes.Profile;
import org.griphyn.cPlanner.classes.ReplicaLocation;
import org.griphyn.cPlanner.classes.ReplicaStore;
import org.griphyn.cPlanner.classes.ADag;
import org.griphyn.cPlanner.classes.SubInfo;
import org.griphyn.cPlanner.classes.PlannerOptions;

import edu.isi.pegasus.common.logging.LogManager;
import org.griphyn.cPlanner.common.PegasusProperties;

import org.griphyn.cPlanner.namespace.ENV;

import org.griphyn.common.catalog.ReplicaCatalog;
import org.griphyn.common.catalog.TransformationCatalogEntry;

import org.griphyn.common.catalog.replica.ReplicaFactory;



import org.griphyn.common.classes.TCType;

import org.griphyn.common.util.Separator;

import java.io.File;
import java.io.FileWriter;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.StringTokenizer;
import org.griphyn.cPlanner.classes.PegasusBag;

/**
 * This coordinates the look up to the Replica Location Service, to determine
 * the logical to physical mappings.
 *
 * @author Karan Vahi
 * @author Gaurang Mehta
 * @version $Revision$
 *
 */
public class ReplicaCatalogBridge
             extends Engine //for the time being.
             {

    /**
     * The transformation namespace for the regostration jobs.
     */
    public static final String RC_TRANSFORMATION_NS = "pegasus";

    /**
     * The logical name of the transformation used.
     */
    public static final String RC_TRANSFORMATION_NAME = "rc-client";

    /**
     * The logical name of the transformation used.
     */
    public static final String RC_TRANSFORMATION_VERSION = null;

    /**
     * The derivation namespace for the transfer jobs.
     */
    public static final String RC_DERIVATION_NS = "pegasus";

    /**
     * The derivation name for the transfer jobs.
     */
    public static final String RC_DERIVATION_NAME = "rc-client";


    /**
     * The version number for the derivations for registration jobs.
     */
    public static final String RC_DERIVATION_VERSION = "1.0";

    /**
     * The name of the Replica Catalog Implementer that serves as the source for
     * cache files.
     */
    public static final String CACHE_REPLICA_CATALOG_IMPLEMENTER = "SimpleFile";

    /**
     * The name of the source key for Replica Catalog Implementer that serves as
     * cache
     */
    public static final String CACHE_REPLICA_CATALOG_KEY = "file";


    /**
     * The name of the URL key for the replica catalog impelementer to be picked
     * up.
     */
    public static final String REPLICA_CATALOG_URL_KEY = "url";

    /**
     * The handle to the main Replica Catalog.
     */
    private ReplicaCatalog mReplicaCatalog;



    /**
     * The Vector of <code>String</code> objects containing the logical
     * filenames of the files whose locations are to be searched in the
     * Replica Catalog.
     */
    protected Set mSearchFiles ;


    /**
     * A boolean variable to desingnate whether the RLI queried was down or not.
     * By default it is up, unless it is set to true explicitly.
     */
    private boolean mRCDown;

    /**
     * The replica store in which we store all the results that are queried from
     * the main replica catalog.
     */
    private ReplicaStore mReplicaStore;

    /**
     * The replica store in which we store all the results that are queried from
     * the cache replica catalogs.
     */
    private ReplicaStore mCacheStore;


    /**
     * A boolean indicating whether the cache file needs to be treated as a
     * replica catalog or not.
     */
    private boolean mTreatCacheAsRC;

    /**
     * The namespace object holding the environment variables for local
     * pool.
     */
    private ENV mLocalEnv;

    /**
     * The default tc entry.
     */
    private TransformationCatalogEntry mDefaultTCRCEntry;

    /**
     * A boolean indicating whether the attempt to create a default tc entry
     * has happened or not.
     */
    private boolean mDefaultTCRCCreated;

    /**
     * The DAG being worked upon.
     */
    private ADag mDag;
    

    /**
     * The overloaded constructor.
     *
     * @param dag         the workflow that is being worked on.
     * @param properties  the properties passed to the planner.
     * @param options     the options passed to the planner at runtime.
     *
     *
     */
    public ReplicaCatalogBridge( ADag dag ,
//                                 PegasusProperties properties,
//                                 PlannerOptions options 
                                 PegasusBag bag ) {
        super( bag );
        this.initialize( dag, bag );
    }

    /**
     * Intialises the refiner.
     *
     * @param dag         the workflow that is being worked on.
     * @param bag         the bag of Pegasus initialization objects
     *
     */
    public void initialize( ADag dag ,
                            PegasusBag bag ){
        
        this.initialize( dag, bag.getPegasusProperties(), bag.getPlannerOptions() );

    }
    
    /**
     * Intialises the refiner.
     *
     * @param dag         the workflow that is being worked on.
     * @param properties  the properties passed to the planner.
     * @param options     the options passed to the planner at runtime.
     *
     */
    public void initialize( ADag dag ,
                            PegasusProperties properties,
                            PlannerOptions options ){
        mDag   = dag;
        mProps = properties;
        mPOptions = options;
        mRCDown = false;
        mCacheStore = new ReplicaStore();
        mTreatCacheAsRC = mProps.treatCacheAsRC();
        mDefaultTCRCCreated = false;

        //converting the Vector into vector of
        //strings just containing the logical
        //filenames
        mSearchFiles = dag.dagInfo.getLFNs( options.getForce() );

        //load the local environment variable
        //from pool config and property file
        mLocalEnv = loadLocalEnvVariables();

        //only for windward for time being
        properties.setProperty( "pegasus.catalog.replica.dax.id", dag.getAbstractWorkflowID() );
        properties.setProperty( "pegasus.catalog.replica.mrc.windward.dax.id", dag.getAbstractWorkflowID() );
        
        try {

            //make sure that RLS can be loaded from local environment
            //Karan May 1 2007
            mReplicaCatalog = null;
            if ( mSearchFiles != null && !mSearchFiles.isEmpty() ){
               mReplicaCatalog = ReplicaFactory.loadInstance(properties);

               //load all the mappings.
               mReplicaStore = new ReplicaStore( mReplicaCatalog.lookup( mSearchFiles ) );
            }

        } catch ( Exception ex ) {
            String msg = "Problem while connecting with the Replica Catalog: ";
            mLogger.log( msg + ex.getMessage(),LogManager.ERROR_MESSAGE_LEVEL );
            //set the flag to denote RLI is down
            mRCDown = true;
            //mReplicaStore = new ReplicaStore();

            //exit if there is no cache overloading specified.
            if ( options.getCacheFiles().isEmpty() ) {
                throw new RuntimeException( msg , ex );
            }
        }
        finally{
            //set replica store to an empty store if required
            mReplicaStore = ( mReplicaStore == null ) ?new ReplicaStore() : mReplicaStore;
        }



//        mTCHandle = TCMode.loadInstance();

        //incorporate the caching if any
        if ( !options.getCacheFiles().isEmpty() ) {
            loadCacheFiles( options.getCacheFiles() );
        }
    }


    /**
     * To close the connection to replica services. This must be defined in the
     * case where one has not done a singleton implementation. In other
     * cases just do an empty implementation of this method.
     */
    public void closeConnection() {
        if ( mReplicaCatalog != null ) {
            mReplicaCatalog.close();
        }
    }

    /**
     * Closes the connection to the rli.
     */
    public void finalize() {
        this.closeConnection();
    }


    /**
     * This returns the files for which mappings exist in the Replica Catalog.
     * This should return a subset of the files which are
     * specified in the mSearchFiles, while getting an instance to this.
     *
     * @return  a <code>Set</code> of logical file names as String objects, for
     *          which logical to physical mapping exists.
     *
     * @see #mSearchFiles
     */
    public Set getFilesInReplica() {

        //check if any exist in the cache
        Set lfnsFound = mCacheStore.getLFNs( mSearchFiles );
        mLogger.log(lfnsFound.size()  + " entries found in cache of total " +
                    mSearchFiles.size(),
                    LogManager.DEBUG_MESSAGE_LEVEL);

        //check in the main replica catalog
        if ( mRCDown || mReplicaCatalog == null ) {
            mLogger.log("Replica Catalog is either down or connection to it was never opened ",
                        LogManager.WARNING_MESSAGE_LEVEL);
            return lfnsFound;
        }

        //look up from the the main replica catalog
        lfnsFound.addAll( mReplicaStore.getLFNs() );


        return lfnsFound;

    }




    /**
     * Returns all the locations as returned from the Replica Lookup Mechanism.
     *
     * @param lfn   The name of the logical file whose PFN mappings are
     *                      required.
     *
     * @return ReplicaLocation containing all the locations for that LFN
     *
     * @see org.griphyn.cPlanner.classes.ReplicaLocation
     */
    public ReplicaLocation getFileLocs( String lfn ) {

        ReplicaLocation rl = retrieveFromCache( lfn );
        //first check from cache
        if(rl != null && !mTreatCacheAsRC){
            mLogger.log( "Location of file " + rl +
                         " retrieved from cache" , LogManager.DEBUG_MESSAGE_LEVEL);
            return rl;
        }

        ReplicaLocation rcEntry = mReplicaStore.getReplicaLocation( lfn );
        if (rl == null) {
            rl = rcEntry;
        }
        else{
            //merge with the ones found in cache
            rl.merge(rcEntry);
        }


        return rl;
    }






    /**
     * Returns a boolean indicating whether all input files of the workflow
     * are in the collection of LFNs passed.
     *
     * @param lfns  collection of LFNs in which to search for existence.
     *
     * @return boolean.
     */
     /*
    public boolean allIPFilesInCollection( Collection lfns ){
        boolean result = true;
        String lfn;
        String type;
        for (Iterator it = mLFNMap.keySet().iterator(); it.hasNext(); ) {
            lfn = (String) it.next();
            type = (String) mLFNMap.get( lfn );

            //search for existence of input file in lfns
            if ( type.equals("i") && !lfns.contains( lfn ) ) {
                mLogger.log("Input LFN not found in collection " + lfn,
                            LogManager.DEBUG_MESSAGE_LEVEL);
                return false;
            }
        }
        return result;
    }
    */

    /**
     * It constructs the SubInfo object for the registration node, which
     * registers the materialized files on the output pool in the RLS.
     * Note that the relations corresponding to this node should already have
     * been added to the concerned <code>DagInfo</code> object.
     *
     * @param regJobName  The name of the job which registers the files in the
     *                    Replica Location Service.
     * @param job         The job whose output files are to be registered in
     *                    the Replica Location Service.
     *
     * @param files       Collection of <code>FileTransfer</code> objects
     *                    containing the information about source and
     *                    destination URLs. The destination
     *                    URLs would be our PFNs.
     *
     * @return SubInfo corresponding to the new registration node.
     */
    public  SubInfo makeRCRegNode( String regJobName, SubInfo job,
                                  Collection files ) {
        //making the files string

        SubInfo newJob = new SubInfo();

        newJob.setName( regJobName );
        newJob.setTransformation( this.RC_TRANSFORMATION_NS,
                                  this.RC_TRANSFORMATION_NAME,
                                  this.RC_TRANSFORMATION_VERSION );
        newJob.setDerivation( this.RC_DERIVATION_NS,
                              this.RC_DERIVATION_NAME,
                              this.RC_DERIVATION_VERSION );

//        SiteInfo site = mPoolHandle.getPoolEntry( mOutputPool, "vanilla" );
        SiteCatalogEntry site = mSiteStore.lookup( mOutputPool );

        //change this function
        List tcentries = null;
        try {
            tcentries = mTCHandle.getTCEntries( newJob.getTXNamespace(),
                                                newJob.getTXName(),
                                                newJob.getTXVersion(),
                                                "local",
                                                TCType.INSTALLED );

        } catch ( Exception e ) {
            mLogger.log( "While retrieving entries from TC " + e.getMessage(),
                         LogManager.ERROR_MESSAGE_LEVEL);
        }

        TransformationCatalogEntry tc;

        if ( tcentries == null || tcentries.isEmpty() ) {

            mLogger.log( "Unable to find in entry for " + newJob.getCompleteTCName() +  " in transformation catalog on site local",
                         LogManager.DEBUG_MESSAGE_LEVEL );
            mLogger.log( "Constructing a default entry for it " , LogManager.DEBUG_MESSAGE_LEVEL );
            tc = defaultTCRCEntry(  );

            if( tc == null ){
                throw new RuntimeException( "Unable to create an entry for " +
                                            newJob.getCompleteTCName() +  " on site local");
            }
        }
        else{
            tc = (TransformationCatalogEntry) tcentries.get(0);
        }
        newJob.setRemoteExecutable( tc.getPhysicalTransformation() );
        newJob.setArguments( this.generateRepJobArgumentString( site, regJobName, files ) );
//        newJob.setUniverse( Engine.REGISTRATION_UNIVERSE );
        newJob.setUniverse( GridGateway.JOB_TYPE.register.toString() );
        newJob.setSiteHandle( tc.getResourceId() );
        newJob.setJobType( SubInfo.REPLICA_REG_JOB );
        newJob.setVDSSuperNode( job.getName() );

        //the profile information from the pool catalog needs to be
        //assimilated into the job.
//        newJob.updateProfiles( mPoolHandle.getPoolProfile( newJob.getSiteHandle() ) );
        newJob.updateProfiles( mSiteStore.lookup( newJob.getSiteHandle() ).getProfiles() );

        //the profile information from the transformation
        //catalog needs to be assimilated into the job
        //overriding the one from pool catalog.
        newJob.updateProfiles( tc );

        //the profile information from the properties file
        //is assimilated overidding the one from transformation
        //catalog.
        newJob.updateProfiles( mProps );

        //in order to make sure that COG picks the default proxy
        //correctly through condor
        newJob.condorVariables.construct( "getenv", "true" );

        return newJob;
    }

    /**
     * Returns a default TC entry to be used in case entry is not found in the
     * transformation catalog.
     *
     *
     *
     * @return  the default entry.
     */
    private  TransformationCatalogEntry defaultTCRCEntry( ){
        String site = "local";
        //generate only once.
        if( !mDefaultTCRCCreated ){
            //check if PEGASUS_HOME is set
            String home = mProps.getPegasusHome(  );
            //if PEGASUS_HOME is not set, use VDS_HOME
            //home = ( home == null )? mPoolHandle.getVDS_HOME( site ): home;

            //if home is still null
            if ( home == null ){
                //cannot create default TC
                mLogger.log( "Unable to create a default entry for " +
                             Separator.combine( this.RC_TRANSFORMATION_NS,
                                                this.RC_TRANSFORMATION_NAME,
                                                this.RC_TRANSFORMATION_VERSION ),
                             LogManager.DEBUG_MESSAGE_LEVEL );
                //set the flag back to true
                mDefaultTCRCCreated = true;
                return mDefaultTCRCEntry;
            }
            //remove trailing / if specified
            home = ( home.charAt( home.length() - 1 ) == File.separatorChar )?
                     home.substring( 0, home.length() - 1 ):
                     home;

            //construct the path to it
            StringBuffer path = new StringBuffer();
            path.append( home ).append( File.separator ).
                 append( "bin" ).append( File.separator ).
                 append( "rc-client" );

            //create Profiles for JAVA_HOME and CLASSPATH
            String jh = mProps.getProperty( "java.home" );
            mLogger.log( "JAVA_HOME set to " + jh,
                         LogManager.DEBUG_MESSAGE_LEVEL );
            Profile javaHome = new Profile( Profile.ENV, "JAVA_HOME", jh );

            Profile classpath = this.getClassPath( home );
            if( classpath == null ){ return mDefaultTCRCEntry ; }

            mDefaultTCRCEntry = new TransformationCatalogEntry( this.RC_TRANSFORMATION_NS,
                                                                this.RC_TRANSFORMATION_NAME,
                                                                this.RC_TRANSFORMATION_VERSION );

            mDefaultTCRCEntry.setPhysicalTransformation( path.toString() );
            mDefaultTCRCEntry.setResourceId( site );
            mDefaultTCRCEntry.setProfile( classpath );
            mDefaultTCRCEntry.setProfile( javaHome );
            mDefaultTCRCEntry.setProfile( new Profile( Profile.ENV,
                                                       "PEGASUS_HOME",
                                                       mProps.getPegasusHome() ));
            //set the flag back to true
            mDefaultTCRCCreated = true;
        }
        return mDefaultTCRCEntry;
    }


    /**
     * Returns the classpath for the default rc-client entry.
     *
     * @param home   the home directory where we need to check for lib directory.
     *
     * @return the classpath in an environment profile.
     */
    private Profile getClassPath( String home ){
        Profile result = null ;

        //create the CLASSPATH from home
        String classpath = mProps.getProperty( "java.class.path" );
        if( classpath == null || classpath.trim().length() == 0 ){
            return result;
        }

        mLogger.log( "JAVA CLASSPATH SET IS " + classpath , LogManager.DEBUG_MESSAGE_LEVEL );

        StringBuffer cp = new StringBuffer();
        String prefix = home + File.separator + "lib";
        for( StringTokenizer st = new StringTokenizer( classpath, ":" ); st.hasMoreTokens(); ){
            String token = st.nextToken();
            if( token.startsWith( prefix ) ){
                //this is a valid lib jar to put in
                cp.append( token ).append( ":" );
            }
        }

        if ( cp.length() == 0 ){
            //unable to create a valid classpath
            mLogger.log( "Unable to create a sensible classpath from " + home,
                         LogManager.DEBUG_MESSAGE_LEVEL );
            return result;
        }

        //we have everything now
        result = new Profile( Profile.ENV, "CLASSPATH", cp.toString() );

        return result;
    }


    /**
     * Generates the argument string to be given to the replica registration job.
     * At present by default it would be picking up the file containing the
     * mappings.
     *
     * @param site     the <code>SiteCatalogEntry</code> object
     * @param regJob   The name of the registration job.
     *
     * @param files Collection of <code>FileTransfer</code> objects containing the
     *                 information about source and destURLs. The destination
     *                 URLs would be our PFNs.
     *
     * @return the argument string.
     */
    private String generateRepJobArgumentString( SiteCatalogEntry site, String regJob, Collection files ) {
        StringBuffer arguments = new StringBuffer();

        //select a LRC. disconnect here. It should be select a RC.

        edu.isi.pegasus.planner.catalog.site.classes.ReplicaCatalog rc =
                                (site == null) ? null : site.selectReplicaCatalog();
        if ( rc  == null || rc.getURL() == null || rc.getURL().length() == 0) {
            throw new RuntimeException(
                "The Replica Catalog URL is not specified in site catalog for site " + mOutputPool );
        }

        //get any command line properties that may need specifying
        arguments.append( this.getCommandLineProperties( mProps ) );

        //we have a lrc selected . construct vds.rc.url property
        arguments.append( "-D" ).append( ReplicaCatalog.c_prefix ).append( "." ).
                  append( this.REPLICA_CATALOG_URL_KEY).append( "=" ).append( rc.getURL() ).
                  append( " " );

        //append the insert option
        arguments.append( "--insert" ).append( " " ).
                  append( this.generateMappingsFile( regJob, files ) );

        return arguments.toString();

    }

    /**
     * Returns the properties that need to be passed to the the rc-client
     * invocation on the command line . It is of the form
     * "-Dprop1=value1 -Dprop2=value2 .."
     *
     * @param properties   the properties object
     *
     * @return the properties list, else empty string.
     */
    protected String getCommandLineProperties( PegasusProperties properties ){
        StringBuffer sb = new StringBuffer();
        appendProperty( sb,
                        "pegasus.user.properties",
                        properties.getPropertiesInSubmitDirectory( ));
        return sb.toString();
    }

    /**
     * Appends a property to the StringBuffer, in the java command line format.
     *
     * @param sb    the StringBuffer to append the property to.
     * @param key   the property.
     * @param value the property value.
     */
    protected void appendProperty( StringBuffer sb, String key, String value ){
        sb.append("-D").append( key ).append( "=" ).append( value ).append( " ");
    }


    /**
     * Generates the registration mappings in a text file that is generated in the
     * dax directory (the directory where all the condor submit files are
     * generated). The pool value for the mapping is the output pool specified
     * by the user when running Pegasus. The name of the file is regJob+.in
     *
     * @param regJob   The name of the registration job.
     * @param files    Collection of <code>FileTransfer</code>objects containing the
     *                 information about source and destURLs. The destination
     *                 URLs would be our PFNs.
     *
     * @return String corresponding to the path of the the file containig the
     *                mappings in the appropriate format.
     */
    private String generateMappingsFile( String regJob, Collection files ) {
        String fileName = regJob + ".in";
        File f = null;
        String submitFileDir = mPOptions.getSubmitDirectory();

        //writing the stdin file
        try {
            f = new File( submitFileDir, fileName );
            FileWriter stdIn = new FileWriter( f );

            for(Iterator it = files.iterator();it.hasNext();){
                FileTransfer ft = ( FileTransfer ) it.next();
                //checking for transient flag
                if ( !ft.getTransientRegFlag() ) {
                    stdIn.write( ftToRC( ft ) );
                    stdIn.flush();
                }
            }

            stdIn.close();

        } catch ( Exception e ) {
            throw new RuntimeException(
                "While writing out the registration file for job " + regJob, e );
        }

        return fileName;
    }


    /**
     * Converts a <code>FileTransfer</code> to a RC compatible string representation.
     *
     * @param ft  the <code>FileTransfer</code> object
     *
     * @return the RC version.
     */
    private String ftToRC( FileTransfer ft ){
        StringBuffer sb = new StringBuffer();
        NameValue destURL = ft.getDestURL();
        sb.append( ft.getLFN() ).append( " " );
        sb.append( destURL.getValue()  ).append( " " );
        sb.append( "pool=\"" ).append( destURL.getKey() ).append( "\"" );
        sb.append( "\n" );
        return sb.toString();
    }




    /**
     * Retrieves a location from the cache table, that contains the contents
     * of the cache files specified at runtime.
     *
     * @param lfn  the logical name of the file.
     *
     * @return <code>ReplicaLocation</code> object corresponding to the entry
     *         if found, else null.
     */
    private ReplicaLocation retrieveFromCache( String lfn ){
        return mCacheStore.getReplicaLocation( lfn );
    }

    /**
     * Ends up loading the cache files so as to enable the lookup for the transient
     * files created by the parent jobs.
     *
     * @param cacheFiles  set of paths to the cache files.
     */
    private void loadCacheFiles( Set cacheFiles ) {
        Properties cacheProps = mProps.getVDSProperties().matchingSubset(
                                                              ReplicaCatalog.c_prefix,
                                                              false );

        mLogger.logEventStart( LoggingKeys.EVENT_PEGASUS_LOAD_TRANSIENT_CACHE, 
                               LoggingKeys.DAX_ID,
                               mDag.getAbstractWorkflowID() );

        ReplicaCatalog simpleFile;
        Map wildcardConstraint = null;

        for ( Iterator it = cacheFiles.iterator(); it.hasNext() ; ) {
            //read each of the cache file and load in memory
            String  file = ( String ) it.next();
            //set the appropriate property to designate path to file
            cacheProps.setProperty( this.CACHE_REPLICA_CATALOG_KEY, file );

            mLogger.log("Loading cache file: " + file,  LogManager.DEBUG_MESSAGE_LEVEL);
            try{
                simpleFile = ReplicaFactory.loadInstance( CACHE_REPLICA_CATALOG_IMPLEMENTER,
                                                          cacheProps );
            }
            catch( Exception e ){
                mLogger.log( "Unable to load cache file " + file,
                             e,
                             LogManager.ERROR_MESSAGE_LEVEL );
                continue;
            }
            //suck in all the entries into the cache replica store.
            //returns an unmodifiable collection. so merging an issue..
            mCacheStore.add( simpleFile.lookup( mSearchFiles ) );

            //no wildcards as we only want to load mappings for files that
            //we require
            //mCacheStore.add( simpleFile.lookup( wildcardConstraint ) );

            //close connection
            simpleFile.close();
        }

        mLogger.logEventCompletion();
    }

    /**
     * Reads in the environment variables into memory from the properties file
     * and the pool catalog.
     *
     * @return  the <code>ENV</code> namespace object holding the environment
     *          variables.
     */
    private ENV loadLocalEnvVariables() {
        //assumes that pool handle, and property handle are initialized.
        ENV env = new ENV();

        //load from the pool.config
//        env.checkKeyInNS( mPoolHandle.getPoolProfile( "local", Profile.ENV ) );
        SiteCatalogEntry local = mSiteStore.lookup( "local" );        
        env.checkKeyInNS( local.getProfiles().get( Profiles.NAMESPACES.env ) );
        //load from property file
        env.checkKeyInNS( mProps.getLocalPoolEnvVar() );

        // the new RC API has a different key. if that is specified use that.
        //mProps.getProperty( ReplicaCatalog.c_prefix )

        return env;
    }



}
