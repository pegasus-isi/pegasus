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


package edu.isi.pegasus.planner.refiner.createdir;

import edu.isi.pegasus.planner.catalog.site.classes.SiteCatalogEntry;
import edu.isi.pegasus.planner.catalog.site.classes.SiteStore;


import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.classes.PegasusBag;

import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.planner.common.PegasusProperties;


import edu.isi.pegasus.planner.catalog.TransformationCatalog;
import edu.isi.pegasus.planner.catalog.transformation.TransformationCatalogEntry;

import edu.isi.pegasus.planner.catalog.transformation.classes.TCType;

import edu.isi.pegasus.common.util.Separator;

import edu.isi.pegasus.planner.code.gridstart.PegasusExitCode;
import edu.isi.pegasus.planner.namespace.Dagman;
import edu.isi.pegasus.planner.namespace.Pegasus;
import java.io.File;

import java.util.List;


/**
 * The default implementation for creating create dir jobs.
 * 
 * @author  Karan Vahi
 * @version $Revision$
 */
public class DefaultImplementation implements Implementation {

    /**
     * The scheme name for file url.
     */
    public static final String FILE_URL_SCHEME = "file:";

    /**
     * The transformation namespace for the create dir jobs.
     */
    public static final String TRANSFORMATION_NAMESPACE = "pegasus";

    /**
     * The logical name of the transformation that creates directories on the
     * remote execution pools.
     */
    public static final String TRANSFORMATION_NAME = "dirmanager";

    /**
     * The version number for the derivations for create dir  jobs.
     */
    public static final String TRANSFORMATION_VERSION = null;

    /**
     * The basename of the pegasus cleanup executable.
     */
    public static final String EXECUTABLE_BASENAME = "pegasus-create-dir";
    
    /**
     * The path to be set for create dir jobs.
     */
    public static final String PATH_VALUE = ".:/bin:/usr/bin:/usr/ucb/bin";

    /**
     * The arguments for pegasus-exitcode when you only want the log files to be rotated.
     */
    public static final String POSTSCRIPT_ARGUMENTS_FOR_ONLY_ROTATING_LOG_FILE = "-r $RETURN";

    /**
     * The complete TC name for kickstart.
     */
    public static final String COMPLETE_TRANSFORMATION_NAME = Separator.combine(
                                                                 TRANSFORMATION_NAMESPACE,
                                                                 TRANSFORMATION_NAME,
                                                                 TRANSFORMATION_VERSION  );


    /**
     * The derivation namespace for the create dir  jobs.
     */
    public static final String DERIVATION_NAMESPACE = "pegasus";

    /**
     * The logical name of the transformation that creates directories on the
     * remote execution pools.
     */
    public static final String DERIVATION_NAME = "dirmanager";


    /**
     * The version number for the derivations for create dir  jobs.
     */
    public static final String DERIVATION_VERSION = "1.0";
    
    /**
     * The handle to the transformation catalog.
     */
    protected TransformationCatalog mTCHandle;
    
    /**
     * The handle to the SiteStore.
     */
    protected SiteStore mSiteStore;
    
    /**
     * The handle to the logging object.
     */
    protected LogManager mLogger;
    
    /**
     * The handle to the pegasus properties.
     */
    protected PegasusProperties mProps;
    
    /**
     * Whether we want to use dirmanager or mkdir directly.
     */
    protected boolean mUseMkdir;
    
    /**
     * Intializes the class.
     *
     * @param bag      bag of initialization objects
     */
    public void initialize( PegasusBag bag ) {
        mLogger    = bag.getLogger();
        mSiteStore = bag.getHandleToSiteStore();
        mTCHandle  = bag.getHandleToTransformationCatalog();
        mProps     = bag.getPegasusProperties();
        //in case of staging of executables/worker package
        //we use mkdir directly
        mUseMkdir =  bag.getPegasusProperties().transferWorkerPackage();
    }
    
    /**
     * It creates a make directoryURL job that creates a directoryURL on the remote pool
     * using the perl executable that Gaurang wrote. It access mkdir underneath.
     * 
     *
     * @param site  the  site for which the create dir job is to be  created.
     * @param name  the name that is to be assigned to the job.
     * @param directoryURL   the externally accessible URL to the directoryURL that is
     *              created
     *
     * @return create dir job.
     */
    public Job makeCreateDirJob( String site, String name, String directoryURL ) {
        Job newJob  = new Job();
        List entries    = null;
        String execPath = null;
        TransformationCatalogEntry entry   = null;


        //associate a credential if required
        newJob.addCredentialType( directoryURL );

        //figure out on the basis of directory URL
        //where to run the job.
        String eSite = getCreateDirJobExecutionSite( site, directoryURL );

        try {
            entries = mTCHandle.lookup( DefaultImplementation.TRANSFORMATION_NAMESPACE,
                                              DefaultImplementation.TRANSFORMATION_NAME,
                                              DefaultImplementation.TRANSFORMATION_VERSION,
                                              eSite,
                                              TCType.INSTALLED);
        }
        catch (Exception e) {
            //non sensical catching
            mLogger.log("Unable to retrieve entries from TC " +
                        e.getMessage(), LogManager.DEBUG_MESSAGE_LEVEL );
        }

        entry = ( entries == null ) ?
            this.defaultTCEntry( eSite ): //try using a default one
            (TransformationCatalogEntry) entries.get(0);

        if( entry == null ){
            //NOW THROWN AN EXCEPTION

            //should throw a TC specific exception
            StringBuffer error = new StringBuffer();
            error.append("Could not find entry in tc for lfn ").
                append( COMPLETE_TRANSFORMATION_NAME ).
                append(" at site ").append( eSite );

            mLogger.log( error.toString(), LogManager.ERROR_MESSAGE_LEVEL);
            throw new RuntimeException( error.toString() );
        }



        SiteCatalogEntry ePool = mSiteStore.lookup( eSite );


        String argString = null;
       
        if( mUseMkdir ){
            
            //no gridstart but arguments to exitcode to add -r $RETURN
            newJob.vdsNS.construct( Pegasus.GRIDSTART_KEY, "None" );
            newJob.dagmanVariables.construct( Dagman.POST_SCRIPT_KEY, 
                                              PegasusExitCode.SHORT_NAME );
            newJob.dagmanVariables.construct( Dagman.POST_SCRIPT_ARGUMENTS_KEY, 
                                              DefaultImplementation.POSTSCRIPT_ARGUMENTS_FOR_ONLY_ROTATING_LOG_FILE );


            StringBuffer sb = new StringBuffer();
            sb.append( mProps.getBinDir() ).
               append( File.separator ).append( DefaultImplementation.EXECUTABLE_BASENAME );
            execPath = sb.toString();

            argString = "-u " +
                        mSiteStore.getExternalWorkDirectoryURL( site );
            
            newJob.condorVariables.setExecutableForTransfer();
            
        }
        else{
            execPath = entry.getPhysicalTransformation();
            argString = "-u " +
                        directoryURL;
        }

        newJob.jobName = name;
        newJob.setTransformation( DefaultImplementation.TRANSFORMATION_NAMESPACE,
                                  DefaultImplementation.TRANSFORMATION_NAME,
                                  DefaultImplementation.TRANSFORMATION_VERSION );
        newJob.setDerivation( DefaultImplementation.DERIVATION_NAMESPACE,
                              DefaultImplementation.DERIVATION_NAME,
                              DefaultImplementation.DERIVATION_VERSION );

        newJob.executable = execPath;
        newJob.executionPool = eSite;
        newJob.strargs = argString;
        newJob.jobClass = Job.CREATE_DIR_JOB;
        newJob.jobID = name;

        //the profile information from the pool catalog needs to be
        //assimilated into the job.
        newJob.updateProfiles( ePool.getProfiles() );

        //add any notifications specified in the transformation
        //catalog for the job. JIRA PM-391
        if( entry != null ){
            newJob.addNotifications( entry );
        }
        //the profile information from the transformation
        //catalog needs to be assimilated into the job
        //overriding the one from pool catalog.
        newJob.updateProfiles(entry);

        //the profile information from the properties file
        //is assimilated overidding the one from transformation
        //catalog.
        newJob.updateProfiles( mProps );

        return newJob;

    }


    /**
     * Returns a default TC entry to be used in case entry is not found in the
     * transformation catalog.
     *
     * @param site   the site for which the default entry is required.
     *
     *
     * @return  the default entry.
     */
    private  TransformationCatalogEntry defaultTCEntry( String site ){
        TransformationCatalogEntry defaultTCEntry = null;
        //check if PEGASUS_HOME is set
        String home = mSiteStore.getPegasusHome( site );

        //if PEGASUS_HOME is not set, use VDS_HOME
        //home = ( home == null )? mSiteStore.getVDSHome( site ): home;

        mLogger.log( "Creating a default TC entry for " +
                     COMPLETE_TRANSFORMATION_NAME +
                     " at site " + site,
                     LogManager.DEBUG_MESSAGE_LEVEL );

        //if home is still null
        if ( home == null ){
            //cannot create default TC
            mLogger.log( "Unable to create a default entry for " +
                         COMPLETE_TRANSFORMATION_NAME,
                         LogManager.DEBUG_MESSAGE_LEVEL );
            //set the flag back to true
            return defaultTCEntry;
        }

        //remove trailing / if specified
        home = ( home.charAt( home.length() - 1 ) == File.separatorChar )?
            home.substring( 0, home.length() - 1 ):
            home;

        //construct the path to it
        StringBuffer path = new StringBuffer();
        path.append( home ).append( File.separator ).
            append( "bin" ).append( File.separator ).
            append( DefaultImplementation.EXECUTABLE_BASENAME );


        defaultTCEntry = new TransformationCatalogEntry( DefaultImplementation.TRANSFORMATION_NAMESPACE,
                                                         DefaultImplementation.TRANSFORMATION_NAME,
                                                         DefaultImplementation.TRANSFORMATION_VERSION );

        defaultTCEntry.setPhysicalTransformation( path.toString() );
        defaultTCEntry.setResourceId( site );
        defaultTCEntry.setType( TCType.INSTALLED );
        defaultTCEntry.setSysInfo( this.mSiteStore.lookup( site ).getSysInfo() );

        //register back into the transformation catalog
        //so that we do not need to worry about creating it again
        try{
            mTCHandle.insert( defaultTCEntry , false );
        }
        catch( Exception e ){
            //just log as debug. as this is more of a performance improvement
            //than anything else
            mLogger.log( "Unable to register in the TC the default entry " +
                          defaultTCEntry.getLogicalTransformation() +
                          " for site " + site, e,
                          LogManager.DEBUG_MESSAGE_LEVEL );
        }

        return defaultTCEntry;

    }

    /**
     * Determines the site where the create dir job should be run , looking at the
     * directory URL passed. Preference is given to local site unless the directoryURL
     * is a file URL. In that case, the create dir job is executed on the site
     * where the directory is to be created.
     *
     * @param site            the site where the directory is to be created
     * @param directoryURL    the URL to the directory.
     *
     * @return  the site for create dir job
     */
    protected String getCreateDirJobExecutionSite( String site, String directoryURL ) {

        String result = "local";

        if( directoryURL != null && directoryURL.startsWith( this.FILE_URL_SCHEME ) ){
            result = site;
        }

        return result;

    }



}
