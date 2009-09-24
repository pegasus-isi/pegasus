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


package org.griphyn.cPlanner.engine.createdir;

import edu.isi.pegasus.planner.catalog.site.classes.GridGateway;
import edu.isi.pegasus.planner.catalog.site.classes.SiteCatalogEntry;
import edu.isi.pegasus.planner.catalog.site.classes.SiteStore;


import org.griphyn.cPlanner.classes.SubInfo;
import org.griphyn.cPlanner.classes.PegasusBag;

import edu.isi.pegasus.common.logging.LogManager;
import org.griphyn.cPlanner.common.PegasusProperties;

import org.griphyn.cPlanner.namespace.VDS;

import org.griphyn.common.catalog.TransformationCatalog;
import org.griphyn.common.catalog.TransformationCatalogEntry;

import org.griphyn.common.classes.TCType;

import org.griphyn.common.util.Separator;

import java.io.File;

import java.util.List;


/**
 * The default implementation for creating create dir jobs.
 * 
 * @author  Karan Vahi
 * @version $Revision$
 */
public class S3 implements Implementation {

    /**
     * The transformation namespace for the amazon bucket creation jobs.
     */
    public static final String TRANSFORMATION_NAMESPACE = "amazon";

    /**
     * The logical name of the transformation that creates buckets on the
     * amazon S3Strategy.
     */
    public static final String TRANSFORMATION_NAME = "s3cmd";

    /**
     * The version number for the derivations for create dir  jobs.
     */
    public static final String TRANSFORMATION_VERSION = null;


    /**
     * The complete TC name for the amazon s3cmd.
     */
    public static final String COMPLETE_TRANSFORMATION_NAME = Separator.combine(
                                                                 TRANSFORMATION_NAMESPACE,
                                                                 TRANSFORMATION_NAME,
                                                                 TRANSFORMATION_VERSION  );

    /**
     * The derivation namespace for the amazon bucket creation jobs.
     */
    public static final String DERIVATION_NAMESPACE = "amazon";

    /**
     * The logical name of the transformation that creates buckets on the
     * amazon S3Strategy.
     */
    public static final String DERIVATION_NAME = "s3cmd";


    /**
     * The version number for the derivations for create dir  jobs.
     */
    public static final String DERIVATION_VERSION = "1.0";
    
    /**
     * The path to be set for create dir jobs.
     */
    public static final String PATH_VALUE = ".:/bin:/usr/bin:/usr/ucb/bin";

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
     * The name of the bucket that is created.
     */
    protected String mBucketName;
    
    /**
     * Intializes the class.
     *
     * @param bag      bag of initialization objects
     */
    public void initialize( PegasusBag bag ) {
        mLogger     = bag.getLogger();
        mSiteStore  = bag.getHandleToSiteStore();
        mTCHandle   = bag.getHandleToTransformationCatalog();
        mProps      = bag.getPegasusProperties();
        mBucketName = bag.getPlannerOptions().getRelativeSubmitDirectory();

        //replace file separators in directory with -
        mBucketName = mBucketName.replace( File.separatorChar,  '-' );
        
        //in case of staging of executables/worker package
        //we use mkdir directly
        mUseMkdir = bag.getHandleToTransformationMapper().isStageableMapper();
    }
    
    /**
     * Return the s3 accessible URL for the bucket created by this implementation.
     * 
     * @return
     */
    public String getBucketNameURL(){
        StringBuffer url = new StringBuffer();
        url.append( "s3://" ).append( mBucketName );
        return url.toString();
    }
    
    /**
     * It creates a make directory job that creates a directory on the remote pool
     * using the perl executable that Gaurang wrote. It access mkdir underneath.
     * 
     *
     * @param site  the execution site for which the create dir job is to be
     *                  created.
     * @param name  the name that is to be assigned to the job.     * 
     * @param directory  the directory to be created on the site.
     *
     * @return create dir job.
     */
    public SubInfo makeCreateDirJob( String site, String name, String directory ) {
        SubInfo newJob  = new SubInfo();
        List entries    = null;
        String execPath = null;
        TransformationCatalogEntry entry   = null;
//        JobManager jobManager = null;
        GridGateway jobManager = null;

        try {
            entries = mTCHandle.getTCEntries( S3.TRANSFORMATION_NAMESPACE,
                                              S3.TRANSFORMATION_NAME,
                                              S3.TRANSFORMATION_VERSION,
                                              site, 
                                              TCType.INSTALLED);
        }
        catch (Exception e) {
            //non sensical catching
            mLogger.log("Unable to retrieve entries from TC " +
                        e.getMessage(), LogManager.DEBUG_MESSAGE_LEVEL );
        }

        entry = (entries == null )? null: (TransformationCatalogEntry) entries.get(0);

        if( entry == null ){
            //NOW THROWN AN EXCEPTION

            //should throw a TC specific exception
            StringBuffer error = new StringBuffer();
            error.append("Could not find entry in tc for lfn ").
                append( COMPLETE_TRANSFORMATION_NAME ).
                append(" at site ").append( site );

            mLogger.log( error.toString(), LogManager.ERROR_MESSAGE_LEVEL);
            throw new RuntimeException( error.toString() );
        }



        SiteCatalogEntry ePool = mSiteStore.lookup( site );
        jobManager = ePool.selectGridGateway( GridGateway.JOB_TYPE.cleanup );

        String argString = null;
        
        
        execPath = entry.getPhysicalTransformation();

        argString = " mb " + "s3://" +   mBucketName;
            
        newJob.jobName = name;
        newJob.setTransformation( S3.TRANSFORMATION_NAMESPACE,
                                  S3.TRANSFORMATION_NAME,
                                  S3.TRANSFORMATION_VERSION  );
        newJob.setDerivation( S3.DERIVATION_NAMESPACE,
                              S3.DERIVATION_NAME,
                              S3.DERIVATION_VERSION  );
//        newJob.condorUniverse = "vanilla";
        newJob.condorUniverse = jobManager.getJobType().toString();
        newJob.globusScheduler = jobManager.getContact();
        newJob.executable = execPath;
        newJob.executionPool = site;
        newJob.strargs = argString;
        newJob.jobClass = SubInfo.CREATE_DIR_JOB;
        newJob.jobID = name;

        //the profile information from the pool catalog needs to be
        //assimilated into the job.
        newJob.updateProfiles( ePool.getProfiles() );

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
        home = ( home == null )? mSiteStore.getVDSHome( site ): home;

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
            append( S3.TRANSFORMATION_NAME  );


        defaultTCEntry = new TransformationCatalogEntry( S3.TRANSFORMATION_NAMESPACE,
                                                         S3.TRANSFORMATION_NAME,
                                                         S3.TRANSFORMATION_VERSION  );

        defaultTCEntry.setPhysicalTransformation( path.toString() );
        defaultTCEntry.setResourceId( site );
        defaultTCEntry.setType( TCType.INSTALLED );

        //register back into the transformation catalog
        //so that we do not need to worry about creating it again
        try{
            mTCHandle.addTCEntry( defaultTCEntry , false );
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



}
