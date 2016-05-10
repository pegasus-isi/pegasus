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


package edu.isi.pegasus.planner.refiner.cleanup;




import edu.isi.pegasus.planner.catalog.site.classes.SiteStore;

import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.classes.PegasusFile;

import edu.isi.pegasus.planner.common.PegasusProperties;
import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.common.util.PegasusURL;

import edu.isi.pegasus.planner.catalog.TransformationCatalog;
import edu.isi.pegasus.planner.catalog.transformation.TransformationCatalogEntry;

import edu.isi.pegasus.planner.catalog.transformation.classes.TCType;

import edu.isi.pegasus.common.util.Separator;

import edu.isi.pegasus.planner.catalog.site.classes.FileServerType.OPERATION;
import edu.isi.pegasus.planner.catalog.site.classes.SiteCatalogEntry;
import edu.isi.pegasus.planner.classes.PlannerCache;

import edu.isi.pegasus.planner.mapper.SubmitMapper;
import edu.isi.pegasus.planner.namespace.Dagman;

import java.util.List;
import java.util.Iterator;
import java.util.HashSet;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.File;
import java.io.IOException;

/**
 * Uses pegasus-transfer to do removal of the files on the remote sites.
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public class Cleanup implements CleanupImplementation{


    

    /**
     * The transformation namespace for the  job.
     */
    public static final String TRANSFORMATION_NAMESPACE = "pegasus";

    /**
     * The name of the underlying transformation that is queried for in the
     * Transformation Catalog.
     */
    public static final String TRANSFORMATION_NAME = "cleanup";

    /**
     * The version number for the job.
     */
    public static final String TRANSFORMATION_VERSION = null;

    /**
     * The derivation namespace for the job.
     */
    public static final String DERIVATION_NAMESPACE = "pegasus";

    /**
     * The name of the underlying derivation.
     */
    public static final String DERIVATION_NAME = "cleanup";

    /**
     * The derivation version number for the job.
     */
    public static final String DERIVATION_VERSION = null;

    /**
     * The basename of the pegasus cleanup executable.
     */
    public static final String EXECUTABLE_BASENAME = "pegasus-transfer";

    /**
     * A short description of the transfer implementation.
     */
    public static final String DESCRIPTION =
                    "Uses our common transfer client, which reads from the stdin the list of files" +
                    " to be cleaned";



    /**
     * The handle to the transformation catalog.
     */
    protected TransformationCatalog mTCHandle;

    /**
     * Handle to the stagingSite catalog.
     */
    protected SiteStore mSiteStore;

    /**
     * Handle to the transient replica catalog.
     */
    protected PlannerCache mPlannerCache;

    /**
     * The handle to the properties passed to Pegasus.
     */
    private PegasusProperties mProps;

    /**
     * The submit directory where the output files have to be written.
     */
    private String mSubmitDirectory;

    /**
     * The handle to the logger.
     */
    private LogManager mLogger;
    
    /**
     * Handle to the Submit directory factory, that returns the relative
     * submit directory for a job
     */
    protected SubmitMapper mSubmitDirFactory;

    /**
     * A convenience method to return the complete transformation name being
     * used to construct jobs in this class.
     *
     * @return the complete transformation name
     */
    public static String getCompleteTranformationName(){
        return Separator.combine( TRANSFORMATION_NAMESPACE,
                                  TRANSFORMATION_NAME,
                                  TRANSFORMATION_VERSION );
    }
    
    /**
     * The default constructor.
     */
    public Cleanup(){
        
    }

    /**
     * Creates a new instance of InPlace
     *
     * @param bag  the bag of initialization objects.
     *
     */
    public void initialize( PegasusBag bag ) {
        mProps           = bag.getPegasusProperties();
        mSubmitDirectory = bag.getPlannerOptions().getSubmitDirectory();
        mSiteStore       = bag.getHandleToSiteStore();
        mTCHandle        = bag.getHandleToTransformationCatalog(); 
        mLogger          = bag.getLogger();
        mPlannerCache     = bag.getHandleToPlannerCache();
        mSubmitDirFactory = bag.getSubmitDirFileFactory();
    }


    /**
     * Creates a cleanup job that removes the files from remote working directory.
     * This will eventually make way to it's own interface.
     *
     * @param id         the identifier to be assigned to the job.
     * @param files      the list of <code>PegasusFile</code> that need to be
     *                   cleaned up.
     * @param job        the primary compute job with which this cleanup job is associated.
     *
     * @return the cleanup job.
     */
    public Job createCleanupJob( String id, List files, Job job ){

        //we want to run the clnjob in the same directory
        //as the compute job. We cannot clone as then the 
        //the cleanup jobs for clustered jobs appears as
        //a clustered job. PM-368
        Job cJob = new Job( job );

        //we dont want credentials to be inherited
        cJob.resetCredentialTypes();

        String stagingSiteHandle = job.getStagingSiteHandle();
        SiteCatalogEntry stagingSite = mSiteStore.lookup( stagingSiteHandle );
        boolean stagingSiteVisibleToLocalSite = stagingSite.isVisibleToLocalSite();
        
        //by default execution site for a cleanup job is local unless
        //overridden because of File URL's in list of files to be cleaned
        String eSite = "local";
        
        //PM-833 set the relative submit directory for the transfer
        //job based on the associated file factory
        cJob.setRelativeSubmitDirectory( this.mSubmitDirFactory.getRelativeDir(cJob));

        //prepare the stdin for the cleanup job
        String stdIn = id + ".in";
        try{
            BufferedWriter writer;
            File directory = new File( this.mSubmitDirectory, cJob.getRelativeSubmitDirectory() );
            writer = new BufferedWriter( new FileWriter( new File( directory, stdIn ) ));
            
            
            writer.write("[\n");
            
            int fileNum = 1;
            for( Iterator it = files.iterator(); it.hasNext(); fileNum++ ){
                PegasusFile file = (PegasusFile)it.next();
                String pfn = mPlannerCache.lookup(file.getLFN(), stagingSiteHandle, OPERATION.put );

                if( pfn == null ){
                    throw new RuntimeException( "Unable to determine cleanup url for lfn " + file.getLFN() + " at site " + stagingSiteHandle );
                }

                if( (pfn.startsWith( PegasusURL.FILE_URL_SCHEME ) || pfn.startsWith( PegasusURL.SYMLINK_URL_SCHEME )) &&
                       (!stagingSiteVisibleToLocalSite) //PM-1024 staging site is not visible to the local site
                        ){
                    //means the cleanup job should run on the staging site
                    mLogger.log( " PFN for file " + file.getLFN() + " on staging site is a file|symlink URL " + pfn,
                                 LogManager.DEBUG_MESSAGE_LEVEL );
                    mLogger.log("Cleanup Job " + id + " instead of running on local site , will run on site " + stagingSiteHandle,
                                 LogManager.DEBUG_MESSAGE_LEVEL );
                    eSite = stagingSiteHandle;
                }

                //associate a credential if required
                cJob.addCredentialType(stagingSiteHandle, pfn );

                if (fileNum > 1) {
                	writer.write("  ,\n");
                }
                
                writer.write("  {\n");
                writer.write("    \"id\": " + fileNum + ",\n");
                writer.write("    \"type\": \"remove\",\n");
                writer.write("    \"target\": {");
                writer.write(" \"site_label\": \"" + stagingSiteHandle + "\",");
                writer.write(" \"url\": \"" + pfn + "\",");
                writer.write(" \"recursive\": \"False\"");
                writer.write(" }");
                writer.write(" }\n");
                
            }

            writer.write("]\n");

            //closing the handle to the writer
            writer.close();
        }
        catch(IOException e){
            mLogger.log( "While writing the stdIn file " + e.getMessage(),
                        LogManager.ERROR_MESSAGE_LEVEL);
            throw new RuntimeException( "While writing the stdIn file " + stdIn, e );
        }


        cJob.setSiteHandle( eSite );

        //we dont want notifications to be inherited
        cJob.resetNotifications();

        //also make sure that user executables staged is set to false
        cJob.setExecutableStagingForJob( false );

        cJob.setJobType( Job.CLEANUP_JOB );
        cJob.setName( id );
        // empty arguments, as this job is a clone
        cJob.setArguments("");
        
        //bug fix for JIRA PM-311
        //we dont want cleanup job to inherit any stdout or stderr
        //specified in the DAX for compute job
        cJob.setStdOut( "" );
        cJob.setStdErr( "" );

        //inconsistency between job name and logical name for now
        cJob.setTransformation( Cleanup.TRANSFORMATION_NAMESPACE,
                                Cleanup.TRANSFORMATION_NAME,
                                Cleanup.TRANSFORMATION_VERSION );

        cJob.setDerivation( Cleanup.DERIVATION_NAMESPACE,
                            Cleanup.DERIVATION_NAME,
                            Cleanup.DERIVATION_VERSION );

        //cJob.setLogicalID( id );

        //set the list of files as input files
        //to change function signature to reflect a set only
        cJob.setInputFiles( new HashSet( files) );

        //the compute job of the VDS supernode is this job itself
        cJob.setVDSSuperNode( job.getID() );

        //set the path to the rm executable
        TransformationCatalogEntry entry = this.getTCEntry( eSite );
        cJob.setRemoteExecutable( entry.getPhysicalTransformation() );


        
        //we want to run the job on fork jobmanager
        //SiteInfo stagingSite = mSiteHandle.getTXPoolEntry( cJob.getSiteHandle() );
        //JobManager jobmanager = stagingSite.selectJobManager( Engine.TRANSFER_UNIVERSE, true );
        //cJob.globusScheduler = (jobmanager == null) ?
        //                        null :
        //                       jobmanager.getInfo(JobManager.URL);


        //set the stdin file for the job
        cJob.setStdIn( stdIn );

        //the cleanup job is a clone of compute
        //need to reset the profiles first
        cJob.resetProfiles();

        //the profile information from the pool catalog needs to be
        //assimilated into the job.
        cJob.updateProfiles( mSiteStore.lookup( eSite ).getProfiles()  );

        //add any notifications specified in the transformation
        //catalog for the job. JIRA PM-391
        cJob.addNotifications( entry );

        //the profile information from the transformation
        //catalog needs to be assimilated into the job
        //overriding the one from pool catalog.
        cJob.updateProfiles( entry );

        //the profile information from the properties file
        //is assimilated overidding the one from transformation
        //catalog.
        cJob.updateProfiles( mProps );

        //if no category is associated with the job, add a default
       //category
       if( !cJob.dagmanVariables.containsKey( Dagman.CATEGORY_KEY ) ){
           cJob.dagmanVariables.construct( Dagman.CATEGORY_KEY, DEFAULT_CLEANUP_CATEGORY_KEY );
       }

        //a remote hack that only works for condor pools
        //cJob.globusRSL.construct( "condorsubmit",
        //                                 "(priority " + DEFAULT_PRIORITY_KEY + ")");
        return cJob;
    }

    /**
     * Returns the TCEntry object for the rm executable on a grid stagingSite.
     *
     * @param stagingSite the stagingSite corresponding to which the entry is required.
     *
     * @return  the TransformationCatalogEntry corresponding to the stagingSite.
     */
    protected TransformationCatalogEntry getTCEntry( String site ){
        List tcentries = null;
        TransformationCatalogEntry entry  = null;
        try {
            tcentries = mTCHandle.lookup( Cleanup.TRANSFORMATION_NAMESPACE,
                                                Cleanup.TRANSFORMATION_NAME,
                                                Cleanup.TRANSFORMATION_VERSION,
                                                site,
                                                TCType.INSTALLED );
        } catch (Exception e) { /* empty catch */ }


        entry = ( tcentries == null ) ?
                 this.defaultTCEntry( site ): //try using a default one
                 (TransformationCatalogEntry) tcentries.get(0);

        if( entry == null ){
            //NOW THROWN AN EXCEPTION

            //should throw a TC specific exception
            StringBuffer error = new StringBuffer();
            error.append("Could not find entry in tc for lfn ").
                  append( Cleanup.getCompleteTranformationName()).
                  append(" at site ").append(site);

              mLogger.log( error.toString(), LogManager.ERROR_MESSAGE_LEVEL);
              throw new RuntimeException( error.toString() );

          }


        return entry;

    }

    /**
     * Returns a default TC entry to be used in case entry is not found in the
     * transformation catalog.
     *
     * @param stagingSite   the stagingSite for which the default entry is required.
     *
     *
     * @return  the default entry.
     */
    private  TransformationCatalogEntry defaultTCEntry( String site ){
        TransformationCatalogEntry defaultTCEntry = null;
        //check if PEGASUS_HOME is set
        String home = mSiteStore.getPegasusHome( site );

        mLogger.log( "Creating a default TC entry for " +
                     Cleanup.getCompleteTranformationName() +
                     " at site " + site,
                     LogManager.DEBUG_MESSAGE_LEVEL );


        //if home is still null
        if ( home == null ){
            //cannot create default TC
            mLogger.log( "Unable to create a default entry for " +
                         Separator.combine( Cleanup.TRANSFORMATION_NAMESPACE,
                                            Cleanup.TRANSFORMATION_NAME,
                                            Cleanup.TRANSFORMATION_VERSION ),
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
            append( Cleanup.EXECUTABLE_BASENAME );


        defaultTCEntry = new TransformationCatalogEntry( Cleanup.TRANSFORMATION_NAMESPACE,
                                                           Cleanup.TRANSFORMATION_NAME,
                                                           Cleanup.TRANSFORMATION_VERSION );

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

}
