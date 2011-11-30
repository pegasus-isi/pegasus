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


import edu.isi.pegasus.planner.cluster.JobAggregator;

import edu.isi.pegasus.planner.common.PegasusProperties;

import edu.isi.pegasus.planner.namespace.Condor;
import edu.isi.pegasus.planner.namespace.ENV;
import edu.isi.pegasus.planner.namespace.Pegasus;

import edu.isi.pegasus.planner.catalog.TransformationCatalog;
import edu.isi.pegasus.planner.catalog.transformation.TransformationCatalogEntry;


import edu.isi.pegasus.planner.catalog.transformation.classes.TCType;

import edu.isi.pegasus.common.util.S3cfg;
import edu.isi.pegasus.common.util.Separator;

import edu.isi.pegasus.common.logging.LogManager;


import edu.isi.pegasus.planner.refiner.CreateDirectory;
import edu.isi.pegasus.planner.refiner.createdir.Implementation;
import java.io.BufferedWriter;
import java.util.List;
import java.util.Iterator;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;


/**
 * Use's RM to do removal of the files on the remote sites.
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public class S3 implements CleanupImplementation{

    /**
     * The transformation namespace for the  job.
     */
    public static final String TRANSFORMATION_NAMESPACE = "pegasus";

    /**
     * The default priority key associated with the cleanup jobs.
     */
    public static final String DEFAULT_PRIORITY_KEY = "1000";

    /**
     * The name of the underlying transformation that is queried for in the
     * Transformation Catalog.
     */
    public static final String TRANSFORMATION_NAME = "s3";

    /**
     * The version number for the job.
     */
    public static final String TRANSFORMATION_VERSION = null;
    
    /**
     * The basename of the pegasus cleanup executable.
     */
    public static final String EXECUTABLE_BASENAME = "pegasus-s3";

    /**
     * The complete TC name for the amazon s3cmd.
     */
    public static final String COMPLETE_TRANSFORMATION_NAME = Separator.combine(
                                                                 TRANSFORMATION_NAMESPACE,
                                                                 TRANSFORMATION_NAME,
                                                                 TRANSFORMATION_VERSION  );

    /**
     * The derivation namespace for the job.
     */
    public static final String DERIVATION_NAMESPACE = "pegasus";

    /**
     * The name of the underlying derivation.
     */
    public static final String DERIVATION_NAME = "s3";

    /**
     * The derivation version number for the job.
     */
    public static final String DERIVATION_VERSION = null;

    /**
     * A short description of the transfer implementation.
     */
    public static final String DESCRIPTION =
                    "s3cmd command client that deletes one file at a time from a S3 bucket";



    /**
     * The handle to the transformation catalog.
     */
    protected TransformationCatalog mTCHandle;

    /**
     * Handle to the site catalog.
     */
//    protected PoolInfoProvider mSiteHandle;
    protected SiteStore mSiteStore;

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
     * The seqexec job aggregator.
     */
    private JobAggregator mSeqExecAggregator;
    
    
    /**
     * The name of the bucket that is created.
     */
    protected String mBucketName;
    
    /**
     * The path to the s3cfg file on the submit host (local pool).
     */
    protected String mLocalS3cfg;

    /**
     * The basename of the user s3cfg file
     */
    protected String mLocalS3cfgBasename;
    

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
     * An instance to the Create Direcotry Implementation being used in Pegasus.
     */
    private edu.isi.pegasus.planner.refiner.createdir.S3 mS3CreateDirImpl;
    
    /**
     * The default constructor.
     */
    public S3(){
        
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
        
        Implementation createDirImpl = 
                CreateDirectory.loadCreateDirectoryImplementationInstance(bag);
        //sanity check on the implementation
        if ( !( createDirImpl instanceof edu.isi.pegasus.planner.refiner.createdir.S3 )){
            throw new RuntimeException( "Only S3 Create Dir implementation can be used with S3 First Level Staging" );
        }
        mS3CreateDirImpl = (edu.isi.pegasus.planner.refiner.createdir.S3 )createDirImpl;
        
        // s3cfg
        mLocalS3cfg = S3cfg.getPathToS3cfg(bag);
        //set the path to s3cfg only if the scfg exists
        if( mLocalS3cfg != null && !new File(mLocalS3cfg).exists() ){
            mLogger.log( "The s3cfg file does not exist - " + mLocalS3cfg,
                         LogManager.DEBUG_MESSAGE_LEVEL );
            mLocalS3cfg = null;
        }

        mLocalS3cfgBasename = (mLocalS3cfg == null) ?
                                  null :
                                  new File(mLocalS3cfg).getName();
        
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
        
        String bucket = mS3CreateDirImpl.getBucketNameURL( job.getSiteHandle() );
        
        Job cJob = new Job();
        
        // run locally
        cJob.setSiteHandle("local");
        cJob.executionPool = "local";

        //we dont want notifications to be inherited
        cJob.resetNotifications();

        cJob.setJobType( Job.CLEANUP_JOB );
        cJob.setName( id );

        //inconsistency between job name and logical name for now
        cJob.setTransformation( S3.TRANSFORMATION_NAMESPACE,
                                S3.TRANSFORMATION_NAME,
                                S3.TRANSFORMATION_VERSION  );

        cJob.setDerivation( S3.DERIVATION_NAMESPACE,
                            S3.DERIVATION_NAME,
                            S3.DERIVATION_VERSION  );

        cJob.setLogicalID( id );

        //set the list of files as input files
        //to change function signature to reflect a set only
        //cJob.setInputFiles( new HashSet( files) );

        //the compute job of the Pegasus supernode is this job itself
        cJob.setVDSSuperNode( job.getID() );

        //set the path to the rm executable
        TransformationCatalogEntry entry = this.getTCEntry( "local" );
        cJob.setRemoteExecutable( entry.getPhysicalTransformation() );


        //prepare the input for the cleanup job
        File stdIn = new File( mSubmitDirectory, id + ".in" );
        
        try{
            BufferedWriter writer;
            writer = new BufferedWriter( new FileWriter( stdIn ));

            for( Iterator it = files.iterator(); it.hasNext(); ){
                PegasusFile file = (PegasusFile)it.next();
                writer.write( bucket + "/" + file.getLFN() );
                writer.write( "\n" );
            }

            //closing the handle to the writer
            writer.close();
        }
        catch(IOException e){
            mLogger.log( "While writing the stdIn file " + e.getMessage(),
                        LogManager.ERROR_MESSAGE_LEVEL);
            throw new RuntimeException( "While writing the stdIn file " + stdIn, e );
        }
        
        
        
        
        //the cleanup job is a clone of compute
        //need to reset the profiles first
        cJob.resetProfiles();
        
        
        //the profile information from the pool catalog needs to be
        //assimilated into the job.
        cJob.updateProfiles( mSiteStore.lookup( cJob.getSiteHandle() ).getProfiles()  );

        
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

        //let us put some priority for the cleaunup jobs
        cJob.condorVariables.construct( Condor.PRIORITY_KEY,
                                        DEFAULT_PRIORITY_KEY );
        
        
        //prepare the argument invocation
        StringBuffer arguments = new StringBuffer();
        arguments.append( "rm -F ");
        
        // s3cfg - for jobs executing on local site, just set the environment variable
        // for remote execution, transfer the s3cfg file
        if( cJob.getSiteHandle().equalsIgnoreCase( "local" ) ){
            //the full path
            cJob.envVariables.checkKeyInNS(S3cfg.S3CFG, this.mLocalS3cfg );
            arguments.append( stdIn.getAbsolutePath() );
            
            //on local site, we have full path the input file.
            //set initialdir to "" to indicate that it should not
            //be set.
            cJob.condorVariables.construct( "initialdir", "" );
            
        }
        else{
            //the input file needs to be transferred via condor file io
            //and arguments refer just to basename
            cJob.condorVariables.addIPFileForTransfer( stdIn.getAbsolutePath() );
            arguments.append( stdIn.getName() );

            cJob.condorVariables.addIPFileForTransfer(mLocalS3cfg);
            //just the basename
            cJob.envVariables.checkKeyInNS(ENV.S3CFG, mLocalS3cfgBasename);
            cJob.envVariables.checkKeyInNS(ENV.GRIDSTART_PREJOB,
                                             "/bin/chmod 600 " +
                                             mLocalS3cfgBasename);
        }
        
        cJob.setArguments( arguments.toString() );
        
        return cJob;
    }
    
   

    /**
     * Returns the TCEntry object for the rm executable on a grid site.
     *
     * @param site the site corresponding to which the entry is required.
     *
     * @return  the TransformationCatalogEntry corresponding to the site.
     */
    protected TransformationCatalogEntry getTCEntry( String site ){
        List tcentries = null;
        TransformationCatalogEntry entry  = null;
        try {
            tcentries = mTCHandle.lookup( S3.TRANSFORMATION_NAMESPACE,
                                                S3.TRANSFORMATION_NAME,
                                                S3.TRANSFORMATION_VERSION,
                                                site,
                                                TCType.INSTALLED  );
        } catch (Exception e) { /* empty catch */ }


        entry = ( tcentries == null ) ?
            this.defaultTCEntry( site ): //try using a default one
            (TransformationCatalogEntry) tcentries.get(0);

        if( entry == null ){
            //NOW THROWN AN EXCEPTION

            //should throw a TC specific exception
            StringBuffer error = new StringBuffer();
            error.append("Could not find entry in tc for lfn ").
                  append( S3.getCompleteTranformationName()).
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
            append( S3.EXECUTABLE_BASENAME );


        defaultTCEntry = new TransformationCatalogEntry( S3.TRANSFORMATION_NAMESPACE,
                                                         S3.TRANSFORMATION_NAME,
                                                         S3.TRANSFORMATION_VERSION );

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
