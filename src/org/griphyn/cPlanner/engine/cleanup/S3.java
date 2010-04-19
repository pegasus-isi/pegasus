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


package org.griphyn.cPlanner.engine.cleanup;



import edu.isi.pegasus.planner.catalog.site.classes.SiteStore;

import org.griphyn.cPlanner.classes.SubInfo;
import org.griphyn.cPlanner.classes.PegasusBag;
import org.griphyn.cPlanner.classes.PegasusFile;


import org.griphyn.cPlanner.classes.ADag;
import org.griphyn.cPlanner.cluster.JobAggregator;
import org.griphyn.cPlanner.cluster.aggregator.JobAggregatorFactory;

import org.griphyn.cPlanner.common.PegasusProperties;

import org.griphyn.cPlanner.namespace.Condor;
import org.griphyn.cPlanner.namespace.VDS;

import edu.isi.pegasus.planner.catalog.TransformationCatalog;
import edu.isi.pegasus.planner.catalog.transformation.TransformationCatalogEntry;


import edu.isi.pegasus.planner.catalog.transformation.classes.TCType;

import org.griphyn.common.util.Separator;

import edu.isi.pegasus.common.logging.LogManager;


import java.util.List;
import java.util.Iterator;
import java.util.LinkedList;

import java.io.File;


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
    public static final String TRANSFORMATION_NAMESPACE = "amazon";

    /**
     * The default priority key associated with the cleanup jobs.
     */
    public static final String DEFAULT_PRIORITY_KEY = "1000";

    /**
     * The name of the underlying transformation that is queried for in the
     * Transformation Catalog.
     */
    public static final String TRANSFORMATION_NAME = "s3cmd";

    /**
     * The version number for the job.
     */
    public static final String TRANSFORMATION_VERSION = null;

    /**
     * The derivation namespace for the job.
     */
    public static final String DERIVATION_NAMESPACE = "amazon";

    /**
     * The name of the underlying derivation.
     */
    public static final String DERIVATION_NAME = "s3cmd";

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
        
        mBucketName = bag.getPlannerOptions().getRelativeDirectory();

        //replace file separators in directory with -
        mBucketName = mBucketName.replace( File.separatorChar,  '-' );
        
        //just to pass the label have to send an empty ADag.
        //should be fixed
        ADag dag = new ADag();
        dag.dagInfo.setLabel( "s3" );

        mSeqExecAggregator = JobAggregatorFactory.loadInstance( JobAggregatorFactory.SEQ_EXEC_CLASS,
                                                                dag,
                                                                bag  );
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
    public SubInfo createCleanupJob( String id, List files, SubInfo job ){
        SubInfo cleanupJob = null;
        
        //create a cleanup job per file
        List<SubInfo> cJobs = new LinkedList<SubInfo>( );
        for( Iterator<PegasusFile> it = files.iterator(); it.hasNext() ; ){
            PegasusFile file = it.next();
            SubInfo cJob = this.createCleanupJob( id, file, job );
            cJobs.add( cJob );
        }
            
        if( files.size() > 1 ){      
            //now lets merge all these jobs
            SubInfo merged = mSeqExecAggregator.construct( cJobs, "cleanup", id  );
            String stdIn = id + ".in";
            //rename the stdin file to make in accordance with tx jobname
            File f = new File( mSubmitDirectory, merged.getStdIn() );
            f.renameTo( new File( mSubmitDirectory, stdIn ) );
            merged.setStdIn( stdIn );
            
            cleanupJob =  merged;

            
            //set the name of the merged job back to the name of
            //transfer job passed in the function call
            cleanupJob.setName( id );
            cleanupJob.setJobType(  SubInfo.CLEANUP_JOB );
            
            
        }else{
            cleanupJob = cJobs.get( 0 );
        }
        
        
        return cleanupJob;
    }
    
    /**
     * Creates a cleanup job that removes the files from remote working directory.
     * This will eventually make way to it's own interface.
     *
     * @param id     the identifier to be assigned to the job.
     * @param file   <code>PegasusFile</code> that need to be cleaned up.
     * @param job    the primary compute job with which this cleanup job is associated.
     *
     * @return the cleanup job.
     */
    protected SubInfo createCleanupJob( String id, PegasusFile file, SubInfo job ){

        //we want to run the clnjob in the same directory
        //as the compute job. So we clone.
        SubInfo cJob = ( SubInfo )job.clone();
        cJob.setJobType( SubInfo.CLEANUP_JOB );
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

        //the compute job of the VDS supernode is this job itself
        cJob.setVDSSuperNode( job.getID() );

        //set the path to the rm executable
        TransformationCatalogEntry entry = this.getTCEntry( job.getSiteHandle() );
        cJob.setRemoteExecutable( entry.getPhysicalTransformation() );


        //prepare the argument invocation
        StringBuffer arguments = new StringBuffer();
        arguments.append( "del "). 
                  append( "s3://" ).
                  append( mBucketName ).
                  append( "/" ).
                  append( file.getLFN() );
        cJob.setArguments( arguments.toString() );
        
        //the cleanup job is a clone of compute
        //need to reset the profiles first
        cJob.resetProfiles();

        //the profile information from the pool catalog needs to be
        //assimilated into the job.
        cJob.updateProfiles( mSiteStore.lookup( job.getSiteHandle() ).getProfiles()  );

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

        //a remote hack that only works for condor pools
        //cJob.globusRSL.construct( "condorsubmit",
        //                                 "(priority " + DEFAULT_PRIORITY_KEY + ")");
        
        //we want the S3 cleanup jobs only execute in /tmp since
        //there is no remote directory being created in S3 environment
        cJob.vdsNS.construct( VDS.REMOTE_INITIALDIR_KEY, "/tmp" );
        //System.out.println( cJob );
        
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
            tcentries = mTCHandle.getTCEntries( S3.TRANSFORMATION_NAMESPACE,
                                                S3.TRANSFORMATION_NAME,
                                                S3.TRANSFORMATION_VERSION,
                                                site,
                                                TCType.INSTALLED  );
        } catch (Exception e) { /* empty catch */ }


        entry = ( tcentries == null ) ?
                 null:
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

  
}
