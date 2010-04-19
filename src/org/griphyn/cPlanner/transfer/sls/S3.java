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

package org.griphyn.cPlanner.transfer.sls;


import edu.isi.pegasus.planner.catalog.site.classes.SiteStore;

import edu.isi.pegasus.common.logging.LogManager;

import org.griphyn.cPlanner.classes.PegasusBag;
import org.griphyn.cPlanner.classes.FileTransfer;
import org.griphyn.cPlanner.classes.SubInfo;
import org.griphyn.cPlanner.classes.PegasusFile;
import org.griphyn.cPlanner.classes.TransferJob;
import org.griphyn.cPlanner.classes.Profile;

import org.griphyn.cPlanner.common.PegasusProperties;

import org.griphyn.cPlanner.engine.CreateDirectory;
import org.griphyn.cPlanner.engine.createdir.Implementation;

import org.griphyn.cPlanner.transfer.SLS;

import edu.isi.pegasus.planner.catalog.TransformationCatalog;
import edu.isi.pegasus.planner.catalog.transformation.TransformationCatalogEntry;
import edu.isi.pegasus.planner.catalog.ReplicaCatalog;

import edu.isi.pegasus.planner.catalog.transformation.classes.TCType;

import edu.isi.pegasus.common.util.Separator;
import edu.isi.pegasus.common.util.Boolean;

import java.io.File;
import java.io.IOException;
import java.io.FileWriter;

import java.util.Iterator;
import java.util.Set;
import java.util.List;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;


/**
 * This implementation of the SLS API allows us to use S3cmd to retrieve
 * data from S3 bucket for worker node execution.
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public class S3   implements SLS {

    /**
     * The transformation namespace for the transfer job.
     */
    public static final String TRANSFORMATION_NAMESPACE = "amazon";

    /**
     * The name of the underlying transformation that is queried for in the
     * Transformation Catalog.
     */
    public static final String TRANSFORMATION_NAME = "s3cmd";

    /**
     * The version number for the transfer job.
     */
    public static final String TRANSFORMATION_VERSION = null;

     /**
     * The complete TC name for s3 transfer client.
     */
    public static final String COMPLETE_TRANSFORMATION_NAME = Separator.combine(
                                                                 TRANSFORMATION_NAMESPACE,
                                                                 TRANSFORMATION_NAME,
                                                                 TRANSFORMATION_VERSION  );
    
    /**
     * The derivation namespace for for the transfer job.
     */
    public static final String DERIVATION_NAMESPACE = "pegasus";

    /**
     * The name of the underlying derivation.
     */
    public static final String DERIVATION_NAME = "amazon";

    /**
     * The derivation version number for the transfer job.
     */
    public static final String DERIVATION_VERSION = "1.0";

    /**
     * A short description of the transfer implementation.
     */
    public static final String DESCRIPTION = "Seqexec around s3";

    /**
     * Name of the property key that determines whether to bypass staging
     * of the sls files or not.
     */
    public static final String STAGE_SLS_FILE_PROPERTY_KEY = "pegasus.transfer.sls.s3.stage.sls.file";

    /**
     * The handle to the site catalog.
     */
//    protected PoolInfoProvider mSiteHandle;
    protected SiteStore mSiteStore;

    /**
     * The handle to the transformation catalog.
     */
    protected TransformationCatalog mTCHandle;

    /**
     * The handle to the properties.
     */
    protected PegasusProperties mProps;

    /**
     * The handle to the logging manager.
     */
    protected LogManager mLogger;

    /**
     * The path to local user proxy.
     */
    protected String mLocalUserProxy;

    /**
     * The basename of the proxy
     */
    protected String mLocalUserProxyBasename;

    /**
     * The local url prefix for the submit host.
     */
    //protected String mLocalURLPrefix;

    /**
     * The handle to S3 transfer implementation.
     */
    protected org.griphyn.cPlanner.transfer.implementation.S3Cmd mS3Transfer;
    
    /**
     * The name of the bucket that is created.
     */
    //protected String mBucketName;
    
    /**
     * The worker node directory where the jobs execute. Same for all jobs.
     */
    protected String mWorkerNodeDirectory;
    
    /**
     * The handle to the transient replica catalog.
     */
    protected ReplicaCatalog mTransientRC;

    /**
     * Boolean to track whether to stage sls file or not
     */
    protected boolean mStageSLSFile;
    
    /**
     * Any extra arguments that need to be passed ahead to the s3 client invocation.
     */
    protected String mExtraArguments;
    
    
    /**
     * An instance to the Create Direcotry Implementation being used in Pegasus.
     */
    private org.griphyn.cPlanner.engine.createdir.S3 mS3CreateDirImpl;
    
    
    /**
     * The default constructor.
     */
    public S3() {
    }


    /**
     * Initializes the SLS implementation.
     *
     * @param bag the bag of objects. Contains access to catalogs etc.
     */
    public void initialize( PegasusBag bag ) {
        mProps      = bag.getPegasusProperties();
        mLogger     = bag.getLogger();
        mSiteStore  = bag.getHandleToSiteStore();
        mTCHandle   = bag.getHandleToTransformationCatalog();
        mTransientRC = bag.getHandleToTransientReplicaCatalog();

        mS3Transfer = new org.griphyn.cPlanner.transfer.implementation.S3Cmd( bag );
       
        //mS3BucketURL = getS3BucketURL( bag );
        //figure out if we are creating any s3 buckets or not
        Implementation createDirImpl = 
                CreateDirectory.loadCreateDirectoryImplementationInstance(bag);
        //sanity check on the implementation
        if ( !( createDirImpl instanceof org.griphyn.cPlanner.engine.createdir.S3 )){
            throw new RuntimeException( "Only S3 Create Dir implementation can be used with S3 SLS" );
        }
        mS3CreateDirImpl = (org.griphyn.cPlanner.engine.createdir.S3 )createDirImpl;
        
        mStageSLSFile = Boolean.parse( mProps.getProperty( S3.STAGE_SLS_FILE_PROPERTY_KEY ),
                                       true );
        mExtraArguments = mProps.getSLSTransferArguments();
    }

    /**
     * Returns a boolean whether the SLS implementation does a condor based
     * modification or not. By condor based modification we mean whether it
     * uses condor specific classads to achieve the second level staging or not.
     *
     * @return false
     */
    public boolean doesCondorModifications(){
        return false;
    }

    /**
     * Constructs a command line invocation for a job, with a given sls file.
     * The SLS maybe null. In the case where SLS impl does not read from a file,
     * it is advised to create a file in generateSLSXXX methods, and then read
     * the file in this function and put it on the command line.
     *
     * @param job          the job that is being sls enabled
     * @param slsFile      the slsFile can be null
     *
     * @return invocation string
     */
    public String invocationString( SubInfo job, File slsFile ){
        //sanity check
        if( slsFile == null ) { return null; }

        StringBuffer invocation = new StringBuffer();

        TransformationCatalogEntry entry = this.getSeqExecTransformationCatalogEntry( job.getSiteHandle() );
        if( entry == null ){
            //cannot create an invocation
            return null;

        }

        String slsBasename = slsFile.getName();


        //we only grab sls file if it has been staged in the
        //first place by first level staging.
        if( mStageSLSFile ){
            invocation.append( "/bin/bash -c \"" );
        
            //construct the s3 cmd invocation for
            //get the sls file to worker node
            FileTransfer ft = new FileTransfer( slsBasename, "dummy");
            ft.addDestination( "s3", mWorkerNodeDirectory);
            invocation.append( this.generateS3InvocationString( job.getSiteHandle(), ft, SubInfo.STAGE_OUT_JOB ) );
       
            invocation.append(" && ");
        }

        invocation.append( entry.getPhysicalTransformation() ).
                   append( " " ). 
                   append( slsBasename );

        if( mStageSLSFile ){
            invocation.append( "\"" );
        }
        
        return invocation.toString();

    }





    /**
     * Returns a boolean indicating whether it will an input file for a job
     * to do the transfers. Transfer reads from stdin the file transfers that
     * it needs to do. Always returns true, as we need to transfer the proxy
     * always.
     *
     * @param job the job being detected.
     *
     * @return true
     */
    public boolean needsSLSInput( SubInfo job ) {
        return true;
    }

    /**
     * Returns a boolean indicating whether it will an output file for a job
     * to do the transfers. Transfer reads from stdin the file transfers that
     * it needs to do.
     *
     * @param job the job being detected.
     *
     * @return true
     */
    public boolean needsSLSOutput( SubInfo job ) {
        Set files = job.getOutputFiles();
        return! (files == null || files.isEmpty());
    }

    /**
     * Returns the LFN of sls input file.
     *
     * @param job SubInfo
     *
     * @return the name of the sls input file.
     */
    public String getSLSInputLFN( SubInfo job ){
        StringBuffer lfn = new StringBuffer();
        lfn.append( "sls_" ).append( job.getName() ).append( ".in" );
        return lfn.toString();
    }


    /**
     * Returns the LFN of sls output file.
     *
     * @param job SubInfo
     *
     * @return the name of the sls input file.
     */
    public String getSLSOutputLFN( SubInfo job ){
        StringBuffer lfn = new StringBuffer();
        lfn.append( "sls_" ).append( job.getName() ).append( ".out" );
        return lfn.toString();
    }


    /**
     * Generates a second level staging file of the input files to the worker
     * node directory.
     *
     * @param job           job for which the file is being created
     * @param fileName      name of the file that needs to be written out.
     * @param submitDir     submit directory where it has to be written out.
     * @param headNodeDirectory    directory on the head node of the compute site.
     * @param workerNodeDirectory  worker node directory
     *
     * @return the full path to lof file created, else null if no file is
     *   written out.
     */
    public File generateSLSInputFile( SubInfo job,
                                      String fileName,
                                      String submitDir,
                                      String headNodeDirectory,
                                      String workerNodeDirectory ) {

        //sanity check
        if ( !needsSLSInput( job ) ){
            mLogger.log( "Not Writing out a SLS input file for job " + job.getName() ,
                         LogManager.DEBUG_MESSAGE_LEVEL );
            return null;
        }

        mWorkerNodeDirectory = workerNodeDirectory;
        
        Set files = job.getInputFiles();
        File sls = new File( submitDir, fileName );
        PegasusFile pf;

        String destDir = workerNodeDirectory;
        Collection slsFiles = new LinkedList<FileTransfer>();//files that need to go to the worker node
        
        //writing the stdin file
        try {
            FileWriter input = new FileWriter( sls );
            
            //construct the FileTransfer objects for files to be transferred
            for( Iterator it = files.iterator(); it.hasNext(); ){
            
                pf = ( PegasusFile ) it.next();
                
                //skip sls input and output file as they are
                //transferred as part of gridstart prejob 
                if( pf.getLFN().equals( getSLSInputLFN(job) ) ||
                    pf.getLFN().equals( getSLSOutputLFN(job) ) ){
                    continue;
                }
                
                FileTransfer ft = new FileTransfer( pf );
                //we just need to add the destination
                //i.e directory on the worker node
                StringBuffer destURL = new StringBuffer();
                destURL.append( destDir ).append( File.separator ).append( pf.getLFN() );
                ft.addDestination( "s3", destURL.toString() );
            
                input.write( this.generateS3InvocationString( job.getSiteHandle(), 
                                                              ft,
                                                              SubInfo.STAGE_OUT_JOB ));
                input.write( "\n" );
            }
            //close the stream
            input.close();

        }catch ( IOException e) {
            mLogger.log( "Unable to write the sls file for job " + job.getName(), e ,
                         LogManager.ERROR_MESSAGE_LEVEL);
        }

        
            
        return sls;
    }

    /**
     * Generates a second level staging file of the input files to the worker
     * node directory.
     *
     * @param job the job for which the file is being created
     * @param fileName the name of the file that needs to be written out.
     * @param submitDir the submit directory where it has to be written out.
     * @param headNodeDirectory the directory on the head node of the
     *   compute site.
     * @param workerNodeDirectory the worker node directory
     *
     * @return the full path to lof file created, else null if no file is
     *   written out.
     *
     */
    public File generateSLSOutputFile( SubInfo job, 
                                       String fileName,
                                       String submitDir,
                                       String headNodeDirectory,
                                       String workerNodeDirectory ) {


        //sanity check
        if ( !needsSLSOutput( job ) ){
            mLogger.log( "Not Writing out a SLS output file for job " + job.getName() ,
                         LogManager.DEBUG_MESSAGE_LEVEL );
            return null;
        }

        File sls = null;
        String sourceDir = workerNodeDirectory;


        //writing the stdin file
        try {
            StringBuffer name = new StringBuffer();
            sls = new File( submitDir, fileName);
            FileWriter input = new FileWriter( sls );
            PegasusFile pf;

            //To do. distinguish the sls file from the other input files
            for( Iterator it = job.getOutputFiles().iterator(); it.hasNext(); ){
                pf = ( PegasusFile ) it.next();
                
                FileTransfer ft = new FileTransfer( pf );
                //we just need to add the destination
                //i.e directory on the worker node
                StringBuffer sourceURL = new StringBuffer();
                sourceURL.append( sourceDir ).append( File.separator ).append( pf.getLFN() );
                ft.addSource( job.getSiteHandle(), sourceURL.toString() );
                
                input.write( this.generateS3InvocationString( job.getSiteHandle(), 
                                                              ft,
                                                              SubInfo.STAGE_IN_JOB ));
                input.write( "\n" );
            }
            //close the stream
            input.close();

        } catch ( IOException e) {
            mLogger.log( "Unable to write the sls output file for job " + job.getName(), e ,
                         LogManager.ERROR_MESSAGE_LEVEL);
        }

        return sls;


    }


    /**
     * Modifies a job for the first level staging to headnode.This is to add
     * any files that needs to be staged to the head node for a job specific
     * to the SLS implementation. If any file needs to be added, a <code>FileTransfer</code>
     * object should be created and added as an input or an output file.
     * A job is not modified the staging of the sls file is turned of by
     * setting the property specified by STAGE_SLS_FILE_PROPERTY_KEY
     *
     *
     * @param job           the job
     * @param submitDir     the submit directory
     * @param slsInputLFN   the sls input file if required, that is used for
     *                      staging in from the head node to worker node directory.
     * @param slsOutputLFN  the sls output file if required, that is used
     *                      for staging in from the head node to worker node directory.
     * @return boolean
     *
     * @see #STAGE_SLS_FILE_PROPERTY_KEY
     */
    public boolean modifyJobForFirstLevelStaging( SubInfo job,
                                                  String submitDir,
                                                  String slsInputLFN,
                                                  String slsOutputLFN ) {

        //sanity check
        if( !this.mStageSLSFile ){
            return true;
        }

        String separator = File.separator;

        //incorporate the sls input file if required
        if( slsInputLFN != null ){

            FileTransfer ft = new FileTransfer( slsInputLFN, job.getName());

            //the source sls is to be sent across from the local site
            //using the grid ftp server at local site.
            StringBuffer sourceURL = new StringBuffer();
            sourceURL.append( submitDir ).append(separator).
                      append( slsInputLFN );
            ft.addSource("local", sourceURL.toString());

            //the destination URL is the working directory on the filesystem
            //on the head node where the job is to be run.
            StringBuffer destURL = new StringBuffer();

            destURL.append( mSiteStore.lookup( job.getSiteHandle() ).getHeadNodeFS().selectScratchSharedFileServer().getURLPrefix() ).
                    append( separator ).
                    append( mSiteStore.getWorkDirectory( job ) ).append( separator ).
                    append( slsInputLFN );
            ft.addDestination( job.getSiteHandle(), destURL.toString() );

            //add this as input file for the job
            job.addInputFile( ft );
        }

        //add the sls out file as input to the job
        if( slsOutputLFN != null ){
            FileTransfer ft = new FileTransfer( slsOutputLFN, job.getName() );

            //the source sls is to be sent across from the local site
            //using the grid ftp server at local site.
            StringBuffer sourceURL = new StringBuffer();
            sourceURL.append( submitDir ).append( separator ).
                      append( slsOutputLFN );

            ft.addSource( "local" , sourceURL.toString() );

            //the destination URL is the working directory on the filesystem
            //on the head node where the job is to be run.
            StringBuffer destURL = new StringBuffer();

            destURL.append( mSiteStore.lookup( job.getSiteHandle() ).getHeadNodeFS().selectScratchSharedFileServer().getURLPrefix() )
                    .append( separator ).
                    append( mSiteStore.getWorkDirectory( job ) ).append( separator ).
                    append( slsOutputLFN );

            ft.addDestination( job.getSiteHandle(), destURL.toString()  );

            //add this as input file for the job
            job.addInputFile( ft );
        }

        
       return true;

    }

    /**
     * Modifies a compute job for second level staging. The appropriate 
     * environment variables are added to the job
     *
     * @param job the job to be modified.
     * @param headNodeURLPrefix the url prefix for the server on the headnode
     * @param headNodeDirectory the directory on the headnode, where the
     *   input data is read from and the output data written out.
     * @param workerNodeDirectory the directory in the worker node tmp
     *
     * @return boolean indicating whether job was successfully modified or
     *   not.
     *
     */
    public boolean modifyJobForWorkerNodeExecution( SubInfo job,
                                                    String headNodeURLPrefix,
                                                    String headNodeDirectory,
                                                    String workerNodeDirectory ) {



        List envs = this.getEnvironmentVariables( job.getSiteHandle() );

        if( envs == null || envs.isEmpty()){
            //cannot create default TC
            mLogger.log( "Unable to set the necessary environment " +
                         Separator.combine( S3.TRANSFORMATION_NAMESPACE, S3.TRANSFORMATION_NAME, S3.TRANSFORMATION_VERSION ) ,
                         LogManager.DEBUG_MESSAGE_LEVEL );
            return false;
        }


        for( Iterator it = envs.iterator(); it.hasNext(); ){
            job.envVariables.checkKeyInNS( (Profile)it.next() );
        }

        /*for( Iterator<PegasusFile> it = job.getInputFiles().iterator(); it.hasNext() ; ){
            PegasusFile pf = it.next();
            System.out.println( "File is " + pf );
            if( pf.getLFN().equals( getSLSInputLFN(job) ) ||
                pf.getLFN().equals( getSLSOutputLFN(job) ) ){
                System.out.println( "Removing file" + pf );
                it.remove();
            }
        }*/
        return true;

    }


    
    /**
     * It constructs the arguments to the s3 transfer executable. For the get
     * command, the source URL is created according to the bucket name only
     * if the file is not in the transient replica catalog that tracks the 
     * locations of the file referred in the DAX.
     * 
     *
     * @param site  the site for which the invocation is required.
     * @param file  the FileTransfer that needs to be done.
     * @param type  type of job.
     * 
     * @return  the argument string
     */
    protected String generateS3InvocationString( String site, FileTransfer file, int type ){
        StringBuffer sb = new StringBuffer();
               
        TransformationCatalogEntry entry = this.getS3TransformationCatalogEntry( site );
        if( entry == null ){
            throw new RuntimeException( "No Entry for transformation " + 
                                         S3.COMPLETE_TRANSFORMATION_NAME + " at site " + site );
        }
        
        sb.append( entry.getPhysicalTransformation() ).append( " " );
        
        //prepend any extra arguments set by user
        //in properties
        if( mExtraArguments != null ){
            sb.append( mExtraArguments ).append( " " );
        }
        
        //determine the type of command to issue on the basis of 
        //type of transfer job
        String command = ( type == TransferJob.STAGED_COMPUTE_JOB ||
                           type == TransferJob.STAGE_IN_JOB ) ?
                           "put" : //used for stagein
                           "get" ; //used for stageout
        
        
        sb.append( command );
        sb.append( " " );
        
        String lfn = file.getLFN();
        if( command.equals( "put" ) ){
            //stagein data to the bucket
            sb.append(  " " );
            sb.append( file.getSourceURL().getValue() );
            sb.append( " " );
        
            /*
            sb.append( "s3://" ).
               append( mBucketName ).*/
            
            sb.append( this.mS3CreateDirImpl.getBucketNameURL(site) ).
               append( "/" ).
               append( lfn );
        } 
        else{
            //stagein data to the bucket
            
            //check if the input file is in the transient RC
            //all files in the DAX should be in the transient RC
            String transientPFN =  mTransientRC.lookup( lfn, site );
            if( transientPFN == null ){
                //create the default path. refer to bucket on 
                //the head node.
                /*
                sb.append( "s3://" ).
                   append( mBucketName ).*/
                sb.append( this.mS3CreateDirImpl.getBucketNameURL(site) ).
                   append( "/" ).
                   append( lfn );
            }
            else{
                //use the trasient PFN
                sb.append( transientPFN );
            }
            
            String dest = file.getDestURL().getValue();
            //some sanitization if reqd 
            if( dest.startsWith( "file:///" ) ){
                dest = dest.substring( 7 );
            }
            sb.append(  " " );
            sb.append( dest );
            sb.append( " " );
            
        }
        
        return sb.toString(); 

    }
    
    /**
     * Retrieves the transformation catalog entry for the executable that is
     * being used to transfer the files in the implementation.
     *
     * @param siteHandle  the handle of the  site where the transformation is
     *                    to be searched.
     *
     * @return  the transformation catalog entry if found, else null.
     */
    public TransformationCatalogEntry getSeqExecTransformationCatalogEntry(String siteHandle){
        List tcentries = null;
        try {
            //namespace and version are null for time being
            tcentries = mTCHandle.getTCEntries( "pegasus",
                                                "seqexec",
                                                null,
                                                siteHandle,
                                                TCType.INSTALLED);
        } catch (Exception e) {
            mLogger.log(
                "Unable to retrieve entry from TC for " +
                Separator.combine( "pegasus",
                                   "seqexec",
                                    null ) +
                " Cause:" + e, LogManager.DEBUG_MESSAGE_LEVEL );
        }

        return ( tcentries == null ) ?
                 this.defaultTCEntry( "pegasus",
                                      "seqexec",
                                       null,
                                       siteHandle ): //try using a default one
                 (TransformationCatalogEntry) tcentries.get(0);



    }

    
    /**
     * Retrieves the transformation catalog entry for the executable that is
     * being used to transfer the files in the implementation.
     *
     * @param siteHandle  the handle of the  site where the transformation is
     *                    to be searched.
     *
     * @return  the transformation catalog entry if found, else null.
     */
    public TransformationCatalogEntry getS3TransformationCatalogEntry(String siteHandle){
        List tcentries = null;
        try {
            //namespace and version are null for time being
            tcentries = mTCHandle.getTCEntries( S3.TRANSFORMATION_NAMESPACE,
                                                S3.TRANSFORMATION_NAME,
                                                S3.TRANSFORMATION_VERSION,
                                                siteHandle,
                                                TCType.INSTALLED);
        } catch (Exception e) {
            mLogger.log(
                "Unable to retrieve entry from TC for " +
                Separator.combine( S3.TRANSFORMATION_NAMESPACE,
                                   S3.TRANSFORMATION_NAME,
                                   S3.TRANSFORMATION_VERSION ) +
                " Cause:" + e, LogManager.DEBUG_MESSAGE_LEVEL );
        }

        return ( tcentries == null ) ?
                 null:
                 (TransformationCatalogEntry) tcentries.get(0);



    }


    /**
     * Returns a default TC entry to be used in case entry is not found in the
     * transformation catalog.
     *
     * @param namespace  the namespace of the transfer transformation
     * @param name       the logical name of the transfer transformation
     * @param version    the version of the transfer transformation
     *
     * @param site  the site for which the default entry is required.
     *
     *
     * @return  the default entry.
     */
    protected  TransformationCatalogEntry defaultTCEntry(
                                                          String namespace,
                                                          String name,
                                                          String version,
                                                          String site ){

        TransformationCatalogEntry defaultTCEntry = null;
        //check if PEGASUS_HOME is set
        String home = mSiteStore.getPegasusHome( site );
        //if PEGASUS_HOME is not set, use VDS_HOME
        home = ( home == null )? mSiteStore.getVDSHome( site ): home;

        mLogger.log( "Creating a default TC entry for " +
                     Separator.combine( namespace, name, version ) +
                     " at site " + site,
                     LogManager.DEBUG_MESSAGE_LEVEL );

        //if home is still null
        if ( home == null ){
            //cannot create default TC
            mLogger.log( "Unable to create a default entry for " +
                         Separator.combine( namespace, name, version ) +
                         " as PEGASUS_HOME or VDS_HOME is not set in Site Catalog" ,
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
            append( name );


        defaultTCEntry = new TransformationCatalogEntry( namespace,
                                                         name,
                                                         version );

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
        mLogger.log( "Created entry with path " + defaultTCEntry.getPhysicalTransformation(),
                     LogManager.DEBUG_MESSAGE_LEVEL );
        return defaultTCEntry;
    }


    /**
     * Returns the environment profiles that are required for the default
     * entry to sensibly work.
     *
     * @param site the site where the job is going to run.
     *
     * @return List of environment variables, else null in case where the
     *         required environment variables could not be found.
     */
    protected List getEnvironmentVariables( String site ){
        List result = new ArrayList(2) ;

        //create the CLASSPATH from home
        String globus = mSiteStore.getEnvironmentVariable( site, "GLOBUS_LOCATION" );
        if( globus == null ){
            mLogger.log( "GLOBUS_LOCATION not set in site catalog for site " + site,
                         LogManager.DEBUG_MESSAGE_LEVEL );
            return null;
        }



        //check for LD_LIBRARY_PATH
        String ldpath = mSiteStore.getEnvironmentVariable( site, "LD_LIBRARY_PATH" );
        if ( ldpath == null ){
            //construct a default LD_LIBRARY_PATH
            ldpath = globus;
            //remove trailing / if specified
            ldpath = ( ldpath.charAt( ldpath.length() - 1 ) == File.separatorChar )?
                                ldpath.substring( 0, ldpath.length() - 1 ):
                                ldpath;

            ldpath = ldpath + File.separator + "lib";
            mLogger.log( "Constructed default LD_LIBRARY_PATH " + ldpath,
                         LogManager.DEBUG_MESSAGE_LEVEL );
        }

        //we have both the environment variables
        result.add( new Profile( Profile.ENV, "GLOBUS_LOCATION", globus) );
        result.add( new Profile( Profile.ENV, "LD_LIBRARY_PATH", ldpath) );

        return result;
    }

    
}
