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

package edu.isi.pegasus.planner.transfer.sls;


import edu.isi.pegasus.planner.catalog.site.classes.SiteStore;

import edu.isi.pegasus.common.logging.LogManager;

import edu.isi.pegasus.planner.catalog.transformation.classes.TCType;

import edu.isi.pegasus.common.util.Separator;

import edu.isi.pegasus.planner.transfer.SLS;

import edu.isi.pegasus.planner.catalog.TransformationCatalog;
import edu.isi.pegasus.planner.catalog.transformation.TransformationCatalogEntry;
import edu.isi.pegasus.planner.catalog.ReplicaCatalog;


import edu.isi.pegasus.planner.catalog.site.classes.SiteCatalogEntry;
import edu.isi.pegasus.planner.code.gridstart.PegasusLite;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.classes.FileTransfer;
import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.classes.PegasusFile;
import edu.isi.pegasus.planner.classes.Profile;
import edu.isi.pegasus.planner.namespace.ENV;

import edu.isi.pegasus.planner.common.PegasusProperties;

import java.io.File;

import java.util.Iterator;
import java.util.Set;
import java.util.List;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;

/**
 * This uses the transfer executable distributed with Pegasus to do the
 * second level staging.
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public class Transfer   implements SLS {

    /**
     * The transformation namespace for the transfer job.
     */
    public static final String TRANSFORMATION_NAMESPACE = "pegasus";

    /**
     * The name of the underlying transformation that is queried for in the
     * Transformation Catalog.
     */
    public static final String TRANSFORMATION_NAME = "transfer";

    /**
     * The version number for the transfer job.
     */
    public static final String TRANSFORMATION_VERSION = null;

    /**
     * The derivation namespace for for the transfer job.
     */
    public static final String DERIVATION_NAMESPACE = "pegasus";

    /**
     * The name of the underlying derivation.
     */
    public static final String DERIVATION_NAME = "transfer";

    /**
     * The derivation version number for the transfer job.
     */
    public static final String DERIVATION_VERSION = "1.0";

    /**
     * A short description of the transfer implementation.
     */
    public static final String DESCRIPTION = "Pegasus Transfer Wrapper around GUC";

    /**
     * The executable basename
     */
    public static final String EXECUTABLE_BASENAME = "pegasus-transfer";

    /**
     * The handle to the site catalog.
     */
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
     * The local url prefix for the submit host.
     */
    protected String mLocalURLPrefix;
    
    /**
     * The handle to the transient replica catalog.
     */
    protected ReplicaCatalog mTransientRC;

    /**
     * Any extra arguments that need to be passed ahead to the s3 client invocation.
     */
    protected String mExtraArguments;

    /**
     * Boolean to track whether to stage sls file or not
     */
    protected boolean mStageSLSFile;

    /**
     * Boolean to track whether the gridstart used in PegasusLite or not
     */
    protected boolean mSeqExecGridStartUsed;


    /**
     * The default constructor.
     */
    public Transfer() {
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


        //PM-590 Stricter checks
        //mLocalURLPrefix = mSiteStore.lookup( "local" ).getHeadNodeFS().selectScratchSharedFileServer().getURLPrefix( );
        mLocalURLPrefix = this.selectHeadNodeScratchSharedFileServerURLPrefix( "local" );
        if( mLocalURLPrefix == null ){
            this.complainForHeadNodeURLPrefix( "local" );
        }
        
        mTransientRC = bag.getHandleToTransientReplicaCatalog();
        mExtraArguments = mProps.getSLSTransferArguments();
        mStageSLSFile = mProps.stageSLSFilesViaFirstLevelStaging();
        mSeqExecGridStartUsed = mProps.getGridStart().equals( PegasusLite.CLASSNAME );
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
    public String invocationString( Job job, File slsFile ){
        StringBuffer invocation = new StringBuffer();


        TransformationCatalogEntry entry = this.getTransformationCatalogEntry( job.getSiteHandle() );
        String executable = ( entry == null )?
                             this.getExecutableBasename() ://nothing in the transformation catalog, rely on the executable basenmae
                             entry.getPhysicalTransformation();//rely on what is in the transformation catalog

        
        invocation.append( executable );


        //append any extra arguments set by user
        //in properties
        if( mExtraArguments != null ){
            invocation.append( " " ).append( mExtraArguments );
        }


        if( slsFile != null ){
            //add the required arguments to transfer
            invocation.append( " -f " );
            //we add absolute path if the sls files are staged via
            //first level staging
            if( this.mStageSLSFile ){
                invocation.append( slsFile.getAbsolutePath() );

            }
           else{
                //only the basename
                invocation.append( slsFile.getName() );
            }
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
    public boolean needsSLSInputTransfers( Job job ) {
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
    public boolean needsSLSOutputTransfers( Job job ) {
        Set files = job.getOutputFiles();
        return! (files == null || files.isEmpty());
    }

    /**
     * Returns the LFN of sls input file.
     *
     * @param job Job
     *
     * @return the name of the sls input file.
     */
    public String getSLSInputLFN( Job job ){
        StringBuffer lfn = new StringBuffer();
        lfn.append( "sls_" ).append( job.getName() ).append( ".in" );
        return lfn.toString();
    }


    /**
     * Returns the LFN of sls output file.
     *
     * @param job Job
     *
     * @return the name of the sls input file.
     */
    public String getSLSOutputLFN( Job job ){
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
     * @param stagingSiteDirectory    directory on the head node of the staging site.
     * @param workerNodeDirectory  worker node directory
     *
     * @return a Collection of FileTransfer objects listing the transfers that
     *         need to be done.
     *
     * @see #needsSLSInputTransfers( Job)
     */
    public Collection<FileTransfer>  determineSLSInputTransfers( Job job,
                                      String fileName,
                                      String submitDir,
                                      String stagingSiteDirectory,
                                      String workerNodeDirectory ) {

        //sanity check
        if ( !needsSLSInputTransfers( job ) ){
            mLogger.log( "Not Writing out a SLS input file for job " + job.getName() ,
                         LogManager.DEBUG_MESSAGE_LEVEL );
            return null;
        }


        Set files = job.getInputFiles();

//      To handle for null conditions?
//        File sls = null;
        Collection<FileTransfer> result = new LinkedList();

        //figure out the remote site's headnode gridftp server
        //and the working directory on it.
        //the below should be cached somehow
//        String sourceURLPrefix = mSiteStore.lookup( job.getStagingSiteHandle() ).getHeadNodeFS().selectScratchSharedFileServer().getURLPrefix( );
        //PM-590 stricter checks
        String sourceURLPrefix = this.selectHeadNodeScratchSharedFileServerURLPrefix( job.getStagingSiteHandle() );
        if( sourceURLPrefix == null ){
            this.complainForHeadNodeURLPrefix( job, job.getStagingSiteHandle() );
        }
        
        String sourceDir = stagingSiteDirectory;
        String destDir = workerNodeDirectory;


        PegasusFile pf;

        //To do. distinguish the sls file from the other input files
        for( Iterator it = files.iterator(); it.hasNext(); ){
            pf = ( PegasusFile ) it.next();
            String lfn = pf.getLFN();

            if( lfn.equals( ENV.X509_USER_PROXY_KEY ) ){
                //ignore the proxy file for time being
                //as we picking it from the head node directory
                continue;
            }

            //check if the input file is in the transient RC
            //all files in the DAX should be in the transient RC
            String transientPFN =  mTransientRC.lookup( lfn, job.getSiteHandle() );
            FileTransfer ft = new FileTransfer();
            if( transientPFN == null ){
                //create the default path from the directory
                //on the head node
                StringBuffer url = new StringBuffer();
                url.append( sourceURLPrefix ).append( File.separator );
                url.append( sourceDir ).append( File.separator );
                url.append( lfn );
                ft.addSource( job.getStagingSiteHandle(), url.toString() );
            }
            else{
                //use the location specified in
                //the transient replica catalog
//                    input.write( transientPFN );
                ft.addSource( job.getStagingSiteHandle(), transientPFN );
            }
                

            //destination
            StringBuffer url = new StringBuffer();
            url.append( "file://" ).append( destDir ).append( File.separator ).
                append( pf.getLFN() );
            ft.addDestination( job.getSiteHandle(), url.toString() );

            result.add( ft );
        }
        return result;
    }

    /**
     * Generates a second level staging file of the input files to the worker
     * node directory.
     *
     * @param job the job for which the file is being created
     * @param fileName the name of the file that needs to be written out.
     * @param submitDir the submit directory where it has to be written out.
     * @param stagingSiteDirectory the directory on the head node of the
     *   staging site.
     * @param workerNodeDirectory the worker node directory
     *
     * @return a Collection of FileTransfer objects listing the transfers that
     *         need to be done.
     *
     * @see #needsSLSOutputTransfers( Job)
     */
    public Collection<FileTransfer>  determineSLSOutputTransfers( Job job,
                                       String fileName,
                                       String submitDir,
                                       String stagingSiteDirectory,
                                       String workerNodeDirectory ) {


        //sanity check
        if ( !needsSLSOutputTransfers( job ) ){
            mLogger.log( "Not Writing out a SLS output file for job " + job.getName() ,
                         LogManager.DEBUG_MESSAGE_LEVEL );
            return null;
        }

        //      To handle for null conditions?
//        File sls = null;
        Collection<FileTransfer> result = new LinkedList();

        Set files = job.getOutputFiles();

        //figure out the remote site's headnode gridftp server
        //and the working directory on it.
        //the below should be cached somehow
//        String destURLPrefix = mSiteStore.lookup( job.getStagingSiteHandle() ).getHeadNodeFS().selectScratchSharedFileServer().getURLPrefix();
        //PM-590 stricter checks
        String destURLPrefix = this.selectHeadNodeScratchSharedFileServerURLPrefix( job.getStagingSiteHandle() );
        if( destURLPrefix == null ){
            this.complainForHeadNodeURLPrefix( job, job.getStagingSiteHandle() );
        }
        
        String destDir = stagingSiteDirectory;
        String sourceDir = workerNodeDirectory;

        PegasusFile pf;

        //To do. distinguish the sls file from the other input files
        for( Iterator it = files.iterator(); it.hasNext(); ){
            pf = ( PegasusFile ) it.next();

            FileTransfer ft = new FileTransfer();
            //source
            StringBuffer url = new StringBuffer();
            url.append( "file://" ).append( sourceDir ).append( File.separator ).
                append( pf.getLFN() );
            ft.addSource( job.getSiteHandle(), url.toString() );

            //destination
            url = new StringBuffer();
            url.append( destURLPrefix ).append( File.separator );
            url.append( destDir ).append( File.separator );
            url.append( pf.getLFN() );
            ft.addDestination( job.getStagingSiteHandle(), url.toString() );

            result.add(ft);

        }

        return result;



    }


    /**
     * Modifies a job for the first level staging to headnode.This is to add
     * any files that needs to be staged to the head node for a job specific
     * to the SLS implementation. If any file needs to be added, a <code>FileTransfer</code>
     * object should be created and added as an input or an output file.
     *
     *
     * @param job           the job
     * @param submitDir     the submit directory
     * @param slsInputLFN   the sls input file if required, that is used for
     *                      staging in from the head node to worker node directory.
     * @param slsOutputLFN  the sls output file if required, that is used
     *                      for staging in from the head node to worker node directory.
     * @return boolean
     */
    public boolean modifyJobForFirstLevelStaging( Job job,
                                                  String submitDir,
                                                  String slsInputLFN,
                                                  String slsOutputLFN ) {

        String separator = File.separator;

        //holds the externally accessible path to the directory on the staging site
        String externalWorkDirectoryURL = mSiteStore.getExternalWorkDirectoryURL( job.getStagingSiteHandle() );
        

        //sanity check
        if( !this.mStageSLSFile ){

            //add condor file transfer keys if input and output lfs are not null
            if( slsInputLFN != null ){
                job.condorVariables.addIPFileForTransfer( submitDir + File.separator + slsInputLFN );
            }
            if( slsOutputLFN != null ){
                job.condorVariables.addIPFileForTransfer( submitDir + File.separator + slsOutputLFN );
            }

            return true;
        }

        
        //incorporate the sls input file if required
        if( slsInputLFN != null ){

            FileTransfer ft = new FileTransfer( slsInputLFN, job.getName());

            //the source sls is to be sent across from the local site
            //using the grid ftp server at local site.
            StringBuffer sourceURL = new StringBuffer();
            sourceURL.append( mLocalURLPrefix ).append( separator ).
                append( submitDir ).append(separator).
                append( slsInputLFN );
            ft.addSource("local", sourceURL.toString());

            //the destination URL is the working directory on the filesystem
            //on the head node where the job is to be run.
            StringBuffer destURL = new StringBuffer();
            destURL.append( externalWorkDirectoryURL ).
                    append( separator ).
                    append( slsInputLFN );
            ft.addDestination( job.getStagingSiteHandle(), destURL.toString() );

            //add this as input file for the job
            job.addInputFile( ft );
        }

        //add the sls out file as input to the job
        if( slsOutputLFN != null ){
            FileTransfer ft = new FileTransfer( slsOutputLFN, job.getName() );

            //the source sls is to be sent across from the local site
            //using the grid ftp server at local site.
            StringBuffer sourceURL = new StringBuffer();
            sourceURL.append( mLocalURLPrefix ).append( separator ).
                      append( submitDir ).append( separator ).
                      append( slsOutputLFN );

            ft.addSource( "local" , sourceURL.toString() );

            //the destination URL is the working directory on the filesystem
            //on the head node where the job is to be run.
            StringBuffer destURL = new StringBuffer();
            destURL.append( externalWorkDirectoryURL )
                    .append( separator ).
                    append( slsOutputLFN );

            ft.addDestination( job.getStagingSiteHandle(), destURL.toString()  );

            //add this as input file for the job
            job.addInputFile( ft );
        }

        

       return true;

    }

    /**
     * Modifies a compute job for second level staging. The only modification
     * it does is add the appropriate environment varialbes to the job
     *
     * @param job                    the job to be modified.
     * @param stagingSiteURLPrefix   the url prefix for the server on the staging site
     * @param stagingSitedirectory   the directory on the staging site, where the inp
     * 
     * @param workerNodeDirectory the directory in the worker node tmp
     *
     * @return boolean indicating whether job was successfully modified or
     *   not.
     *
     */
    public boolean modifyJobForWorkerNodeExecution( Job job, 
                                                    String stagingSiteURLPrefix,
                                                    String stagingSitedirectory,
                                                    String workerNodeDirectory ) {



        List envs = this.getEnvironmentVariables( job.getSiteHandle() );

        if( envs == null || envs.isEmpty()){
            //no hard failure.
            mLogger.log( "No special environment set for  " +
                         Separator.combine( this.TRANSFORMATION_NAMESPACE, this.TRANSFORMATION_NAME, this.TRANSFORMATION_VERSION ) +
                         " for job " + job.getID(),
                         LogManager.TRACE_MESSAGE_LEVEL );
            return true;
        }


        for( Iterator it = envs.iterator(); it.hasNext(); ){
            job.envVariables.checkKeyInNS( (Profile)it.next() );
        }

        return true;

    }



    /**
     * Retrieves the transformation catalog entry for the executable that is
     * being used to transfer the files in the implementation. If an entry is
     * not specified in the Transformation Catalog, then null is returned.
     *
     * @param siteHandle  the handle of the  site where the transformation is
     *                    to be searched.
     *
     * @return  the transformation catalog entry if found, else null.
     */
    public TransformationCatalogEntry getTransformationCatalogEntry(String siteHandle){
        List tcentries = null;
        try {
            //namespace and version are null for time being
            tcentries = mTCHandle.lookup( Transfer.TRANSFORMATION_NAMESPACE,
                                                Transfer.TRANSFORMATION_NAME,
                                                Transfer.TRANSFORMATION_VERSION,
                                                siteHandle,
                                                TCType.INSTALLED);
        } catch (Exception e) {
            mLogger.log(
                "Unable to retrieve entry from TC for " +
                Separator.combine( Transfer.TRANSFORMATION_NAMESPACE,
                                   Transfer.TRANSFORMATION_NAME,
                                   Transfer.TRANSFORMATION_VERSION ) +
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
     * @param executableBasename  the basename of the executable
     * @param site  the site for which the default entry is required.
     *
     *
     * @return  the default entry.
     */
    protected  TransformationCatalogEntry defaultTCEntry( String namespace,
                                                          String name,
                                                          String version,
                                                          String executableBasename,
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
            append( Transfer.EXECUTABLE_BASENAME );


        defaultTCEntry = new TransformationCatalogEntry( namespace,
                                                         name,
                                                         version );

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
        mLogger.log( "Created entry with path " + defaultTCEntry.getPhysicalTransformation(),
                     LogManager.DEBUG_MESSAGE_LEVEL );
        return defaultTCEntry;
    }


    /**
     * Returns the environment profiles that are required for the default
     * entry to sensibly work. Tries to retrieve the following variables
     *
     * <pre>
     * PEGASUS_HOME
     * GLOBUS_LOCATION
     * LD_LIBRARY_PATH
     * </pre>
     *
     *
     * @param site the site where the job is going to run.
     *
     * @return List of environment variables, else empty list if none are found
     */
    protected List getEnvironmentVariables( String site ){
        List result = new ArrayList(2) ;

        String pegasusHome =  mSiteStore.getEnvironmentVariable( site, "PEGASUS_HOME" );
        if( pegasusHome != null ){
            //we have both the environment variables
            result.add( new Profile( Profile.ENV, "PEGASUS_HOME", pegasusHome ) );
        }

        String globus = mSiteStore.getEnvironmentVariable( site, "GLOBUS_LOCATION" );
        if( globus != null ){
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
        }

        return result;
    }

    /**
     * Return the executable basename for transfer executable used.
     *
     * @return the executable basename.
     */
    protected String getExecutableBasename() {
        return Transfer.EXECUTABLE_BASENAME;
    }

    /**
     * A convenience method to select the URL Prefix for the FileServer for 
     * the shared scratch space on the HeadNode.
     * 
     * @param site the site for which we need the URL prefix
     * 
     * @return  URL Prefix for the FileServer for the shared scratch space
     * 
     * 
     */
    protected String selectHeadNodeScratchSharedFileServerURLPrefix( String site ){
        return this.selectHeadNodeScratchSharedFileServerURLPrefix( this.mSiteStore.lookup( site ) );
    }
    
    /**
     * A convenience method to select the URL Prefix for the FileServer for 
     * the shared scratch space on the HeadNode.
     * 
     * @param site the entry for the site for which we need the URL prefix
     * 
     * @return  URL Prefix for the FileServer for the shared scratch space
     * 
     * 
     */
    protected String selectHeadNodeScratchSharedFileServerURLPrefix( SiteCatalogEntry entry ){
         
        if( entry == null ){
            return null;
        }
        
        String prefix = entry.selectHeadNodeScratchSharedFileServerURLPrefix();
        if( prefix == null ){
            return null;
        }
        
        return prefix;
    }
    
    /**
     * Complains for head node url prefix not specified
     * 
     * @param site   the site handle
     * 
     * @throws RuntimeException when URL Prefix cannot be determined for various reason.
     */
    protected void complainForHeadNodeURLPrefix( String site ) {
         this.complainForHeadNodeURLPrefix( null, site );
    }

    /**
     * Complains for head node url prefix not specified
     * 
     * @param job    the related job if any
     * @param site   the site handle
     * 
     * @throws RuntimeException when URL Prefix cannot be determined for various reason.
     */
    protected void complainForHeadNodeURLPrefix(Job job, String site ) {
        StringBuffer error = new StringBuffer();
        if( job != null ){
            error.append( "[SLS Transfer] For job (" ).append( job.getID() ).append( ")." );
        }
        error.append( "Unable to determine URL Prefix for the FileServer for scratch shared file system on site: " ).
              append( site );
        throw new RuntimeException( error.toString() );
    }

}
