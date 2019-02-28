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

package edu.isi.pegasus.planner.refiner;

import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.common.util.DynamicLoader;
import edu.isi.pegasus.common.util.FactoryException;
import edu.isi.pegasus.common.util.PegasusURL;
import edu.isi.pegasus.common.util.Separator;
import edu.isi.pegasus.common.util.Version;
import edu.isi.pegasus.planner.catalog.classes.SysInfo;
import edu.isi.pegasus.planner.catalog.site.classes.FileServer;
import edu.isi.pegasus.planner.catalog.site.classes.SiteCatalogEntry;
import edu.isi.pegasus.planner.catalog.site.classes.SiteStore;
import edu.isi.pegasus.planner.catalog.transformation.Mapper;
import edu.isi.pegasus.planner.catalog.transformation.TransformationCatalogEntry;
import edu.isi.pegasus.planner.catalog.transformation.classes.TCType;
import edu.isi.pegasus.planner.classes.ADag;
import edu.isi.pegasus.planner.classes.FileTransfer;
import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.classes.NameValue;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.classes.Profile;
import edu.isi.pegasus.planner.classes.TransferJob;
import edu.isi.pegasus.planner.code.gridstart.PegasusExitCode;
import edu.isi.pegasus.planner.common.CreateWorkerPackage;
import edu.isi.pegasus.planner.common.PegasusConfiguration;
import edu.isi.pegasus.planner.mapper.SubmitMapper;
import edu.isi.pegasus.planner.namespace.Dagman;
import edu.isi.pegasus.planner.namespace.Pegasus;
import edu.isi.pegasus.planner.partitioner.graph.GraphNode;
import edu.isi.pegasus.planner.selector.TransformationSelector;
import edu.isi.pegasus.planner.transfer.Implementation;
import edu.isi.pegasus.planner.transfer.Refiner;
import edu.isi.pegasus.planner.transfer.RemoteTransfer;
import edu.isi.pegasus.planner.transfer.implementation.ImplementationFactory;
import edu.isi.pegasus.planner.transfer.refiner.RefinerFactory;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The refiner that is responsible for adding 
 *  - setup nodes that deploy a worker package on each deployment site at start 
 *    of workflow execution
 *  - cleanup nodes that undeploy a worker package on each deployment site at end
 *    workflow execution
 * 
 * @author Karan Vahi
 * @author Gaurang Mehta
 *
 * @version $Revision: 538 $
 */
public class DeployWorkerPackage
    extends Engine {

    /**
     * Constant suffix for the names of the deployment nodes.
     */
    public static final String DEPLOY_WORKER_PREFIX = "stage_worker_";
    
    /**
     * Constant suffix for the names of the deployment nodes.
     */
    public static final String UNTAR_PREFIX = "untar_";

    /**
     * Constant suffix for the names of the deployment nodes.
     */
    public static final String CLEANUP_PREFIX = "cleanup_";

    /**
     * The arguments for pegasus-exitcode when you only want the log files to be rotated.
     */
    public static final String POSTSCRIPT_ARGUMENTS_FOR_ONLY_ROTATING_LOG_FILE = "-r $RETURN";

    /**
     * Array storing the names of the executables in the $PEGASUS_HOME/bin directory
     * Associates the transformation name with the executable basenames
     */
    public static final String PEGASUS_WORKER_EXECUTABLES[][] = {
        { "transfer", "pegasus-transfer" },
        { "kickstart", "pegasus-kickstart" },
        { "cleanup", "pegasus-transfer" },
        { "seqexec", "pegasus-cluster"},
        { "dirmanager", "pegasus-transfer" },
        { "keg" , "pegasus-keg" },

    };


    /**
     * Store the regular expressions necessary to parse the basename from the worker
     * package url to retrieve the version of pegasus.
     * 
     * pegasus-worker-4.6.0dev-x86_64_macos_10.tar.gz 
     */
    private static final String mRegexExpression =
    //                                 "(pegasus-)(binary|worker)-([0-9]\\.[0-9]\\.[0-9][a-zA-Z]*)-x86.*";
                                        "(pegasus-)(binary|worker)-([0-9]\\.[0-9]\\.[0-9][a-zA-Z0-9]*)-(x86|x86_64|ia64|ppc)_([a-zA-Z0-9]*)_([0-9]*).tar.gz";

    /**
     * The path to be set for create dir jobs.
     */
    public static final String PATH_VALUE = ".:/bin:/usr/bin:/usr/ucb/bin";
    
    /**
     * The default transfer refiner name.
     */
    public static final String DEFAULT_REFINER = "Basic";

    /**
     * The transformation namespace for the worker package
     */
    public static final String TRANSFORMATION_NAMESPACE = "pegasus";

    /**
     * The logical name of the worker package
     */
    public static final String TRANSFORMATION_NAME = "worker";

    /**
     * The version number for the worker package.
     */
    public static final String TRANSFORMATION_VERSION = null;

    /**
     * The transformation namespace for the worker package
     */
    public static final String UNTAR_TRANSFORMATION_NAMESPACE = null;

    /**
     * The logical name of the worker package
     */
    public static final String UNTAR_TRANSFORMATION_NAME = "tar";

    /**
     * The version number for the worker package.
     */
    public static final String UNTAR_TRANSFORMATION_VERSION = null;

    /**
     * The complete TC name for untar.
     */
    public static final String COMPLETE_UNTAR_TRANSFORMATION_NAME = Separator.combine(
                                                                    UNTAR_TRANSFORMATION_NAMESPACE,
                                                                    UNTAR_TRANSFORMATION_NAME,
                                                                    UNTAR_TRANSFORMATION_VERSION  );

    
    /**
     * The complete TC name for pegasus worker package.
     */
    public static final String COMPLETE_TRANSFORMATION_NAME = Separator.combine(
                                                                    TRANSFORMATION_NAMESPACE,
                                                                    TRANSFORMATION_NAME,
                                                                    TRANSFORMATION_VERSION  );

    /**
     * The derivation namespace for the worker package.
     */
    public static final String DERIVATION_NAMESPACE = "pegasus";

    /**
     * The logical name of the transformation for the worker package
     */
    public static final String DERIVATION_NAME = "worker";


    /**
     * The version number for the derivations for worker package.
     */
    public static final String DERIVATION_VERSION = "2.0";

    /**
     * The derivation namespace for the untar job.
     */
    public static final String UNTAR_DERIVATION_NAMESPACE = null;

    /**
     * The logical name of the transformation for the untar job.
     */
    public static final String UNTAR_DERIVATION_NAME = "worker";


    /**
     * The version number for the derivations for untar job.
     */
    public static final String UNTAR_DERIVATION_VERSION = "2.0";

    /**
     * The name of the package in which all the implementing classes are.
     */
    public static final String PACKAGE_NAME = "edu.isi.pegasus.planner.refiner.";

    /**
     * The base directory URL for the builds.
     */
    public static final String BASE_BUILD_DIRECTORY_URL = "http://download.pegasus.isi.edu/pegasus/";
    
    
    /**
     * The version of pegasus matching the planner.
     */
    public static final String PEGASUS_VERSION = Version.instance().toString();

    /**
     * The name of the refiner for purposes of error logging
     */
    public static final String REFINER_NAME = "DeployWorkerPackage";
    
    /**
     * Stores compiled patterns at first use, quasi-Singleton.
     */
    private static Pattern mPattern = null;

    
    /**
     * The map storing OS to corresponding NMI OS platforms.
     */
    private static Map<SysInfo.OS,String> mOSToNMIOSReleaseAndVersion = null;
    
    /**
     * A set of supported OS release and versions that our build process
     * builds for.
     */
    private static Set<String> mSupportedOSReleaseVersions = null;
    
    /**
     * Maps each to OS to a specific OS release for purposes of picking up the 
     * correct worker package for a site. The mapping is to be kept consistent
     * with the NMI builds for the releases.
     * 
     * 
     * @return map
     */
    private static Map<SysInfo.OS,String> osToOSReleaseAndVersion(){
        //singleton access
        if( mOSToNMIOSReleaseAndVersion == null ){
            mOSToNMIOSReleaseAndVersion = new HashMap();
            mOSToNMIOSReleaseAndVersion.put( SysInfo.OS.linux, "rhel_6" );
            mOSToNMIOSReleaseAndVersion.put( SysInfo.OS.macosx, "macos_10" );
        }
        return mOSToNMIOSReleaseAndVersion;
    }
    
    /**
     * A set of OS release and version combinations for which our build
     * processes build Pegasus binaries.
     * 
     * @return 
     */
    private static Set<String> supportedOSReleaseAndVersions(){
        if( mSupportedOSReleaseVersions == null ){
            mSupportedOSReleaseVersions = new HashSet();
            mSupportedOSReleaseVersions.add( "rhel_6" );
            mSupportedOSReleaseVersions.add( "rhel_7" );
            mSupportedOSReleaseVersions.add( "deb_8" );
            mSupportedOSReleaseVersions.add( "deb_9" );
            mSupportedOSReleaseVersions.add( "deb_10" );
            mSupportedOSReleaseVersions.add( "ubuntu_16" );
            mSupportedOSReleaseVersions.add( "ubuntu_17" );
            mSupportedOSReleaseVersions.add( "ubuntu_18" );
            mSupportedOSReleaseVersions.add( "macos_10" );
        }
        return mSupportedOSReleaseVersions;
    }
    
    /**
     * It is a reference to the Concrete Dag so far.
     */
    protected ADag mCurrentDag;


    /**
     * The job prefix that needs to be applied to the job file basenames.
     */
    protected String mJobPrefix;

    /**
     * The transfer implementation to be used for staging in the data as part
     * of setup job.
     */
    protected Implementation mSetupTransferImplementation;

    
    /**
     * The FileTransfer map indexed by site id.
     */
    protected Map<String, FileTransfer> mFTMap;
    
    
    /**
     * Map that indicates whether we need local setup transfer jobs for a site or
     * not.
     */
    protected Map<String, Boolean> mLocalTransfers;
    
    /**
     * Maps a site to the the directory where the pegasus worker package has
     * been untarred during workflow execution.
     */
    protected Map<String,String> mSiteToPegasusHomeMap;
    
    /**
     * The user specified location from where to stage the worker packages.
     */
    protected String mUserSpecifiedSourceLocation;

    /**
     * Boolean indicating whether to use the user specified location or not
     */
    protected boolean mUseUserSpecifiedSourceLocation;
    
    /**
     * Boolean indicating whether user wants the worker package to be transferred
     * or not.
     */
    protected boolean mTransferWorkerPackage;

    /**
     * Boolean indicating worker node execution.
     */
    //protected boolean mWorkerNodeExecution;

    /**
     * Loads the implementing class corresponding to the mode specified by the
     * user at runtime.
     *
     * @param bag      bag of initialization objects
     *
     * @return instance of a DeployWorkerPackage implementation
     *
     * @throws FactoryException that nests any error that
     *         might occur during the instantiation of the implementation.
     */
    public static DeployWorkerPackage loadDeployWorkerPackage( PegasusBag bag ) throws FactoryException {

        //prepend the package name
        String className = PACKAGE_NAME + "DeployWorkerPackage";

        //try loading the class dynamically
        DeployWorkerPackage dp = null;
        DynamicLoader dl = new DynamicLoader(className);
        try {
            Object argList[] = new Object[ 1 ];
            argList[0] = bag;
            dp = (DeployWorkerPackage) dl.instantiate(argList);
        } catch (Exception e) {
            throw new FactoryException( "Instantiating Deploy Worker Package",
                                        className,
                                        e );
        }

        return dp;
    }
    private Refiner mDefaultTransferRefiner;
    private File mSubmitHostWorkerPackage;

    /**
     * A pratically nothing constructor !
     *
     *
     * @param bag      bag of initialization objects
     */
    public DeployWorkerPackage( PegasusBag bag ) {
        super( bag );
        mCurrentDag = null;
        mFTMap = new HashMap();
        mLocalTransfers = new HashMap();
        mSiteToPegasusHomeMap = new HashMap<String,String>();
        mJobPrefix  = bag.getPlannerOptions().getJobnamePrefix();

        mTransferWorkerPackage = mProps.transferWorkerPackage();
        
        //mWorkerNodeExecution   = mProps.executeOnWorkerNode();

        //load the transfer setup implementation
        //To DO . specify type for loading
        /* PM-833
        mSetupTransferImplementation = ImplementationFactory.loadInstance(
                                                          bag,
                                                          ImplementationFactory.TYPE_SETUP );
        */
        mUserSpecifiedSourceLocation = mProps.getBaseSourceURLForSetupTransfers();
        mUseUserSpecifiedSourceLocation =
                  !( mUserSpecifiedSourceLocation == null || mUserSpecifiedSourceLocation.trim().length()== 0 );
    
        Version version = Version.instance();
    }


    /**
     * Initialize with the scheduled graph.  Results in the appropriate
     * population of the transformation catalog with pegasus-worker executables.
     *
     *
     * @param scheduledDAG the scheduled workflow.
     */
    public void initialize( ADag scheduledDAG ) {
        Mapper m = mBag.getHandleToTransformationMapper();

        if( !m.isStageableMapper() ){
            //we want to load a stageable mapper
            mLogger.log( "User set mapper is not a stageable mapper. Loading a stageable mapper ", LogManager.DEBUG_MESSAGE_LEVEL );
            m = Mapper.loadTCMapper( "Staged", mBag );
        }

        
        RemoteTransfer remoteTransfers = new RemoteTransfer( mProps );
        remoteTransfers.buildState();

        Set[] deploymentSites = this.getDeploymentSites( scheduledDAG );
        

        //figure if we need to deploy or not
        if( !mTransferWorkerPackage && ( deploymentSites[1].isEmpty() ) ){
            //PM-810 user specified property is false for worker package
            //and nonsharedfs|condorio deployment is empty
            mLogger.log( "No Deployment of Worker Package needed" ,
                         LogManager.DEBUG_MESSAGE_LEVEL );
            return;
        }

        mLogger.log( "Deployment of Worker Package needed",
                     LogManager.DEBUG_MESSAGE_LEVEL );
        
        //PM-888 we would need to deploy the worker package.
        //create one out of existing pegasus installation on the submit
        //host and set it up for use.
        CreateWorkerPackage cw = new CreateWorkerPackage( mBag );       
        //PM-1046 copy the worker package instead of creating our own
        mSubmitHostWorkerPackage = cw.copy( ); 

        //load the transformation selector. different
        //selectors may end up being loaded for different jobs.
        TransformationSelector txSelector = TransformationSelector.loadTXSelector( mProps.getTXSelectorMode() );

        mDefaultTransferRefiner = RefinerFactory.loadInstance( DeployWorkerPackage.DEFAULT_REFINER, mBag, scheduledDAG ) ;
        /* PM-833
        mSetupTransferImplementation.setRefiner( mDefaultTransferRefiner );
        */

        if( mTransferWorkerPackage && !deploymentSites[0].isEmpty() ){
            //PM-810 for sharedfs case, worker package transfer can only happen
            //if the property is set by the user in the proeprties file
             setupTCForWorkerPackageLocations( deploymentSites[0], m, txSelector, false );
        }
        setupTCForWorkerPackageLocations( deploymentSites[1], m, txSelector, true );
        
    }
    
    /**
     * Sets up the transformation catalog and populates it with executables
     * contained in the Pegasus worker package for the remote compute sites, 
     * and the Pegasus worker package itself
     * 
     * @param sites
     * @param mapper
     * @param selector
     * @param workerNodeExecution boolean indicating whether a job runs on sharedfs or local node filesytem
     */
    protected void setupTCForWorkerPackageLocations( Set sites, 
                                                     Mapper mapper,
                                                     TransformationSelector selector,
                                                     boolean workerNodeExecution) {
        
        //a map indexed by execution site and the corresponding worker package
        //location in the submit directory
        Map<String,String> workerPackageMap = new HashMap<String,String>();

        SiteStore siteStore = mBag.getHandleToSiteStore();

        //for the   pegasus lite case, we insert entries into the
        //transformation catalog for all worker package executables
        //the planner requires with just the basename
        boolean useFullPath = !( workerNodeExecution );
        
        //for each site insert default entries for Pegasus 
        //Worker Package in the Transformation Catalog
        for( Iterator it = sites.iterator(); it.hasNext(); ){
            String site = ( String ) it.next();
            TransformationCatalogEntry entry = this.getTCEntryForPegasusWorkerPackage(mapper, selector, site, workerNodeExecution);
            mLogger.log( "Worker Package Entry used for site " + site + " "  + entry,
                         LogManager.DEBUG_MESSAGE_LEVEL );
            //register back into the transformation catalog
            //so that we do not need to worry about creating it again
            try{
                mTCHandle.insert( entry , false );
            }
            catch( Exception e ){
                //just log as debug. as this is more of a performance improvement
                //than anything else
                mLogger.log( "Unable to register in the TC the default entry " +
                              entry.getLogicalTransformation() +
                              " for site " + site, e,
                              LogManager.ERROR_MESSAGE_LEVEL );
                //throw exception as
                throw new RuntimeException( e );
            }
            
        }
        
        
        //for each scheduled site query TCMapper
        for( Iterator it = sites.iterator(); it.hasNext(); ){
            String site = ( String ) it.next();
            String stagingSite = workerNodeExecution ? 
                                 "local":// For PegasusLite Jobs we stage to the submit directory
                                 site;
            
            //get the set of valid tc entries
            List entries = mapper.getTCList( DeployWorkerPackage.TRANSFORMATION_NAMESPACE,
                                        DeployWorkerPackage.TRANSFORMATION_NAME,
                                        DeployWorkerPackage.TRANSFORMATION_VERSION,
                                        site );

            //get selected entries
            List selectedEntries = selector.getTCEntry( entries, site );
            if( selectedEntries == null || selectedEntries.size() == 0 ){
                throw new RuntimeException( "Unable to find a valid location to stage " +
                                            Separator.combine( DeployWorkerPackage.TRANSFORMATION_NAMESPACE,
                                                               DeployWorkerPackage.TRANSFORMATION_NAME,
                                                               DeployWorkerPackage.TRANSFORMATION_VERSION ) );
            }
            //select the first entry from selector
            TransformationCatalogEntry selected = ( TransformationCatalogEntry )selectedEntries.get( 0 );
            mLogger.log( "Selected entry " + selected, LogManager.DEBUG_MESSAGE_LEVEL );

            FileServer destDirServer = this.getScratchFileServer( stagingSite );
            String destURLPrefix =  destDirServer.getURLPrefix();

            //figure out the directory where to stage the data
            //data will be staged to the staging site corresponding to
            //the execution site and also pick up a FileServer accordingly
            String baseRemoteWorkDir = null;
            String baseRemoteWorkDirURL = null;//externally accessible URL
            if(  workerNodeExecution ){
                //for pegasus-lite the worker package goes
                //to the submit directory on the local site.
                //the staging site is set to local
                baseRemoteWorkDir     = mPOptions.getSubmitDirectory();
                baseRemoteWorkDirURL  = destDirServer.getURLPrefix() + File.separator + baseRemoteWorkDir;

            }
            else{
                //sharedfs case
                baseRemoteWorkDir     = siteStore.getInternalWorkDirectory( stagingSite );
                baseRemoteWorkDirURL  = siteStore.getExternalWorkDirectoryURL( destDirServer, stagingSite );
            }




            if( useFullPath ){
                // we insert entries into the transformation catalog for all worker
                // package executables the planner requires with full paths
                //this is the shared fs case
                String name = getRootDirectoryNameForPegasus( selected.getPhysicalTransformation() );
                File pegasusHome = new File( baseRemoteWorkDir, name );

                StringBuffer sb = new StringBuffer();
                sb.append( "Directory where pegasus worker executables will reside on site ").append( stagingSite ).
                   append( " " ).append( pegasusHome.getAbsolutePath() );
                mLogger.log( sb.toString(), LogManager.DEBUG_MESSAGE_LEVEL );
                mSiteToPegasusHomeMap.put( stagingSite, pegasusHome.getAbsolutePath() );

                
            
                //now create transformation catalog entry objects for each
                //worker package executable
                for( int i = 0; i < PEGASUS_WORKER_EXECUTABLES.length; i++){
                    TransformationCatalogEntry entry = addDefaultTCEntry( stagingSite,
                                                                          pegasusHome.getAbsolutePath(),
                                                                          selected.getSysInfo(),
                                                                          useFullPath,
                                                                          PEGASUS_WORKER_EXECUTABLES[i][0],
                                                                          PEGASUS_WORKER_EXECUTABLES[i][1]  );

                    mLogger.log( "Entry constructed " + entry , LogManager.DEBUG_MESSAGE_LEVEL );
                }
            }
            else{
                // we insert entries into the transformation catalog for all worker
                // package executables the planner requires with relative paths
                // and for the execution sites instead of staging site
                //this is the PegasusLite case
                //now create transformation catalog entry objects for each
                //worker package executable
                for( int i = 0; i < PEGASUS_WORKER_EXECUTABLES.length; i++){
                    TransformationCatalogEntry entry = addDefaultTCEntry( site,
                                                                          null,
                                                                          selected.getSysInfo(),
                                                                          useFullPath,
                                                                          PEGASUS_WORKER_EXECUTABLES[i][0],
                                                                          PEGASUS_WORKER_EXECUTABLES[i][1]  );

                    mLogger.log( "Entry constructed " + entry , LogManager.DEBUG_MESSAGE_LEVEL );
                }
            }

            //create the File Transfer object for shipping the worker executable
            String sourceURL = selected.getPhysicalTransformation();
            FileTransfer ft = new FileTransfer( COMPLETE_TRANSFORMATION_NAME, null );
            ft.addSource( selected.getResourceId(), sourceURL );
            String baseName = sourceURL.substring( sourceURL.lastIndexOf( "/" ) + 1 );
            
            //figure out the URL prefix depending on
            //the TPT configuration                            

            boolean localTransfer = this.runTransferOnLocalSite( mDefaultTransferRefiner, stagingSite, destURLPrefix, Job.STAGE_IN_JOB);
            if( localTransfer ){
                //then we use the external work directory url
                ft.addDestination( stagingSite, baseRemoteWorkDirURL  + File.separator + baseName );
            }
            else{
                ft.addDestination( stagingSite, "file://" + new File( baseRemoteWorkDir, baseName ).getAbsolutePath() );
            }

            if( workerNodeExecution ){
                //populate the map with the submit directory locations
                workerPackageMap.put( site, new File( baseRemoteWorkDir, baseName ).getAbsolutePath() );
            }

            mFTMap.put( site, ft );
            mLocalTransfers.put( stagingSite, localTransfer );
        }
        
        //for pegasus lite and worker package execution
        //we add the worker package map to PegasusBag
        if( workerNodeExecution ){
            mBag.add( PegasusBag.WORKER_PACKAGE_MAP,  workerPackageMap );
        }
    }

    /**
     * Returns whether to run a transfer job on local site or not.
     *
     *
     * @param site  the site handle associated with the destination URL.
     * @param destURL the destination URL
     * @param type  the type of transfer job for which the URL is being constructed.
     *
     * @return true indicating if the associated transfer job should run on local
     *              site or not.
     */
    public boolean runTransferOnLocalSite( Refiner refiner, String site, String destinationURL, int type) {
        //check if user has specified any preference in config
        boolean result = true;

        //short cut for local site
        if( site.equals( "local" ) ){
            //transfer to run on local site
            return result;
        }

        if( refiner.runTransferRemotely( site, type )){
            //always use user preference
            return !result;
        }
        //check to see if destination URL is a file url
        else if( destinationURL != null && destinationURL.startsWith( PegasusURL.FILE_URL_SCHEME ) ){
           result = false;
            
        }

        return result;
    }


    /**
     * Does regex magic to figure out the version of pegasus from the url, and
     * use it to construct the name of pegasus directory, when worker package
     * is untarred.
     *
     * @param url   the url.
     *
     * @return basename for pegasus directory
     */
    protected String getRootDirectoryNameForPegasus( String url ){
        StringBuffer result = new StringBuffer();
        result.append( "pegasus-" );
        //compile the pattern only once.
         if( mPattern == null ){
             mPattern = Pattern.compile( mRegexExpression );
         }

         String base = url.substring( url.lastIndexOf( "/" ) + 1 );
         mLogger.log( "Base is " + base, LogManager.DEBUG_MESSAGE_LEVEL );
         Matcher matcher = mPattern.matcher( base );

         String version = null;
         if(  matcher.matches() ){
             version = matcher.group(3);
         }
         else{
             throw new RuntimeException( "Unable to determine pegasus version from url " + url );
         }


         mLogger.log( "Version is " + version, LogManager.DEBUG_MESSAGE_LEVEL );
         result.append( version );

         return result.toString();

    }
    
    
    /**
     * Determines the sysinfo from the name of a worker package
     *
     * @param name the name
     * 
     * @return SysInfo matching the worker package created
     */
    protected SysInfo determineSysInfo( String name ){
        SysInfo result = new SysInfo();
        
        //compile the pattern only once.
         if( mPattern == null ){
             mPattern = Pattern.compile( mRegexExpression );
         }

         Matcher matcher = mPattern.matcher( name );

         String version = null;
         if(  matcher.matches() ){
             result.setArchitecture(SysInfo.Architecture.valueOf(matcher.group(4)));
             result.setOSRelease( matcher.group(5) );
             result.setOSVersion( matcher.group(6));
         }
         else{
             throw new RuntimeException( "Unable to determine system information of package from name  " + name );
         }
         
         //guess the main OS
         String osrelease = result.getOSRelease();
         if( osrelease.startsWith( "rhel" ) || osrelease.startsWith( "deb" ) || osrelease.startsWith( "ubuntu" ) ||
                osrelease.startsWith( "fc" ) || osrelease.startsWith( "suse" ) ){
            result.setOS(SysInfo.OS.linux );
         }
         else if( osrelease.startsWith( "macos" ) ){
            result.setOS(SysInfo.OS.macosx );
         }


         mLogger.log( "System information for " + name + " is " + result, LogManager.DEBUG_MESSAGE_LEVEL );

         return result;

    }
    
    

    /**
     * Adds a setup node per execution site in the workflow that will stage the
     * worker node executables to the workdirectory on the sites the workflow
     * has been scheduled to.
     *
     * @param dag   the scheduled workflow.
     *
     * @return  the workflow with setup jobs added
     */
    public ADag addSetupNodes( ADag dag ){
        Mapper m = mBag.getHandleToTransformationMapper();
        
        //PM-833 we need to instantiate the setup tx implementation
        //to ensure it has the right submit directory creator associated
        //load the transfer setup implementation
        mSetupTransferImplementation = ImplementationFactory.loadInstance(
                                                          this.mBag,
                                                          ImplementationFactory.TYPE_SETUP );
        mSetupTransferImplementation.setRefiner( mDefaultTransferRefiner );

        //boolean addUntarJobs = !mWorkerNodeExecution;

        Set[] deploymentSites = this.getDeploymentSites( dag );
                
        if( !mTransferWorkerPackage && ( deploymentSites[1].isEmpty() ) ){
            //PM-810 user specified property is false for worker package
            //and nonsharedfs|condorio deployment is empty
            mLogger.log( "No Deployment of Worker Package needed" ,
                         LogManager.DEBUG_MESSAGE_LEVEL );
            return dag;
        }
        
        if( mTransferWorkerPackage && !deploymentSites[0].isEmpty() ){
            //PM-810 for sharedfs case, worker package transfer can only happen
            //if the property is set by the user in the proeprties file
            
            //we add untar nodes only if worker node execution/pegasus lite
            //mode is disabled
            dag =addSetupNodesWithUntarNodes( dag , deploymentSites[0] );//non pegasus lite case. shared fs
        }
        if( !deploymentSites[1].isEmpty() ){
            dag = addSetupNodesWithoutUntarNodes( dag, deploymentSites[1] );
        }
        
       
        return dag;
    }

    /**
     * Adds untar nodes to the workflow, in addition to the stage worker nodes
     * 
     * @param dag  the dag
     * @param deploymentSites    the sites for which the worker package has to be deployed
     * 
     * @return  the workflow in the graph representation with the nodes added.
     */
    private ADag addSetupNodesWithUntarNodes( ADag dag, Set<String> deploymentSites ) {
        //convert the dag to a graph representation and walk it
        //in a top down manner
        //PM-747 no need for conversion as ADag now implements Graph interface
        ADag workflow =  dag;
        
        //get the root nodes of the workflow
        List<GraphNode> roots = workflow.getRoots();

        //add a setup job per execution site
        for( Iterator it = deploymentSites.iterator(); it.hasNext(); ){
            String site = ( String ) it.next();

            //for pegauss lite mode the staging site for worker package
            //should be local site , submit directory.
            //String stagingSite = this.getStagingSite(site);
            String stagingSite = site; //PM-810

            mLogger.log( "Adding worker package deployment node for " + site +
                         " and staging site as " + stagingSite,
                         LogManager.DEBUG_MESSAGE_LEVEL );



            FileTransfer ft = (FileTransfer)mFTMap.get( site );
            List<FileTransfer> fts = new ArrayList<FileTransfer>(1);
            fts.add( ft );
            
            //hmm need to propogate site info with a dummy job on fly
            Job dummy = new Job() ;
            dummy.setSiteHandle( site );

            //stage worker job runs locally or on the staging site
            boolean localTransfer = mLocalTransfers.get( stagingSite ) ;
            String tsite = localTransfer? "local" : stagingSite;
            
            TransferJob setupTXJob = mSetupTransferImplementation.createTransferJob(
                                         dummy,
                                         tsite,
                                         fts,
                                         null,
                                         this.getDeployJobName( dag, site , localTransfer),
                                         Job.STAGE_IN_WORKER_PACKAGE_JOB );

            //the setupTXJob non third party site, has to be the staging site
            setupTXJob.setNonThirdPartySite( stagingSite );

            
            //the setup and untar jobs need to be launched without kickstart.
            setupTXJob.vdsNS.construct( Pegasus.GRIDSTART_KEY, "None" );
            //no empty postscript but arguments to exitcode to add -r $RETURN
            setupTXJob.dagmanVariables.construct( Dagman.POST_SCRIPT_KEY,
                                                  PegasusExitCode.SHORT_NAME );
            setupTXJob.dagmanVariables.construct( Dagman.POST_SCRIPT_ARGUMENTS_KEY,
                                                  POSTSCRIPT_ARGUMENTS_FOR_ONLY_ROTATING_LOG_FILE );

            GraphNode setupNode = new GraphNode( setupTXJob.getName(), setupTXJob );


            //add the untar job
            Job untarJob = this.makeUntarJob( stagingSite,
                                              this.getUntarJobName( dag, site ),
                                              getBasename( ((NameValue)ft.getSourceURL()).getValue() )
                                                  );
            untarJob.vdsNS.construct( Pegasus.GRIDSTART_KEY, "None" );
            untarJob.dagmanVariables.construct( Dagman.POST_SCRIPT_KEY,
                                                  PegasusExitCode.SHORT_NAME );
            untarJob.dagmanVariables.construct( Dagman.POST_SCRIPT_ARGUMENTS_KEY,
                                                POSTSCRIPT_ARGUMENTS_FOR_ONLY_ROTATING_LOG_FILE );
            
            GraphNode untarNode  = new GraphNode( untarJob.getName(), untarJob );

            //untar node is child of setup
            setupNode.addChild( untarNode );
            untarNode.addParent( setupNode );
            
            //add the original roots as children to untar node
            for( Iterator<GraphNode> rIt = roots.iterator(); rIt.hasNext(); ){
                    GraphNode n = ( GraphNode ) rIt.next();
                    mLogger.log( "Added edge " + untarNode.getID() + " -> " + n.getID(),
                                  LogManager.DEBUG_MESSAGE_LEVEL );
                    untarNode.addChild( n );
                    n.addParent( untarNode );
            }
                
            workflow.addNode( untarNode );
            workflow.addNode( setupNode );
        }
        
        return workflow;
        
    }
    
    /**
     * Adds only the stage worker nodes to the workflow. This is used when
     * Pegasus Lite is used to launch the jobs on the execution sites.
     *
     * @param dag  the dag
     * @param deploymentSites    the sites for which the worker package has to be deployed
     * 
     * @return  the workflow in the graph representation with the nodes added.
     */
    private ADag addSetupNodesWithoutUntarNodes( ADag dag, Set<String> deploymentSites ) {
        //convert the dag to a graph representation and walk it
        //in a top down manner
        //PM-747 no need for conversion as ADag now implements Graph interface
        ADag workflow = dag ;
        
        //get the root nodes of the workflow
        List<GraphNode> roots = workflow.getRoots();

        
        Set<FileTransfer> fts = new HashSet<FileTransfer>();

        //for pegauss lite mode the staging site for worker package
        //should be local site , submit directory.
        String stagingSite = "local";

        if( deploymentSites.isEmpty() ){
            //PM-706 in case of condorio and full workflow reduction
            mLogger.log( "Skipping staging of worker package as no deployment site detected ",
                          LogManager.INFO_MESSAGE_LEVEL );
            return workflow;
        }
        
        //stage worker job runs locally or on the staging site
        //PM-706 check for null returns
        boolean localTransfer = mLocalTransfers.containsKey( stagingSite) ?
                                mLocalTransfers.get( stagingSite ) :
                                true;


        //add a setup job per execution site
        for( Iterator it = deploymentSites.iterator(); it.hasNext(); ){
            String site = ( String ) it.next();

            mLogger.log( "Adding worker package deployment node for " + site,
                         LogManager.DEBUG_MESSAGE_LEVEL );

            FileTransfer ft = (FileTransfer)mFTMap.get( site );
            
            //PM-1127 do an extra site if both source and destination
            //site match and are set to local
            NameValue source      = ft.getSourceURL();
            NameValue destination = ft.getSourceURL();
            if( source.getKey().equals( destination.getKey()) &&
                source.getKey().equals( "local") ){
                //make sure the full canonical urls are different
                //only if source and destination urls both are file
                String sourceURL = source.getValue();
                String destURL   = destination.getValue();
                if( sourceURL.startsWith( PegasusURL.FILE_URL_SCHEME ) && destURL.startsWith( PegasusURL.FILE_URL_SCHEME ) ){
                    try {
                        //do the actual canonical check to make sure if the stage worker job
                        //should not be added
                        if( new File(sourceURL).getCanonicalPath().equals( new File(destURL).getCanonicalPath()) ){
                            mLogger.log( "Skipping stage worker job for site " + site + " as worker package already in submit directory " + sourceURL,
                                    LogManager.DEBUG_MESSAGE_LEVEL );
                            continue;
                        }
                    } catch (IOException ex) {
                        mLogger.log( "Error in Canonical compare for " + ft , ex, LogManager.ERROR_MESSAGE_LEVEL);
                    }
                }
            }
            //add the file transfer for stage worker job
            fts.add( ft );
        }//end of for loop
        
        if( fts.isEmpty() ){
            //PM-1127 no file transfer to be done for stage worker job
            //return the workflow unmodified
            return workflow;
        }

        //hmm need to propogate site info with a dummy job on fly
        Job dummy = new Job() ;
        dummy.setSiteHandle( stagingSite );

        String tsite =  "local" ;

        TransferJob setupTXJob = mSetupTransferImplementation.createTransferJob(
                                         dummy,
                                         tsite,
                                         fts,
                                         null,
                                         this.getDeployJobName( dag, tsite , localTransfer),
                                         Job.STAGE_IN_WORKER_PACKAGE_JOB );

        //the setupTXJob is null as stage worker job pulls in
        //data to the submit host directory
        setupTXJob.setNonThirdPartySite( null );

        //the setup and untar jobs need to be launched without kickstart.
        setupTXJob.vdsNS.construct( Pegasus.GRIDSTART_KEY, "None" );
         //no empty postscript but arguments to exitcode to add -r $RETURN
        setupTXJob.dagmanVariables.construct( Dagman.POST_SCRIPT_ARGUMENTS_KEY, 
                                              POSTSCRIPT_ARGUMENTS_FOR_ONLY_ROTATING_LOG_FILE );
        GraphNode setupNode = new GraphNode( setupTXJob.getName(), setupTXJob );


        //add the original roots as children to setup node
        for( Iterator rIt = roots.iterator(); rIt.hasNext(); ){
            GraphNode n = ( GraphNode ) rIt.next();
            mLogger.log( "Added edge " + setupNode.getID() + " -> " + n.getID(),
                                  LogManager.DEBUG_MESSAGE_LEVEL );
            setupNode.addChild( n );
            n.addParent( setupNode );
        }

        workflow.addNode( setupNode );

        return workflow;
    }
    
    /**
     * Retrieves the sites for which the deployment jobs need to be created.
     *
     * @param dag   the dag on which the jobs need to execute.
     *
     * @return  a Set array containing a list of siteID's of the sites where the
     *          dag has to be run. Index[0] has the sites for sharedfs deployment
     *          while Index[1] for ( nonsharedfs and condorio).
     */
    protected Set[] getDeploymentSites( ADag dag ){
        Set[] result = new Set[2];
        result[0]    = new HashSet();
        result[1]    = new HashSet();
        
        Set sharedFSSet    = result[0];
        Set nonsharedFSSet = result[1];
        
        for(Iterator<GraphNode> it = dag.jobIterator();it.hasNext();){
            GraphNode node = it.next();
            Job job = (Job)node.getContent();

            //PM-497
            //we ignore any clean up jobs that may be running
            if( job.getJobType() == Job.CLEANUP_JOB || 
                ( job.getJobType() == Job.STAGE_IN_JOB || job.getJobType() == Job.STAGE_OUT_JOB || job.getJobType() == Job.INTER_POOL_JOB )){
                //we also ignore any remote transfer jobs that maybe associated to run in the work directory
                //because of the fact that staging site may have a file server URL.
                continue;
            }

            //add to the set only if the job is
            //being run in the work directory
            //this takes care of local site create dir
            String conf = job.getDataConfiguration();
            if( conf != null && job.runInWorkDirectory()){
                
                if( conf.equalsIgnoreCase( PegasusConfiguration.SHARED_FS_CONFIGURATION_VALUE) ){
                    sharedFSSet.add( job.getSiteHandle() );
                }
                else{
                    nonsharedFSSet.add( job.getSiteHandle() );
                }
            }
        }
        
        //PM-810 sanity check to make sure sites are not setup
        //for both modes
        Set<String> intersection = new HashSet<String>( sharedFSSet );
        intersection.retainAll( nonsharedFSSet );
        if( !intersection.isEmpty() ){
            throw new RuntimeException( 
                    "Worker package is set to be staged for both sharedfs and nonsharedfs|condorio mode for sites " 
                            + intersection );
        }

        //remove the stork pool
        sharedFSSet.remove("stork");
        nonsharedFSSet.remove( "stork" );
        
        return result;
    }


    /**
     * It returns the name of the deployment  job, that is to be assigned.
     * The name takes into account the workflow name while constructing it, as
     * that is thing that can guarentee uniqueness of name in case of deferred
     * planning.
     *
     * @param dag   the workflow so far.
     * @param site  the execution pool for which the create directory job
     *                  is responsible.
     * @param localTransfer  whether the transfer needs to run locally or not.
     *
     * @return String corresponding to the name of the job.
     */
    protected String getDeployJobName( ADag dag, String site , boolean localTransfer ){
        StringBuffer sb = new StringBuffer();

        //append setup prefix
        sb.append( DeployWorkerPackage.DEPLOY_WORKER_PREFIX );
        
        if( localTransfer ){
            sb.append( Refiner.LOCAL_PREFIX );
        }
        else{
            sb.append( Refiner.REMOTE_PREFIX );
        }
        
        //append the job prefix if specified in options at runtime
        if ( mJobPrefix != null ) { sb.append( mJobPrefix ); }

        sb.append( dag.getLabel() ).append( "_" ).
           append( dag.getIndex() ).append( "_" );
        sb.append( site );

        return sb.toString();
    }

    /**
     * It returns the name of the untar  job, that is to be assigned.
     * The name takes into account the workflow name while constructing it, as
     * that is thing that can guarentee uniqueness of name in case of deferred
     * planning.
     *
     * @param dag   the workflow so far.
     * @param site  the execution pool for which the create directory job
     *                  is responsible.
     *
     * @return String corresponding to the name of the job.
     */
    protected String getUntarJobName( ADag dag, String site ){
        StringBuffer sb = new StringBuffer();

        //append setup prefix
        sb.append( DeployWorkerPackage.UNTAR_PREFIX );
        //append the job prefix if specified in options at runtime
        if ( mJobPrefix != null ) { sb.append( mJobPrefix ); }

        sb.append( dag.getLabel() ).append( "_" ).
           append( dag.getIndex() ).append( "_" );


        sb.append( site );

        return sb.toString();
    }

    /**
     * It returns the name of the untar  job, that is to be assigned.
     * The name takes into account the workflow name while constructing it, as
     * that is thing that can guarentee uniqueness of name in case of deferred
     * planning.
     *
     * @param dag   the workflow so far.
     * @param site  the execution pool for which the create directory job
     *                  is responsible.
     *
     * @return String corresponding to the name of the job.
     */
    protected String getCleanupJobname( ADag dag, String site ){
        StringBuffer sb = new StringBuffer();

        //append setup prefix
        sb.append( DeployWorkerPackage.CLEANUP_PREFIX );
        //append the job prefix if specified in options at runtime
        if ( mJobPrefix != null ) { sb.append( mJobPrefix ); }

        sb.append( dag.getLabel() ).append( "_" ).
           append( dag.getIndex() ).append( "_" );


        sb.append( site );

        return sb.toString();
    }


    /**
     * It creates a untar job , that untars the worker package that is staged
     * by the setup transfer job.
     * 
     * @param site       the execution pool for which the create dir job is to be
     *                   created.
     * @param jobName    the name that is to be assigned to the job.
     * @param wpBasename the basename of the worker package that is staged to remote site.
     *
     * @return create dir job.
     */
    protected Job makeUntarJob( String site, String jobName, String wpBasename ) {
        Job newJob  = new Job();
        List entries    = null;
        String execPath = null;
        TransformationCatalogEntry entry   = null;
//        GridGateway jobManager = null;

        try {
            entries = mTCHandle.lookup( DeployWorkerPackage.UNTAR_TRANSFORMATION_NAMESPACE,
                                              DeployWorkerPackage.UNTAR_TRANSFORMATION_NAME,
                                              DeployWorkerPackage.UNTAR_TRANSFORMATION_VERSION,
                                              site, TCType.INSTALLED);
        }
        catch (Exception e) {
            //non sensical catching
            mLogger.log("Unable to retrieve entries from TC " +
                        e.getMessage(), LogManager.DEBUG_MESSAGE_LEVEL );
        }

        entry = ( entries == null ) ?
            this.defaultUntarTCEntry( mSiteStore.lookup(site) ): //try using a default one
            (TransformationCatalogEntry) entries.get(0);

        if( entry == null ){
            //NOW THROWN AN EXCEPTION

            //should throw a TC specific exception
            StringBuffer error = new StringBuffer();
            error.append("Could not find entry in tc for lfn ").
                append( DeployWorkerPackage.COMPLETE_UNTAR_TRANSFORMATION_NAME ).
                append(" at site ").append( site );

            mLogger.log( error.toString(), LogManager.ERROR_MESSAGE_LEVEL);
            throw new RuntimeException( error.toString() );
        }

        execPath = entry.getPhysicalTransformation();


        SiteCatalogEntry ePool = mSiteStore.lookup( site );
        
        //String argString = "zxvf " + wpBasename;
        // tar -C /tmp/ -zxvf pegasus-worker-2.4.0dev-x86_rhas_3.tar.gz 
        //we want to fully specify the directory where we want tar file
        //untarred
        StringBuffer arguments = new StringBuffer();
        arguments.append( " -C " ).append( mSiteStore.getInternalWorkDirectory( site ) ).
                  append( " -zxvf " ).append( mSiteStore.getInternalWorkDirectory( site ) ).
                  append( File.separator ).append( wpBasename );
        
        newJob.jobName = jobName;
        newJob.setTransformation( DeployWorkerPackage.UNTAR_TRANSFORMATION_NAMESPACE,
                                  DeployWorkerPackage.UNTAR_TRANSFORMATION_NAME,
                                  DeployWorkerPackage.UNTAR_TRANSFORMATION_VERSION );
        newJob.setDerivation( DeployWorkerPackage.UNTAR_DERIVATION_NAMESPACE,
                              DeployWorkerPackage.UNTAR_DERIVATION_NAME,
                              DeployWorkerPackage.UNTAR_DERIVATION_VERSION );
        
        newJob.executable = execPath;
        newJob.executionPool = site;
        //PM-845 set staging site handle to same as execution site of compute job
        newJob.setStagingSiteHandle(site);
        newJob.strargs = arguments.toString();
        
        //for JIRA PM-38 we used to run as a compute job
        //this creates problem with stampede schema as the job is counted
        //towards a dax task. the untar job is classified as chmod job
        //internally now. JIRA PM-326
        newJob.jobClass = Job.CHMOD_JOB;
        newJob.jobID = jobName;

        //the profile information from the pool catalog needs to be
        //assimilated into the job.
        newJob.updateProfiles( ePool.getProfiles() );

        //add any notifications specified in the transformation
        //catalog for the job. JIRA PM-391
        newJob.addNotifications( entry );


        //the profile information from the transformation
        //catalog needs to be assimilated into the job
        //overriding the one from pool catalog.
        newJob.updateProfiles(entry);

        //the profile information from the properties file
        //is assimilated overidding the one from transformation
        //catalog.
        newJob.updateProfiles(mProps);

        return newJob;

    }

    
    /**
     * Returns a default TC entry to be used in case entry is not found in the
     * transformation catalog. It also attempts to add the transformation catalog 
     * entry to the underlying TC store.
     *
     * @param site         the site for which the default entry is required.
     * @param pegasusHome  the path to deployed worker package
     * @param sysinfo      the system information of that site.
     * @param useFullPath  boolean indicating whether to use just the basename or
     *                     the full path
     * @param name         the logical name of the transformation
     * @param executable   the basename of the executable
     *
     *
     * @return  the default entry.
     */
    private  TransformationCatalogEntry addDefaultTCEntry( String site,
                                                        String pegasusHome,
                                                        SysInfo sysinfo,
                                                        boolean useFullPath,
                                                        String name,
                                                        String executable ){
        TransformationCatalogEntry defaultTCEntry = null;

        mLogger.log( "Creating a default TC entry for " +
                     Separator.combine( "pegasus", name, null ) +
                     " at site " + site,
                     LogManager.DEBUG_MESSAGE_LEVEL );


        //construct the path to the executable
        StringBuffer path = new StringBuffer();
        if( useFullPath ){
            path.append( pegasusHome ).append( File.separator ).
                 append( "bin" ).append( File.separator );
        }
        path.append( executable );

        mLogger.log( "Remote Path set is " + path.toString(),
                     LogManager.DEBUG_MESSAGE_LEVEL );

        defaultTCEntry = new TransformationCatalogEntry( "pegasus",
                                                         name ,
                                                         null );

        defaultTCEntry.setPhysicalTransformation( path.toString() );
        defaultTCEntry.setResourceId( site );
        defaultTCEntry.setType( TCType.INSTALLED );
        defaultTCEntry.setSysInfo( sysinfo );

        if( useFullPath ){
            //add pegasus home as an environment variable
            defaultTCEntry.addProfile( new Profile( Profile.ENV, "PEGASUS_HOME", pegasusHome ));
        }
        
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
                          LogManager.ERROR_MESSAGE_LEVEL );
            //throw exception as
            throw new RuntimeException( e );
        }
        
        
        return defaultTCEntry;
    }

    
    /**
     * Returns a transformation catalog entry to be used for the pegasus worker
     * package location. The following rules are followed for determining the
     * entry
     * <pre>
     *    - if a pegasus::worker entry exists in the trasnformation catalog for the site, it is used
     *    - else, the worker package based on the submit host install is used if
     *          a) user specified osrelease for the site in the site catalog and there is a 
     *             complete match for sysinfo ( os, osrelease, version, and arch)
     *             with the submit host wp OR
     *          b) just the OS and architecture of remote site matches the submit host created worker package
     *    - else, the worker package is pulled from the Pegasus website.
     * </pre>
     * 
     * @param mapper
     * @param selector
     * @param site        the execution site for which we need a matching static binary.
     * @param workerNodeExecution boolean indicating whether worker package is 
     *                     staged  to compute site via a staging site or not.
     *                            
     *
     *
     * @return  the default entry.
     */
    protected TransformationCatalogEntry getTCEntryForPegasusWorkerPackage(
                                                                            Mapper mapper,
                                                                            TransformationSelector selector,
                                                                            String site, 
                                                                            boolean workerNodeExecution) {
       
        //check if there is a valid entry for worker package
        List<TransformationCatalogEntry> entries, selectedEntries = null;
        try{
            entries = mapper.getTCList( DeployWorkerPackage.TRANSFORMATION_NAMESPACE,
                                   DeployWorkerPackage.TRANSFORMATION_NAME,
                                   DeployWorkerPackage.TRANSFORMATION_VERSION,
                                   site );

            selectedEntries = selector.getTCEntry( entries , site);
        }catch( Exception e ){ /*ignore*/}

        if( selectedEntries != null && selectedEntries.size() > 0 ){
            //PM-888
            //We prefer whatever a user wants us to use as a worker package
            //we return the first selection.
            return selectedEntries.get(0);
        }
        
        TransformationCatalogEntry entry = null;
        
        //check to see if the one created on the submit host is
        //suffficient to use for the site.
        SysInfo remoteSiteSysInfo = mSiteStore.getSysInfo( site );  
        SysInfo submitHostSysInfo = this.determineSysInfo( this.mSubmitHostWorkerPackage.getName() );
        mLogger.log( "Compute site sysinfo " + site + " "  + remoteSiteSysInfo , LogManager.DEBUG_MESSAGE_LEVEL);
                
        boolean useSubmitHostWF = false;
        if( remoteSiteSysInfo.getOSRelease() != null && remoteSiteSysInfo.getOSRelease().length() > 0 ){
            if (remoteSiteSysInfo.equals( submitHostSysInfo )){
                //user has specified a specific architecture in the site catalog
                //we can only use the worker package created on the submit host, if
                //they match completely
                useSubmitHostWF = true;
            }
        }
        else{
            //osrelease is not specified. 
            //just do a shallow match on OS and architecture
            if( remoteSiteSysInfo.getOS().equals( submitHostSysInfo.getOS() ) &&
                   remoteSiteSysInfo.getArchitecture().equals( submitHostSysInfo.getArchitecture() )){
                useSubmitHostWF = true;
            }
        }
        
        if( useSubmitHostWF ){
            //sanity check to see if a transfer of the worker package from
            //local site to compute site will even work based on scratch
            //dir url of compute site. we do this check only if stage worker
            //job has to transfer package to the compute site and not to a
            //staging site
            if( !site.equals( "local") && !workerNodeExecution ){
                //need to check for the site scratch dir url
                FileServer stagingSiteServer = this.getScratchFileServer(site);
                if( stagingSiteServer.getURLPrefix().startsWith( PegasusURL.FILE_URL_SCHEME) ){
                    //if staging site is a file URL then we disable
                    //using the worker package created on the submit host
                    mLogger.log( "Not using the worker package from the submit host install " + mSubmitHostWorkerPackage + 
                                 " The scratch file server is a file url scheme for compute site " + site,
                                 LogManager.DEBUG_MESSAGE_LEVEL );
                    useSubmitHostWF = false; 
                }
            }
        }
        
        if( useSubmitHostWF ){
            //create a default transformation catalog entry for the submit host location
            entry = new TransformationCatalogEntry( DeployWorkerPackage.TRANSFORMATION_NAMESPACE,
                                                    DeployWorkerPackage.TRANSFORMATION_NAME ,
                                                    null );

            entry.setPhysicalTransformation( PegasusURL.FILE_URL_SCHEME  + "//" + mSubmitHostWorkerPackage );
            entry.setResourceId( "local" );
            entry.setType( TCType.STAGEABLE );
            //we deliberately set the sysinfo for this to be the remote site
            //so that it allows for exact matches later on
            entry.setSysInfo( remoteSiteSysInfo );
            
        }
        else{
           //try and create a default entry for pegasus::worker if 
            entry = this.getDefaultTCEntryForPegasusWorkerPackage( site );
            if( entry == null ){
                StringBuffer error = new StringBuffer();
                error.append( "Unable to construct default entry for pegasus::worker for site " ).append( site )
                     .append( " Add entry in TC for pegasus::worker of type STAGEABLE for sysinfo ")
                     .append(  mSiteStore.getSysInfo( site ) );
                throw new RuntimeException( error.toString() );
            }
        }
        
        return entry;
    }
    
    /**
     * Returns a default TC entry for the Pegasus worker package from the
     * Pegasus website. The entry points to 
     * the http webserver on the pegasus website. It also attempts to add 
     * the transformation catalog entry to the TC store.   
     * 
     * @param site        the execution site for which we need a matching static binary.
     *
     *
     * @return  the default entry.
     */
    protected TransformationCatalogEntry getDefaultTCEntryForPegasusWorkerPackage( 
                                                                String site  ){
        TransformationCatalogEntry defaultTCEntry = null;
       
        //String site = "pegasus";
        SysInfo sysinfo = mSiteStore.getSysInfo( site );
        if( sysinfo == null ){
            mLogger.log( "Unable to get System Information for site " + site,
                         LogManager.ERROR_MESSAGE_LEVEL  );
            return null;
        }

        //construct the path to the executable
        String path = constructDefaultURLToPegasusWorkerPackage( DeployWorkerPackage.TRANSFORMATION_NAME, sysinfo );
        if( path == null ){
            mLogger.log( "Unable to determine path for worker package for " + sysinfo,
                         LogManager.DEBUG_MESSAGE_LEVEL );
            return null;
        }
        
        mLogger.log( "Creating a default TC entry for " +
                     Separator.combine( "pegasus", DeployWorkerPackage.TRANSFORMATION_NAME, null ) +
                     " at site pegasus for sysinfo " + sysinfo,
                     LogManager.DEBUG_MESSAGE_LEVEL );

        mLogger.log( "Remote Path set is " + path.toString(),
                     LogManager.DEBUG_MESSAGE_LEVEL );

        defaultTCEntry = new TransformationCatalogEntry( DeployWorkerPackage.TRANSFORMATION_NAMESPACE,
                                                         DeployWorkerPackage.TRANSFORMATION_NAME ,
                                                         null );

        defaultTCEntry.setPhysicalTransformation( path );
        defaultTCEntry.setResourceId( "pegasus" );
        defaultTCEntry.setType( TCType.STAGEABLE );
        defaultTCEntry.setSysInfo( sysinfo );

        return defaultTCEntry;
    }
    
    /**
     * Constructs the default URL's for the pegasus worker package. If the user
     * has not specified the URL to the source directory in Pegaus Properties then
     * the URL constructed points to the  pegasus website.
     * 
     * The version of Pegasus retrieved is the one against which the planner
     * is executing.
     * 
     * @param name     the logical name of the executable, usually worker|binary.
     * @param sysinfo  the sysinfo for which the path is required.
     * 
     * @return url
     */
    protected String constructDefaultURLToPegasusWorkerPackage( String name, SysInfo sysinfo ) {
        //get the matching architecture
        //String arch = ( String )DeployWorkerPackage.archToNMIArch().get( sysinfo.getArch() );
        String arch = sysinfo.getArchitecture().toString();
        
        //PM-732 lets figure out if a user has specified an OS release or OS version
        //or not
        String osRelease = sysinfo.getOSRelease();
        String osVersion = sysinfo.getOSVersion();
        //for mac there is only single OSRelease
        osRelease = ( sysinfo.getOS() == SysInfo.OS.macosx ) ? "macos" : osRelease;
        
        String osReleaseAndVersion = null;
        if ( ( osRelease != null && osRelease.length() != 0 ) &&
             ( osVersion != null && osVersion.length() != 0 ) ){
            //if both os release and version are specified by user
            //then we know user is being specific. check to see if it
            //is built by our built system
            osReleaseAndVersion = osRelease + "_" + osVersion;
            if( !DeployWorkerPackage.supportedOSReleaseAndVersions().contains(osReleaseAndVersion) ){
                //set back to null  and log a warning
                mLogger.log( "Worker Package Deployment: Unsupported os release and version " + osReleaseAndVersion + " for OS " + sysinfo.getOS() +
                             " . Will rely on default package for the OS.",
                             LogManager.WARNING_MESSAGE_LEVEL );
                osReleaseAndVersion = null;
            }
        }
        
        //if osReleaseAndVersion is null, then construct a default one based on OS
        if( osReleaseAndVersion == null){ 
            osReleaseAndVersion = ( String )DeployWorkerPackage.osToOSReleaseAndVersion().get( sysinfo.getOS() );
        }
        
        if( arch == null || osReleaseAndVersion == null ){
            mLogger.log( "Unable to construct url for pegasus worker for " + sysinfo , 
                         LogManager.DEBUG_MESSAGE_LEVEL );
            return null;
        }
     
        StringBuffer url = new StringBuffer();
        
        //construct the base path
        if( mUseUserSpecifiedSourceLocation ){
            url.append( mUserSpecifiedSourceLocation ).append( File.separator);   
        }
        else{
            url.append( DeployWorkerPackage.BASE_BUILD_DIRECTORY_URL ).
                append( PEGASUS_VERSION ).append( "/" );
        }
        
        url.append( "pegasus-" ).append( name ).append( "-" ).
            append( PEGASUS_VERSION ).append( "-" ).
            append( arch ).append( "_" ).
            append( osReleaseAndVersion ).append( ".tar.gz" );
            
        return url.toString();
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
    private  TransformationCatalogEntry defaultUntarTCEntry( SiteCatalogEntry site ){
        TransformationCatalogEntry defaultTCEntry = null;

        mLogger.log( "Creating a default TC entry for " +
                     DeployWorkerPackage.COMPLETE_UNTAR_TRANSFORMATION_NAME +
                     " at site " + site,
                     LogManager.DEBUG_MESSAGE_LEVEL );


        //construct the path to the executable
        StringBuffer path = new StringBuffer();
        path.append( "/bin/tar" );

        mLogger.log( "Remote Path set is " + path.toString(),
                     LogManager.DEBUG_MESSAGE_LEVEL );

        defaultTCEntry = new TransformationCatalogEntry( DeployWorkerPackage.UNTAR_TRANSFORMATION_NAMESPACE,
                                                         DeployWorkerPackage.UNTAR_TRANSFORMATION_NAME ,
                                                         DeployWorkerPackage.UNTAR_TRANSFORMATION_VERSION );

        defaultTCEntry.setPhysicalTransformation( path.toString() );
        defaultTCEntry.setResourceId( site.getSiteHandle() );
        defaultTCEntry.setType( TCType.INSTALLED );
        defaultTCEntry.setSysInfo( site.getSysInfo());

        //add path as an environment variable
        //addDefaultTCEntry.addProfile( new Profile( Profile.ENV, "PATH", DeployWorkerPackage.PATH_VALUE ));

        
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
                          LogManager.ERROR_MESSAGE_LEVEL );
            //throw exception as
            throw new RuntimeException( e );
        }
        
        
        return defaultTCEntry;
    }

    
    /**
     * Returns the basename of the URL using substring.
     * 
     * @param url
     * 
     * @return basename
     */
    protected String getBasename( String url ){
        
        
        return ( url == null || url.length() == 0 )? 
                 null:
                 url.substring( url.lastIndexOf( File.separator ) + 1 );
    }

    /**
     * A convenience method to return a scratch file server on a staging site.
     * The method ensures that FileServer is populated sufficiently with the url
     * prefix
     * 
     * @param site
     * 
     * @return FileServer
     * 
     * @throws RuntimeException when URL Prefix cannot be determined for various reason.
     */
    protected FileServer getScratchFileServer( String site ){
        SiteCatalogEntry stagingSiteEntry = this.mSiteStore.lookup( site );
        FileServer fileServer = ( stagingSiteEntry == null )?
                                null:
                                stagingSiteEntry.selectHeadNodeScratchSharedFileServer( FileServer.OPERATION.put );
        String destURLPrefix =  ( fileServer == null )?
                                    null:
                                    fileServer.getURLPrefix();
        if( destURLPrefix == null ){
            this.complainForHeadNodeURLPrefix( REFINER_NAME, site , FileServer.OPERATION.put);
        }
        
        return fileServer;
    }
    
}
