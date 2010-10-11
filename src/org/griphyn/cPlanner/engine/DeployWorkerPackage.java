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

import edu.isi.pegasus.planner.catalog.site.classes.SiteCatalogEntry;
import edu.isi.pegasus.planner.catalog.site.classes.SiteStore;
import edu.isi.pegasus.planner.catalog.site.classes.GridGateway;

import org.griphyn.cPlanner.classes.ADag;
import org.griphyn.cPlanner.classes.SubInfo;
import org.griphyn.cPlanner.classes.PegasusBag;
import org.griphyn.cPlanner.classes.FileTransfer;
import org.griphyn.cPlanner.classes.Profile;
import org.griphyn.cPlanner.classes.NameValue;


import edu.isi.pegasus.common.logging.LogManager;

import org.griphyn.cPlanner.partitioner.graph.GraphNode;
import org.griphyn.cPlanner.partitioner.graph.Graph;
import org.griphyn.cPlanner.partitioner.graph.Adapter;



import org.griphyn.cPlanner.namespace.Pegasus;

import edu.isi.pegasus.planner.transfer.Implementation;
import edu.isi.pegasus.planner.transfer.implementation.ImplementationFactory;
import edu.isi.pegasus.planner.transfer.Refiner;
import edu.isi.pegasus.planner.transfer.refiner.RefinerFactory;

import edu.isi.pegasus.planner.selector.TransformationSelector;

import edu.isi.pegasus.common.util.DynamicLoader;
import edu.isi.pegasus.common.util.FactoryException;
import edu.isi.pegasus.common.util.Separator;
import edu.isi.pegasus.common.util.Version;

import edu.isi.pegasus.planner.catalog.classes.SysInfo;
import edu.isi.pegasus.planner.catalog.transformation.Mapper;

import edu.isi.pegasus.planner.catalog.transformation.TransformationCatalogEntry;

import edu.isi.pegasus.planner.catalog.transformation.classes.TCType;

import edu.isi.pegasus.planner.transfer.RemoteTransfer;
import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Set;
import java.util.HashSet;
import java.util.Map;
import java.util.HashMap;

import java.io.File;

import java.util.LinkedList;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

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
     * Array storing the names of the executables in the $PEGASUS_HOME/bin directory
     * Associates the transformation name with the executable basenames
     */
    public static final String PEGASUS_WORKER_EXECUTABLES[][] = {
        { "T2", "T2" },
        { "transfer", "transfer" },
        { "kickstart", "kickstart" },
        { "cleanup", "pegasus-cleanup" },
        { "seqexec", "seqexec"},
        { "dirmanager", "pegasus-dirmanager" },
        { "invoke", "invoke" },
        { "keg" , "keg" },
        { "symlink", "symlink"},
        { "pegasus-transfer", "pegasus-transfer" }

    };


    /**
     * Store the regular expressions necessary to parse the basename from the worker
     * package url to retrieve the version of pegasus.
     */
    private static final String mRegexExpression =
    //                                 "(pegasus-)(binary|worker)-([0-9]\\.[0-9]\\.[0-9][a-zA-Z]*)-x86.*";
                                        "(pegasus-)(binary|worker)-([0-9]\\.[0-9]\\.[0-9][a-zA-Z]*)-(x86|x86_64|ia64|ppc).*";

    /**
     * The path to be set for create dir jobs.
     */
    public static final String PATH_VALUE = ".:/bin:/usr/bin:/usr/ucb/bin";
    
    /**
     * The default transfer refiner name.
     */
    public static final String DEFAULT_REFINER = "Default";

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
    public static final String PACKAGE_NAME = "org.griphyn.cPlanner.engine.";

    /**
     * The base directory URL for the builds.
     */
    public static final String BASE_BUILD_DIRECTORY_URL = "http://pegasus.isi.edu/wms/download/";
    
    
    /**
     * The version of pegasus matching the planner.
     */
    public static final String PEGASUS_VERSION = Version.instance().toString();

    
    /**
     * Stores compiled patterns at first use, quasi-Singleton.
     */
    private static Pattern mPattern = null;

    
    /**
     * The map storing OS to corresponding NMI OS platforms.
     */
    private static Map<SysInfo.OS,String> mOSToNMIOSReleaseAndVersion = null;
    
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
            mOSToNMIOSReleaseAndVersion.put( SysInfo.OS.LINUX, "rhel_5" );
            mOSToNMIOSReleaseAndVersion.put( SysInfo.OS.MACOSX, "macos_10.4" );
        }
        return mOSToNMIOSReleaseAndVersion;
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
     * The major minor version that is used to construct the URL for the
     * pegasus website.
     */
    protected String mPlannerMajorMinorVersion;

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

        //load the transfer setup implementation
        //To DO . specify type for loading
        mSetupTransferImplementation = ImplementationFactory.loadInstance(
                                                          bag,
                                                          ImplementationFactory.TYPE_SETUP );
        
        mUserSpecifiedSourceLocation = mProps.getBaseSourceURLForSetupTransfers();
        mUseUserSpecifiedSourceLocation =
                  !( mUserSpecifiedSourceLocation == null || mUserSpecifiedSourceLocation.trim().length()== 0 );
    
        Version version = Version.instance();
        StringBuffer sb = new StringBuffer();
        sb.append( version.MAJOR ).append( "." ).append( version.MINOR );
        mPlannerMajorMinorVersion = sb.toString();
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
        SiteStore siteStore = mBag.getHandleToSiteStore();
        
        RemoteTransfer remoteTransfers = new RemoteTransfer( mProps );
        remoteTransfers.buildState();

        

        //figure if we need to deploy or not
        if( !mTransferWorkerPackage ){
            mLogger.log( "No Deployment of Worker Package needed" ,
                         LogManager.DEBUG_MESSAGE_LEVEL );
            return;
        }

        mLogger.log( "Deployment of Worker Package needed" ,
                     LogManager.DEBUG_MESSAGE_LEVEL );

        //load the transformation selector. different
        //selectors may end up being loaded for different jobs.
        TransformationSelector txSelector = TransformationSelector.loadTXSelector( mProps.getTXSelectorMode() );

        Refiner defaultRefiner = RefinerFactory.loadInstance( DeployWorkerPackage.DEFAULT_REFINER, mBag, scheduledDAG ) ;
        mSetupTransferImplementation.setRefiner( defaultRefiner );
        
        //for each site insert default entries in the Transformation Catalog
        //for each scheduled site query TCMapper
        Set deploymentSites = this.getDeploymentSites( scheduledDAG );
        for( Iterator it = deploymentSites.iterator(); it.hasNext(); ){
            String site = ( String ) it.next();
            
            //check if there is a valid entry for worker package
            List entries, selectedEntries = null;
            try{
                entries = m.getTCList( DeployWorkerPackage.TRANSFORMATION_NAMESPACE,
                                       DeployWorkerPackage.TRANSFORMATION_NAME,
                                       DeployWorkerPackage.TRANSFORMATION_VERSION,
                                       site );

                selectedEntries = txSelector.getTCEntry( entries );
            }catch( Exception e ){ /*ignore*/}
            
            //try and create a default entry for pegasus::worker if 
             //not specified in transformation catalog
            if( selectedEntries == null || selectedEntries.size() == 0 ){
                TransformationCatalogEntry entry = this.addDefaultTCEntryForPegasusWebsite( site, DeployWorkerPackage.TRANSFORMATION_NAME );
                if( entry == null ){
                    StringBuffer error = new StringBuffer();
                    error.append( "Unable to construct default entry for pegasus::worker for site " ).append( site )
                         .append( " Add entry in TC for pegasus::worker of type STATIC_BINARY for sysinfo ")
                         .append(  mSiteStore.getSysInfo( site ) );
                    throw new RuntimeException( error.toString() );
                }
            }
            
        }
        
        //for each scheduled site query TCMapper
        for( Iterator it = deploymentSites.iterator(); it.hasNext(); ){
            String site = ( String ) it.next();
            //get the set of valid tc entries
            List entries = m.getTCList( DeployWorkerPackage.TRANSFORMATION_NAMESPACE,
                                        DeployWorkerPackage.TRANSFORMATION_NAME,
                                        DeployWorkerPackage.TRANSFORMATION_VERSION,
                                        site );

            //get selected entries
            List selectedEntries = txSelector.getTCEntry( entries );
            if( selectedEntries == null || selectedEntries.size() == 0 ){
                throw new RuntimeException( "Unable to find a valid location to stage " +
                                            Separator.combine( DeployWorkerPackage.TRANSFORMATION_NAMESPACE,
                                                               DeployWorkerPackage.TRANSFORMATION_NAME,
                                                               DeployWorkerPackage.TRANSFORMATION_VERSION ) );
            }
            //select the first entry from selector
            TransformationCatalogEntry selected = ( TransformationCatalogEntry )selectedEntries.get( 0 );
            mLogger.log( "Selected entry " + selected, LogManager.DEBUG_MESSAGE_LEVEL );

            //figure out the directory where to stage the data
            String baseRemoteWorkDir = siteStore.getWorkDirectory( site );
            String name = getRootDirectoryNameForPegasus( selected.getPhysicalTransformation() );
            File pegasusHome = new File( baseRemoteWorkDir, name );

            StringBuffer sb = new StringBuffer();
            sb.append( "Directory where pegasus worker executables will reside on site ").append( site ).
               append( " " ).append( pegasusHome.getAbsolutePath() );
            mLogger.log( sb.toString(), LogManager.DEBUG_MESSAGE_LEVEL );
            mSiteToPegasusHomeMap.put( site, pegasusHome.getAbsolutePath() );
            
            //now create transformation catalog entry objects for each
            //worker package executable
            for( int i = 0; i < PEGASUS_WORKER_EXECUTABLES.length; i++){
                TransformationCatalogEntry entry = addDefaultTCEntry( site,
                                                                      pegasusHome.getAbsolutePath(),
                                                                      selected.getSysInfo(),
                                                                      PEGASUS_WORKER_EXECUTABLES[i][0],
                                                                      PEGASUS_WORKER_EXECUTABLES[i][1]  );

                mLogger.log( "Entry constructed " + entry , LogManager.DEBUG_MESSAGE_LEVEL );
            }

            //create the File Transfer object for shipping the worker executable
            String sourceURL = selected.getPhysicalTransformation();
            FileTransfer ft = new FileTransfer( COMPLETE_TRANSFORMATION_NAME, null );
            ft.addSource( selected.getResourceId(), sourceURL );
            String baseName = sourceURL.substring( sourceURL.lastIndexOf( "/" ) + 1 );
            
            //figure out the URL prefix depending on
            //the TPT configuration
            String destURLPrefix = 
                               siteStore.lookup( site ).getHeadNodeFS().selectScratchSharedFileServer().getURLPrefix();
            
            boolean localTransfer = this.runTransferOnLocalSite( defaultRefiner, site, destURLPrefix, SubInfo.STAGE_IN_JOB);
            String urlPrefix =  localTransfer ?
                               //lookup the site catalog to get the URL prefix
                               destURLPrefix :
                               //push pull mode. File URL will do
                               "file://";
            ft.addDestination( site, urlPrefix + new File( baseRemoteWorkDir, baseName ).getAbsolutePath() );
            mFTMap.put( site, ft );
            mLocalTransfers.put( site, localTransfer );
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
        else if( destinationURL != null && destinationURL.startsWith( TransferEngine.FILE_URL_SCHEME ) ){
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
        
        //figure if we need to deploy or not
        if( !mTransferWorkerPackage ){
            mLogger.log( "No Deployment of Worker Package needed" ,
                         LogManager.DEBUG_MESSAGE_LEVEL );
            return dag;
        }

        //convert the dag to a graph representation and walk it
        //in a top down manner
        Graph workflow = Adapter.convert( dag );


        //get the root nodes of the workflow
        List roots = workflow.getRoots();

        //add a setup job per execution site
        Set deploymentSites = this.getDeploymentSites( dag );
        for( Iterator it = deploymentSites.iterator(); it.hasNext(); ){
            String site = ( String ) it.next();
            mLogger.log( "Adding worker package deployment node for " + site,
                         LogManager.DEBUG_MESSAGE_LEVEL );


            FileTransfer ft = (FileTransfer)mFTMap.get( site ); 
            List<FileTransfer> fts = new ArrayList<FileTransfer>(1);
            fts.add( ft );
            
            //hmm need to propogate site info with a dummy job on fly
            SubInfo dummy = new SubInfo() ;
            dummy.setSiteHandle( site );
            
            boolean localTransfer = mLocalTransfers.get( site ) ;
            String tsite = localTransfer? "local" : site;
            
            SubInfo setupTXJob = mSetupTransferImplementation.createTransferJob(
                                         dummy,
                                         tsite,
                                         fts,
                                         null,
                                         this.getDeployJobName( dag, site , localTransfer),
                                         SubInfo.STAGE_IN_JOB );

            
            //add the untar job
            SubInfo untarJob = this.makeUntarJob( site,
                                                  this.getUntarJobName( dag, site ),  
                                                  getBasename( ((NameValue)ft.getSourceURL()).getValue() )
                                                  );
            
            //the setup and untar jobs need to be launched without kickstart.
            setupTXJob.vdsNS.construct( Pegasus.GRIDSTART_KEY, "None" );
            untarJob.vdsNS.construct( Pegasus.GRIDSTART_KEY, "None" );
            
            GraphNode untarNode  = new GraphNode( untarJob.getName(), untarJob );
            GraphNode setupNode = new GraphNode( setupTXJob.getName(), setupTXJob );

            //untar node is child of setup
            setupNode.addChild( untarNode );
            
            //add the original roots as children to untar node 
            for( Iterator rIt = roots.iterator(); rIt.hasNext(); ){
                GraphNode n = ( GraphNode ) rIt.next();
                mLogger.log( "Added edge " + untarNode.getID() + " -> " + n.getID(),
                              LogManager.DEBUG_MESSAGE_LEVEL );
                untarNode.addChild( n );
            }

            workflow.addNode( setupNode );
            workflow.addNode( untarNode );
        }

       
        //convert back to ADag and return
        ADag result = dag;
        //we need to reset the jobs and the relations in it
        result.clearJobs();

        //traverse through the graph and jobs and edges
        for( Iterator it = workflow.nodeIterator(); it.hasNext(); ){
            GraphNode node = ( GraphNode )it.next();

            //get the job associated with node
            result.add( ( SubInfo )node.getContent() );

            //all the children of the node are the edges of the DAG
            for( Iterator childrenIt = node.getChildren().iterator(); childrenIt.hasNext(); ){
                GraphNode child = ( GraphNode ) childrenIt.next();
                result.addNewRelation( node.getID(), child.getID() );
            }
        }

        return result;
    }

    /**
     * Adds cleanup nodes in the workflow for sites specified.
     * 
     * @param dag  the workflow
     *
     * @return workflow with cleanup jobs added
     */
    public ADag addCleanupNodesForWorkerPackage( ADag dag ) {
        Mapper m = mBag.getHandleToTransformationMapper();
        
        //figure if we need to deploy or not
        if( !mTransferWorkerPackage ){
            mLogger.log( "No cleanup of Worker Package needed" ,
                         LogManager.DEBUG_MESSAGE_LEVEL );
            return dag;
        }
        
        //convert the dag to a graph representation and walk it
        //in a top down manner
        Graph workflow = Adapter.convert( dag );

        RemoveDirectory removeDirectory = new RemoveDirectory( dag, mBag );
        
        //add a setup job per execution site
        Set sites = this.getDeploymentSites( dag );       
        for( Iterator it = sites.iterator(); it.hasNext(); ){
            String site = ( String ) it.next();
            mLogger.log( "Adding worker package cleanup node for " + site,
                         LogManager.DEBUG_MESSAGE_LEVEL );

            String baseRemoteWorkDir = mSiteStore.getWorkDirectory( site );

            //figure out what needs to be deleted for the site
            FileTransfer ft = (FileTransfer)mFTMap.get( site ); 
            List<String> cleanupFiles = new LinkedList<String>();
            cleanupFiles.add( new File ( baseRemoteWorkDir,
                                         getBasename( ft.getSourceURL().getValue() )).getAbsolutePath() );
            cleanupFiles.add( mSiteToPegasusHomeMap.get( site ) );
            cleanupFiles.add(  new File ( baseRemoteWorkDir,
                                          this.getDeployJobName( dag, site, this.mLocalTransfers.get( site ) ) + ".in" ).getAbsolutePath() );//to remove the GUC .in file
            for( String f : cleanupFiles ){
                StringBuffer sb = new StringBuffer();
                sb.append( "Need to cleanup file " ).append( f ).append( " on site " ).append( site );
                mLogger.log( sb.toString(),
                             LogManager.DEBUG_MESSAGE_LEVEL );
            }
            
            //create a remove directory job per site
            String cleanupJobname = this.getCleanupJobname( dag, site );
            SubInfo cleanupJob = removeDirectory.makeRemoveDirJob( site, cleanupJobname, cleanupFiles);
                
            //add the original leaves as parents to cleanup node 
            for( Iterator lIt = workflow.getLeaves().iterator(); lIt.hasNext(); ){                
                GraphNode gn = ( GraphNode ) lIt.next();
                mLogger.log( "Added edge " + gn.getID() + " -> " + cleanupJobname,
                              LogManager.DEBUG_MESSAGE_LEVEL );
                
                GraphNode cleanupNode = new GraphNode( cleanupJob.getName(), cleanupJob );
                cleanupNode.addParent( gn );
                gn.addChild( cleanupNode );
                
                workflow.addNode( cleanupNode );
            }
        }
        
        //convert back to ADag and return
        ADag result = dag;
        //we need to reset the jobs and the relations in it
        result.clearJobs();

        //traverse through the graph and jobs and edges
        for( Iterator it = workflow.nodeIterator(); it.hasNext(); ){
            GraphNode node = ( GraphNode )it.next();

            //get the job associated with node
            result.add( ( SubInfo )node.getContent() );

            //all the children of the node are the edges of the DAG
            for( Iterator childrenIt = node.getChildren().iterator(); childrenIt.hasNext(); ){
                GraphNode child = ( GraphNode ) childrenIt.next();
                result.addNewRelation( node.getID(), child.getID() );
            }
        }

        return result;
    }
 
    
    /**
     * Retrieves the sites for which the deployment jobs need to be created.
     *
     * @param dag   the dag on which the jobs need to execute.
     *
     * @return  a Set containing a list of siteID's of the sites where the
     *          dag has to be run.
     */
    protected Set getDeploymentSites( ADag dag ){
        Set set = new HashSet();

        for(Iterator it = dag.vJobSubInfos.iterator();it.hasNext();){
            SubInfo job = (SubInfo)it.next();
            //add to the set only if the job is
            //being run in the work directory
            //this takes care of local site create dir
            if(job.runInWorkDirectory()){
                set.add(job.executionPool);
            }
        }

        //remove the stork pool
        set.remove("stork");

        return set;
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

        sb.append( dag.dagInfo.nameOfADag ).append( "_" ).
           append( dag.dagInfo.index ).append( "_" );
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

        sb.append( dag.dagInfo.nameOfADag ).append( "_" ).
           append( dag.dagInfo.index ).append( "_" );


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

        sb.append( dag.dagInfo.nameOfADag ).append( "_" ).
           append( dag.dagInfo.index ).append( "_" );


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
    protected SubInfo makeUntarJob( String site, String jobName, String wpBasename ) {
        SubInfo newJob  = new SubInfo();
        List entries    = null;
        String execPath = null;
        TransformationCatalogEntry entry   = null;
        GridGateway jobManager = null;

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
        jobManager = ePool.selectGridGateway( GridGateway.JOB_TYPE.transfer );
        //String argString = "zxvf " + wpBasename;
        // tar -C /tmp/ -zxvf pegasus-worker-2.4.0cvs-x86_rhas_3.tar.gz 
        //we want to fully specify the directory where we want tar file
        //untarred
        StringBuffer arguments = new StringBuffer();
        arguments.append( " -C " ).append( mSiteStore.getWorkDirectory( site ) ).
                  append( " -zxvf " ).append( mSiteStore.getWorkDirectory( site ) ).
                  append( File.separator ).append( wpBasename );
        
        newJob.jobName = jobName;
        newJob.setTransformation( DeployWorkerPackage.UNTAR_TRANSFORMATION_NAMESPACE,
                                  DeployWorkerPackage.UNTAR_TRANSFORMATION_NAME,
                                  DeployWorkerPackage.UNTAR_TRANSFORMATION_VERSION );
        newJob.setDerivation( DeployWorkerPackage.UNTAR_DERIVATION_NAMESPACE,
                              DeployWorkerPackage.UNTAR_DERIVATION_NAME,
                              DeployWorkerPackage.UNTAR_DERIVATION_VERSION );
//        newJob.condorUniverse = "vanilla";
        newJob.condorUniverse = GridGateway.JOB_TYPE.auxillary.toString();
        newJob.globusScheduler = jobManager.getContact();
        newJob.executable = execPath;
        newJob.executionPool = site;
        newJob.strargs = arguments.toString();
        
        //hack for Pegasus bug 41. for local site the untar job
        //needs to have initialdir specified. that is only set for 
        //compute jobs.
        newJob.jobClass = SubInfo.COMPUTE_JOB;
        newJob.jobID = jobName;

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
        newJob.updateProfiles(mProps);

        return newJob;

    }

    
    /**
     * Returns a default TC entry to be used in case entry is not found in the
     * transformation catalog. It also attempts to add the transformation catalog 
     * entry to the underlying TC store.
     *
     * @param site   the site for which the default entry is required.
     * @param pegasusHome  the path to deployed worker package
     * @param sysinfo the system information of that site.
     * @param name        the logical name of the transformation
     * @param executable  the basename of the executable
     *
     *
     * @return  the default entry.
     */
    private  TransformationCatalogEntry addDefaultTCEntry( String site,
                                                        String pegasusHome,
                                                        SysInfo sysinfo,
                                                        String name,
                                                        String executable ){
        TransformationCatalogEntry defaultTCEntry = null;

        mLogger.log( "Creating a default TC entry for " +
                     Separator.combine( "pegasus", name, null ) +
                     " at site " + site,
                     LogManager.DEBUG_MESSAGE_LEVEL );


        //construct the path to the executable
        StringBuffer path = new StringBuffer();
        path.append( pegasusHome ).append( File.separator ).
            append( "bin" ).append( File.separator ).
            append( executable );

        mLogger.log( "Remote Path set is " + path.toString(),
                     LogManager.DEBUG_MESSAGE_LEVEL );

        defaultTCEntry = new TransformationCatalogEntry( "pegasus",
                                                         name ,
                                                         null );

        defaultTCEntry.setPhysicalTransformation( path.toString() );
        defaultTCEntry.setResourceId( site );
        defaultTCEntry.setType( TCType.INSTALLED );
        defaultTCEntry.setSysInfo( sysinfo );

        //add pegasus home as an environment variable
        defaultTCEntry.addProfile( new Profile( Profile.ENV, "PEGASUS_HOME", pegasusHome ));

        
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
     * Returns a default TC entry for the pegasus site. The entry points to 
     * the http webserver on the pegasus website. It also attempts to add 
     * the transformation catalog entry to the TC store.   
     * 
     * @param site        the execution site for which we need a matching static binary.
     * @param name        logical name of the transformation
     *
     *
     * @return  the default entry.
     */
    protected TransformationCatalogEntry addDefaultTCEntryForPegasusWebsite( 
                                                                String site,
                                                                String name ){
        TransformationCatalogEntry defaultTCEntry = null;
       
        //String site = "pegasus";
        SysInfo sysinfo = mSiteStore.getSysInfo( site );
        if( sysinfo == null ){
            mLogger.log( "Unable to get System Information for site " + site,
                         LogManager.ERROR_MESSAGE_LEVEL  );
            return null;
        }

        //construct the path to the executable
        String path = constructDefaultURLToPegasusWorkerPackage( name, sysinfo );
        if( path == null ){
            mLogger.log( "Unable to determine path for worker package for " + sysinfo,
                         LogManager.DEBUG_MESSAGE_LEVEL );
            return null;
        }
        
        mLogger.log( "Creating a default TC entry for " +
                     Separator.combine( "pegasus", name, null ) +
                     " at for sysinfo " + sysinfo,
                     LogManager.DEBUG_MESSAGE_LEVEL );

        mLogger.log( "Remote Path set is " + path.toString(),
                     LogManager.DEBUG_MESSAGE_LEVEL );

        defaultTCEntry = new TransformationCatalogEntry( "pegasus",
                                                         name ,
                                                         null );

        defaultTCEntry.setPhysicalTransformation( path );
        defaultTCEntry.setResourceId( "pegasus" );
        defaultTCEntry.setType( TCType.STATIC_BINARY );
        defaultTCEntry.setSysInfo( sysinfo );


        
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
        String os   = ( String )DeployWorkerPackage.osToOSReleaseAndVersion().get( sysinfo.getOS() );
        
        if( arch == null || os == null ){
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
                append( this.mPlannerMajorMinorVersion ).append( "/" );
            url.append( PEGASUS_VERSION.endsWith( "cvs" ) ?
                    "nightly/" :
                    "" );
        }
        
        url.append( "pegasus-" ).append( name ).append( "-" ).
            append( PEGASUS_VERSION ).append( "-" ).
            append( arch ).append( "_" ).
            append( os ).append( ".tar.gz" );
            
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

    
}
