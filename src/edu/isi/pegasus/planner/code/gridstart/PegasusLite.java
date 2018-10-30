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

package edu.isi.pegasus.planner.code.gridstart;

import edu.isi.pegasus.common.logging.LogManager;

import edu.isi.pegasus.common.util.DefaultStreamGobblerCallback;
import edu.isi.pegasus.common.util.StreamGobbler;
import edu.isi.pegasus.common.util.StreamGobblerCallback;
import edu.isi.pegasus.common.util.Version;

import edu.isi.pegasus.planner.catalog.TransformationCatalog;
import edu.isi.pegasus.planner.catalog.replica.ReplicaCatalogEntry;

import edu.isi.pegasus.planner.catalog.site.classes.Directory;

import edu.isi.pegasus.planner.catalog.site.classes.FileServer;
import edu.isi.pegasus.planner.catalog.site.classes.SiteCatalogEntry;
import edu.isi.pegasus.planner.catalog.site.classes.SiteStore;
import edu.isi.pegasus.planner.catalog.transformation.Mapper;
import edu.isi.pegasus.planner.catalog.transformation.TransformationCatalogEntry;
import edu.isi.pegasus.planner.catalog.transformation.classes.TCType;

import edu.isi.pegasus.planner.classes.ADag;
import edu.isi.pegasus.planner.classes.AggregatedJob;
import edu.isi.pegasus.planner.classes.DAGJob;
import edu.isi.pegasus.planner.classes.DAXJob;
import edu.isi.pegasus.planner.classes.FileTransfer;
import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.classes.NameValue;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.classes.PegasusFile;
import edu.isi.pegasus.planner.classes.PlannerOptions;
import edu.isi.pegasus.planner.classes.TransferJob;

import edu.isi.pegasus.planner.code.GridStart;

import edu.isi.pegasus.planner.code.gridstart.container.ContainerShellWrapper;
import edu.isi.pegasus.planner.code.gridstart.container.ContainerShellWrapperFactory;

import edu.isi.pegasus.planner.common.PegasusConfiguration;
import edu.isi.pegasus.planner.common.PegasusProperties;

import edu.isi.pegasus.planner.namespace.Condor;
import edu.isi.pegasus.planner.namespace.Namespace;
import edu.isi.pegasus.planner.namespace.Pegasus;


import edu.isi.pegasus.planner.refiner.DeployWorkerPackage;

import edu.isi.pegasus.planner.selector.ReplicaSelector;

import edu.isi.pegasus.planner.transfer.SLS;
import edu.isi.pegasus.planner.transfer.sls.SLSFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * This class launches all the jobs using Pegasus Lite a shell script based wrapper.
 *
 * The Pegasus Lite shell script for the compute jobs contains the commands to
 * 
 * <pre>
 * 1) create directory on worker node
 * 2) fetch input data files
 * 3) execute the job
 * 4) transfer the output data files
 * 5) cleanup the directory
 * </pre>
 *
 * 
 * The following property should be set to false to disable the staging of the 
 * SLS files via the first level staging jobs
 * 
 * <pre>
 * pegasus.transfer.stage.sls.file     false
 * </pre>
 *
 * To enable this implementation at runtime set the following property
 * <pre>
 * pegasus.gridstart PegasusLite
 * </pre>
 *
 * 
 * @author Karan Vahi
 * @version $Revision$
 */

public class PegasusLite implements GridStart {
    private PegasusBag mBag;
    private ADag mDAG;

    public static final String SEPARATOR = "########################";
    public static final char SEPARATOR_CHAR = '#';
    public static final String  MESSAGE_PREFIX = "[Pegasus Lite]";
    public static final int  MESSAGE_STRING_LENGTH = 80;
    
    /**
     * The basename of the class that is implmenting this. Could have
     * been determined by reflection.
     */
    public static final String CLASSNAME = "PegasusLite";

    /**
     * The SHORTNAME for this implementation.
     */
    public static final String SHORT_NAME = "pegasus-lite";


    /**
     * The basename of the pegasus lite common shell functions file.
     */
    public static final String PEGASUS_LITE_COMMON_FILE_BASENAME = "pegasus-lite-common.sh";

    /**
     * The logical name of the transformation that creates directories on the
     * remote execution pools.
     */
    public static final String XBIT_TRANSFORMATION = "chmod";


    /**
     * The basename of the pegasus dirmanager  executable.
     */
    public static final String XBIT_EXECUTABLE_BASENAME = "chmod";


    /**
     * The transformation namespace for the setXBit jobs.
     */
    public static final String XBIT_TRANSFORMATION_NS = "system";

    /**
     * The version number for the derivations for setXBit  jobs.
     */
    public static final String XBIT_TRANSFORMATION_VERSION = null;

    /**
     * The derivation namespace for the setXBit  jobs.
     */
    public static final String XBIT_DERIVATION_NS = "system";

    /**
     * The version number for the derivations for setXBit  jobs.
     */
    public static final String XBIT_DERIVATION_VERSION = null;
  
    /**
     * The pegasus lite exitcode success message.
     */
    public static final String PEGASUS_LITE_EXITCODE_SUCCESS_MESSAGE = "PegasusLite: exitcode 0";

    /**
     * The environment variable to set if we want a job to execute in 
     * a particular directory on the worker node
     */
    public static final String WORKER_NODE_DIRECTORY_KEY = "PEGASUS_WN_TMP";
    
    /**
     * The environment/shell variable that if set points to the file where
     * PegasusLite log goes.
     */
    public static final String PEGASUS_LITE_LOG_ENV_KEY = "pegasus_lite_log_file";
    
    /**
     * Stores the major version of the planner.
     */
    private String mMajorVersionLevel;

    /**
     * Stores the major version of the planner.
     */
    private String mMinorVersionLevel;

    /**
     * Stores the major version of the planner.
     */
    private String mPatchVersionLevel;


    /**
     * The LogManager object which is used to log all the messages.
     */
    protected LogManager mLogger;

    /**
     * The object holding all the properties pertaining to Pegasus.
     */
    protected PegasusProperties mProps;

    /**
     * The submit directory where the submit files are being generated for
     * the workflow.
     */
    protected String mSubmitDir;

    /**
     * The argument string containing the arguments with which the exitcode
     * is invoked on kickstart output.
     */
//    protected String mExitParserArguments;

    /**
     * A boolean indicating whether to generate lof files or not.
     */
    protected boolean mGenerateLOF;

    /**
     * A boolean indicating whether to have worker node execution or not.
     */
    //protected boolean mWorkerNodeExecution;

    //PM-810 done throught PegasusConfiguration
    /**
     * Handle to PegasusConfiguration
     */
    private PegasusConfiguration mPegasusConfiguration;
    
    /**
     * The handle to the SLS implementor
     */
    protected SLSFactory mSLSFactory;

    /**
     * The options passed to the planner.
     */
    protected PlannerOptions mPOptions;


    /**
     * Handle to the site catalog store.
     */
    //protected PoolInfoProvider mSiteHandle;
    protected SiteStore mSiteStore;

    /**
     * An instance variable to track if enabling is happening as part of a clustered job.
     * See Bug 21 comments on Pegasus Bugzilla
     */
    protected boolean mEnablingPartOfAggregatedJob;

    /**
     * Handle to kickstart GridStart implementation.
     */
    private Kickstart mKickstartGridStartImpl;

   
     /**
     * Handle to Transformation Catalog.
     */
    private TransformationCatalog mTCHandle;

    /**
     * Boolean to track whether to stage sls file or not
     */
    protected boolean mStageSLSFile;


    /**
     * The local path on the submit host to pegasus-lite-common.sh
     */
    protected String mLocalPathToPegasusLiteCommon;

    /**
     * Boolean indicating whether worker package transfer is enabled or not
     */
    protected boolean mTransferWorkerPackage;

    /** A map indexed by execution site and the corresponding worker package
     *location in the submit directory
     */
    Map<String,String> mWorkerPackageMap ;

    /**
     * A map indexed by the execution site and value is the path to chmod on
     * that site.
     */
    private Map<String,String> mChmodOnExecutionSiteMap;
    
    /**
     * Indicates whether to enforce strict checks against the worker package 
     * provided for jobs in PegasusLite mode.
     * if a job comes with worker package and it does not match fully with
     * worker node architecture , PegasusLite will revert to Pegasus download website.
     */
    protected boolean mEnforceStrictChecksOnWPVersion;
    
    /**
     * This member variable if set causes the destination URL for the symlink jobs
     * to have symlink:// url if the pool attributed associated with the pfn
     * is same as a particular jobs execution pool. 
     */
    protected boolean mUseSymLinks;
    
    /**
     * Whether PegasusLite should download from the worker package from website
     * in any case or not
     */
    protected boolean mAllowWPDownloadFromWebsite;
    

    /**
     * The shell wrapper to use to wrap job in container
     */
    protected ContainerShellWrapper mContainerWrapper;

    /*
     * Factory for Container Shell Wrapper
     */
    protected ContainerShellWrapperFactory mContainerWrapperFactory;

    
    /**
     * Initializes the GridStart implementation.
     *
     *  @param bag   the bag of objects that is used for initialization.
     * @param dag   the concrete dag so far.
     */
    public void initialize( PegasusBag bag, ADag dag ){
        mBag       = bag;
        mDAG       = dag;
        mLogger    = bag.getLogger();
        mSiteStore = bag.getHandleToSiteStore();
        mPOptions  = bag.getPlannerOptions();
        mSubmitDir = mPOptions.getSubmitDirectory();
        mProps     = bag.getPegasusProperties();
        mGenerateLOF  = mProps.generateLOFFiles();
        mTCHandle  = bag.getHandleToTransformationCatalog();

        mTransferWorkerPackage = mProps.transferWorkerPackage();
        mWorkerPackageMap = bag.getWorkerPackageMap();
        if( mWorkerPackageMap == null ){
                mWorkerPackageMap = new HashMap<String,String>();
        }
        mEnforceStrictChecksOnWPVersion = mProps.enforceStrictChecksForWorkerPackage();
        mUseSymLinks                    = mProps.getUseOfSymbolicLinks();
        mAllowWPDownloadFromWebsite     = mProps.allowDownloadOfWorkerPackageFromPegasusWebsite();
        
        mChmodOnExecutionSiteMap = new HashMap<String,String>();

        Version version = Version.instance();
        mMajorVersionLevel = version.getMajor();
        mMinorVersionLevel = version.getMinor();
        mPatchVersionLevel = version.getPatch();

        mPegasusConfiguration = new PegasusConfiguration( bag.getLogger() );
        
        mSLSFactory = new SLSFactory();
        mSLSFactory.initialize(bag);
        
        //pegasus lite needs to disable invoke functionality
        mProps.setProperty( PegasusProperties.DISABLE_INVOKE_PROPERTY, "true" );

        mEnablingPartOfAggregatedJob = false;
        mKickstartGridStartImpl = new Kickstart();
        mKickstartGridStartImpl.initialize( bag, dag );
        //for pegasus lite we dont want ot use the full path, unless
        //a user has specifically catalogued in the transformation catalog
        mKickstartGridStartImpl.useFullPathToGridStarts( false );

        //for pegasus-lite work, worker node execution is no
        //longer handled in kickstart/no kickstart cases
        //mKickstartGridStartImpl.mWorkerNodeExecution = false;

        mStageSLSFile = mProps.stageSLSFilesViaFirstLevelStaging();

        
        mLocalPathToPegasusLiteCommon = getSubmitHostPathToPegasusLiteCommon( );
        mContainerWrapperFactory = new ContainerShellWrapperFactory();
        mContainerWrapperFactory.initialize(bag, dag);
        
    }
    
    /**
     * Enables a job to run on the grid. This also determines how the
     * stdin,stderr and stdout of the job are to be propogated.
     * To grid enable a job, the job may need to be wrapped into another
     * job, that actually launches the job. It usually results in the job
     * description passed being modified modified.
     *
     * @param job  the <code>Job</code> object containing the job description
     *             of the job that has to be enabled on the grid.
     * @param isGlobusJob is <code>true</code>, if the job generated a
     *        line <code>universe = globus</code>, and thus runs remotely.
     *        Set to <code>false</code>, if the job runs on the submit
     *        host in any way.
     *
     * @return boolean true if enabling was successful,else false.
     */
    public boolean enable( AggregatedJob job,boolean isGlobusJob){

        //in pegasus lite mode we dont want kickstart to change or create
        //worker node directories
        for( Iterator it = job.constituentJobsIterator(); it.hasNext() ; ){
            Job j = (Job) it.next();
            j.vdsNS.construct( Pegasus.CHANGE_DIR_KEY , "false" );
            j.vdsNS.construct( Pegasus.CREATE_AND_CHANGE_DIR_KEY, "false" );
        }

        //for time being we treat clustered jobs same as normal jobs
        //in pegasus-lite
        return this.enable( (Job)job, isGlobusJob );

    }


    /**
     * Enables a job to run on the grid by launching it directly. It ends
     * up running the executable directly without going through any intermediate
     * launcher executable. It connects the stdio, and stderr to underlying
     * condor mechanisms so that they are transported back to the submit host.
     *
     * @param job  the <code>Job</code> object containing the job description
     *             of the job that has to be enabled on the grid.
     * @param isGlobusJob is <code>true</code>, if the job generated a
     *        line <code>universe = globus</code>, and thus runs remotely.
     *        Set to <code>false</code>, if the job runs on the submit
     *        host in any way.
     *
     * @return boolean true if enabling was successful,else false in case when
     *         the path to kickstart could not be determined on the site where
     *         the job is scheduled.
     */
    public boolean enable(Job job, boolean isGlobusJob) {
        //take care of relative submit directory if specified
        String submitDir = mSubmitDir + mSeparator;


        
        
        //consider case for non worker node execution first
        if( !mPegasusConfiguration.jobSetupForWorkerNodeExecution( job ) ){
            //shared filesystem case.
            StringBuilder error = new StringBuilder();
            error.append( "Job " ).append( job.getID() ).
                  append( " cannot be wrapped with PegasusLite. Invalid data.configuration associated " ).
                  append( job.getDataConfiguration() );
            throw new RuntimeException( error.toString() );
                
        }
        enableForWorkerNodeExecution( job, isGlobusJob );

        if( mGenerateLOF ){
            //but generate lof files nevertheless
            //inefficient check here again. just a prototype
            //we need to generate -S option only for non transfer jobs
            //generate the list of filenames file for the input and output files.
            if (! (job instanceof TransferJob)) {
                generateListofFilenamesFile( job.getInputFiles(),
                                             job.getID() + ".in.lof");
            }

            //for cleanup jobs no generation of stats for output files
            if (job.getJobType() != Job.CLEANUP_JOB) {
                generateListofFilenamesFile(job.getOutputFiles(),
                                           job.getID() + ".out.lof");

            }
        }///end of mGenerateLOF

        return true;
    }

    /**
     * Enables jobs for worker node execution.
     *
     *
     *
     * @param job           the job to be enabled.
     * @param isGlobusJob is <code>true</code>, if the job generated a
     *        line <code>universe = globus</code>, and thus runs remotely.
     *        Set to <code>false</code>, if the job runs on the submit
     *        host in any way.
     */
    private void enableForWorkerNodeExecution(Job job, boolean isGlobusJob ) {

        if (job.getJobType() == Job.COMPUTE_JOB
                || 
                //PM-971 all auxillary jobs that are setup to be
                //run on non local sites should also be wrapped
                //with PegasusLite. Todo? maybe have no distinction in future
                (job.getJobType() != Job.COMPUTE_JOB
                && !job.getSiteHandle().equals("local"))) {

            //in pegasus lite mode we dont want kickstart to change or create
            //worker node directories
            job.vdsNS.construct( Pegasus.CHANGE_DIR_KEY , "false" );
            job.vdsNS.construct( Pegasus.CREATE_AND_CHANGE_DIR_KEY, "false" );

            File jobWrapper = wrapJobWithPegasusLite( job, isGlobusJob );

            //the job wrapper requires the common functions file
            //from the submit host
            job.condorVariables.addIPFileForTransfer( this.mLocalPathToPegasusLiteCommon );

            //PM-1225 worker package transfer is only triggered for non sub dax jobs
            if ( !(job instanceof DAXJob) ){
                //figure out transfer of worker package for compute jobs
                if( mTransferWorkerPackage ){
                    //sanity check to see if PEGASUS_HOME is defined
                    if( mSiteStore.getEnvironmentVariable( job.getSiteHandle(), "PEGASUS_HOME" ) == null ){
                        //yes we need to add from the location in the worker package map
                        String location = this.mWorkerPackageMap.get( job.getSiteHandle() );

                        if( location == null ){
                            throw new RuntimeException( "Unable to figure out worker package location for job " + job.getID() );
                        }
                        job.condorVariables.addIPFileForTransfer(location);
                    }
                    else{
                        mLogger.log( "No worker package staging for job " + job.getID() +
                                     " PEGASUS_HOME specified in the site catalog for site " + job.getSiteHandle(),
                                     LogManager.DEBUG_MESSAGE_LEVEL );
                    }
                }
                else{
                    //we don't want pegasus to add a stage worker job.
                    //but transfer directly if required.
                    if( mSiteStore.getEnvironmentVariable( job.getSiteHandle(), "PEGASUS_HOME" ) == null ){
                        //yes we need to add from the location in the worker package map
                        String location = this.mWorkerPackageMap.get( job.getSiteHandle() );

                        if( !mWorkerPackageMap.containsKey( job.getSiteHandle()) ){
                            location = retrieveLocationForWorkerPackageFromTC( job.getSiteHandle() );
                            //null can be populated as value
                            this.mWorkerPackageMap.put( job.getSiteHandle(), location );
                        }
                        //add only if location is not null
                        if( location != null ){
                            job.condorVariables.addIPFileForTransfer( location );
                        }
                    }
                    else{
                        mLogger.log( "No worker package staging for job " + job.getID()  +
                                     " PEGASUS_HOME specified in the site catalog for site " + job.getSiteHandle(),
                                     LogManager.DEBUG_MESSAGE_LEVEL );
                    }
                }
            }

            //the .sh file is set as the executable for the job
            //in addition to setting transfer_executable as true
            job.setRemoteExecutable( jobWrapper.getAbsolutePath() );
            job.condorVariables.construct( "transfer_executable", "true" );
        }
        //for all auxillary jobs let kickstart figure what to do
        else{
            mKickstartGridStartImpl.enable( job, isGlobusJob );
        }

        
        if (job.stdIn.length() > 0) {
            //PM-694
            //job has a stdin explicitly tracked
            //the transfer setup will ensure that the input file appears
            //in the directory where the job is launched via pegauss lite

            //we just need to unset it, so that the stdin is not transferred
            //from the submit directory as happens in sharedfs cases
            job.setStdIn( "" );

        }

       
    }

   


    
    /**
     * Indicates whether the enabling mechanism can set the X bit
     * on the executable on the remote grid site, in addition to launching
     * it on the remote grid stie
     *
     * @return false, as no wrapper executable is being used.
     */
    public  boolean canSetXBit(){
        return false;
    }

    /**
     * Returns the value of the vds profile with key as Pegasus.GRIDSTART_KEY,
     * that would result in the loading of this particular implementation.
     * It is usually the name of the implementing class without the
     * package name.
     *
     * @return the value of the profile key.
     * @see org.griphyn.cPlanner.namespace.Pegasus#GRIDSTART_KEY
     */
    public  String getVDSKeyValue(){
        return PegasusLite.CLASSNAME;
    }


    /**
     * Returns a short textual description in the form of the name of the class.
     *
     * @return  short textual description.
     */
    public String shortDescribe(){
        return PegasusLite.SHORT_NAME;
    }

    /**
     * Returns the SHORT_NAME for the POSTScript implementation that is used
     * to be as default with this GridStart implementation.
     *
     * @return  the identifier for the default POSTScript implementation for
     *          kickstart gridstart module.
     *
     * @see Kickstart#defaultPOSTScript() 
     */
    public String defaultPOSTScript(){
        return this.mKickstartGridStartImpl.defaultPOSTScript();
    }

    /**
     * Returns the directory that is associated with the job to specify
     * the directory in which the job needs to run
     * 
     * @param job  the job
     * 
     * @return the condor key . can be initialdir or remote_initialdir
     */
    private String getDirectoryKey(Job job) {
        /*
        String style = (String)job.vdsNS.get( Pegasus.STYLE_KEY );
                    //remove the remote or initial dir's for the compute jobs
                    String key = ( style.equalsIgnoreCase( Pegasus.GLOBUS_STYLE )  )?
                                   "remote_initialdir" :
                                   "initialdir";
         */
        String universe = (String) job.condorVariables.get( Condor.UNIVERSE_KEY );
        return ( universe.equals( Condor.STANDARD_UNIVERSE ) ||
                 universe.equals( Condor.LOCAL_UNIVERSE) ||
                 universe.equals( Condor.SCHEDULER_UNIVERSE ) )?
                "initialdir" :
                "remote_initialdir";
    }


    /**
     * Returns a boolean indicating whether to remove remote directory
     * information or not from the job. This is determined on the basis of the
     * style key that is associated with the job.
     *
     * @param job the job in question.
     *
     * @return boolean
     */
    private boolean removeDirectoryKey(Job job){
        String style = job.vdsNS.containsKey(Pegasus.STYLE_KEY) ?
                       null :
                       (String)job.vdsNS.get(Pegasus.STYLE_KEY);

        //is being run. Remove remote_initialdir if there
        //condor style associated with the job
        //Karan Nov 15,2005
        return (style == null)?
                false:
                style.equalsIgnoreCase(Pegasus.CONDOR_STYLE);

    }

    /**
     * Constructs a condor variable in the condor profile namespace
     * associated with the job. Overrides any preexisting key values.
     *
     * @param job   contains the job description.
     * @param key   the key of the profile.
     * @param value the associated value.
     */
    private void construct(Job job, String key, String value){
        job.condorVariables.construct(key,value);
    }

    /**
     * Writes out the list of filenames file for the job.
     *
     * @param files  the list of <code>PegasusFile</code> objects contains the files
     *               whose stat information is required.
     *
     * @param basename   the basename of the file that is to be created
     *
     * @return the full path to lof file created, else null if no file is written out.
     */
     public String generateListofFilenamesFile( Set files, String basename ){
         //sanity check
         if ( files == null || files.isEmpty() ){
             return null;
         }

         String result = null;
         //writing the stdin file
        try {
            File f = new File( mSubmitDir, basename );
            FileWriter input;
            input = new FileWriter( f );
            PegasusFile pf;
            for( Iterator it = files.iterator(); it.hasNext(); ){
                pf = ( PegasusFile ) it.next();
                input.write( pf.getLFN() );
                input.write( "\n" );
            }
            //close the stream
            input.close();
            result = f.getAbsolutePath();

        } catch ( IOException e) {
            mLogger.log("Unable to write the lof file " + basename, e ,
                        LogManager.ERROR_MESSAGE_LEVEL);
        }

        return result;
     }

     
     
     /**
     * Returns the directory in which the job executes on the worker node.
     * 
     * @param job
     * 
     * @return  the full path to the directory where the job executes
     */
    public String getWorkerNodeDirectory( Job job ){
        //for pegasus-lite for time being we rely on 
        //$PWD that is resolved in the directory at runtime
        return "$PWD";
    }

    
    /**
     * Generates a seqexec input file for the job. The function first enables the
     * job via kickstart module for worker node execution and then retrieves
     * the commands to put in the input file from the environment variables specified
     * for kickstart.
     *
     * It creates a single input file for the seqexec invocation.
     * The input file contains commands to
     *
     * <pre>
     * 1) create directory on worker node
     * 2) fetch input data files
     * 3) execute the job
     * 4) transfer the output data files
     * 5) cleanup the directory
     * </pre>
     *
     * @param job          the job to be enabled.
     * @param isGlobusJob is <code>true</code>, if the job generated a
     *        line <code>universe = globus</code>, and thus runs remotely.
     *        Set to <code>false</code>, if the job runs on the submit
     *        host in any way.
     * 
     * @return the file handle to the seqexec input file
     */
    protected File wrapJobWithPegasusLite(Job job, boolean isGlobusJob) {
        File shellWrapper = new File( job.getFileFullPath( mSubmitDir,  ".sh" ));

        //PM-971 for auxillary jobs we don't need to worry about 
        //or compute any staging site directories
        boolean isCompute = job.getJobType() == Job.COMPUTE_JOB;
        SiteCatalogEntry stagingSiteEntry = null;
        FileServer stagingSiteServerForRetrieval = null;
        String stagingSiteDirectory = null;
        String workerNodeDir = null;
        if( isCompute ){
            stagingSiteEntry = mSiteStore.lookup( job.getStagingSiteHandle() );
            if( stagingSiteEntry == null ){
                this.complainForHeadNodeFileServer( job.getID(),  job.getStagingSiteHandle());
            }
            stagingSiteServerForRetrieval = stagingSiteEntry.selectHeadNodeScratchSharedFileServer( FileServer.OPERATION.get );
            if( stagingSiteServerForRetrieval == null ){
                this.complainForHeadNodeFileServer( job.getID(),  job.getStagingSiteHandle());
            }

            stagingSiteDirectory      = mSiteStore.getInternalWorkDirectory( job, true );
            workerNodeDir             = getWorkerNodeDirectory( job );
        }

        //PM-810 load SLS factory per job
        SLS sls = mSLSFactory.loadInstance(job);
            

        try{
            OutputStream ostream = new FileOutputStream( shellWrapper , true );
            PrintWriter writer = new PrintWriter( new BufferedWriter(new OutputStreamWriter(ostream)) );

            StringBuffer sb = new StringBuffer( );
            sb.append( "#!/bin/bash" ).append( '\n' );
            sb.append( "set -e" ).append( '\n' );
            sb.append( "pegasus_lite_version_major=\"" ).append( this.mMajorVersionLevel ).append( "\"").append( '\n' );
            sb.append( "pegasus_lite_version_minor=\"" ).append( this.mMinorVersionLevel ).append( "\"").append( '\n' );
            sb.append( "pegasus_lite_version_patch=\"" ).append( this.mPatchVersionLevel ).append( "\"").append( '\n' );
            sb.append( "pegasus_lite_enforce_strict_wp_check=\"" ).append( this.mEnforceStrictChecksOnWPVersion ).append( "\"").append( '\n' );
            sb.append( "pegasus_lite_version_allow_wp_auto_download=\"" ).append( this.mAllowWPDownloadFromWebsite ).append( "\"").append( '\n' );
            
            //PM-1132 set the variable to point to a log file for pegasus lite output
            if( job.envVariables.containsKey( PegasusLite.PEGASUS_LITE_LOG_ENV_KEY) ){
                sb.append( PegasusLite.PEGASUS_LITE_LOG_ENV_KEY).append( "=\"" ).
                  append( job.envVariables.get(PegasusLite.PEGASUS_LITE_LOG_ENV_KEY) ).append( "\"").append( '\n' );
            }

            sb.append( '\n' );
            
            sb.append( ". " ).append( PegasusLite.PEGASUS_LITE_COMMON_FILE_BASENAME ).append( '\n' );
            sb.append( '\n' );

            sb.append( "pegasus_lite_init\n" );
            sb.append( '\n' );

            sb.append( "# cleanup in case of failures" ).append( '\n' );
            sb.append( "trap pegasus_lite_signal_int INT" ).append( '\n' );
            sb.append( "trap pegasus_lite_signal_term TERM" ).append( '\n' );
            sb.append( "trap pegasus_lite_unexpected_exit EXIT" ).append( '\n' );
            sb.append( '\n' );

            appendStderrFragment( sb, "Setting up workdir" );
            sb.append( "# work dir" ).append( '\n' );

            
            if( sls.doesCondorModifications() ){
                //when using condor IO with pegasus lite we dont want
                //pegasus lite to change the directory where condor
                //launches the jobs
                sb.append( "export pegasus_lite_work_dir=$PWD" ).append( '\n' );
            }
            else{
                //PM-822 check if the user has specified a local directory 
                //for the execution site then set that as PEGASUS_WN_TMP
                //and let pegasus lite at runtime launch the job in that
                //directory
                SiteCatalogEntry execSiteEntry = mSiteStore.lookup( job.getSiteHandle() );
                String dir = null;
                
                if( job.envVariables.containsKey(PegasusLite.WORKER_NODE_DIRECTORY_KEY  )){
                    //user metioned it as a profile that got assocaited with the job
                    dir = (String) job.envVariables.get( PegasusLite.WORKER_NODE_DIRECTORY_KEY);
                }
                else if( execSiteEntry != null ){
                    Directory directory = execSiteEntry.getDirectory( Directory.TYPE.local_scratch );
                    if( directory != null ){
                        dir = directory.getInternalMountPoint().getMountPoint();
                    }
                }
                                    
                if( dir != null ){
                    StringBuilder message = new StringBuilder();
                    message.append( "Job " ).append( job.getID()  ).append( " will execute in directory " ).
                            append( dir ).append( " on the local filesystem at site "  ).
                            append( job.getSiteHandle() );
                    mLogger.log( message.toString(),
                                 LogManager.DEBUG_MESSAGE_LEVEL );
                    sb.append( "export ").append( PegasusLite.WORKER_NODE_DIRECTORY_KEY ).
                       append( "=" ).append( dir ).append( '\n' );
                }

            }

            sb.append( "pegasus_lite_setup_work_dir" ).append( '\n' );
            sb.append( '\n' );

            appendStderrFragment( sb, "Figuring out the worker package to use" );
            sb.append( "# figure out the worker package to use" ).append( '\n' );
            sb.append( "pegasus_lite_worker_package" ).append( '\n' );
            sb.append( '\n' );

            if(  isCompute && //PM-971 for non compute jobs we don't do any sls transfers
                 sls.needsSLSInputTransfers( job ) ){
                //generate the sls file with the mappings in the submit exectionSiteDirectory
                Collection<FileTransfer> files = sls.determineSLSInputTransfers( job,
                                                          sls.getSLSInputLFN( job ),
                                                          stagingSiteServerForRetrieval,
                                                          stagingSiteDirectory,
                                                          workerNodeDir );

                //PM-779 split the checkpoint files from the input files
                //as we want to stage them separately
                Collection<FileTransfer> inputFiles = new LinkedList();
                Collection<FileTransfer> chkpointFiles = new LinkedList();
                Collection<FileTransfer> containerFiles = new LinkedList();
                for(FileTransfer ft: files ){
                    if( ft.isCheckpointFile() ){
                        chkpointFiles.add(ft);
                    }
                    else if( ft.isContainerFile() ){
                        containerFiles.add(ft);
                    }
                    else{
                        inputFiles.add(ft);
                    }
                }

                //stage the container first
                if( !containerFiles.isEmpty() ){
                    appendStderrFragment( sb, "Staging in container" );
                    sb.append( "# stage in container file " ).append( '\n' );
                    sb.append(  sls.invocationString( job, null ) );
                    sb.append( " 1>&2" ).append( " << 'EOF'" ).append( '\n' );
                    sb.append( convertToTransferInputFormat( inputFiles, PegasusFile.LINKAGE.input ) );
                    sb.append( "EOF" ).append( '\n' );
                    sb.append( '\n' );
                }

                //stage input files if this is a dax or a dag job
                if( (job instanceof DAXJob || job instanceof DAGJob) && (!inputFiles.isEmpty()) ){
                    appendStderrFragment( sb, "Staging in input data and executables" );
                    sb.append( "# stage in data and executables" ).append( '\n' );
                    sb.append(  sls.invocationString( job, null ) );
                    if ( mUseSymLinks && job.getContainer() == null ){
                        //PM-1135 allow the transfer executable to symlink input file urls
                        //PM-1197 we have to disable symlink if a job is set to 
                        //be launched via a container
                        sb.append( " --symlink " );
                    }
                    sb.append( " 1>&2" ).append( " << 'EOF'" ).append( '\n' );
                    sb.append( convertToTransferInputFormat( inputFiles, PegasusFile.LINKAGE.input ) );
                    sb.append( "EOF" ).append( '\n' );
                    sb.append( '\n' );
                }

            }
            
            writer.print( sb.toString() );
            writer.flush();
            
            
            sb = new StringBuffer();
            
            //enable the job via kickstart
            //separate calls for aggregated and normal jobs
            ContainerShellWrapper containerWrapper = this.mContainerWrapperFactory.loadInstance(job);
            mLogger.log( "Setting job " + job.getID() + 
                         " to run via " + containerWrapper.describe() , LogManager.DEBUG_MESSAGE_LEVEL );
            if( job instanceof AggregatedJob ){
                this.mKickstartGridStartImpl.enable( (AggregatedJob)job, isGlobusJob );
                sb.append( containerWrapper.wrap( (AggregatedJob)job));
            }
            else{
                this.mKickstartGridStartImpl.enable( job, isGlobusJob );
                //sb.append( job.getRemoteExecutable() ).append( job.getArguments() ).append( '\n' );
                
                sb.append( containerWrapper.wrap(job));
            }
            sb.append( "\n" );
            
            //PM-701 enable back fail on error
            //Fixme: has to go in no container wrapper implementation
            //sb.append( "job_ec=$?" ).append( "\n" );
           
            sb.append( "set -e").append( "\n" );
            sb.append( '\n' );
            
            //the pegasus lite wrapped job itself does not have any
            //arguments passed
            job.setArguments( "" );

            sb.append( "\n" );
            sb.append( "# clear the trap, and exit cleanly" ).append( '\n' );
            sb.append( "trap - EXIT" ).append( '\n' );
            sb.append( "pegasus_lite_final_exit" ).append( '\n' );
            sb.append( "\n" );
           
            writer.print( sb.toString() );
            writer.flush();
            
            writer.close();
            ostream.close();

            //set the xbit on the shell script
            //for 3.2, we will have 1.6 as the minimum jdk requirement
            shellWrapper.setExecutable( true );

            //JIRA PM-543
            job.setDirectory( null );
            
            //PM-737 explicitly set the success string to look for 
            //in pegasus lite stderr, when pegasus-exitcode is invoked
            //at runtime. we should merge so as not override any existing
            //success message patterns
            Namespace addOnPegasusProfiles = new Pegasus();
            addOnPegasusProfiles.construct(Pegasus.EXITCODE_SUCCESS_MESSAGE, PEGASUS_LITE_EXITCODE_SUCCESS_MESSAGE );
            job.vdsNS.merge(addOnPegasusProfiles);
            
            //this.setXBitOnFile( shellWrapper.getAbsolutePath() );
        }
        catch( IOException ioe ){
            throw new RuntimeException( "[Pegasus-Lite] Error while writing out pegasus lite wrapper " + shellWrapper , ioe );
        }

        //modify the constituentJob if required
        //PM-971 for non compute jobs we don't do any core modification to job
        if ( isCompute && !sls.modifyJobForWorkerNodeExecution( job,
                                                    stagingSiteServerForRetrieval.getURLPrefix(),
                                                    stagingSiteDirectory,
                                                    workerNodeDir ) ){

                throw new RuntimeException( "Unable to modify job " + job.getName() + " for worker node execution" );

            }


        return shellWrapper;
    }
    
   
    /**
     * Convers the collection of files into an input format suitable for the
     * transfer executable
     * 
     * @param files    Collection of <code>FileTransfer</code> objects.
     * @param linkage file type of transfers
     * 
     * @return  the blurb containing the files in the input format for the transfer
     *          executable
     */
    protected StringBuffer convertToTransferInputFormat( Collection<FileTransfer> files, PegasusFile.LINKAGE linkage ){
        StringBuffer sb = new StringBuffer();
        
        sb.append("[\n");

        int num = 1;
        for( FileTransfer ft :  files ){
            
            if (num > 1) {
            	sb.append(" ,\n");
            }
            Collection<String> sourceSites = ft.getSourceSites( );
            sb.append(" { \"type\": \"transfer\",\n");
            sb.append("   \"linkage\": ").append("\"").append( linkage ).append("\"").append(",\n");
            sb.append("   \"lfn\": ").append("\"").append(ft.getLFN()).append("\"").append(",\n");
            sb.append("   \"id\": ").append(num).append(",\n");
           
            //PM-1298 
            if( !ft.verifySymlinkSource() ){
                sb.append("   \"verify_symlink_source\": false").append(",\n");
            }
            
            sb.append("   \"src_urls\": [");
            
            boolean notFirst = false;
            for( String sourceSite: sourceSites ){
                //traverse through all the URL's on that site
                for( ReplicaCatalogEntry url : ft.getSourceURLs(sourceSite) ){
                    if( notFirst ){
                        sb.append(",");
                    }
                    String prio =  (String) url.getAttribute( ReplicaSelector.PRIORITY_KEY);
                    
                    sb.append("\n     {");
                    sb.append(" \"site_label\": \"").append(sourceSite).append("\",");
                    sb.append(" \"url\": \"").append( url.getPFN() ).append("\",");
                    if( prio != null ){
                        sb.append(" \"priority\": ").append( prio ).append(",");
                    }
                    sb.append(" \"checkpoint\": \"").append(ft.isCheckpointFile()).append("\"");
                    sb.append(" }");
                    notFirst = true;
                }
            }
            
            sb.append("\n   ],\n");
            NameValue nv = ft.getDestURL();
            sb.append("   \"dest_urls\": [\n");
            sb.append("     {");
            sb.append(" \"site_label\": \"").append(nv.getKey()).append("\",");
            sb.append(" \"url\": \"").append(nv.getValue()).append("\"");
            //PM-1300 tag that we are transferring a container
            if( ft.isTransferringContainer() ){
                sb.append(",");
                sb.append(" \"type\": \"").append( ft.typeToString()).append("\"");
            }
            sb.append(" }\n");
            sb.append("   ]");
            sb.append(" }\n"); // end of this transfer

            num++;
        }
        
        sb.append("]\n");

        return sb;
    }

    /**
     * Returns the path to the chmod executable for a particular execution
     * site by looking up the transformation executable.
     * 
     * @param site   the execution site.
     * 
     * @return   the path to chmod executable
     */
    protected String getPathToChmodExecutable( String site ){
        String path;

        //check if the internal map has anything
        path = mChmodOnExecutionSiteMap.get( site );

        if( path != null ){
            //return the cached path
            return path;
        }

        List entries;
        try {
            //try to look up the transformation catalog for the path
            entries = mTCHandle.lookup( PegasusLite.XBIT_TRANSFORMATION_NS,
                          PegasusLite.XBIT_TRANSFORMATION,
                          PegasusLite.XBIT_TRANSFORMATION_VERSION,
                          site,
                          TCType.INSTALLED );
        } catch (Exception e) {
            //non sensical catching
            mLogger.log("Unable to retrieve entries from TC " +
                        e.getMessage(), LogManager.ERROR_MESSAGE_LEVEL );
            return null;
        }

        TransformationCatalogEntry entry = ( entries == null ) ?
                                       null: //try using a default one
                                       (TransformationCatalogEntry) entries.get(0);

        if( entry == null ){
            //construct the path the default path.
            //construct the path to it
            StringBuffer sb = new StringBuffer();
            sb.append( File.separator ).append( "bin" ).append( File.separator ).
               append( PegasusLite.XBIT_EXECUTABLE_BASENAME  );
            path = sb.toString();
        }
        else{
            path = entry.getPhysicalTransformation();
        }

        mChmodOnExecutionSiteMap.put( site, path );

        return path;
    }

     /**
     * Sets the xbit on the file.
     *
     * @param file   the file for which the xbit is to be set
     *
     * @return boolean indicating whether xbit was set or not.
     */
    protected boolean  setXBitOnFile( String file ) {
        boolean result = false;

        //do some sanity checks on the source and the destination
        File f = new File( file );
        if( !f.exists() || !f.canRead()){
            mLogger.log("The file does not exist " + file,
                        LogManager.ERROR_MESSAGE_LEVEL);
            return result;
        }

        try{
            //set the callback and run the grep command
            Runtime r = Runtime.getRuntime();
            String command = "chmod +x " + file;
            mLogger.log("Setting xbit " + command,
                        LogManager.DEBUG_MESSAGE_LEVEL);
            Process p = r.exec(command);

            //the default gobbler callback always log to debug level
            StreamGobblerCallback callback =
               new DefaultStreamGobblerCallback(LogManager.DEBUG_MESSAGE_LEVEL);
            //spawn off the gobblers with the already initialized default callback
            StreamGobbler ips =
                new StreamGobbler(p.getInputStream(), callback);
            StreamGobbler eps =
                new StreamGobbler(p.getErrorStream(), callback);

            ips.start();
            eps.start();

            //wait for the threads to finish off
            ips.join();
            eps.join();

            //get the status
            int status = p.waitFor();
            if( status != 0){
                mLogger.log("Command " + command + " exited with status " + status,
                            LogManager.DEBUG_MESSAGE_LEVEL);
                return result;
            }
            result = true;
        }
        catch(IOException ioe){
            mLogger.log("IOException while creating symbolic links ", ioe,
                        LogManager.ERROR_MESSAGE_LEVEL);
        }
        catch( InterruptedException ie){
            //ignore
        }
        return result;
    }

    /**
     * Determines the path to common shell functions file that Pegasus Lite 
     * wrapped jobs use.
     * 
     * @return the path on the submit host.
     */
    protected String getSubmitHostPathToPegasusLiteCommon() {
        StringBuffer path = new StringBuffer();

        //first get the path to the share directory
        File share = mProps.getSharedDir();
        if( share == null ){
            throw new RuntimeException( "Property for Pegasus share directory is not set" );
        }

        path.append( share.getAbsolutePath() ).append( File.separator ).
             append( "sh" ).append( File.separator ).append( PegasusLite.PEGASUS_LITE_COMMON_FILE_BASENAME );

        return path.toString();
    }

    public void useFullPathToGridStarts(boolean fullPath) {
        mKickstartGridStartImpl.useFullPathToGridStarts(fullPath);
    }

    /**
     * Associates credentials with the job corresponding to the files that
     * are being transferred.
     * 
     * @param job    the job for which credentials need to be added.
     * @param files  the files that are being transferred.
     */
    private void associateCredentials(Job job, Collection<FileTransfer> files) {
        for( FileTransfer ft: files ){
            NameValue source = ft.getSourceURL();
            job.addCredentialType( source.getKey(), source.getValue() );
            NameValue dest   = ft.getDestURL();
            job.addCredentialType( dest.getKey(), dest.getValue() );
        }
    }

    /**
     * Retrieves the location for the pegasus worker package from the TC for a site
     * 
     * 
     * @return the path to worker package tar file on the site, else null if unable
     *         to determine
     */
    protected String retrieveLocationForWorkerPackageFromTC( String site ) {
        String location = null;
        Mapper m = mBag.getHandleToTransformationMapper();

        if( !m.isStageableMapper() ){
            //we want to load a stageable mapper
            mLogger.log( "User set mapper is not a stageable mapper. Loading a stageable mapper ", LogManager.DEBUG_MESSAGE_LEVEL );
            m = Mapper.loadTCMapper( "Staged", mBag );
        }

        //check if there is a valid entry for worker package
        List entries, selectedEntries = null;
        TransformationCatalogEntry entry = null;
        try{
            entries = m.getTCList( DeployWorkerPackage.TRANSFORMATION_NAMESPACE,
                                   DeployWorkerPackage.TRANSFORMATION_NAME,
                                   DeployWorkerPackage.TRANSFORMATION_VERSION,
                                   site );

            if( entries != null && !entries.isEmpty() ){
                entry = (TransformationCatalogEntry)entries.get( 0 );
            }

        }catch( Exception e ){
            mLogger.log( "Unable to figure out worker package location for site " + site ,
                          LogManager.DEBUG_MESSAGE_LEVEL );

        }
        if( entry != null ){
            location = entry.getPhysicalTransformation();
            if( location.startsWith( "file:/") ){
                location = location.substring( 6 );
            }
        }
        return location;
    }
    
    /**
     * Complains for a missing head node file server on a site for a job
     * 
     * @param jobname  the name of the job
     * @param site     the site 
     */
     void complainForHeadNodeFileServer(String jobname, String site) {
        StringBuffer error = new StringBuffer();
        error.append( "[PegasusLite] " );
        if( jobname != null ){
            error.append( "For job (" ).append( jobname).append( ")." ); 
        }
        error.append( " File Server not specified for head node scratch shared filesystem for site: ").
              append( site );
        throw new RuntimeException( error.toString() );
        
    }


    /**
     * Appends a fragment to the pegasus lite script that logs a message to
     * stderr
     * 
     * @param sb       string buffer
     * @param message  the message  
     */
    private void appendStderrFragment(StringBuffer sb, String message ) {
        //prefix + 1 + message
        int len = PegasusLite.MESSAGE_PREFIX.length() + 1 + message.length();
        if( len > PegasusLite.MESSAGE_STRING_LENGTH ){
            throw new RuntimeException( "Message string for PegasusLite exceeds " + PegasusLite.MESSAGE_STRING_LENGTH + " characters");
        }
        
        int pad = ( PegasusLite.MESSAGE_STRING_LENGTH - len )/2;
        sb.append( "echo -e \"\\n" );
        for( int i = 0; i <= pad ; i ++ ){
            sb.append( PegasusLite.SEPARATOR_CHAR );
        }
        sb.append( PegasusLite.MESSAGE_PREFIX ).append( " " ).append( message ).append( " " );
        for( int i = 0; i <= pad ; i ++ ){
            sb.append( PegasusLite.SEPARATOR_CHAR );
        }
        sb.append( "\"  1>&2").append( "\n" );
        
        return;
        
    }

}
