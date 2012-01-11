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

import edu.isi.pegasus.planner.catalog.site.classes.SiteStore;

import edu.isi.pegasus.common.logging.LogManager;

import edu.isi.pegasus.common.util.DefaultStreamGobblerCallback;

import edu.isi.pegasus.common.util.StreamGobbler;
import edu.isi.pegasus.common.util.StreamGobblerCallback;
import edu.isi.pegasus.common.util.Version;

import edu.isi.pegasus.planner.common.PegasusProperties;

import edu.isi.pegasus.planner.code.GridStart;

import edu.isi.pegasus.planner.classes.ADag;
import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.classes.AggregatedJob;
import edu.isi.pegasus.planner.classes.PegasusFile;
import edu.isi.pegasus.planner.classes.TransferJob;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.classes.PlannerOptions;

import edu.isi.pegasus.planner.namespace.Condor;
import edu.isi.pegasus.planner.namespace.Pegasus;

import edu.isi.pegasus.planner.transfer.sls.SLSFactory;
import edu.isi.pegasus.planner.transfer.SLS;

import edu.isi.pegasus.planner.catalog.TransformationCatalog;

import edu.isi.pegasus.planner.catalog.site.classes.FileServer;

import edu.isi.pegasus.planner.catalog.transformation.TransformationCatalogEntry;
import edu.isi.pegasus.planner.catalog.transformation.classes.TCType;
import edu.isi.pegasus.planner.classes.FileTransfer;
import edu.isi.pegasus.planner.classes.NameValue;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.IOException;
import java.io.FileWriter;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
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
    protected boolean mWorkerNodeExecution;

    /**
     * The handle to the SLS implementor
     */
    protected SLS mSLS;

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

        if( mTransferWorkerPackage ){
            mWorkerPackageMap = bag.getWorkerPackageMap();
            if( mWorkerPackageMap == null ){
                mWorkerPackageMap = new HashMap<String,String>();
            }
        }

        mChmodOnExecutionSiteMap = new HashMap<String,String>();

        Version version = Version.instance();
        mMajorVersionLevel = Integer.toString( Version.MAJOR );
        mMinorVersionLevel = Integer.toString( Version.MINOR );
        mPatchVersionLevel = Integer.toString( Version.PLEVEL );
        if( version.toString().endsWith( "cvs" ) ){
            mPatchVersionLevel += "cvs";
        }


        mWorkerNodeExecution = mProps.executeOnWorkerNode();
        if( mWorkerNodeExecution ){
            //load SLS
            mSLS = SLSFactory.loadInstance( bag );
        }
        else{
            //sanity check
            throw new RuntimeException( "PegasusLite only works if worker node execution is set. Please set  " +
                                        PegasusProperties.PEGASUS_WORKER_NODE_EXECUTION_PROPERTY  + " to true .");
        }

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

        /*
        boolean result = true;

        if( mWorkerNodeExecution ){
            File jobWrapper = wrapJobWithPegasusLite( job, isGlobusJob );
             //the .sh file is set as the executable for the job
            //in addition to setting transfer_executable as true
            job.setRemoteExecutable( jobWrapper.getAbsolutePath() );
            job.condorVariables.construct( "transfer_executable", "true" );
        }
        return result;
         */
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


        //NOT CLEAR HOW Pegasus-Lite will handle stdout and stdin
        // handle stdin
        if (job.stdIn.length() > 0) {
            construct(job,"input",submitDir + job.stdIn);
            if (isGlobusJob) {
                //this needs to be true as you want the stdin
                //to be transfered to the remote execution
                //pool, as in case of the transfer script.
                //it needs to be set if the stdin is already
                //prepopulated at the remote side which
                //it is not.
                construct(job,"transfer_input","true");
            }
        }

        if (job.stdOut.length() > 0) {
            //handle stdout
            construct(job,"output",job.stdOut);
            if (isGlobusJob) {
                construct(job,"transfer_output","false");
            }
        } else {
            // transfer output back to submit host, if unused
            construct(job,"output",submitDir + job.jobName + ".out");
            if (isGlobusJob) {
                construct(job,"transfer_output","true");
            }
        }

        if (job.stdErr.length() > 0) {
            //handle stderr
            construct(job,"error",job.stdErr);
            if (isGlobusJob) {
                construct(job,"transfer_error","false");
            }
        } else {
            // transfer error back to submit host, if unused
            construct(job,"error",submitDir + job.jobName + ".err");
            if (isGlobusJob) {
                construct(job,"transfer_error","true");
            }
        }
        
        //consider case for non worker node execution first

        if( !mWorkerNodeExecution ){
            //shared filesystem case.

            //for now a single job is launched via kickstart only
            //no point launching it via seqexec and then kickstart
            return mKickstartGridStartImpl.enable( job, isGlobusJob );
                
        }//end of handling of non worker node execution
        else{
            //handle stuff differently 
            enableForWorkerNodeExecution( job, isGlobusJob );
        }//end of worker node execution



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

        if( job.getJobType() == Job.COMPUTE_JOB  ){

            //in pegasus lite mode we dont want kickstart to change or create
            //worker node directories
            job.vdsNS.construct( Pegasus.CHANGE_DIR_KEY , "false" );
            job.vdsNS.construct( Pegasus.CREATE_AND_CHANGE_DIR_KEY, "false" );

            File jobWrapper = wrapJobWithPegasusLite( job, isGlobusJob );

            //the job wrapper requires the common functions file
            //from the submit host
            job.condorVariables.addIPFileForTransfer( this.mLocalPathToPegasusLiteCommon );

            //figure out transfer of worker package
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
                    mLogger.log( "No worker package staging for job " + job.getSiteHandle() +
                                 " PEGASUS_HOME specified in the site catalog for site " + job.getSiteHandle(),
                                 LogManager.DEBUG_MESSAGE_LEVEL );
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
        File shellWrapper = new File( mSubmitDir, job.getID() + ".sh" );

//           Removed for JIRA PM-543
//
//         //remove the remote or initial dir's for the compute jobs
//         String key = getDirectoryKey( job );
//
//         String exectionSiteDirectory     = (String)job.condorVariables.removeKey( key );

        FileServer stagingSiteFileServer = mSiteStore.lookup( job.getStagingSiteHandle() ).getHeadNodeFS().selectScratchSharedFileServer();
        String stagingSiteDirectory      = mSiteStore.getExternalWorkDirectory(stagingSiteFileServer, job.getStagingSiteHandle() );
        String workerNodeDir             = getWorkerNodeDirectory( job );


        try{
            OutputStream ostream = new FileOutputStream( shellWrapper , true );
            PrintWriter writer = new PrintWriter( new BufferedWriter(new OutputStreamWriter(ostream)) );

            StringBuffer sb = new StringBuffer( );
            sb.append( "#!/bin/bash" ).append( '\n' );
            sb.append( "set -e" ).append( '\n' );
            sb.append( "pegasus_lite_version_major=\"" ).append( this.mMajorVersionLevel ).append( "\"").append( '\n' );
            sb.append( "pegasus_lite_version_minor=\"" ).append( this.mMinorVersionLevel ).append( "\"").append( '\n' );
            sb.append( "pegasus_lite_version_patch=\"" ).append( this.mPatchVersionLevel ).append( "\"").append( '\n' );
            sb.append( '\n' );

            sb.append( ". " ).append( PegasusLite.PEGASUS_LITE_COMMON_FILE_BASENAME ).append( '\n' );
            sb.append( '\n' );

            sb.append( "# cleanup in case of failures" ).append( '\n' );
            sb.append( "trap pegasus_lite_exit INT TERM EXIT" ).append( '\n' );
            sb.append( '\n' );

            sb.append( "# work dir" ).append( '\n' );

            if( mSLS.doesCondorModifications() ){
                //when using condor IO with pegasus lite we dont want
                //pegasus lite to change the directory where condor
                //launches the jobs
                sb.append( "export pegasus_lite_work_dir=$PWD" ).append( '\n' );
            }

            sb.append( "pegasus_lite_setup_work_dir" ).append( '\n' );
            sb.append( '\n' );

            sb.append( "# figure out the worker package to use" ).append( '\n' );
            sb.append( "pegasus_lite_worker_package" ).append( '\n' );
            sb.append( '\n' );

            if(  mSLS.needsSLSInputTransfers( job ) ){
                //generate the sls file with the mappings in the submit exectionSiteDirectory
                Collection<FileTransfer> files = mSLS.determineSLSInputTransfers( job,
                                                          mSLS.getSLSInputLFN( job ),
                                                          mSubmitDir,
                                                          stagingSiteDirectory,
                                                          workerNodeDir );


                sb.append( "# stage in " ).append( '\n' );
                sb.append(  mSLS.invocationString( job, null ) );

                sb.append( " 1>&2" ).append( " << EOF" ).append( '\n' );

                sb.append( convertToTransferInputFormat( files ) );
                sb.append( "EOF" ).append( '\n' );
                sb.append( '\n' );

                //associate any credentials if required with the job
                associateCredentials( job, files );
            }

            if( job.userExecutablesStagedForJob() ){
                sb.append( "# set the xbit for any executables staged" ).append( '\n' );
                sb.append( getPathToChmodExecutable( job.getSiteHandle() ) );
                sb.append( " +x " );

                for( Iterator it = job.getInputFiles().iterator(); it.hasNext(); ){
                    PegasusFile pf = ( PegasusFile )it.next();
                    if( pf.getType() == PegasusFile.EXECUTABLE_FILE ){
                        sb.append( pf.getLFN() ).append( " " );
                    }

                }
                sb.append( '\n' );
                sb.append( '\n' );
            }
           

            sb.append( "# execute the tasks" ).append( '\n' );

            writer.print( sb.toString() );
            writer.flush();
            
            sb = new StringBuffer();

            //enable the job via kickstart
            //separate calls for aggregated and normal jobs
            if( job instanceof AggregatedJob ){
                this.mKickstartGridStartImpl.enable( (AggregatedJob)job, isGlobusJob );
                //for clustered jobs we embed the contents of the input
                //file in the shell wrapper itself
                sb.append( job.getRemoteExecutable() ).append( " " ).append( job.getArguments() );
                sb.append( " << EOF" ).append( '\n' );

                sb.append( slurpInFile( mSubmitDir, job.getStdIn() ) );
                sb.append( "EOF" ).append( '\n' );

                //rest the jobs stdin
                job.setStdIn( "" );
                job.condorVariables.removeKey( "input" );
            }
            else{
                this.mKickstartGridStartImpl.enable( job, isGlobusJob );
                sb.append( job.getRemoteExecutable() ).append( job.getArguments() ).append( '\n' );
            }
            sb.append( '\n' );
            
            //the pegasus lite wrapped job itself does not have any
            //arguments passed
            job.setArguments( "" );

             if( mSLS.needsSLSOutputTransfers( job ) ){
                //construct the postjob that transfers the output files
                //back to head node exectionSiteDirectory
                //to fix later. right now post constituentJob only created is pre constituentJob
                //created
                Collection<FileTransfer> files = mSLS.determineSLSOutputTransfers( job,
                                                            mSLS.getSLSOutputLFN( job ),
                                                            mSubmitDir,
                                                            stagingSiteDirectory,
                                                            workerNodeDir );


                //generate the post constituentJob
                String postJob = mSLS.invocationString( job, null );
                sb.append( "# stage out" ).append( '\n' );
                sb.append( postJob );
                
                sb.append( " 1>&2" ).append( " << EOF" ).append( '\n' );
                sb.append( convertToTransferInputFormat( files ) );
                sb.append( "EOF" ).append( '\n' );
                sb.append( '\n' );

                //associate any credentials if required with the job
                associateCredentials( job, files );
            }
            
           
            writer.print( sb.toString() );
            writer.flush();
            
            writer.close();
            ostream.close();

            //set the xbit on the shell script
            //for 3.2, we will have 1.6 as the minimum jdk requirement
            shellWrapper.setExecutable( true );

            //JIRA PM-543
            job.setDirectory( null );
            
            //this.setXBitOnFile( shellWrapper.getAbsolutePath() );
        }
        catch( IOException ioe ){
            throw new RuntimeException( "[Pegasus-Lite] Error while writing out pegasus lite wrapper " + shellWrapper , ioe );
        }

        //modify the constituentJob if required
        if ( !mSLS.modifyJobForWorkerNodeExecution( job,
                                                    stagingSiteFileServer.getURLPrefix(),
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
     * @param files   Collection of <code>FileTransfer</code> objects.
     * 
     * @return  the blurb containing the files in the input format for the transfer
     *          executable
     */
    protected StringBuffer convertToTransferInputFormat( Collection<FileTransfer> files ){
        StringBuffer sb = new StringBuffer();

        int num = 1;
        for( FileTransfer ft :  files ){
            NameValue nv = ft.getSourceURL();
            sb.append( "# "  ).append( "src " ).append( num ).append( " " ).append( nv.getKey() ).append( '\n' );
            sb.append( nv.getValue() );
            sb.append( '\n' );

            nv = ft.getDestURL();
            sb.append( "# "  ).append( "dst " ).append( num ).append( " " ).append( nv.getKey() ).append( '\n' );
            sb.append( nv.getValue() );
            sb.append( '\n' );

            num++;
        }

        return sb;
    }

    /**
     * Convenience method to slurp in contents of a file into memory.
     *
     * @param directory  the directory where the file resides
     * @param file    the file to be slurped in.
     * 
     * @return StringBuffer containing the contents
     */
    protected StringBuffer slurpInFile( String directory, String file ) throws  IOException{
        StringBuffer result = new StringBuffer();
        //sanity check
        if( file == null ){
            return result;
        }

        BufferedReader in = new BufferedReader( new FileReader( new File(  directory, file )) );

        String line = null;

        while(( line = in.readLine() ) != null ){
            //System.out.println( line );
            result.append( line ).append( '\n' );
        }

        in.close();


        return result;
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
        throw new UnsupportedOperationException("Not supported yet.");
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
            job.addCredentialType( ft.getSourceURL().getValue() );
            job.addCredentialType( ft.getDestURL().getValue() );
        }
    }
}
