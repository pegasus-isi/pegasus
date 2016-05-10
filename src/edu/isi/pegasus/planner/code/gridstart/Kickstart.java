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
import edu.isi.pegasus.planner.catalog.site.classes.SiteCatalogEntry;
import edu.isi.pegasus.planner.catalog.site.classes.SiteStore;
import edu.isi.pegasus.planner.classes.ADag;
import edu.isi.pegasus.planner.classes.AggregatedJob;
import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.classes.PegasusFile;
import edu.isi.pegasus.planner.classes.PlannerOptions;
import edu.isi.pegasus.planner.classes.TransferJob;
import edu.isi.pegasus.planner.code.GridStart;
import edu.isi.pegasus.planner.code.generator.condor.CondorQuoteParser;
import edu.isi.pegasus.planner.code.generator.condor.CondorQuoteParserException;
import edu.isi.pegasus.planner.common.PegasusConfiguration;
import edu.isi.pegasus.planner.common.PegasusProperties;
import edu.isi.pegasus.planner.namespace.Condor;
import edu.isi.pegasus.planner.namespace.Globus;
import edu.isi.pegasus.planner.namespace.Pegasus;
import edu.isi.pegasus.planner.transfer.SLS;

import edu.isi.pegasus.common.util.Separator;

import edu.isi.pegasus.planner.catalog.transformation.classes.TCType;

import edu.isi.pegasus.planner.catalog.TransformationCatalog;
import edu.isi.pegasus.planner.catalog.transformation.TransformationCatalogEntry;

import edu.isi.pegasus.planner.cluster.JobAggregator;
import edu.isi.pegasus.planner.namespace.ENV;
import edu.isi.pegasus.planner.partitioner.graph.GraphNode;

import edu.isi.pegasus.common.util.Boolean;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;


/**
 * This enables a constituentJob to be run on the grid, by launching it through kickstart.
 * The kickstart executable is a light-weight program which  connects  the
 * stdin,  stdout  and  stderr  filehandles for Pegasus jobs on the remote
 * site.
 * <p>
 * Sitting in between the remote scheduler and the executable, it is
 * possible  for  kickstart  to  gather additional information about the
 * executable run-time behavior, including the  exit  status  of  jobs.
 * <p>
 * Kickstart is an executable distributed with Pegasus that can generally be found
 * at $PEGASUS_HOME/bin/kickstart
 *
 * @author Karan Vahi vahi@isi.edu
 * @version $Revision$
 */
public class Kickstart implements GridStart {
    
    

    /**
     * The transformation namespace for the kickstart
     */
    public static final String TRANSFORMATION_NAMESPACE = "pegasus";

    /**
     * The logical name of kickstart
     */
    public static final String TRANSFORMATION_NAME = "kickstart";

    /**
     * The version number for kickstart.
     */
    public static final String TRANSFORMATION_VERSION = null;

    /**
     * The basename of the kickstart executable.
     */
    public static final String EXECUTABLE_BASENAME = "pegasus-kickstart";


    /**
     * The complete TC name for kickstart.
     */
    public static final String COMPLETE_TRANSFORMATION_NAME = Separator.combine(
                                                                    TRANSFORMATION_NAMESPACE,
                                                                    TRANSFORMATION_NAME,
                                                                    TRANSFORMATION_VERSION  );

    /**
     * The suffix for the kickstart input file, that is generated to use
     * invoke at the remote end.
     */
    public static final String KICKSTART_INPUT_SUFFIX = "arg";

    /**
     * The basename of the class that is implmenting this. Could have
     * been determined by reflection.
     */
    public static final String CLASSNAME = "Kickstart";

    /**
     * The SHORTNAME for this implementation.
     */
    public static final String SHORT_NAME = "kickstart";


    /**
     * The environment variable used to the set Kickstart SETUP JOB.
     */
    public static final String KICKSTART_SETUP = "GRIDSTART_SETUP";


    /**
     * The environment variable used to the set Kickstart PREJOB.
     */
    public static final String KICKSTART_PREJOB = "GRIDSTART_PREJOB";


    /**
     * The environment variable used to the set Kickstart POSTJOB.
     */
    public static final String KICKSTART_POSTJOB = "GRIDSTART_POSTJOB";

    /**
     * The environment variable used to the set Kickstart CLEANUP JOB.
     */
    public static final String KICKSTART_CLEANUP = "GRIDSTART_CLEANUP";


    /**
     * The LogManager object which is used to log all the messages.
     */
    private LogManager mLogger;

    /**
     * The object holding all the properties pertaining to Pegasus.
     */
    private PegasusProperties mProps;

    /**
     * The options passed to the planner.
     */
    private PlannerOptions mPOptions;

    /**
     * The handle to the workflow that is being enabled.
     */
    private ADag mConcDAG;

    /**
     * Handle to the site catalog store.
     */
    private SiteStore mSiteStore;
    //private PoolInfoProvider mSiteHandle;

    /**
     * Handle to Transformation Catalog.
     */
    private TransformationCatalog mTCHandle;

    /**
     * The submit exectionSiteDirectory where the submit files are being generated for
     * the workflow.
     */
    private String mSubmitDir;

    /**
     * A boolean indicating whether to use invoke always or not.
     */
    private boolean mInvokeAlways;

    /**
     * A boolean indicating whether to stat files or not.
     */
    private boolean mDoStat;

    /**
     * A boolean indicating whether to generate lof files or not.
     */
    private boolean mGenerateLOF;

    /**
     * The invoke limit trigger.
     */
    private long mInvokeLength;
    
    /**
     * A flag to indicate if outputs are being registered or not
     */
    private boolean mRegisterOutputs;

    
    /**
     * handle to PegasusConfiguration
     */
    private PegasusConfiguration mPegasusConfiguration;
    
    /**
     * The handle to the SLS implementor
     */
    private SLS mSLS;

    /**
     * An instance variable to track if enabling is happening as part of a clustered constituentJob.
     * See Bug 21 comments on Pegasus Bugzilla
     */
    private boolean mEnablingPartOfAggregatedJob;

    /**
     * A boolean indicating whether kickstart is deployed dynamically or not.
     */
    private boolean mDynamicDeployment;
    
    /**
     * The label that is passed to kickstart.
     */
    private String mKickstartLabel;

    /**
     * Whether kickstart should set the X Bit on the staged executables.
     */
    private boolean mSetXBit;

    /**
     * Handle to NoGridStart implementation.
     */
    private GridStart mNoGridStartImpl;


    /**
     * Boolean indicating whether to use full path or not
     */
    private boolean mUseFullPathToGridStart;

    /**
     * Boolean indicating whether to disable invoke functionality.
     */
    private boolean mDisableInvokeFunctionality;
    private boolean mDisableKickstartStatCompletely;

    /**
     * Initializes the GridStart implementation.
     *
     * @param bag   the bag of objects that is used for initialization.
     * @param dag   the concrete dag so far.
     */
    public void initialize( PegasusBag bag, ADag dag ){
        
        mProps        = bag.getPegasusProperties();
        mPOptions     = bag.getPlannerOptions();
        mLogger       = bag.getLogger();
        mSubmitDir    = mPOptions.getSubmitDirectory();
        mKickstartLabel = ( dag == null ) ? null :
                                            ( mPOptions.getBasenamePrefix() == null )? dag.getLabel():
                                                                                       mPOptions.getBasenamePrefix() ;
        mInvokeAlways = mProps.useInvokeInGridStart();
        mInvokeLength = mProps.getGridStartInvokeLength();
        
        mGenerateLOF  = mProps.generateLOFFiles();
        mConcDAG      = dag;
        mSiteStore    = bag.getHandleToSiteStore();
        mTCHandle     = bag.getHandleToTransformationCatalog();

        mDynamicDeployment =  mProps.transferWorkerPackage();
        
        //mWorkerNodeExecution = mProps.executeOnWorkerNode();
        mPegasusConfiguration = new PegasusConfiguration( bag.getLogger() );


        mEnablingPartOfAggregatedJob = false;
        mSetXBit = mProps.setXBitWithKickstart();
        
        mNoGridStartImpl = new NoGridStart();
        mNoGridStartImpl.initialize( bag, dag );
        mUseFullPathToGridStart = true;
        mDisableInvokeFunctionality = mProps.disableInvokeInGridStart();
        
        //PM-1060 we set stat based on whether a user 
        //has specified a value or not
        String value     = mProps.doStatWithKickstart();
        mRegisterOutputs =  mProps.createRegistrationJobs();
        mDisableKickstartStatCompletely = false;
        if( value == null ){
           //stat is disabled unless there is registration job
           mDoStat = false;
        }
        else{
            //user specified a stat value .
            mDoStat = Boolean.parse( value, false );
            mDisableKickstartStatCompletely = !mDoStat;
        }
        mLogger.log( "Kickstart Stating Disabled Completely - " + mDisableKickstartStatCompletely, LogManager.CONFIG_MESSAGE_LEVEL );
    }

     /**
     * Setter method to control whether a full path to Gridstart should be
     * returned while wrapping a job or not.
     *
     * @param fullPath    if set to true, indicates that full path would be used.
     */
    public void useFullPathToGridStarts( boolean fullPath ){
        this.mUseFullPathToGridStart = fullPath;
    }
    
    /**
     * Enables a constituentJob to run on the grid. This also determines how the
     * stdin,stderr and stdout of the constituentJob are to be propogated.
     * To grid enable a constituentJob, the constituentJob may need to be wrapped into another
     * constituentJob, that actually launches the constituentJob. It usually results in the constituentJob
     * description passed being modified modified.
     *
     * @param constituentJob  the <code>Job</code> object containing the constituentJob description
     *             of the constituentJob that has to be enabled on the grid.
     * @param isGlobusJob is <code>true</code>, if the constituentJob generated a
     *        line <code>universe = globus</code>, and thus runs remotely.
     *        Set to <code>false</code>, if the constituentJob runs on the submit
     *        host in any way.
     *
     * @return boolean true if enabling was successful,else false.
     */
    public boolean enable( AggregatedJob job,boolean isGlobusJob){
        //PM-817 when the recursion first starts parameter first is true
        return this.enable(job, isGlobusJob, true);
    }

    /**
     * Enables a constituentJob to run on the grid. This also determines how the
     * stdin,stderr and stdout of the constituentJob are to be propogated.
     * To grid enable a constituentJob, the constituentJob may need to be wrapped into another
     * constituentJob, that actually launches the constituentJob. It usually results in the constituentJob
     * description passed being modified modified.
     *
     * @param job
     * @param isGlobusJob is <code>true</code>, if the constituentJob generated a
     *        line <code>universe = globus</code>, and thus runs remotely.
     *        Set to <code>false</code>, if the constituentJob runs on the submit
     *        host in any way.
     * @param first
     *
     * @return boolean true if enabling was successful,else false.
     */
    public boolean enable( AggregatedJob job, boolean isGlobusJob, boolean first){
         //boolean first = true;
        
        //get hold of the JobAggregator determined for this clustered job
        //during clustering
        JobAggregator aggregator = job.getJobAggregator();
        if( aggregator == null ){
            throw new RuntimeException( "Clustered job not associated with a job aggregator " + job.getID() );
        }
        
        boolean partOfClusteredJob = true;
        
        //we want to evaluate the exectionSiteDirectory only once
        //for the clustered job
       for (Iterator it = aggregator.topologicalOrderingRequired() ?
                            job.topologicalSortIterator()://PM-817 we care about order, else -H option maynot be omitted always for first job
                            job.nodeIterator();
               it.hasNext(); ) {
            //PM-817Job constituentJob = (Job)it.next();
            GraphNode node = ( GraphNode )it.next();
            Job constituentJob = (Job) node.getContent();
            if( constituentJob instanceof AggregatedJob ){
                //PM-817 we need to make sure that the constituten
                //clustered job also gets enabled correctly
                AggregatedJob constituentClusteredJob = (AggregatedJob)constituentJob;
                
                if( !aggregator.getClass().equals( constituentClusteredJob.getJobAggregator().getClass() ) ){
                    //sanity check first to ensure the aggreagtors are not mixed
                    StringBuilder error = new StringBuilder();
                    error.append( "Recursive Clustering does not support different job aggregators. Job Aggregator for clustered job " ).
                          append( job.getID() ).append( " ").append( aggregator.getClass() ).
                          append( " does not match with constitutent job " ).append( constituentJob.getID() ).
                          append( " " ).append( constituentClusteredJob.getJobAggregator().getClass() );
                    throw new RuntimeException( error.toString() );
                }
                this.enable( constituentClusteredJob, isGlobusJob, first );
            }

            //earlier was set in SeqExec JobAggregator in the enable function
            constituentJob.vdsNS.construct( Pegasus.GRIDSTART_KEY,
                                            this.getVDSKeyValue() );

            if (first) {
                first = false;
            } else {
                //we need to pass -H to kickstart
                //to suppress the header creation
                // PM-823 Add -H to pegasus.gridstart.arguments if it is already set
                Pegasus jobconf = constituentJob.vdsNS;
                if (jobconf.containsKey(Pegasus.GRIDSTART_ARGUMENTS_KEY)) {
                    String args = jobconf.getStringValue(Pegasus.GRIDSTART_ARGUMENTS_KEY);
                    jobconf.construct(Pegasus.GRIDSTART_ARGUMENTS_KEY, args + " -H");
                } else {
                    jobconf.construct(Pegasus.GRIDSTART_ARGUMENTS_KEY, "-H");
                }
            }
            
            //no worker node case
            //always pass isGlobus true as always
            //interested only in executable strargs
            //due to the fact that seqexec does not allow for setting environment
            //per constitutent constituentJob, we cannot set the postscript removal option
            this.enable( constituentJob, isGlobusJob, mDoStat, false, partOfClusteredJob  );
                
            //PM-1021 add any of the lof files (and any other) that maybe transferred via condor io
            job.condorVariables.addIPFileForTransfer( constituentJob.condorVariables.getIPFilesForTransfer() );
            
            //System.out.println( constituentJob.condorVariables );
        }

        //all the constitutent jobs are enabled.
        //get the job aggregator to render the job 
        //to it's executable form
        aggregator.makeAbstractAggregatedJobConcrete( job  );

        
        //the aggregated job itself needs to be enabled via NoGridStart
        mNoGridStartImpl.enable( (Job)job, isGlobusJob);
        
        return true;
    }

    /**
     * Enables a constituentJob to run on the grid by launching it through kickstart.
     * Does the stdio, and stderr handling of the constituentJob to be run on the grid.
     * It modifies the constituentJob description, and also constructs all the valid
     * option to be passed to kickstart for launching the executable.
     *
     * @param constituentJob  the <code>Job</code> object containing the constituentJob description
     *             of the constituentJob that has to be enabled on the grid.
     * @param isGlobusJob is <code>true</code>, if the constituentJob generated a
     *        line <code>universe = globus</code>, and thus runs remotely.
     *        Set to <code>false</code>, if the constituentJob runs on the submit
     *        host in any way.
     *
     * @return boolean true if enabling was successful,else false in case when
     *         the path to kickstart could not be determined on the site where
     *         the constituentJob is scheduled.
     */
    public boolean enable( Job job, boolean isGlobusJob ){
        return this.enable( job, isGlobusJob, mDoStat , true, false );
    }


    /**
     * Enables a constituentJob to run on the grid by launching it through kickstart.
     * Does the stdio, and stderr handling of the constituentJob to be run on the grid.
     * It modifies the constituentJob description, and also constructs all the valid
     * option to be passed to kickstart for launching the executable.
     *
     * @param constituentJob  the <code>Job</code> object containing the constituentJob description
     *             of the constituentJob that has to be enabled on the grid.
     * @param isGlobusJob is <code>true</code>, if the constituentJob generated a
     *        line <code>universe = globus</code>, and thus runs remotely.
     *        Set to <code>false</code>, if the constituentJob runs on the submit
     *        host in any way.
     * @param stat  boolean indicating whether to generate the lof files
     *                     for kickstart stat option or not.
     * @param addPostScript boolean indicating whether to add a postscript or not.
     * @param partOfClusteredJob boolean indicating whether the job being enabled
     *                           is part of a clustered job or not.
     *
     * @return boolean true if enabling was successful,else false in case when
     *         the path to kickstart could not be determined on the site where
     *         the constituentJob is scheduled.
     */
    protected boolean enable( Job job, boolean isGlobusJob, boolean stat, boolean addPostScript , boolean partOfClusteredJob) {

        //take care of relative submit exectionSiteDirectory if specified.
        String submitDir = mSubmitDir + mSeparator;
//        String submitDir = getSubmitDirectory( mSubmitDir , constituentJob) + mSeparator;

        //To get the gridstart/kickstart path on the remote
        //pool, querying with entry for vanilla universe.
        //In the new format the gridstart is associated with the
        //pool not pool, condor universe
        SiteCatalogEntry site = mSiteStore.lookup( job.getSiteHandle() );
        
        //the executable path and arguments are put
        //in the Condor namespace and not printed to the
        //file so that they can be overriden if desired
        //later through profiles and key transfer_executable
        String gridStartPath = handleTransferOfExecutable( job, getKickstartPath( site ) );
        
        //sanity check
        if (gridStartPath == null){
            return false;
        }
        StringBuffer gridStartArgs = new StringBuffer();

        // the executable is gridstart, the application becomes its argument
        gridStartArgs.append(' ');
        gridStartArgs.append("-n ");
        gridStartArgs.append(job.getCompleteTCName());
        gridStartArgs.append(' ');

        //for derivation we now pass the logical id in the DAX
        //for the job JIRA PM-329
        gridStartArgs.append("-N ").append( job.getDAXID() ).append( " " );

        // handle stdin
        if (job.stdIn.length() > 0) {

            //for using the transfer script and other vds executables the
            //input file is transferred from the submit host by Condor to
            //stdin. We fool the kickstart to pick up the input file from
            //standard stdin by giving the input file name as -
            if (job.logicalName.equals(
                edu.isi.pegasus.planner.transfer.implementation.Transfer.TRANSFORMATION_NAME)
                || job.logicalName.equals(edu.isi.pegasus.planner.refiner.cleanup.Cleanup.TRANSFORMATION_NAME )
                || job.logicalName.equals( edu.isi.pegasus.planner.refiner.createdir.DefaultImplementation.TRANSFORMATION_NAME )
                || job.logicalName.equals(edu.isi.pegasus.planner.cluster.aggregator.SeqExec.
                                         COLLAPSE_LOGICAL_NAME)
                || job.logicalName.equals(edu.isi.pegasus.planner.cluster.aggregator.MPIExec.
                                         COLLAPSE_LOGICAL_NAME)
                                         ) {


                //condor needs to pick up the constituentJob stdin and
                //transfer it to the remote end
                construct( job, "input" , job.getFileFullPath( submitDir, ".in") );
                gridStartArgs.append("-i ").append("-").append(' ');

            } else {
                //kickstart provides the app's *tracked* stdin
                gridStartArgs.append("-i ").append(job.stdIn).append(' ');
            }
        }

        // handle stdout
        if (job.stdOut.length() > 0) {
            // gridstart saves the app's *tracked* stdout
            gridStartArgs.append("-o ").append(job.stdOut).append(' ');
        }

        // the Condor output variable and kickstart -o option
        // must not point to the same file for any local constituentJob.
        if (job.stdOut.equals(job.jobName + ".out") && !isGlobusJob) {
            mLogger.log("Detected WAW conflict for stdout",LogManager.WARNING_MESSAGE_LEVEL);
        }
        // the output of gridstart is propagated back to the submit host
        construct(job,"output", job.getFileFullPath( submitDir, ".out") );


        if (isGlobusJob) {
            construct(job,"transfer_output","true");
        }

        // handle stderr
        if (job.stdErr.length() > 0) {
            // gridstart saves the app's *tracked* stderr
            gridStartArgs.append("-e ").append(job.stdErr).append(' ');
        }

        // the Condor error variable and kickstart -e option
        // must not point to the same file for any local constituentJob.
        if (job.stdErr.equals(job.jobName + ".err") && !isGlobusJob) {
            mLogger.log("Detected WAW conflict for stderr",LogManager.WARNING_MESSAGE_LEVEL);
        }
        // the error from gridstart is propagated back to the submit host
        construct(job,"error",job.getFileFullPath( submitDir, ".err"));
        if (isGlobusJob) {
            construct(job,"transfer_error","true");
        }

        //we need to pass the resource handle
        //to kickstart as argument
        gridStartArgs.append("-R ").append(job.executionPool).append(' ');


        //Added for JIRA PM-543
        String directory = this.getDirectory( job );
        boolean setScratchEnvVariable = false;
        
        //handle the -W option that asks kickstart to create and change
        //exectionSiteDirectory before launching an executable.
        if(job.vdsNS.getBooleanValue(Pegasus.CREATE_AND_CHANGE_DIR_KEY ) ){
	    //pass the exectionSiteDirectory as an argument to kickstart
            gridStartArgs.append(" -W ").append(directory).append(' ');
            setScratchEnvVariable = true;
        }
        else  if(job.vdsNS.getBooleanValue(Pegasus.CHANGE_DIR_KEY)  ){
            //handle the -w option that asks kickstart to change
            //exectionSiteDirectory before launching an executable.
            gridStartArgs.append(" -w ").append( directory ).append(' ');
            setScratchEnvVariable = true;
        }
        else{
            //set the directory key with the job
            //for kickstart -w and -W it is not set
            if( requiresToSetDirectory( job ) ){
                job.setDirectory( directory );
            }
        }

        //PM-961 set the Pegasus scratch dir only for -w and -W cases
        //for rest we associate them in the styles
        if( setScratchEnvVariable ){
            job.envVariables.construct( ENV.PEGASUS_SCRATCH_DIR_KEY, directory );
        }

        if(   job.vdsNS.getBooleanValue(Pegasus.TRANSFER_PROXY_KEY) ){
            job.setDirectory( null );
        }

        //check if the constituentJob type indicates staging of executable
        //The -X functionality is handled by the setup jobs that
        //are added as childern to the stage in jobs, unless they are 
        //disabled and users set a property to set the xbit
        //Karan November 22, 2005
        if( mSetXBit &&
                job.userExecutablesStagedForJob()  ){
            //add the -X flag to denote turning on
            gridStartArgs.append( " -X " );
       }

        
        String statArgs = generateStatArgumentOptions( job, stat, mRegisterOutputs, addPostScript );
        if( !statArgs.isEmpty() ){
            gridStartArgs.append( statArgs );
        }
        else if( mGenerateLOF ){
            //dostat is false. so no generation of stat option
            //but generate lof files nevertheless


            //inefficient check here again. just a prototype
            //we need to generate -S option only for non transfer jobs
            //generate the list of filenames file for the input and output files.
            if (! (job instanceof TransferJob)) {
                 generateListofFilenamesFile( job.getInputFiles(),
                                              job, ".in.lof");
            }

            //for cleanup jobs no generation of stats for output files
            if (job.getJobType() != Job.CLEANUP_JOB) {
                generateListofFilenamesFile(job.getOutputFiles(),
                                            job, ".out.lof");

            }
        }///end of mGenerateLOF

        //append any arguments that need to be passed
        //kickstart directly, set elsewhere
        if(job.vdsNS.containsKey(Pegasus.GRIDSTART_ARGUMENTS_KEY)){
            gridStartArgs.append(job.vdsNS.get(Pegasus.GRIDSTART_ARGUMENTS_KEY))
                         .append(' ');
        }

        if(mProps.generateKickstartExtraOptions() && mConcDAG != null){
            gridStartArgs.append("-L ").append( mKickstartLabel ).append(" ");
            gridStartArgs.append("-T ").append(mConcDAG.getMTime()).append(" ");
        }

        gridStartArgs.append( getKickstartTimeoutOptions( job ) );
        
        /*
        mLogger.log( "User executables staged for job " + job.getID() + " " + job.userExecutablesStagedForJob() ,
                     LogManager.DEBUG_MESSAGE_LEVEL );
        */

        //figure out job executable
        String jobExecutable = ( !this.mUseFullPathToGridStart && job.userExecutablesStagedForJob() )?
                                //the basename of the executable used for pegasus lite
                                //and staging of executables
                                "." + File.separator  + job.getStagedExecutableBaseName( ):
                                //use whatever is set in the executable field
                                job.executable;


        long argumentLength = gridStartArgs.length() +
                              jobExecutable.length() +
                              1 +
                              job.strargs.length();

        //invoke is disabled if part of clustered job or because of a global disable
        //JIRA PM-526
        boolean disableInvoke = mDisableInvokeFunctionality ||
                                partOfClusteredJob ||
                                job.getJobType() != Job.COMPUTE_JOB; //PM-851
                                
        if( !disableInvoke && (mInvokeAlways || argumentLength > mInvokeLength) ){
            if(!useInvoke(job, jobExecutable, gridStartArgs)){
                mLogger.log("Unable to use invoke for job ",
                            LogManager.ERROR_MESSAGE_LEVEL);
                return false;
            }
        }
        else{
            
             gridStartArgs.append( jobExecutable );

             gridStartArgs.append(' ').append(job.strargs);
        }






        //the executable path and arguments are put
        //in the Condor namespace and not printed to the
        //file so that they can be overriden if desired
        //later through profiles and key transfer_executable

        // the arguments are no longer set as condor profiles
        // they are now set to the corresponding profiles in
        // the Condor Code Generator only.
/*
        construct(job, "executable", gridStartPath );
        construct(job, "arguments", gridStartArgs.toString());
*/
        job.setArguments( gridStartArgs.toString() );
        job.setRemoteExecutable( gridStartPath );
        
        //all finished successfully
        return true;
    }


    /**
     * It changes the paths to the executable depending on whether we want to
     * transfer the executable or not.
     *
     * If the transfer_executable is set to true, then the executable needs to be
     * shipped from the submit host meaning the local pool. This function changes
     * the path of the executable to the one on the local pool, so that it can
     *  be shipped.
     *
     * If the worker package is being deployed dynamically, then the path is set
     * to the exectionSiteDirectory where the worker package is deployed.
     *
     * Else, we pick up the path from the site catalog that is passed as input
     *
     * @param constituentJob   the <code>Job</code> containing the constituentJob description.
     * @param path  the path to kickstart on the remote compute site, as determined
     *              from the site catalog.
     *
     * @return the path that needs to be set as the executable
     */
    protected String handleTransferOfExecutable( Job job, String path ) {
        Condor cvar = job.condorVariables;
        
        

        if ( cvar.getBooleanValue("transfer_executable")) {
            SiteCatalogEntry site = mSiteStore.lookup( "local" );
            TransformationCatalogEntry entry = this.getTransformationCatalogEntry( site.getSiteHandle() );

            
            String gridStartPath = ( entry == null )?
                             //rely on the path determined from sc
                             getKickstartPath( site ):
                             //the tc entry has highest priority
                             entry.getPhysicalTransformation();
            
            if (gridStartPath == null) {
                mLogger.log(
                    "Gridstart needs to be shipped from the submit host to pool" +
                    job.getSiteHandle() + ".\nNo entry for it in pool local",
                    LogManager.ERROR_MESSAGE_LEVEL);
                throw new RuntimeException(
                    "GridStart needs to be shipped from submit host to site " +
                    job.getSiteHandle() + " for job " + job.getName());

            }

            return gridStartPath;
        }
        else if( mDynamicDeployment &&
                 job.runInWorkDirectory()  && ! mPegasusConfiguration.jobSetupForWorkerNodeExecution(job ) ){

            //worker package deployment for sharedfs
            //pick up the path from the transformation catalog of
            //dynamic deployment
            //in case of pegasus lite mode, we dont look up here.
            TransformationCatalogEntry entry = this.getTransformationCatalogEntry( job.getSiteHandle() );
            if( entry == null ){
                //NOW THROWN AN EXCEPTION

                //should throw a TC specific exception
                StringBuffer error = new StringBuffer();
                error.append("Could not find entry in tc for lfn ").
                    append( COMPLETE_TRANSFORMATION_NAME ).
                    append(" at site ").append( job.getSiteHandle() );

                if ( job.getSiteHandle().equalsIgnoreCase( "local" ) ){
                    //for local site in case of worker package staging also
                    //we can pick up the path on submit host, if not staged
                    //PM-497
                    SiteCatalogEntry site = mSiteStore.lookup( "local" );
                    String p = this.getKickstartPath( site );

                    if( p != null ){
                        return p;
                    }
                }

                mLogger.log( error.toString(), LogManager.ERROR_MESSAGE_LEVEL);
                throw new RuntimeException( error.toString() );
            }
            return entry.getPhysicalTransformation();
        }
        else{
            //the vanilla case where kickstart is pre installed.
            TransformationCatalogEntry entry = this.getTransformationCatalogEntry( job.getSiteHandle() );
            
            String ksProfilePath = (String)job.vdsNS.get( Pegasus.GRIDSTART_PATH_KEY );
            String ksPath = ( entry == null )?
                             //rely on the path determined from profiles 
                             ksProfilePath:
                             //the tc entry has highest priority
                             entry.getPhysicalTransformation();


            //we use full paths  for pegasus auxillary jobs
            //even when pegasus lite is used i.e mUseFullPathToGridStart is set to true
            boolean useFullPath = mUseFullPathToGridStart || job.getJobType() != Job.COMPUTE_JOB ;
            if( useFullPath ){
                ksPath =  ( ksPath == null )?
                          //rely on the path from the site catalog
                          path:
                          ksPath;
            }
            else{
                //pegasus lite case. we dont want to rely on site catalog
                //constructed path
                
                /* commented out for PM-1097
                ksPath = ( ksPath == null )?
                          this.EXECUTABLE_BASENAME ://use the basename
                          ksPath;
                */
                if ( ksPath == null ){
                          ksPath = this.EXECUTABLE_BASENAME ;//use the basename
                }
                else{
                    //PM-1097 check again to see if user had different gs profile set
                    if ( ksProfilePath != null ){
                        //we prefer the kickstart path as determined from profile
                        //only for PegasusLite case.
                        ksPath = ksProfilePath;
                    }
                }
            }
            
            //sanity check 
            if( ksPath == null ){
                StringBuilder error = new StringBuilder();
                error.append( "Unable to determine path to kickstart for site " ).append( job.getSiteHandle()).
                   append( " for job " ).append( job.getID() );
                if( path == null ){
                    //we know there was no path determined from site catalog.
                    error.append( " . " ).append( "Make sure PEGASUS_HOME is set as an env profile in the site catalog for site " ).
                          append( job.getSiteHandle() );
                }
                
                throw new RuntimeException( error.toString() );
            }
            
            return ksPath;
        }

    }

    /**
     * Returns the transformation catalog entry for kickstart on a site
     * 
     * @param site  the site on which the entry is required
     * 
     * @return the entry if found else null
     */
    public TransformationCatalogEntry getTransformationCatalogEntry( String site ){
        List entries = null;
        try {
            entries = mTCHandle.lookup( Kickstart.TRANSFORMATION_NAMESPACE,
                                        Kickstart.TRANSFORMATION_NAME,
                                        Kickstart.TRANSFORMATION_VERSION,
                                        site,
                                        TCType.INSTALLED );
        } catch (Exception e) {
            //non sensical catching
            mLogger.log("Unable to retrieve entries from TC " +
                    e.getMessage(), LogManager.DEBUG_MESSAGE_LEVEL);


        }
        
        return ( entries == null ) ?
                null  :
               (TransformationCatalogEntry) entries.get(0);
    }

    /**
     * Returns the default path to kickstart as constructed from the
     * environment variable associated with a site in the site catalog
     *
     * @param site   the SiteCatalogEntry object for the site.
     *
     * @return value if set else null
     */
    public String getKickstartPath( SiteCatalogEntry site ) {


        //try to construct the default path on basis of
        //PEGASUS_HOME environment variable.
        String home = site.getPegasusHome();
        if( home == null ){
            return null;
        }

        StringBuffer ks = new StringBuffer();
        ks.append( home ).append( File.separator ).
           append( "bin").append( File.separator ).
           append( Kickstart.EXECUTABLE_BASENAME );
        return ks.toString();
    }



    /**
     * Returns the exectionSiteDirectory in which the constituentJob executes on the worker node.
     *
     * 
     * @param constituentJob
     * 
     * @return  the full path to the exectionSiteDirectory where the constituentJob executes
     */
    public String getWorkerNodeDirectory( Job job ){
        //check for Pegasus Profile
        if( job.vdsNS.containsKey( Pegasus.WORKER_NODE_DIRECTORY_KEY ) ){
            return job.vdsNS.getStringValue( Pegasus.WORKER_NODE_DIRECTORY_KEY );
        }
        
        if( mSLS.doesCondorModifications() ){
            //indicates the worker node exectionSiteDirectory is the exectionSiteDirectory
            //in which condor launches the job
            // JIRA PM-380
            return ".";
        }
                

        StringBuffer workerNodeDir = new StringBuffer();
        String destDir = mSiteStore.getEnvironmentVariable( job.getSiteHandle() , "wntmp" );
        destDir = ( destDir == null ) ? "/tmp" : destDir;

        String relativeDir = mPOptions.getRelativeDirectory();
        
        workerNodeDir.append( destDir ).append( File.separator ).
                      append( relativeDir.replaceAll( "/" , "-" ) ).
                      //append( File.separator ).append( constituentJob.getCompleteTCName().replaceAll( ":[:]*", "-") );
                      append( "-" ).append( job.getID() );


        return workerNodeDir.toString();
    }

    /**
     * Indicates whether the enabling mechanism can set the X bit
     * on the executable on the remote grid site, in addition to launching
     * it on the remote grid site.
     *
     * @return true indicating Kickstart can set the X bit or not.
     */
    public  boolean canSetXBit(){
        return true;
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
        return Kickstart.CLASSNAME;
    }


    /**
     * Returns a short textual description in the form of the name of the class.
     *
     * @return  short textual description.
     */
    public String shortDescribe(){
        return Kickstart.SHORT_NAME;
    }

    /**
     * Returns the SHORT_NAME for the POSTScript implementation that is used
     * to be as default with this GridStart implementation.
     *
     * @return  the identifier for the PegasusExitCode POSTScript implementation.
     *
     *
     */
    public String defaultPOSTScript(){
        return PegasusExitCode.SHORT_NAME;
    }

    /**
     * Returns a boolean indicating whether we need to set the directory for
     * the job or not.
     *
     * @param job the job for which to set directory.
     *
     * @return
     */
    protected boolean requiresToSetDirectory( Job job ) {
        //the cleanup jobs should never have directory set as full path
        //is specified
        return ( job.getJobType() != Job.CLEANUP_JOB &&
                 job.getJobType() != Job.REPLICA_REG_JOB );
    }


    /**
     * Returns the directory in which the job should run.
     *
     * @param job   the job in which the directory has to run.
     *
     * @return
     */
    protected String getDirectory( Job job ){
        String execSiteWorkDir = mSiteStore.getInternalWorkDirectory(job);
        String workdir = (String) job.globusRSL.removeKey("directory"); // returns old value
        workdir = (workdir == null)?execSiteWorkDir:workdir;

        return workdir;
    }
    
    
    /**
     * Triggers the creation of the kickstart input file, that contains the
     * the remote executable and the arguments with which it has to be invoked.
     * The kickstart input file is created in the submit directory.
     *
     * @param constituentJob  the <code>Job</code> object containing the constituentJob description.
     * @param executable      the path to the executable used.
     * @param args            the arguments buffer for gridstart invocation so far.
     *
     * @return boolean indicating whether kickstart input file was generated or not.
     *                 false in case of any error.
     */
    private boolean useInvoke(Job job, String executable, StringBuffer args){
        boolean result = true;

        String inputBaseName = job.jobName + "." + Kickstart.KICKSTART_INPUT_SUFFIX;

        //writing the stdin file
        File argFile = new File(mSubmitDir, inputBaseName);
        try {
            FileWriter input;
            input = new FileWriter( argFile );
            //the first thing that goes in is the executable name
            input.write( executable );
            input.write("\n");
            //write out all the arguments
            //one on each line
            StringTokenizer st = new StringTokenizer(job.strargs);
            while(st.hasMoreTokens()){
                input.write(st.nextToken());
                input.write("\n");
            }
            //close the stream
            input.close();
        } catch (Exception e) {
            mLogger.log("Unable to write the kickstart input file for job " +
                        job.getCompleteTCName() + " " + e.getMessage(),
                        LogManager.ERROR_MESSAGE_LEVEL);
            return false;
        }

        //check if a directory is associated with the job
        String directory = job.getDirectory();
        if( directory != null ){
            //for PM-526
            //we want to trigger the -w option if a directory is associated with
            //the jobs
            args.append( " -w " ).append( directory );
            job.setDirectory( null );

        }


        job.condorVariables.addIPFileForTransfer( argFile.getAbsolutePath() );

        //add the -I argument to kickstart
        args.append(" -I ").append(inputBaseName).append(" ");
        return result;
    }

    /**
     * Constructs a kickstart setup constituentJob
     *
     * @param constituentJob           the constituentJob to be run.
     * @param workerNodeTmp the worker node tmp to run the constituentJob in.
     *
     * @return String
     */
    protected String constructSetupJob( Job job, String workerNodeTmp ){
       StringBuffer setup = new StringBuffer();

       setup.append( "/bin/mkdir -p " ).append( workerNodeTmp );


       return setup.toString();
    }

    /**
     * Constructs a kickstart setup constituentJob
     *
     * @param constituentJob           the constituentJob to be run.
     * @param workerNodeTmp the worker node tmp to run the constituentJob in.
     *
     * @return String
     */
    protected String constructCleanupJob( Job job, String workerNodeTmp ){
       StringBuffer setup = new StringBuffer();

       setup.append( "/bin/rm -rf " ).append( workerNodeTmp );


       return setup.toString();
    }



    /**
     * Constructs the prejob  that fetches sls file, and then invokes transfer
     * again.
     *
     * @param constituentJob   the constituentJob for which the prejob is being created
     * @param headNodeURLPrefix String
     * @param headNodeDirectory String
     * @param workerNodeDirectory String
     * @param slsFile String
     *
     * @return String containing the prescript invocation
     */
    protected String constructPREJob( Job job,
                                      String headNodeURLPrefix,
                                      String headNodeDirectory,
                                      String workerNodeDirectory,
                                      String slsFile ){



        File headNodeSLS = new File( headNodeDirectory, slsFile );
        return mSLS.invocationString( job, headNodeSLS );

        //first we need to get the sls file to worker node
        /*
        preJob.append( "/bin/echo -e \" " ).
               append( headNodeURLPrefix ).append( File.separator ).
               append( headNodeDirectory ).append( File.separator ).
               append( slsFile ).append( " \\n " ).
               append( "file://" ).append( workerNodeDirectory ).append( File.separator ).
               append( slsFile ).append( "\"" ).
               append( " | " ).append( transfer ).append( " base mnt " );

        preJob.append( " && " );

        //now we need to get transfer to execute this sls file
        preJob.append( transfer ).append( " base mnt < " ).append( slsFile );
        */

    }

    /**
     * Returns the stat argument options to be appended for kickstart invocation
     * 
     * @param job
     * @param stat
     * @return 
     */
    protected String generateStatArgumentOptions(Job job, boolean stat, boolean registerOutputs, boolean addPostScript ) {
        
        //sanity check
        if ( !( stat || registerOutputs ) || this.mDisableKickstartStatCompletely){
            return "";
        }
        
        StringBuffer args = new StringBuffer();
        //PM-992 we stat outputs either if stat property is set
        //or registration is enabled. inputs are only stated if
        //stat property is turned on.
        if ( stat || registerOutputs ){
            //add the stat options to kickstart only for certain jobs for time being
             //and if the input variable is true
            if (job.getJobType() == Job.COMPUTE_JOB ||
//                job.getJobType() == Job.STAGED_COMPUTE_JOB ||
                job.getJobType() == Job.CLEANUP_JOB ||
                job.getJobType() == Job.STAGE_IN_JOB ||
                job.getJobType() == Job.INTER_POOL_JOB) {

                String lof;
                List files = new ArrayList(2);

                //inefficient check here again. just a prototype
                //we need to generate -S option only for non transfer jobs
                //generate the list of filenames file for the input and output files.
                if (! (job instanceof TransferJob) && stat ) {
                    lof = generateListofFilenamesFile(job.getInputFiles(),
                                                      job,
                                                      ".in.lof");
                    if (lof != null) {
                        File file = new File(lof);
                        job.condorVariables.addIPFileForTransfer(lof);
                        //arguments just need basename . no path component
                        args.append(" -S @").append(file.getName()).
                            append(" ");
                        files.add(file.getName());
                    }
                }

                if( stat ) {
                    //for cleanup jobs no generation of stats for output files
                    if (job.getJobType() != Job.CLEANUP_JOB) {
                        lof = generateListofFilenamesFile( job.getOutputFiles(),
                                                           job,
                                                           ".out.lof");
                        if (lof != null) {
                            File file = new File(lof);
                            job.condorVariables.addIPFileForTransfer(lof);
                            //arguments just need basename . no path component
                            args.append(" -s @").append(file.getName()).append(" ");
                            files.add(file.getName());
                        }
                    }
                }
                else if( registerOutputs ){
                    //PM-992 we generate lfn=pfn -s options on command line
                    //for files that need to be registered
                    if( job.getJobType() == Job.COMPUTE_JOB ){
                        for( PegasusFile file : job.getOutputFiles() ){
                            if( file.getRegisterFlag() ){
                                args.append( " -s " ).append( file.getLFN() ).
                                     append( "=" ).append( file.getLFN() );
                            }
                        }
                    }
                }
                //add kickstart postscript that removes these files
                if( addPostScript ) {
                    addCleanupPostScript(job, files); 
                }
            }
        }
        args.append( " " );
        return args.toString();
    }
    
    /**
     * Writes out the list of filenames file for the job.
     *
     * @param files  the list of <code>PegasusFile</code> objects contains the files
     *               whose stat information is required.
     * @param job     the job
     * @param suffix  the suffix to be applied to files
     *
     * @return the full path to lof file created, else null if no file is written out.
     */
     protected String generateListofFilenamesFile( Set files, Job job, String suffix ){
         //sanity check
         if ( files == null || files.isEmpty() ){
             return null;
         }

         String result = null;
         //writing the stdin file
        try {
            File f = new File( job.getFileFullPath(mSubmitDir, suffix) );
            FileWriter input;
            input = new FileWriter( f );
            PegasusFile pf;
            for( Iterator it = files.iterator(); it.hasNext(); ){
                pf = ( PegasusFile ) it.next();
                String lfn= pf.getLFN();
                StringBuilder sb = new StringBuilder();
                //to make sure that kickstart generates lfn attribute in statcall
                //element
                sb.append( lfn ).append( "=" ).
                   append( lfn ).append(  "\n" );
                input.write( sb.toString() );
            }
            //close the stream
            input.close();
            result = f.getAbsolutePath();

        } catch ( IOException e) {
            mLogger.log("Unable to write the lof file for job " + job.getID() + " with suffix " + suffix , e ,
                        LogManager.ERROR_MESSAGE_LEVEL);
        }

        return result;
     }

    /**
     * Constructs a condor variable in the condor profile namespace
     * associated with the constituentJob. Overrides any preexisting key values.
     *
     * @param constituentJob   contains the constituentJob description.
     * @param key   the key of the profile.
     * @param value the associated value.
     */
    private void construct(Job job, String key, String value){
        job.condorVariables.construct(key,value);
    }


    /**
     * Condor Quotes a string
     *
     * @param string   the string to be quoted.
     *
     * @return quoted string.
     */
    private String quote( String string ){
        String result;
        try{
            mLogger.log("Unquoted Prejob is  " + string, LogManager.DEBUG_MESSAGE_LEVEL);
            result = CondorQuoteParser.quote( string, false );
            mLogger.log("Quoted Prejob is  " + result, LogManager.DEBUG_MESSAGE_LEVEL );
        }
        catch (CondorQuoteParserException e) {
            throw new RuntimeException("CondorQuoting Problem " +
                                       e.getMessage());
        }
        return result;

    }



    /**
     * Adds a /bin/rm post constituentJob to kickstart that removes the files passed.
     * The post jobs is added as an environment variable.
     *
     * @param constituentJob   the constituentJob in which the post constituentJob needs to be added.
     * @param files the files to be deleted.
     */
    private void addCleanupPostScript( Job job, List files ){
        //sanity check
        if ( files == null || !mDoStat || files.isEmpty() ) { return; }

        //do not add if constituentJob already has a postscript specified
        if( job.envVariables.containsKey( this.KICKSTART_CLEANUP ) ){
            mLogger.log( "Not adding lof cleanup as another kickstart cleanup already exists",
                         LogManager.DEBUG_MESSAGE_LEVEL );
            return;
        }

        StringBuffer ps = new StringBuffer();
        //maybe later we might want to pick it up from the TC
        ps.append( "/bin/rm -rf").append( " " );
        for( Iterator it = files.iterator(); it.hasNext(); ){
            ps.append( it.next() ).append( " " );
        }

        job.envVariables.construct( this.KICKSTART_CLEANUP, ps.toString() );

        return;
    }

    /**
     * Generates the argument fragment related to kickstart -k and -K options
     * 
     * @param job   the job.
     * 
     * @return 
     */
    private String getKickstartTimeoutOptions(Job job) {
        StringBuilder sb = new StringBuilder();
        
        //get the checkout time in seconds
        long checkpointTime = this.getJobCheckpointTimeInSeconds(job);

        if( checkpointTime == Long.MAX_VALUE ){
            //no value specified
            return sb.toString();
        }

        //expected time is the time after which kickstart sends
        //the TERM signal to job 
        sb.append( "-k " ).append( checkpointTime ).append( " " );

        long max = Long.MAX_VALUE;
        long multiplier = 60;
        if( job.vdsNS.containsKey( Pegasus.MAX_WALLTIME) ){
            max = job.vdsNS.getIntValue( Pegasus.MAX_WALLTIME, Integer.MAX_VALUE  );
        }
        else if ( job.globusRSL.containsKey(Globus.MAX_WALLTIME_KEY) ){
            max = job.globusRSL.getIntValue(Globus.MAX_WALLTIME_KEY, Integer.MAX_VALUE  );
        }
        else if ( job.vdsNS.containsKey( Pegasus.RUNTIME_KEY) ){
            //PM-962 last fallback to pegasus profile runtime which is in seconds
            max = job.vdsNS.getIntValue( Pegasus.RUNTIME_KEY, Integer.MAX_VALUE  );
            multiplier = 1;
        }
        

        if( max == Long.MAX_VALUE ){
            //means user never specified a maxwalltime
            //or a malformed value
            //we don't determnine the -K parameter
            return sb.toString();
        }

        //maxwalltime is specified in minutes, while pegasus runtime
        //is in seconds. convert to seconds for kickstart
        max = max * multiplier;

        //we set the -K parameter to half the difference between
        //maxwalltime - checkpointTime
        long diff = max - checkpointTime;
        long minDiff = 10;
        if( diff < minDiff ){
            //throw error
            throw new RuntimeException( "Insufficient difference between maxwalltime " + 
                                        max + " and checkpoint time " + checkpointTime +
                                        " Should be at least " + minDiff + " seconds ");
        }

        //we divide the difference equaully.
        //give equal time to generate the checkpoint file and 
        //the time to transfer the file
        //kill time is the time after which kickstart sends
        //the KILL signal to job 
        sb.append( "-K " ).append( diff/2 ).append( " " );

        return sb.toString();

    }


    /**
     * Returns the job's checkpoint time in seconds. 
     * 
     * @param j
     * 
     * @return job checkpointime in seconds, else Long.MAX_VALUE if not 
     *         specified 
     * @throws RuntimeException for malformed values
     */
    private long getJobCheckpointTimeInSeconds( Job job ){
       long time = Long.MAX_VALUE;
       
       //check for checkpoint.time that is specified in minutes.
       String key = Pegasus.CHECKPOINT_TIME_KEY;
       if( job.vdsNS.containsKey( key ) ){
            //means there is expectation of timeout functionality
            time = job.vdsNS.getLongValue( key, Long.MAX_VALUE );
            if( time == Long.MAX_VALUE ){
                //malformed value
                throw new RuntimeException( "Malformed Pegasus Profile " + key + " value " + 
                                            job.vdsNS.getStringValue( key ) + " for job " + job.getID());
            }
            //pegasus checkpoint.time key is in minutes. convert to seconds.
            time = time * 60;
            return time;
       }
       
       //check for deprecated value
       key = Pegasus.DEPRECATED_CHECKPOINT_TIME_KEY;
       if( job.vdsNS.containsKey( key ) ){
            //means there is expectation of timeout functionality
            time = job.vdsNS.getLongValue( key, Long.MAX_VALUE );
            if( time == Long.MAX_VALUE ){
                //malformed value
                throw new RuntimeException( "Malformed Pegasus Profile " + key + " value " + 
                                            job.vdsNS.getStringValue( key ) + " for job " + job.getID());
            }
            //log deprecated value
            mLogger.log( "Deprecated Pegasus profile key " + key + " found for job " + job.getID(),
                         LogManager.DEBUG_MESSAGE_LEVEL );
       }
       return time;
    }



}
